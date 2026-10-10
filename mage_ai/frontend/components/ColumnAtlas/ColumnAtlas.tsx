import {
  KeyboardEvent,
  MouseEvent as ReactMouseEvent,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { ThemeContext } from 'styled-components';

import { AtlasClient, atlasClient } from './client';
import { useRowCount, useRows, useSummaries } from './data';
import { FilterEditor } from './FilterEditor';
import {
  HEADER_HEIGHT,
  ROW_HEIGHT,
  columnOffsets,
  columnWidth,
  filterLabel,
  formatCount,
  indexWidth,
  nextSort,
  rowGeometry,
  scrollTopFor,
  visibleColumns,
} from './logic';
import { HeaderProfile, SummaryDetail, TipLayer } from './Profile';
import { sourceKey, useAtlasViewState } from './store';
import { AtlasRoot, themeVariables } from './styles';
import { AtlasColumn, AtlasMetadata, AtlasSource, AtlasView, ColumnKind } from './types';

export type ColumnAtlasMode = 'inline' | 'expanded';

type ColumnAtlasProps = {
  client?: AtlasClient;
  height?: number;
  metadata: AtlasMetadata;
  mode: ColumnAtlasMode;
  onClose?: () => void;
  onExpand?: () => void;
  source: AtlasSource;
};

const KIND_MARKS: Record<ColumnKind, string> = {
  boolean: '◐',
  nested: '{}',
  numeric: '#',
  other: '?',
  string: 'Aa',
  temporal: '◷',
};
const HEADER_BINS = 16;
const DETAIL_BINS = 32;
const PANEL_LIST_LIMIT = 300;

interface Cell {
  column: number;
  row: number;
}

export function ColumnAtlas({
  client = atlasClient,
  height = 420,
  metadata,
  mode,
  onClose,
  onExpand,
  source,
}: ColumnAtlasProps) {
  const theme = useContext(ThemeContext);
  const key = sourceKey(source);
  const [state, setState] = useAtlasViewState(key);
  const view: AtlasView = useMemo(
    () => ({ filters: state.filters, sort: state.sort }),
    [state.filters, state.sort],
  );
  const columns = metadata.columns;
  const version = metadata.source_version;

  const scrollRef = useRef<HTMLDivElement>(null);
  const [viewport, setViewport] = useState({ height: 300, left: 0, top: 0, width: 800 });
  const [rootWidth, setRootWidth] = useState(1000);
  const rootRef = useRef<HTMLElement>(null);
  const [panelOpen, setPanelOpen] = useState(mode === 'expanded');
  const [editor, setEditor] = useState<{ column: number | null } | null>(null);
  const [selection, setSelection] = useState<Cell | null>(null);
  const [inspecting, setInspecting] = useState(false);
  const [widths, setWidths] = useState<Record<number, number>>({});
  const [columnSearch, setColumnSearch] = useState('');
  const [jump, setJump] = useState('');
  const [copied, setCopied] = useState(false);

  const { count, error: countError } = useRowCount(client, source, version, view, metadata.row_count);
  const total = count ?? 0;
  const indexColumnWidth = indexWidth(Math.max(total, metadata.row_count));
  const columnWidths = useMemo(
    () => columns.map(column => widths[column.id] ?? columnWidth(column)),
    [columns, widths],
  );
  const offsets = useMemo(
    () => columnOffsets(columnWidths, indexColumnWidth),
    [columnWidths, indexColumnWidth],
  );
  const totalWidth = offsets[offsets.length - 1];
  const geometry = rowGeometry(total, viewport.height, viewport.top);
  const shownColumns = useMemo(
    () => visibleColumns(offsets, viewport.left, viewport.width),
    [offsets, viewport.left, viewport.width],
  );
  const { cell, error: rowError, loading } = useRows(
    client, source, version, view, count, geometry.firstRow, geometry.visibleRows,
    shownColumns, columns.length,
  );
  const headerSummaries = useSummaries(client, source, version, view, shownColumns, HEADER_BINS);
  const selectedColumn = columns.find(column => column.id === state.selectedColumn) ?? columns[0];
  const detailSummaries = useSummaries(
    client, source, version, view, selectedColumn && panelOpen ? [selectedColumn.id] : [],
    DETAIL_BINS, panelOpen,
  );

  // Viewport and container sizes.
  useEffect(() => {
    const scroller = scrollRef.current;
    const root = rootRef.current;
    if (!scroller || !root) return undefined;
    const measure = () => {
      setViewport(previous => ({
        ...previous,
        height: scroller.clientHeight,
        width: scroller.clientWidth,
      }));
      setRootWidth(root.clientWidth);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(scroller);
    observer.observe(root);
    return () => observer.disconnect();
  }, []);

  // A new view starts at the top.
  const viewKeyText = JSON.stringify(view);
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
    setViewport(previous => ({ ...previous, top: 0 }));
    setSelection(null);
    setInspecting(false);
  }, [viewKeyText]);

  const onScroll = useCallback(() => {
    const scroller = scrollRef.current;
    if (!scroller) return;
    setViewport(previous =>
      previous.top === scroller.scrollTop && previous.left === scroller.scrollLeft
        ? previous
        : { ...previous, left: scroller.scrollLeft, top: scroller.scrollTop },
    );
  }, []);

  const setFilters = (filters: AtlasView['filters']) => setState(previous => ({ ...previous, filters }));
  const sortBy = (columnId: number, additive: boolean) =>
    setState(previous => ({ ...previous, sort: nextSort(previous.sort, columnId, additive) }));
  const selectColumn = (columnId: number) =>
    setState(previous => ({ ...previous, selectedColumn: columnId }));

  const scrollIntoView = useCallback(
    (target: Cell) => {
      const scroller = scrollRef.current;
      if (!scroller) return;
      if (!geometry.isScaled) {
        const top = target.row * ROW_HEIGHT;
        const bodyHeight = scroller.clientHeight - HEADER_HEIGHT;
        if (top < scroller.scrollTop) scroller.scrollTop = top;
        else if (top + ROW_HEIGHT > scroller.scrollTop + bodyHeight) {
          scroller.scrollTop = top + ROW_HEIGHT - bodyHeight;
        }
      } else if (target.row < geometry.firstRow || target.row >= geometry.firstRow + geometry.visibleRows - 4) {
        scroller.scrollTop = scrollTopFor(target.row, total, scroller.clientHeight);
      }
      const left = offsets[target.column] - indexColumnWidth;
      const right = offsets[target.column + 1];
      if (left < scroller.scrollLeft) scroller.scrollLeft = left;
      else if (right > scroller.scrollLeft + scroller.clientWidth) {
        scroller.scrollLeft = right - scroller.clientWidth;
      }
    },
    [geometry.firstRow, geometry.isScaled, geometry.visibleRows, indexColumnWidth, offsets, total],
  );

  const copy = useCallback(
    (target: Cell | null) => {
      if (!target) return;
      const value = cell(target.row, target.column);
      if (value === undefined || !navigator.clipboard) return;
      navigator.clipboard.writeText(value ?? '').then(
        () => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        },
        () => undefined,
      );
    },
    [cell],
  );

  const onGridKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!total || !columns.length) return;
    const current = selection ?? { column: shownColumns[0] ?? 0, row: geometry.firstRow };
    const page = Math.max(1, geometry.visibleRows - 5);
    let next: Cell | null = null;
    switch (event.key) {
      case 'ArrowDown':
        next = { ...current, row: Math.min(total - 1, current.row + 1) };
        break;
      case 'ArrowUp':
        next = { ...current, row: Math.max(0, current.row - 1) };
        break;
      case 'ArrowRight':
        next = { ...current, column: Math.min(columns.length - 1, current.column + 1) };
        break;
      case 'ArrowLeft':
        next = { ...current, column: Math.max(0, current.column - 1) };
        break;
      case 'PageDown':
        next = { ...current, row: Math.min(total - 1, current.row + page) };
        break;
      case 'PageUp':
        next = { ...current, row: Math.max(0, current.row - page) };
        break;
      case 'Home':
        next = event.metaKey || event.ctrlKey ? { column: 0, row: 0 } : { ...current, column: 0 };
        break;
      case 'End':
        next = event.metaKey || event.ctrlKey
          ? { column: columns.length - 1, row: total - 1 }
          : { ...current, column: columns.length - 1 };
        break;
      case 'Enter':
        if (selection) setInspecting(true);
        event.preventDefault();
        return;
      case 'Escape':
        // In the full window, Escape after the inspector closes the window itself.
        if (inspecting) setInspecting(false);
        else if (selection && mode === 'inline') setSelection(null);
        else return;
        event.preventDefault();
        event.stopPropagation();
        return;
      case 'c':
        if ((event.metaKey || event.ctrlKey) && selection) {
          copy(selection);
          event.preventDefault();
        }
        return;
      default:
        return;
    }
    event.preventDefault();
    setSelection(next);
    scrollIntoView(next);
  };

  const startResize = (event: ReactMouseEvent, columnId: number, index: number) => {
    event.preventDefault();
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = columnWidths[index];
    const move = (moveEvent: MouseEvent) => {
      const width = Math.max(64, Math.min(900, startWidth + moveEvent.clientX - startX));
      setWidths(previous => ({ ...previous, [columnId]: width }));
    };
    const up = () => {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', up);
    };
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', up);
  };

  const goToRow = () => {
    const row = Number(jump.replace(/,/g, ''));
    const scroller = scrollRef.current;
    if (!scroller || !Number.isInteger(row) || row < 1 || row > total) return;
    scroller.scrollTop = scrollTopFor(row - 1, total, scroller.clientHeight);
    setSelection({ column: selection?.column ?? shownColumns[0] ?? 0, row: row - 1 });
  };

  const lastShownRow = Math.min(total, geometry.firstRow + Math.max(1, geometry.visibleRows - 4));
  const narrow = rootWidth < 760;
  const searched = columnSearch
    ? columns.filter(column => column.name.toLowerCase().includes(columnSearch.toLowerCase()))
    : columns;

  const rowElements = [];
  if (count !== null) {
    for (let offset = 0; offset < geometry.visibleRows; offset += 1) {
      const row = geometry.firstRow + offset;
      if (row >= total) break;
      rowElements.push(
        <div
          className={`ca-row${row % 2 ? ' ca-row-odd' : ''}`}
          key={row}
          role="row"
          style={{ top: geometry.top + offset * ROW_HEIGHT, width: totalWidth }}
        >
          <div className="ca-index" role="rowheader" style={{ width: indexColumnWidth }}>
            {formatCount(row + 1)}
          </div>
          {shownColumns.map(index => {
            const column = columns[index];
            const value = cell(row, index);
            const selected = selection?.row === row && selection?.column === index;
            return (
              <div
                aria-selected={selected}
                className={[
                  'ca-cell',
                  column.kind === 'numeric' ? 'ca-cell-number' : '',
                  value === null ? 'ca-cell-missing' : '',
                  value === undefined ? 'ca-cell-loading' : '',
                  selected ? 'ca-cell-selected' : '',
                ].join(' ')}
                key={index}
                onClick={() => setSelection({ column: index, row })}
                onDoubleClick={() => {
                  setSelection({ column: index, row });
                  setInspecting(true);
                }}
                role="gridcell"
                style={{ left: offsets[index], width: columnWidths[index] }}
                title={value ?? undefined}
              >
                {value === null ? 'missing' : value}
              </div>
            );
          })}
        </div>,
      );
    }
  }

  const inspectedValue = selection && inspecting ? cell(selection.row, selection.column) : undefined;

  return (
    <AtlasRoot
      $mode={mode}
      aria-label={`Output ${metadata.label}`}
      className={narrow ? 'ca-narrow' : ''}
      ref={rootRef}
      style={{ ...themeVariables(theme), ...(mode === 'inline' ? { height } : {}) }}
    >
      <TipLayer rootRef={rootRef}>
        <header className="ca-toolbar">
          <div className="ca-title">
            <strong title={metadata.label}>{metadata.label}</strong>
            <span>
              {count === null && !countError ? 'Counting rows…' : `${formatCount(count)} rows`}
              {view.filters.length > 0 && count !== null && ` of ${formatCount(metadata.row_count)}`}
              {` × ${formatCount(columns.length)} columns`}
            </span>
          </div>
          <div className="ca-actions">
            <button
            className="ca-button"
            onClick={() => setEditor(editor ? null : { column: selection?.column ?? null })}
            type="button"
          >
              Filter
            </button>
            {(view.filters.length > 0 || view.sort.length > 0) && (
            <button
              className="ca-button"
              onClick={() => setState(previous => ({ ...previous, filters: [], sort: [] }))}
              type="button"
            >
              Reset
            </button>
          )}
            <button
            aria-pressed={panelOpen}
            className={`ca-button${panelOpen ? ' ca-button-active' : ''}`}
            onClick={() => setPanelOpen(open => !open)}
            type="button"
          >
              Columns
            </button>
            {mode === 'inline' && onExpand && (
            <button className="ca-button" onClick={onExpand} title="Open in a full window" type="button">
              Expand
            </button>
          )}
            {mode === 'expanded' && onClose && (
            <button className="ca-button" onClick={onClose} title="Close (Esc)" type="button">
              Close
            </button>
          )}
          </div>
        </header>

        {(view.filters.length > 0 || view.sort.length > 0 || editor) && (
        <div className="ca-filterbar">
          {view.filters.map((filter, index) => (
            <span className="ca-chip" key={`${index}-${filter.column_id}`}>
              {filterLabel(filter, columns[filter.column_id])}
              <button
                aria-label={`Remove filter ${filterLabel(filter, columns[filter.column_id])}`}
                onClick={() => setFilters(view.filters.filter((_, position) => position !== index))}
                type="button"
              >
                ×
              </button>
            </span>
          ))}
          {view.sort.length > 0 && (
            <span className="ca-sort-summary">
              Sorted by{' '}
              {view.sort
                .map(key => `${columns[key.column_id]?.name} ${key.descending ? '↓' : '↑'}`)
                .join(', ')}
            </span>
          )}
          {editor && (
            <FilterEditor
              columns={columns}
              initialColumn={editor.column}
              onAdd={filter => {
                setFilters([...view.filters, filter]);
                setEditor(null);
              }}
              onCancel={() => setEditor(null)}
            />
          )}
        </div>
      )}

        <div className={`ca-body${panelOpen ? ' ca-body-panel' : ''}`}>
          <div className="ca-grid-area">
            <div
            aria-colcount={columns.length + 1}
            aria-label={`${metadata.label} rows`}
            aria-rowcount={total + 1}
            className="ca-scroll"
            onKeyDown={onGridKeyDown}
            onScroll={onScroll}
            ref={scrollRef}
            role="grid"
            tabIndex={0}
          >
              <div className="ca-stage" style={{ height: geometry.scrollHeight, width: totalWidth }}>
                <div className="ca-header" role="row" style={{ height: HEADER_HEIGHT, width: totalWidth }}>
                  <div className="ca-corner" role="columnheader" style={{ width: indexColumnWidth }}>
                    #
                  </div>
                  {shownColumns.map(index => (
                    <HeaderCell
                    column={columns[index]}
                    descending={view.sort.find(sortKey => sortKey.column_id === index)?.descending}
                    key={index}
                    left={offsets[index]}
                    onFilter={() => setEditor({ column: index })}
                    onResizeStart={event => startResize(event, columns[index].id, index)}
                    onSelect={() => {
                      selectColumn(index);
                      setPanelOpen(true);
                    }}
                    onSort={additive => sortBy(index, additive)}
                    sortIndex={view.sort.findIndex(sortKey => sortKey.column_id === index)}
                    sortKeys={view.sort.length}
                    summary={headerSummaries.get(index)}
                    width={columnWidths[index]}
                  />
                ))}
                </div>
                {rowElements}
              </div>
              {count === 0 && (
              <div className="ca-empty">
                <p>No rows match the filters.</p>
                <button className="ca-button" onClick={() => setFilters([])} type="button">
                  Clear filters
                </button>
              </div>
            )}
            </div>
            {(rowError || countError) && (
            <div className="ca-banner" role="alert">
              {rowError || countError}
            </div>
          )}
            {inspecting && selection && (
            <div aria-label="Cell value" className="ca-inspector" role="dialog">
              <div className="ca-inspector-head">
                <span>
                  <strong>{columns[selection.column]?.name}</strong>, row {formatCount(selection.row + 1)}
                </span>
                <span>
                  <button className="ca-button" onClick={() => copy(selection)} type="button">
                    {copied ? 'Copied' : 'Copy'}
                  </button>
                  <button aria-label="Close" className="ca-button" onClick={() => setInspecting(false)} type="button">
                    ×
                  </button>
                </span>
              </div>
              <pre>{inspectedValue === null ? 'missing' : inspectedValue ?? 'Loading…'}</pre>
              {inspectedValue && inspectedValue.endsWith('…') && (
                <p className="ca-muted-text">Values longer than 256 characters are cut.</p>
              )}
            </div>
          )}
            <footer className="ca-status">
              <span>
                {total > 0
                ? `Rows ${formatCount(geometry.firstRow + 1)} to ${formatCount(lastShownRow)}`
                  + ` of ${formatCount(total)}`
                : ''}
              </span>
              {loading && <span aria-label="Loading" className="ca-spinner" />}
              {copied && <span>Copied</span>}
              {selection && !inspecting && (
              <span className="ca-hint">
                Enter to open the cell, {navigator?.platform?.includes('Mac') ? '⌘' : 'Ctrl'}+C to copy
              </span>
            )}
              <form
              className="ca-jump"
              onSubmit={event => {
                event.preventDefault();
                goToRow();
              }}
            >
                <label>
                  Row
                  <input
                  inputMode="numeric"
                  onChange={event => setJump(event.target.value)}
                  placeholder={formatCount(total || 1)}
                  value={jump}
                />
                </label>
              </form>
            </footer>
          </div>

          {panelOpen && (
          <aside aria-label="Columns" className="ca-panel">
            <div className="ca-panel-head">
              <input
                aria-label="Search columns"
                onChange={event => setColumnSearch(event.target.value)}
                placeholder={`Search ${formatCount(columns.length)} columns`}
                value={columnSearch}
              />
              {narrow && (
                <button
                  aria-label="Close columns"
                  className="ca-button"
                  onClick={() => setPanelOpen(false)}
                  type="button"
                >
                  ×
                </button>
              )}
            </div>
            <ul className="ca-column-list">
              {searched.slice(0, PANEL_LIST_LIMIT).map(column => (
                <li key={column.id}>
                  <button
                    className={column.id === selectedColumn?.id ? 'ca-column-active' : ''}
                    onClick={() => {
                      selectColumn(column.id);
                      const scroller = scrollRef.current;
                      if (scroller) {
                        scroller.scrollLeft = Math.max(0, offsets[column.id] - indexColumnWidth - 24);
                      }
                    }}
                    type="button"
                  >
                    <span className="ca-kind" title={column.kind}>{KIND_MARKS[column.kind]}</span>
                    <span className="ca-column-name">{column.name}</span>
                    <code>{column.dtype}</code>
                  </button>
                </li>
              ))}
              {searched.length > PANEL_LIST_LIMIT && (
                <li className="ca-muted-text ca-list-note">
                  {formatCount(searched.length - PANEL_LIST_LIMIT)} more; search to find them
                </li>
              )}
            </ul>
            {selectedColumn && (
              <SummaryDetail column={selectedColumn} state={detailSummaries.get(selectedColumn.id)} />
            )}
          </aside>
        )}
        </div>
      </TipLayer>
    </AtlasRoot>
  );
}

function HeaderCell({
  column,
  descending,
  left,
  onFilter,
  onResizeStart,
  onSelect,
  onSort,
  sortIndex,
  sortKeys,
  summary,
  width,
}: {
  column: AtlasColumn;
  descending?: boolean;
  left: number;
  onFilter: () => void;
  onResizeStart: (event: ReactMouseEvent) => void;
  onSelect: () => void;
  onSort: (additive: boolean) => void;
  sortIndex: number;
  sortKeys: number;
  summary?: Parameters<typeof HeaderProfile>[0]['state'];
  width: number;
}) {
  const sorted = sortIndex >= 0;
  return (
    <div
      aria-label={column.name}
      aria-sort={sorted ? (descending ? 'descending' : 'ascending') : 'none'}
      className={`ca-head${sorted ? ' ca-head-sorted' : ''}`}
      role="columnheader"
      style={{ left, width }}
    >
      <button
        className="ca-head-main"
        onClick={event => onSort(event.shiftKey)}
        title={`${column.name}: click to sort, shift-click to add a sort key`}
        type="button"
      >
        <span className="ca-head-name">
          <span className="ca-kind">{KIND_MARKS[column.kind]}</span>
          <span className="ca-head-text">{column.name}</span>
          {sorted && (
            <span className="ca-sort-mark">
              {descending ? '↓' : '↑'}
              {sortKeys > 1 ? sortIndex + 1 : ''}
            </span>
          )}
        </span>
        <code className="ca-head-type">{column.dtype}</code>
      </button>
      <div className="ca-head-profile" onClick={onSelect} role="presentation" title="Show the summary">
        <HeaderProfile state={summary} />
      </div>
      <button aria-label={`Filter ${column.name}`} className="ca-head-filter" onClick={onFilter} type="button">
        ⌕
      </button>
      <span aria-hidden className="ca-resize" onMouseDown={onResizeStart} />
    </div>
  );
}
