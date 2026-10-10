import {
  createContext,
  MouseEvent as ReactMouseEvent,
  ReactNode,
  RefObject,
  useCallback,
  useContext,
  useMemo,
  useState,
} from 'react';

import { formatCount, formatNumber, formatPercent } from './logic';
import { SummaryState } from './data';
import { AtlasColumn, ColumnSummary, HistogramBin } from './types';

function binLabel(bin: HistogramBin): string {
  const start = bin.start_label ?? formatNumber(bin.start);
  const end = bin.end_label ?? formatNumber(bin.end);
  return start === end ? start : `${start} to ${end}`;
}

type Tip = { text: string; x: number; y: number };
type TipApi = { hide: () => void; show: (text: string, event: ReactMouseEvent) => void };

const TipContext = createContext<TipApi>({ hide: () => undefined, show: () => undefined });

const TIP_WIDTH = 260;

// The tooltip of the mini charts, drawn at the explorer's root: inside the grid it would be
// clipped by the scrolled panes. It shows at once, unlike a native title.
export function TipLayer({ children, rootRef }: { children: ReactNode; rootRef: RefObject<HTMLElement> }) {
  const [tip, setTip] = useState<Tip | null>(null);
  const api = useMemo<TipApi>(() => ({
    hide: () => setTip(null),
    show: (text, event) => {
      const rect = rootRef.current?.getBoundingClientRect();
      if (!rect) return;
      const x = event.clientX - rect.left;
      setTip({
        text,
        x: x + 12 + TIP_WIDTH > rect.width ? Math.max(4, x - TIP_WIDTH - 8) : x + 12,
        y: event.clientY - rect.top + 16,
      });
    },
  }), [rootRef]);
  return (
    <TipContext.Provider value={api}>
      {children}
      {tip && (
        <div className="ca-tip" role="tooltip" style={{ left: tip.x, maxWidth: TIP_WIDTH, top: tip.y }}>
          {tip.text}
        </div>
      )}
    </TipContext.Provider>
  );
}

// Props that show text in the tooltip while the pointer is over an element. Nested
// elements with their own text win over their container.
function useTip() {
  const { hide, show } = useContext(TipContext);
  return useCallback((text: string, onEnter?: () => void, onLeave?: () => void) => ({
    onMouseEnter: (event: ReactMouseEvent) => {
      onEnter?.();
      show(text, event);
    },
    onMouseLeave: () => {
      onLeave?.();
      hide();
    },
    onMouseMove: (event: ReactMouseEvent) => {
      event.stopPropagation();
      show(text, event);
    },
  }), [hide, show]);
}

function binTip(bin: HistogramBin, present: number): string {
  return `${binLabel(bin)}\n${formatCount(bin.count)} rows (${formatPercent(bin.count, present)})`;
}

function valueTip(value: string | null, count: number, present: number): string {
  const name = value === '' ? '(empty)' : value ?? '(missing)';
  return `${name}\n${formatCount(count)} rows (${formatPercent(count, present)})`;
}

export function Sparkline({
  height = 22,
  summary,
  width = 120,
}: {
  height?: number;
  summary: ColumnSummary;
  width?: number;
}) {
  const tip = useTip();
  const [hovered, setHovered] = useState<number | null>(null);
  const present = Math.max(1, summary.count - summary.missing);
  if (summary.histogram.length) {
    const bins = summary.histogram;
    const max = Math.max(1, ...bins.map(bin => bin.count));
    const unit = width / bins.length;
    return (
      <svg aria-hidden className="ca-spark" height={height} viewBox={`0 0 ${width} ${height}`} width={width}>
        {bins.map((bin, index) => {
          const barHeight = bin.count ? Math.max(1.5, (bin.count / max) * (height - 2)) : 0;
          return (
            <g key={index}>
              <rect
                className={hovered === index ? 'ca-spark-hover' : undefined}
                height={barHeight}
                rx={1}
                width={Math.max(1, unit - 1)}
                x={index * unit}
                y={height - barHeight}
              />
              {/* The whole slot answers to the pointer, so short bars are easy to hover. */}
              <rect
                className="ca-hit"
                height={height}
                width={unit}
                x={index * unit}
                y={0}
                {...tip(binTip(bin, present), () => setHovered(index), () => setHovered(null))}
              />
            </g>
          );
        })}
      </svg>
    );
  }
  if (summary.top_values.length) {
    let left = 0;
    const shown = summary.top_values.slice(0, 4);
    const rest = present - shown.reduce((total, entry) => total + entry.count, 0);
    return (
      <svg
        aria-hidden
        className="ca-spark ca-spark-shares"
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        width={width}
      >
        <rect className="ca-spark-track" height={8} rx={2} width={width} x={0} y={height - 8} />
        {shown.map((entry, index) => {
          const share = (entry.count / present) * width;
          const element = (
            <rect
              className={`ca-share-${index}${hovered === index ? ' ca-spark-hover' : ''}`}
              height={8}
              key={index}
              width={Math.max(0, share - 1)}
              x={left}
              y={height - 8}
              {...tip(
                valueTip(entry.value, entry.count, present),
                () => setHovered(index),
                () => setHovered(null),
              )}
            />
          );
          left += share;
          return element;
        })}
        {rest > 0 && (
          <rect
            className="ca-hit"
            height={8}
            width={Math.max(0, width - left)}
            x={left}
            y={height - 8}
            {...tip(`Other values\n${formatCount(rest)} rows (${formatPercent(rest, present)})`)}
          />
        )}
      </svg>
    );
  }
  return null;
}

// Header summary: missing share and distribution; nothing while loading or on error.
export function HeaderProfile({ state }: { state?: SummaryState }) {
  if (!state || state === 'loading') {
    return <div className="ca-profile ca-profile-wait" />;
  }
  if ('error' in state) {
    return <div className="ca-profile" title={state.error} />;
  }
  return <HeaderProfileSummary summary={state} />;
}

function HeaderProfileSummary({ summary }: { summary: ColumnSummary }) {
  const tip = useTip();
  const missing = summary.missing;
  return (
    <div className="ca-profile" {...tip(profileTitle(summary))}>
      {spreadThin(summary) ? (
        <span className="ca-distinct-mark">
          {summary.distinct_exact ? '' : '≈'}{formatCount(summary.distinct)} distinct
        </span>
      ) : (
        <Sparkline height={18} summary={summary} width={100} />
      )}
      {missing > 0 && (
        <span
          className="ca-missing-mark"
          {...tip(`${formatCount(missing)} of ${formatCount(summary.count)} rows missing`)}
        >
          {formatPercent(missing, summary.count)} missing
        </span>
      )}
    </div>
  );
}

// No histogram and the most frequent values are a sliver of the column: a share bar would
// look empty, so the header shows the distinct count.
function spreadThin(summary: ColumnSummary): boolean {
  if (summary.histogram.length || summary.distinct === null) return false;
  const present = summary.count - summary.missing;
  if (present <= 0) return false;
  const shown = summary.top_values.slice(0, 4).reduce((total, entry) => total + entry.count, 0);
  return shown / present < 0.2;
}

function profileTitle(summary: ColumnSummary): string {
  const parts = [`${formatCount(summary.count)} rows`];
  if (summary.missing) parts.push(`${formatCount(summary.missing)} missing`);
  if (summary.distinct !== null) {
    parts.push(`${summary.distinct_exact ? '' : '≈'}${formatCount(summary.distinct)} distinct`);
  }
  return parts.join(' · ');
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="ca-metric">
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function Histogram({ summary }: { summary: ColumnSummary }) {
  const tip = useTip();
  const [hovered, setHovered] = useState<number | null>(null);
  const width = 280;
  const height = 96;
  const bins = summary.histogram;
  const max = Math.max(1, ...bins.map(bin => bin.count));
  const unit = width / bins.length;
  const first = bins[0];
  const last = bins[bins.length - 1];
  const present = Math.max(1, summary.count - summary.missing);
  return (
    <figure className="ca-histogram">
      <svg aria-label="Distribution of the values" role="img" viewBox={`0 0 ${width} ${height}`}>
        {bins.map((bin, index) => {
          const barHeight = bin.count ? Math.max(1.5, (bin.count / max) * (height - 4)) : 0;
          return (
            <g key={index}>
              <rect
                className={hovered === index ? 'ca-spark-hover' : undefined}
                height={barHeight}
                rx={1.5}
                width={Math.max(1, unit - 1.5)}
                x={index * unit}
                y={height - barHeight}
              />
              <rect
                aria-label={binTip(bin, present).replace('\n', ': ')}
                className="ca-hit"
                height={height}
                width={unit}
                x={index * unit}
                y={0}
                {...tip(binTip(bin, present), () => setHovered(index), () => setHovered(null))}
              />
            </g>
          );
        })}
      </svg>
      <figcaption>
        <span>{first ? first.start_label ?? formatNumber(first.start) : ''}</span>
        <span>{last ? last.end_label ?? formatNumber(last.end) : ''}</span>
      </figcaption>
    </figure>
  );
}

function TopValues({ summary }: { summary: ColumnSummary }) {
  const tip = useTip();
  const present = Math.max(1, summary.count - summary.missing);
  return (
    <ol className="ca-top-values">
      {summary.top_values.map((entry, index) => (
        <li key={`${entry.value}-${index}`} {...tip(valueTip(entry.value, entry.count, present))}>
          <div className="ca-top-value-head">
            <span className={entry.value === '' ? 'ca-muted-text' : ''} title={entry.value ?? ''}>
              {entry.value === '' ? '(empty)' : entry.value}
            </span>
            <span className="ca-numeric-text">
              {formatCount(entry.count)}
              <small>{formatPercent(entry.count, present)}</small>
            </span>
          </div>
          <div className="ca-bar">
            <span className={`ca-share-${Math.min(index, 3)}`} style={{ width: `${(100 * entry.count) / present}%` }} />
          </div>
        </li>
      ))}
      {(summary.other_count ?? 0) > 0 && (
        <li
          className="ca-top-other"
          {...tip(`Other values\n${formatCount(summary.other_count)} rows (${formatPercent(
            summary.other_count ?? 0, present,
          )})`)}
        >
          <span>Other values</span>
          <span className="ca-numeric-text">
            {formatCount(summary.other_count)}
            <small>{formatPercent(summary.other_count ?? 0, present)}</small>
          </span>
        </li>
      )}
    </ol>
  );
}

function Completeness({ present, summary }: { present: number; summary: ColumnSummary }) {
  const tip = useTip();
  return (
    <div
      className="ca-completeness"
      {...tip(
        `${formatCount(present)} present (${formatPercent(present, summary.count)})\n`
        + `${formatCount(summary.missing)} missing (${formatPercent(summary.missing, summary.count)})`,
      )}
    >
      <span style={{ width: `${summary.count ? (100 * present) / summary.count : 0}%` }} />
    </div>
  );
}

export function SummaryDetail({
  column,
  state,
}: {
  column: AtlasColumn;
  state?: SummaryState;
}) {
  if (!state || state === 'loading') {
    return <div className="ca-detail-wait">Computing the summary of {column.name}…</div>;
  }
  if ('error' in state) {
    return <div className="ca-detail-wait ca-error-text">{state.error}</div>;
  }
  const summary = state;
  const m = summary.metrics;
  const present = summary.count - summary.missing;
  return (
    <div className="ca-detail">
      <div className="ca-detail-head">
        <strong title={column.name}>{column.name}</strong>
        <code>{column.dtype}</code>
      </div>
      <Completeness present={present} summary={summary} />
      <dl className="ca-metrics">
        <Metric label="Rows" value={formatCount(summary.count)} />
        <Metric
          label="Missing"
          value={`${formatCount(summary.missing)} (${formatPercent(summary.missing, summary.count)})`}
        />
        {summary.distinct !== null && (
          <Metric
            label="Distinct"
            value={`${summary.distinct_exact ? '' : '≈ '}${formatCount(summary.distinct)}`}
          />
        )}
      </dl>
      {summary.histogram.length > 0 && <Histogram summary={summary} />}
      {summary.kind === 'numeric' && (
        <dl className="ca-metrics">
          <Metric label="Minimum" value={formatNumber(m.min)} />
          <Metric label="25%" value={formatNumber(m.q25)} />
          <Metric label="Median" value={formatNumber(m.median)} />
          <Metric label="75%" value={formatNumber(m.q75)} />
          <Metric label="Maximum" value={formatNumber(m.max)} />
          <Metric label="Mean" value={formatNumber(m.mean)} />
          <Metric label="Std. deviation" value={formatNumber(m.std)} />
          <Metric label="Zeros" value={formatCount(m.zeros as number)} />
          <Metric label="Negative" value={formatCount(m.negatives as number)} />
          {Number(m.nan) > 0 && <Metric label="NaN" value={formatCount(m.nan as number)} />}
          {Number(m.infinite) > 0 && <Metric label="Infinite" value={formatCount(m.infinite as number)} />}
        </dl>
      )}
      {summary.kind === 'temporal' && (
        <dl className="ca-metrics">
          <Metric label="Earliest" value={formatNumber(m.min)} />
          <Metric label="Latest" value={formatNumber(m.max)} />
        </dl>
      )}
      {summary.kind === 'string' && (
        <dl className="ca-metrics">
          <Metric label="Empty" value={formatCount(m.empty as number)} />
          <Metric label="Mean length" value={formatNumber(m.mean_length)} />
          <Metric label="Shortest" value={formatCount(m.min_length as number)} />
          <Metric label="Longest" value={formatCount(m.max_length as number)} />
        </dl>
      )}
      {summary.top_values.length > 0 && (
        <>
          <h4>{summary.kind === 'boolean' ? 'Values' : 'Most frequent'}</h4>
          <TopValues summary={summary} />
        </>
      )}
      {summary.kind === 'nested' && (
        <p className="ca-muted-text">Lists and structs show as JSON in the grid; open a cell to read it.</p>
      )}
    </div>
  );
}
