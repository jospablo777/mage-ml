// Pure functions of ColumnAtlas: layout, filters and formatting. No React, no requests, so
// `yarn test:unit` runs them with Node directly.
import type {
  AtlasColumn,
  AtlasFilter,
  AtlasSort,
  AtlasView,
  ColumnKind,
  FilterOperator,
} from './types';

export const ROW_HEIGHT = 30;
export const HEADER_HEIGHT = 58;
export const INDEX_MIN_WIDTH = 56;
// Browsers stop scrolling a single element past a few million pixels; taller tables map
// the scroll position onto the rows.
export const MAX_SCROLL_HEIGHT = 6_000_000;
export const PAGE_ROWS = 128;
export const COLUMN_BLOCK = 16;

export const OPERATOR_LABELS: Record<FilterOperator, string> = {
  between: 'between',
  contains: 'contains',
  ends_with: 'ends with',
  eq: '=',
  gt: '>',
  gte: '≥',
  is_empty: 'is empty',
  is_false: 'is false',
  is_nan: 'is NaN',
  is_not_empty: 'is not empty',
  is_not_null: 'is not missing',
  is_null: 'is missing',
  is_true: 'is true',
  lt: '<',
  lte: '≤',
  ne: '≠',
  not_contains: 'does not contain',
  starts_with: 'starts with',
};

const MISSING: FilterOperator[] = ['is_null', 'is_not_null'];

export function operatorsFor(column: Pick<AtlasColumn, 'kind' | 'dtype'>): FilterOperator[] {
  switch (column.kind) {
    case 'numeric': {
      const float = /^Float/.test(column.dtype);
      return [
        'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'between', ...MISSING,
        ...(float ? (['is_nan'] as FilterOperator[]) : []),
      ];
    }
    case 'temporal':
      return ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'between', ...MISSING];
    case 'string':
      return [
        'contains', 'not_contains', 'eq', 'ne', 'starts_with', 'ends_with',
        'is_empty', 'is_not_empty', ...MISSING,
      ];
    case 'boolean':
      return ['is_true', 'is_false', ...MISSING];
    default:
      return [...MISSING];
  }
}

export function needsValue(op: FilterOperator): boolean {
  return ![
    'is_null', 'is_not_null', 'is_empty', 'is_not_empty', 'is_true', 'is_false', 'is_nan',
  ].includes(op);
}

export function placeholderFor(column: Pick<AtlasColumn, 'kind' | 'dtype'>): string {
  if (column.kind === 'temporal') {
    return column.dtype === 'Date' ? '2024-01-31' : '2024-01-31 12:00:00';
  }
  if (column.kind === 'numeric') return 'Number';
  return 'Text';
}

// Integers stay text all the way to the engine, so values past 2^53 keep every digit.
export function filterError(filter: AtlasFilter, column: AtlasColumn): string | null {
  if (!operatorsFor(column).includes(filter.op)) return 'Choose a condition';
  if (!needsValue(filter.op)) return null;
  const values = filter.op === 'between' ? [filter.value, filter.high] : [filter.value];
  for (const value of values) {
    if (value === undefined || value.trim() === '') return 'Enter a value';
    if (column.kind === 'numeric') {
      if (column.exact_integer && !/^-?\d+$/.test(value.trim())) return 'Enter a whole number';
      if (!column.exact_integer && !Number.isFinite(Number(value))) return 'Enter a number';
    }
  }
  return null;
}

export function filterLabel(filter: AtlasFilter, column?: AtlasColumn): string {
  const name = column?.name ?? `column ${filter.column_id}`;
  const op = OPERATOR_LABELS[filter.op];
  if (!needsValue(filter.op)) return `${name} ${op}`;
  if (filter.op === 'between') return `${name} between ${filter.value} and ${filter.high}`;
  return `${name} ${op} ${filter.value}`;
}

export function viewKey(view: AtlasView): string {
  return JSON.stringify([view.filters, view.sort]);
}

// Click: sort by this column only, cycling ascending, descending, off. Shift-click: add or
// cycle this column as another key.
export function nextSort(sort: AtlasSort[], columnId: number, additive: boolean): AtlasSort[] {
  const current = sort.find(key => key.column_id === columnId);
  const others = additive ? sort.filter(key => key.column_id !== columnId) : [];
  if (!current) return [...others, { column_id: columnId, descending: false }];
  if (!current.descending) {
    return additive
      ? sort.map(key => (key.column_id === columnId ? { ...key, descending: true } : key))
      : [{ column_id: columnId, descending: true }];
  }
  return others;
}

const BASE_WIDTH: Record<ColumnKind, number> = {
  boolean: 96,
  nested: 220,
  numeric: 120,
  other: 160,
  string: 176,
  temporal: 196,
};

export function columnWidth(column: Pick<AtlasColumn, 'kind' | 'name' | 'dtype'>): number {
  const nameWidth = column.name.length * 7.4 + 44;
  const typeWidth = column.dtype.length * 6.2 + 30;
  return Math.round(Math.min(340, Math.max(BASE_WIDTH[column.kind], nameWidth, typeWidth)));
}

export function indexWidth(rowCount: number): number {
  return Math.max(INDEX_MIN_WIDTH, String(rowCount).length * 8 + 26);
}

// Left edges of the columns, after the row index; one more entry holds the total width.
export function columnOffsets(widths: number[], start: number): number[] {
  const offsets = [start];
  widths.forEach(width => offsets.push(offsets[offsets.length - 1] + width));
  return offsets;
}

// Indexes of the columns in view, with overscan on both sides.
export function visibleColumns(
  offsets: number[],
  scrollLeft: number,
  viewportWidth: number,
  overscan = 2,
): number[] {
  const count = offsets.length - 1;
  if (count <= 0) return [];
  let first = 0;
  while (first < count - 1 && offsets[first + 1] <= scrollLeft + offsets[0]) first += 1;
  let last = first;
  while (last < count - 1 && offsets[last + 1] < scrollLeft + viewportWidth) last += 1;
  const start = Math.max(0, first - overscan);
  const end = Math.min(count - 1, last + overscan);
  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
}

export interface RowGeometry {
  firstRow: number;
  isScaled: boolean;
  scrollHeight: number;
  // Distance from the top of the scroll area to the first rendered row.
  top: number;
  visibleRows: number;
}

export function rowGeometry(
  totalRows: number,
  viewportHeight: number,
  scrollTop: number,
  overscan = 4,
): RowGeometry {
  const bodyHeight = Math.max(ROW_HEIGHT, viewportHeight - HEADER_HEIGHT);
  const visibleRows = Math.ceil(bodyHeight / ROW_HEIGHT) + overscan;
  const fullHeight = totalRows * ROW_HEIGHT + HEADER_HEIGHT;
  if (fullHeight <= MAX_SCROLL_HEIGHT) {
    const firstRow = Math.max(0, Math.min(Math.floor(scrollTop / ROW_HEIGHT), totalRows - 1));
    return {
      firstRow,
      isScaled: false,
      scrollHeight: fullHeight,
      top: HEADER_HEIGHT + firstRow * ROW_HEIGHT,
      visibleRows,
    };
  }
  const range = MAX_SCROLL_HEIGHT - viewportHeight;
  const lastStart = Math.max(0, totalRows - visibleRows + overscan);
  const fraction = range > 0 ? Math.min(1, Math.max(0, scrollTop / range)) : 0;
  const firstRow = Math.round(fraction * lastStart);
  return {
    firstRow,
    isScaled: true,
    scrollHeight: MAX_SCROLL_HEIGHT,
    top: scrollTop + HEADER_HEIGHT,
    visibleRows,
  };
}

// The scroll position that shows a row at the top.
export function scrollTopFor(row: number, totalRows: number, viewportHeight: number): number {
  const fullHeight = totalRows * ROW_HEIGHT + HEADER_HEIGHT;
  if (fullHeight <= MAX_SCROLL_HEIGHT) return row * ROW_HEIGHT;
  const geometry = rowGeometry(totalRows, viewportHeight, 0);
  const lastStart = Math.max(1, totalRows - geometry.visibleRows + 4);
  return (Math.min(row, lastStart) / lastStart) * (MAX_SCROLL_HEIGHT - viewportHeight);
}

export function pagesFor(firstRow: number, rowCount: number, total: number): number[] {
  if (total <= 0) return [];
  const first = Math.floor(firstRow / PAGE_ROWS);
  const last = Math.floor(Math.min(total - 1, firstRow + rowCount - 1) / PAGE_ROWS);
  return Array.from({ length: last - first + 1 }, (_, index) => first + index);
}

export function blocksFor(columns: number[]): number[] {
  return Array.from(new Set(columns.map(column => Math.floor(column / COLUMN_BLOCK))));
}

const integerFormat = new Intl.NumberFormat('en-US');
const decimalFormat = new Intl.NumberFormat('en-US', { maximumSignificantDigits: 6 });

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '–';
  return integerFormat.format(value);
}

// Statistics arrive as text when a number would lose digits: integers past 2^53 stay as they
// are, decimals are rounded for display, and dates and times pass through.
export function formatNumber(value: number | string | null | undefined): string {
  if (value === null || value === undefined || value === '') return '–';
  if (typeof value === 'string') {
    const text = value.trim();
    if (!/^[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(text)) return value;
    const parsed = Number(text);
    if (/^[-+]?\d+$/.test(text) && !Number.isSafeInteger(parsed)) return text;
    return formatNumber(parsed);
  }
  if (!Number.isFinite(value)) return '–';
  if (Math.abs(value) >= 1e15 || (value !== 0 && Math.abs(value) < 1e-4)) {
    return value.toExponential(3);
  }
  return decimalFormat.format(value);
}

export function formatPercent(part: number, whole: number): string {
  if (!whole) return '0%';
  const share = (100 * part) / whole;
  if (share > 0 && share < 0.1) return '<0.1%';
  if (share < 100 && share > 99.9) return '>99.9%';
  return `${share.toFixed(share < 10 ? 1 : 0)}%`;
}
