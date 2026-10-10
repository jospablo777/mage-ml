// The ColumnAtlas protocol, as served by POST /api/column_atlas_queries.

export type ColumnKind = 'numeric' | 'string' | 'boolean' | 'temporal' | 'nested' | 'other';

export type FilterOperator =
  | 'is_null'
  | 'is_not_null'
  | 'eq'
  | 'ne'
  | 'lt'
  | 'lte'
  | 'gt'
  | 'gte'
  | 'between'
  | 'contains'
  | 'not_contains'
  | 'starts_with'
  | 'ends_with'
  | 'is_empty'
  | 'is_not_empty'
  | 'is_true'
  | 'is_false'
  | 'is_nan';

export interface AtlasSource {
  block_run_id?: number | null;
  block_uuid: string;
  pipeline_uuid: string;
  variable_uuid: string;
}

export interface AtlasColumn {
  dtype: string;
  exact_integer: boolean;
  id: number;
  kind: ColumnKind;
  name: string;
}

export interface AtlasMetadata {
  columns: AtlasColumn[];
  label: string;
  row_count: number;
  source_version: string;
  version: number;
}

export interface AtlasFilter {
  column_id: number;
  high?: string;
  op: FilterOperator;
  value?: string;
}

export interface AtlasSort {
  column_id: number;
  descending: boolean;
}

export interface AtlasView {
  filters: AtlasFilter[];
  sort: AtlasSort[];
}

export interface AtlasRows {
  column_ids: number[];
  offset: number;
  rows: (string | null)[][];
}

export interface HistogramBin {
  count: number;
  end: number;
  end_label?: string | null;
  start: number;
  start_label?: string | null;
}

export interface TopValue {
  count: number;
  value: string | null;
}

export interface ColumnSummary {
  column_id: number;
  count: number;
  distinct: number | null;
  distinct_exact: boolean;
  histogram: HistogramBin[];
  kind: ColumnKind;
  metrics: Record<string, number | string | null>;
  missing: number;
  other_count?: number;
  top_values: TopValue[];
}
