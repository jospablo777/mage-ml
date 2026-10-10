import { useEffect, useMemo, useRef, useState } from 'react';

import { AtlasClient, isAbort } from './client';
import { COLUMN_BLOCK, PAGE_ROWS, blocksFor, pagesFor, viewKey } from './logic';
import { AtlasRows, AtlasSource, AtlasView, ColumnSummary } from './types';

const MAX_PAGES = 240;
const MAX_SUMMARIES = 600;
const SUMMARY_BATCH = 16;

interface Page {
  index: Map<number, number>;
  rows: AtlasRows;
}

// A map that drops its oldest entries past a size.
class Lru<V> {
  private entries = new Map<string, V>();

  constructor(private readonly size: number) {}

  get(key: string): V | undefined {
    const value = this.entries.get(key);
    if (value !== undefined) {
      this.entries.delete(key);
      this.entries.set(key, value);
    }
    return value;
  }

  has(key: string): boolean {
    return this.entries.has(key);
  }

  set(key: string, value: V): void {
    this.entries.delete(key);
    this.entries.set(key, value);
    while (this.entries.size > this.size) {
      this.entries.delete(this.entries.keys().next().value);
    }
  }

  delete(key: string): void {
    this.entries.delete(key);
  }

  clear(): void {
    this.entries.clear();
  }
}

export function useRowCount(
  client: AtlasClient,
  source: AtlasSource,
  version: string | null,
  view: AtlasView,
  total: number | null,
): { count: number | null; error: string | null } {
  const [state, setState] = useState<{ count: number | null; error: string | null; key: string }>(
    { count: null, error: null, key: '' },
  );
  const key = `${version}|${viewKey(view)}`;
  const filtered = view.filters.length > 0;

  useEffect(() => {
    if (!version || !filtered) return undefined;
    const controller = new AbortController();
    client
      .count(source, view, controller.signal)
      .then(result => setState({ count: result.row_count, error: null, key }))
      .catch(error => {
        if (!isAbort(error)) setState({ count: null, error: String(error?.message || error), key });
      });
    return () => controller.abort();
    // The key covers the view and version.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, key, filtered]);

  if (!version) return { count: null, error: null };
  // Sorting keeps every row: the count is the output's.
  if (!filtered) return { count: total, error: null };
  return state.key === key ? { count: state.count, error: state.error } : { count: null, error: null };
}

export function useRows(
  client: AtlasClient,
  source: AtlasSource,
  version: string | null,
  view: AtlasView,
  total: number | null,
  firstRow: number,
  rowCount: number,
  columns: number[],
  columnCount: number,
): {
  cell: (row: number, column: number) => string | null | undefined;
  error: string | null;
  loading: boolean;
} {
  const cache = useRef(new Lru<Page>(MAX_PAGES));
  const inflight = useRef(new Map<string, AbortController>());
  const [, setTick] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const prefix = `${version}|${viewKey(view)}`;
  const prefixRef = useRef(prefix);

  if (prefixRef.current !== prefix) {
    prefixRef.current = prefix;
    cache.current.clear();
  }

  const pages = useMemo(() => pagesFor(firstRow, rowCount, total ?? 0), [firstRow, rowCount, total]);
  const blocks = useMemo(() => blocksFor(columns), [columns]);
  const wanted = useMemo(
    () => pages.flatMap(page => blocks.map(block => ({ block, key: `${prefix}|${page}|${block}`, page }))),
    [blocks, pages, prefix],
  );
  const wantedKey = wanted.map(({ key }) => key).join(',');

  useEffect(() => {
    if (!version || !total) return undefined;
    const wantedSet = new Set(wanted.map(({ key }) => key));
    // Windows that scrolled out of view are cancelled; the server stops reading them.
    inflight.current.forEach((controller, key) => {
      if (!wantedSet.has(key)) {
        controller.abort();
        inflight.current.delete(key);
      }
    });
    const timer = setTimeout(() => {
      wanted.forEach(({ block, key, page }) => {
        if (cache.current.has(key) || inflight.current.has(key)) return;
        const ids = Array.from(
          { length: Math.min(COLUMN_BLOCK, columnCount - block * COLUMN_BLOCK) },
          (_, index) => block * COLUMN_BLOCK + index,
        );
        if (!ids.length) return;
        const controller = new AbortController();
        inflight.current.set(key, controller);
        client
          .rows(source, view, page * PAGE_ROWS, PAGE_ROWS, ids, controller.signal)
          .then(rows => {
            inflight.current.delete(key);
            if (prefixRef.current !== prefix) return;
            const index = new Map<number, number>();
            rows.column_ids.forEach((id, position) => index.set(id, position));
            cache.current.set(key, { index, rows });
            setError(null);
            setTick(tick => tick + 1);
          })
          .catch(failure => {
            inflight.current.delete(key);
            if (!isAbort(failure)) setError(String(failure?.message || failure));
          });
      });
    }, 30);
    return () => clearTimeout(timer);
    // wantedKey stands for wanted; the view is part of the prefix.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, wantedKey, version, total, columnCount]);

  useEffect(
    () => () => {
      inflight.current.forEach(controller => controller.abort());
      inflight.current.clear();
    },
    [],
  );

  const cell = (row: number, column: number): string | null | undefined => {
    const page = Math.floor(row / PAGE_ROWS);
    const block = Math.floor(column / COLUMN_BLOCK);
    const entry = cache.current.get(`${prefix}|${page}|${block}`);
    if (!entry) return undefined;
    const position = entry.index.get(column);
    const values = entry.rows.rows[row - entry.rows.offset];
    if (position === undefined || !values) return undefined;
    return values[position];
  };

  return { cell, error, loading: inflight.current.size > 0 };
}

export type SummaryState = ColumnSummary | 'loading' | { error: string };

export function useSummaries(
  client: AtlasClient,
  source: AtlasSource,
  version: string | null,
  view: AtlasView,
  columns: number[],
  bins: number,
  enabled = true,
): Map<number, SummaryState> {
  const cache = useRef(new Lru<SummaryState>(MAX_SUMMARIES));
  const [, setTick] = useState(0);
  const prefix = `${version}|${viewKey(view)}|${bins}`;
  const columnsKey = columns.join(',');

  useEffect(() => {
    if (!version || !enabled || !columns.length) return undefined;
    const missing = columns.filter(column => !cache.current.has(`${prefix}|${column}`));
    if (!missing.length) return undefined;
    const controller = new AbortController();
    // A short pause, so scrolling through columns does not ask for each one on the way.
    const timer = setTimeout(() => {
      for (let start = 0; start < missing.length; start += SUMMARY_BATCH) {
        const batch = missing.slice(start, start + SUMMARY_BATCH);
        batch.forEach(column => cache.current.set(`${prefix}|${column}`, 'loading'));
        client
          .summaries(source, view, batch, bins, controller.signal)
          .then(summaries => {
            summaries.forEach(summary => cache.current.set(`${prefix}|${summary.column_id}`, summary));
            setTick(tick => tick + 1);
          })
          .catch(failure => {
            const message = String(failure?.message || failure);
            batch.forEach(column => {
              if (isAbort(failure)) {
                // Asked again when the column is in view again.
                cache.current.delete(`${prefix}|${column}`);
              } else {
                cache.current.set(`${prefix}|${column}`, { error: message });
              }
            });
            setTick(tick => tick + 1);
          });
      }
      setTick(tick => tick + 1);
    }, 120);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
    // columnsKey stands for columns; the view is part of the prefix.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, columnsKey, prefix, enabled]);

  const result = new Map<number, SummaryState>();
  columns.forEach(column => {
    const value = cache.current.get(`${prefix}|${column}`);
    if (value !== undefined) result.set(column, value);
  });
  return result;
}
