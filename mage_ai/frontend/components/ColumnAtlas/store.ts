import { useCallback, useSyncExternalStore } from 'react';

import { AtlasFilter, AtlasSort, AtlasSource } from './types';

// The view of one output, shared by its inline and expanded explorers, so expanding keeps
// the filters, sort and selected column.
export interface AtlasViewState {
  filters: AtlasFilter[];
  selectedColumn: number | null;
  sort: AtlasSort[];
}

const EMPTY: AtlasViewState = { filters: [], selectedColumn: null, sort: [] };
const MAX_SOURCES = 100;

const states = new Map<string, AtlasViewState>();
const listeners = new Map<string, Set<() => void>>();

export function sourceKey(source: AtlasSource): string {
  return [
    source.pipeline_uuid,
    source.block_uuid,
    source.variable_uuid,
    source.block_run_id ?? '',
  ].join('|');
}

export function getViewState(key: string): AtlasViewState {
  return states.get(key) ?? EMPTY;
}

export function setViewState(
  key: string,
  update: (state: AtlasViewState) => AtlasViewState,
): void {
  const next = update(getViewState(key));
  states.delete(key);
  states.set(key, next);
  // Oldest outputs first out, so the map does not grow with every block run.
  while (states.size > MAX_SOURCES) {
    const oldest = states.keys().next().value;
    if (oldest === undefined || listeners.get(oldest)?.size) break;
    states.delete(oldest);
  }
  listeners.get(key)?.forEach(listener => listener());
}

function subscribe(key: string, listener: () => void): () => void {
  if (!listeners.has(key)) listeners.set(key, new Set());
  listeners.get(key).add(listener);
  return () => {
    listeners.get(key)?.delete(listener);
  };
}

export function useAtlasViewState(
  key: string,
): [AtlasViewState, (update: (state: AtlasViewState) => AtlasViewState) => void] {
  const state = useSyncExternalStore(
    useCallback(listener => subscribe(key, listener), [key]),
    () => getViewState(key),
    () => EMPTY,
  );
  const update = useCallback(
    (change: (state: AtlasViewState) => AtlasViewState) => setViewState(key, change),
    [key],
  );
  return [state, update];
}

// The newest version of each output that a response reported. A block that runs again
// writes a new version; explorers that show an older one reload their metadata.
const versions = new Map<string, string>();
const versionListeners = new Map<string, Set<(version: string) => void>>();

export function reportVersion(key: string, version: string): void {
  if (!version || versions.get(key) === version) return;
  versions.set(key, version);
  while (versions.size > MAX_SOURCES) {
    versions.delete(versions.keys().next().value);
  }
  versionListeners.get(key)?.forEach(listener => listener(version));
}

export function subscribeVersion(key: string, listener: (version: string) => void): () => void {
  if (!versionListeners.has(key)) versionListeners.set(key, new Set());
  versionListeners.get(key).add(listener);
  return () => {
    versionListeners.get(key)?.delete(listener);
  };
}

export function resetViewState(key: string): void {
  setViewState(key, () => EMPTY);
}
