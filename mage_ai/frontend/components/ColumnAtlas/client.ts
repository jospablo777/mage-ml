import AuthToken from '@api/utils/AuthToken';
import { API_KEY } from '@api/utils/fetcher';
import { buildUrl } from '@api/utils/url';

import { reportVersion, sourceKey } from './store';
import {
  AtlasMetadata,
  AtlasRows,
  AtlasSource,
  AtlasView,
  ColumnSummary,
} from './types';

export const RESOURCE = 'column_atlas_queries';

export class AtlasRequestError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }

  // The output cannot be explored here; the plain table shows it.
  get unavailable(): boolean {
    return this.status === 404;
  }
}

async function query<T>(
  source: AtlasSource,
  action: string,
  params: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<T> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  const authorization = new AuthToken().authorizationString;
  if (authorization) {
    headers.Authorization = authorization;
  }
  const response = await fetch(`${buildUrl(RESOURCE)}?api_key=${API_KEY}`, {
    body: JSON.stringify({
      api_key: API_KEY,
      column_atlas_query: { action, source, ...params },
    }),
    headers,
    method: 'POST',
    signal,
  });
  let body: any = null;
  try {
    body = await response.json();
  } catch {
    throw new AtlasRequestError(`The server answered ${response.status}`, response.status || 500);
  }
  // Mage reports API errors in the body, with status 200.
  if (body?.error) {
    throw new AtlasRequestError(body.error.message || 'The query failed', body.error.code || 500);
  }
  if (!response.ok) {
    throw new AtlasRequestError(`The server answered ${response.status}`, response.status);
  }
  const result = body?.column_atlas_query?.result;
  if (result?.source_version) {
    reportVersion(sourceKey(source), result.source_version);
  }
  return result as T;
}

export const atlasClient = {
  count(source: AtlasSource, view: AtlasView, signal?: AbortSignal) {
    return query<{ row_count: number }>(source, 'count', { view }, signal);
  },
  metadata(source: AtlasSource, signal?: AbortSignal) {
    return query<AtlasMetadata>(source, 'metadata', {}, signal);
  },
  rows(
    source: AtlasSource,
    view: AtlasView,
    offset: number,
    limit: number,
    columns: number[],
    signal?: AbortSignal,
  ) {
    return query<AtlasRows>(source, 'rows', { columns, limit, offset, view }, signal);
  },
  summaries(
    source: AtlasSource,
    view: AtlasView,
    columns: number[],
    bins: number,
    signal?: AbortSignal,
  ) {
    return query<{ summaries: ColumnSummary[] }>(
      source,
      'summaries',
      { bins, columns, view },
      signal,
    ).then(result => result.summaries);
  },
};

export type AtlasClient = typeof atlasClient;

export function isAbort(error: unknown): boolean {
  return (error as { name?: string })?.name === 'AbortError';
}
