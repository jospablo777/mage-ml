import { useCallback, useEffect, useRef } from 'react';

import AuthToken from '@api/utils/AuthToken';
import { API_KEY } from '@api/utils/fetcher';
import { buildUrl } from '@api/utils/url';

import { RustDiagnostic, toMarkers } from './rustDiagnostics';

const OWNER = 'rust';
const DELAY_MS = 700;

async function check(
  pipelineUUID: string,
  blockUUID: string,
  content: string,
  signal: AbortSignal,
): Promise<RustDiagnostic[]> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  const authorization = new AuthToken().authorizationString;
  if (authorization) {
    headers.Authorization = authorization;
  }
  const response = await fetch(`${buildUrl('rust_checks')}?api_key=${API_KEY}`, {
    body: JSON.stringify({
      api_key: API_KEY,
      rust_check: { block_uuid: blockUUID, content, pipeline_uuid: pipelineUUID },
    }),
    headers,
    method: 'POST',
    signal,
  });
  const body = await response.json();
  if (body?.error) {
    throw new Error(body.error.message || 'The check failed');
  }
  return body?.rust_check?.diagnostics || [];
}

// Compiler errors and warnings of a Rust block, drawn in its editor as the user types.
export default function useRustDiagnostics({
  blockUUID,
  enabled,
  pipelineUUID,
}: {
  blockUUID?: string;
  enabled: boolean;
  pipelineUUID?: string;
}) {
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<any>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const controller = useRef<AbortController | null>(null);
  const sequence = useRef(0);

  const run = useCallback(async () => {
    const editor = editorRef.current;
    const monaco = monacoRef.current;
    const model = editor?.getModel?.();
    if (!enabled || !model || !monaco || !pipelineUUID || !blockUUID) {
      return;
    }
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    const requested = ++sequence.current;
    const version = model.getVersionId();
    try {
      const diagnostics = await check(pipelineUUID, blockUUID, model.getValue(), current.signal);
      // An answer for older text or an older request would mark the wrong lines.
      if (
        requested !== sequence.current
        || model.isDisposed?.()
        || model.getVersionId() !== version
      ) {
        return;
      }
      monaco.editor.setModelMarkers(model, OWNER, toMarkers(diagnostics, monaco));
    } catch (error) {
      if ((error as { name?: string })?.name !== 'AbortError') {
        // Checking is a help while editing; running the block still reports errors.
        console.warn('Rust check failed', error);
      }
    }
  }, [blockUUID, enabled, pipelineUUID]);

  const schedule = useCallback(() => {
    if (!enabled) {
      return;
    }
    if (timer.current) {
      clearTimeout(timer.current);
    }
    timer.current = setTimeout(run, DELAY_MS);
  }, [enabled, run]);

  const onMount = useCallback((editor: any, monaco: any) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    if (enabled) {
      run();
    }
  }, [enabled, run]);

  useEffect(() => () => {
    if (timer.current) {
      clearTimeout(timer.current);
    }
    controller.current?.abort();
    const model = editorRef.current?.getModel?.();
    if (model && monacoRef.current && !model.isDisposed?.()) {
      monacoRef.current.editor.setModelMarkers(model, OWNER, []);
    }
  }, []);

  return { onChange: schedule, onMount };
}
