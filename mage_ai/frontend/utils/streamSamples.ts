import { useCallback, useEffect, useState } from 'react';

import { get, set } from '@storage/localStorage';

const EVENT = 'mage-stream-sample';

function key(pipelineUUID: string): string {
  return `stream_sample_${pipelineUUID}`;
}

// The sample that "Run on sample" uses for a pipeline: chosen in the Samples panel, kept per
// browser, and shared with every block of the pipeline as it changes.
export function useStreamSample(pipelineUUID: string): [string, (name: string) => void] {
  const [sample, setSampleState] = useState<string>(null);

  useEffect(() => {
    if (!pipelineUUID) {
      return;
    }
    setSampleState(get(key(pipelineUUID), null));
    const listener = (event: CustomEvent) => {
      if (event.detail?.pipelineUUID === pipelineUUID) {
        setSampleState(event.detail.name);
      }
    };
    window.addEventListener(EVENT, listener as EventListener);
    return () => window.removeEventListener(EVENT, listener as EventListener);
  }, [pipelineUUID]);

  const setSample = useCallback((name: string) => {
    set(key(pipelineUUID), name);
    setSampleState(name);
    window.dispatchEvent(new CustomEvent(EVENT, { detail: { name, pipelineUUID } }));
  }, [pipelineUUID]);

  return [sample, setSample];
}

// JSON lines or one JSON array, as people paste messages.
export function parseMessages(text: string): any[] {
  const trimmed = (text || '').trim();
  if (!trimmed) {
    return [];
  }
  if (trimmed.startsWith('[')) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      throw new Error('Paste a JSON array or one JSON message per line.');
    }
    return parsed;
  }
  return trimmed.split('\n').filter(line => line.trim()).map((line, index) => {
    try {
      return JSON.parse(line);
    } catch {
      throw new Error(`Line ${index + 1} is not JSON.`);
    }
  });
}
