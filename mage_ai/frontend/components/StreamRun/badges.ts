import { useEffect, useState } from 'react';

import api from '@api';

// A running stream's per-block numbers on the dependency graph's nodes: the graph reads the
// local run's status (every 2 seconds while it runs, every 10 otherwise), and the Run tab
// shares each status it reads.

const EVENT = 'mage-stream-run-blocks';

export type StreamBlockNumbers = {
  block: string;
  errors: number;
  messages_in: number;
  messages_out: number;
  p95_ms: number;
};

export function badgeText(block: StreamBlockNumbers): string {
  const errors = block.errors ? ` · ${block.errors.toLocaleString()} errors` : '';
  return `${block.messages_in.toLocaleString()} in · ${block.messages_out.toLocaleString()} out`
    + ` · p95 ${block.p95_ms.toFixed(1)} ms${errors}`;
}

export function publishStreamBlocks(pipelineUUID: string, blocks: StreamBlockNumbers[]) {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(EVENT, { detail: { blocks, pipelineUUID } }));
  }
}

function toBadges(blocks: StreamBlockNumbers[]): { [block: string]: string } {
  const next = {};
  (blocks || []).forEach((block) => {
    next[block.block] = badgeText(block);
  });
  return next;
}

export function useStreamBadges(
  pipelineUUID: string,
  streaming: boolean,
): { [block: string]: string } {
  const [badges, setBadges] = useState<{ [block: string]: string }>({});

  useEffect(() => {
    if (!pipelineUUID || !streaming || typeof window === 'undefined') {
      return;
    }
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      let running = false;
      try {
        const response = await api.stream_runs.pipelines.detailAsync(pipelineUUID, 'current');
        const run = (response?.data || response)?.stream_run;
        running = !!run?.running;
        if (!cancelled) {
          setBadges(running ? toBadges(run.blocks) : {});
        }
      } catch {
        // The badges are a convenience: a failed read shows none.
      }
      if (!cancelled) {
        timer = setTimeout(poll, running ? 2000 : 10000);
      }
    };
    poll();
    const listener = (event: CustomEvent) => {
      if (event.detail?.pipelineUUID === pipelineUUID) {
        setBadges(toBadges(event.detail.blocks));
      }
    };
    window.addEventListener(EVENT, listener as EventListener);
    return () => {
      cancelled = true;
      clearTimeout(timer);
      window.removeEventListener(EVENT, listener as EventListener);
    };
  }, [pipelineUUID, streaming]);

  return badges;
}
