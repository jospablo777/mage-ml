import { useEffect, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';

const POLL_MS = 1000;

export type PythonWorkerJobType = {
  lines?: string[];
  problems?: string[];
  state?: 'running' | 'completed' | 'failed';
};

export type PythonWorkerType = {
  // False when its pyproject.toml keeps it apart from other workers.
  combine?: boolean;
  definition?: string;
  dependencies?: string[];
  // [tool.mageml.worker] in its pyproject.toml.
  description?: string;
  job?: PythonWorkerJobType;
  // The name people gave it ("Feast worker"), or its alias.
  label?: string;
  locked?: boolean;
  name: string;
  packages?: string[];
  prepared?: boolean;
  python?: string;
};

// How a worker is shown: its label, with its alias when they differ ("Feast worker
// (feast)"), as YAML names the alias.
export function workerLabel(worker?: PythonWorkerType): string {
  if (!worker) {
    return '';
  }
  return worker.label && worker.label !== worker.name
    ? `${worker.label} (${worker.name})`
    : worker.name;
}

const LogStyle = styled.pre`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 1.7;
  margin: 0;
  max-height: ${UNIT * 30}px;
  overflow: auto;
  padding: ${UNIT * 1.5}px;
  white-space: pre-wrap;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

// The worker a pipeline's Python blocks run in, and Prepare when it is not prepared yet.
// Compact: only the prepare, for a worker named next to it already.
function PythonWorkerStatus({
  compact,
  onPrepared,
  worker,
}: {
  compact?: boolean;
  onPrepared: () => void;
  worker: PythonWorkerType;
}) {
  const [job, setJob] = useState<PythonWorkerJobType>(worker?.job || null);
  const logRef = useRef(null);
  const [prepare, { isLoading: isStarting }] = useMutation(
    api.python_workers.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker: started }) => setJob(started?.job),
        onErrorCallback: response => setJob({
          lines: [],
          problems: [response?.error?.message || 'Preparing did not start.'],
          state: 'failed',
        }),
      }),
    },
  );
  // A worker added a moment ago appears with its prepare already running: the list
  // reloads after this panel first rendered without it.
  useEffect(() => {
    if (worker?.job && !job) {
      setJob(worker.job);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [worker?.job]);
  const running = job?.state === 'running';
  const { data } = api.python_workers.detail(
    running ? worker?.name : null,
    {},
    { refreshInterval: running ? POLL_MS : 0 },
  );
  useEffect(() => {
    const latest = data?.python_worker?.job;
    if (latest && running) {
      setJob(latest);
      if (latest.state === 'completed') {
        onPrepared();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [job?.lines?.length]);

  if (!worker) {
    return null;
  }

  return (
    <Spacing mt={1}>
      {!compact && worker.description && <Text small>{worker.description}</Text>}
      {!compact && (
        <Text muted small>
          {worker.definition} · Python {worker.python}
          {worker.dependencies?.length >= 1 && ` · ${worker.dependencies.join(', ')}`}
        </Text>
      )}
      {!worker.locked && (
        <Text small warning>
          It has no uv.lock: run uv lock in {worker.definition} and commit it.
        </Text>
      )}
      {worker.prepared && job?.state !== 'running' && (
        <Text small success>
          Prepared: {worker.packages?.length || 0} packages from its uv.lock.
        </Text>
      )}
      {!worker.prepared && (
        <Spacing mt={1}>
          {!compact && <Text small warning>Not prepared: its blocks fail until it is.</Text>}
          <Spacing mt={compact ? 0 : 1}>
            <Button
              compact
              disabled={running || !worker.locked}
              loading={running || isStarting}
              // @ts-ignore
              onClick={() => prepare({ python_worker: { name: worker.name } })}
              small
            >
              Prepare {workerLabel(worker)}
            </Button>
          </Spacing>
          {!compact && (
            <Text muted xsmall>
              Installs the packages its uv.lock pins, on this server (uv sync --locked).
            </Text>
          )}
        </Spacing>
      )}
      {(job?.problems || []).map(problem => (
        <Text danger key={problem} small>{problem}</Text>
      ))}
      {job?.lines?.length >= 1 && (
        <Spacing mt={1}>
          <LogStyle ref={logRef}>{job.lines.join('\n')}</LogStyle>
        </Spacing>
      )}
    </Spacing>
  );
}

export default PythonWorkerStatus;
