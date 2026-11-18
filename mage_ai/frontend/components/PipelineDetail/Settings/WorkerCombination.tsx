import styled from 'styled-components';
import { useEffect, useMemo, useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';

// The answer of POST /api/python_worker_combinations (environments.combine_check).
type CombinationType = {
  changes?: { [worker: string]: { from: string; package: string; to: string }[] };
  combined_size?: number;
  names?: { [worker: string]: string };
  reason?: string;
  saved_bytes?: number;
  sizes?: { [worker: string]: number };
  state: 'changes' | 'conflict' | 'kept_apart' | 'same' | 'unchecked';
  workers: string[];
};

const BarStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  display: flex;
  overflow: hidden;
  width: 100%;
`;

const SegmentStyle = styled.div<{ share: number }>`
  min-width: ${UNIT * 10}px;
  padding: ${UNIT * 0.5}px ${UNIT}px;
  width: ${props => props.share * 100}%;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border-right: 2px solid ${(props.theme.background || dark.background).page};
  `}
`;

function megabytes(bytes?: number): string {
  return bytes === undefined || bytes === null ? '-' : `${Math.round(bytes / 1e6)} MB`;
}

function list(names: string[]): string {
  return names.length <= 1 ? names.join('')
    : `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

// Whether the Python workers a pipeline uses can run as one, said in one line; opened, what
// each installs, what combining changes, its trade-off, and Combine into one worker.
function WorkerCombination({ labels, pipelineUUID, workers }: {
  labels?: { [worker: string]: string };
  pipelineUUID: string;
  workers: string[];
}) {
  const key = useMemo(() => [...workers].sort().join(','), [workers]);
  const [result, setResult] = useState<CombinationType>(null);
  const [error, setError] = useState<string>(null);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('combined');
  const [check, { isLoading: checking }] = useMutation(
    api.python_worker_combinations.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker_combination: answer }) => setResult(answer),
        onErrorCallback: response => setError(response?.error?.message || 'Not checked.'),
      }),
    },
  );
  const [combine, { isLoading: combining }] = useMutation(
    api.python_workers.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        // The pipeline changed on the server: the page shows it, and the new worker's
        // prepare, once loaded again.
        callback: () => window.location.reload(),
        onErrorCallback: response => setError(response?.error?.message || 'Not combined.'),
      }),
    },
  );

  useEffect(() => {
    if (workers.length >= 2) {
      setResult(null);
      // @ts-ignore
      check({ python_worker_combination: { workers } });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  if (workers.length < 2) {
    return null;
  }

  const names = workers.map(w => result?.names?.[w] || labels?.[w] || w);
  const changeCount = Object.values(result?.changes || {})
    .reduce((sum, changes) => sum + changes.length, 0);
  const headline = {
    changes: `${list(names)} can run as one worker if ${changeCount} package`
      + `${changeCount === 1 ? '' : 's'} change version.`,
    conflict: `${list(names)} cannot run as one worker: ${result?.reason}`,
    kept_apart: `${list(names)} stay apart: ${result?.reason}`,
    same: `${list(names)} can run as one worker with the same package versions`
      + (result?.saved_bytes ? `, ${megabytes(result.saved_bytes)} less to install.` : '.'),
    unchecked: `Could not check whether ${list(names)} can run as one worker: ${result?.reason}`,
  }[result?.state];
  const total = Object.values(result?.sizes || {}).reduce((sum, size) => sum + size, 0) || 1;
  const combinable = result?.state === 'same' || result?.state === 'changes';

  return (
    <Spacing mt={2}>
      <Text bold>Size and speed</Text>
      {checking && <Text muted small>Checking whether {list(names)} can run as one worker...</Text>}
      {error && <Text danger small>{error}</Text>}
      {result && (
        <>
          <Text small warning={result.state === 'conflict'}>{headline}</Text>
          {result.state === 'unchecked' && (
            // @ts-ignore
            <Link onClick={() => check({ python_worker_combination: { workers } })} small>
              Check again
            </Link>
          )}
          {!open && (
            <Link onClick={() => setOpen(true)} small>Show what each worker installs</Link>
          )}
          {open && (
            <Spacing mt={1}>
              <BarStyle>
                {result.workers.map(worker => (
                  <SegmentStyle key={worker} share={(result.sizes?.[worker] || 0) / total}>
                    <Text small>{result.names?.[worker] || worker}</Text>
                    <Text monospace muted small>{megabytes(result.sizes?.[worker])}</Text>
                  </SegmentStyle>
                ))}
              </BarStyle>
              <Text muted xsmall>
                What each worker installs on Linux x86-64, from its uv.lock.
                {result.combined_size ? ` Combined: ${megabytes(result.combined_size)}.` : ''}
              </Text>
              {Object.entries(result.changes || {}).map(([worker, changes]) => changes.length > 0 && (
                <Spacing key={worker} mt={1}>
                  <Text small>Versions that change for {result.names?.[worker] || worker}:</Text>
                  {changes.map(change => (
                    <Text key={change.package} monospace small>
                      {change.package} {change.from} → {change.to}
                    </Text>
                  ))}
                </Spacing>
              ))}
              {combinable && (
                <Spacing mt={1}>
                  <Text muted small>
                    Blocks of one worker share one process: they share memory, and a crash
                    stops the blocks after it in the same run.
                    {result.state === 'changes' && ' Packages change version: run the '
                      + 'pipeline\'s tests after combining.'}
                    {' '}The old workers stay until you delete them.
                  </Text>
                  <Spacing mt={1}>
                    <FlexContainer alignItems="flex-end">
                      <TextInput
                        aria-label="Name of the combined worker"
                        compact
                        labelFixed="Name of the combined worker"
                        monospace
                        onChange={e => setName(e.target.value)}
                        value={name}
                      />
                      <Spacing ml={1} />
                      <Button
                        compact
                        disabled={!name}
                        loading={combining}
                        onClick={() => {
                          setError(null);
                          // @ts-ignore
                          combine({ python_worker: {
                            combine: result.workers,
                            name,
                            pipeline_uuid: pipelineUUID,
                          } });
                        }}
                        primary
                      >
                        Combine into one worker
                      </Button>
                    </FlexContainer>
                  </Spacing>
                </Spacing>
              )}
            </Spacing>
          )}
        </>
      )}
    </Spacing>
  );
}

export default WorkerCombination;
