import { useMemo, useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { UNIT } from '@oracle/styles/units/spacing';
import { displayTimestamp } from '@utils/timestamp';
import { onSuccess } from '@api/utils/response';
import { shouldDisplayLocalTimezone } from '@components/settings/workspace/utils';

type CheckType = {
  check: string;
  column?: string;
  detail?: string;
  result: string;
  segment?: string;
  value?: number;
};

type SilenceType = {
  by?: string;
  reason?: string;
  until: string;
};

type HistoryType = {
  baseline_run_id?: number;
  checked_at?: string;
  checks: CheckType[];
  engines?: { [engine: string]: { error?: string; reports?: string[] } };
  pipeline_run_id: number;
  result: string;
  silenced_until?: string;
};

type MonitorType = {
  block_uuid?: string;
  error?: string;
  history?: HistoryType[];
  latest?: HistoryType;
  name: string;
  pipeline_uuid?: string;
  runs?: number;
  silence?: SilenceType;
};

const RUNS_SHOWN = 20;

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT / 2}px ${UNIT}px;
    text-align: left;
    vertical-align: top;
    white-space: nowrap;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

const CellStyle = styled.div<{ result: string }>`
  border-radius: ${UNIT / 2}px;
  font-size: 12px;
  padding: 2px ${UNIT / 2}px;

  ${props => {
    const accent = props.theme.accent || dark.accent;
    const background = {
      error: accent.negativeTransparent,
      fail: accent.negativeTransparent,
      hold: accent.warningTransparent,
      pass: 'rgba(0, 168, 26, 0.25)',
      warn: accent.warningTransparent,
    }[props.result];
    return background ? `background-color: ${background};` : '';
  }}
`;

function ResultText({ result }: { result: string }) {
  return (
    <Text
      danger={['error', 'fail'].includes(result)}
      inline
      small
      success={result === 'pass'}
      warning={['hold', 'warn'].includes(result)}
    >
      {result}
    </Text>
  );
}

function checkKey(check: CheckType): string {
  return [check.check, check.column, check.segment && `[${check.segment}]`]
    .filter(Boolean)
    .join(' · ');
}

const SILENCE_HOURS = 24;

// While a known failure is worked on: the checks still run, and fail_block still fails the
// block; only the notifications stop.
function SilenceControl({ monitor, onChanged }: {
  monitor: MonitorType;
  onChanged: () => void;
}) {
  const [update, { isLoading }] = useMutation(
    (silence: { hours: number; reason: string } | null) => api.monitors.useUpdate(
      monitor.name,
    )({ monitor: { silence } }),
    { onSuccess: (response: any) => onSuccess(response, { callback: onChanged }) },
  );
  const silence = monitor.silence;

  return (
    <FlexContainer alignItems="center">
      <Text muted={!silence} small warning={!!silence}>
        {silence
          ? `Notifications silenced until ${displayTimestamp(silence.until, shouldDisplayLocalTimezone())}`
            + (silence.reason ? `: ${silence.reason}` : '')
          : 'A failed run sends the pipeline\'s notifications.'}
      </Text>
      <Spacing ml={2} />
      {silence ? (
        <Button compact disabled={isLoading} onClick={() => update(null)}>
          Notify again
        </Button>
      ) : (
        <Button
          compact
          disabled={isLoading}
          onClick={() => update({
            hours: SILENCE_HOURS,
            reason: typeof window === 'undefined'
              ? ''
              : (window.prompt('Why? (shown with the silence)') || ''),
          })}
        >
          Silence for a day
        </Button>
      )}
    </FlexContainer>
  );
}

function MonitorDetail({ name, onBack }: { name: string; onBack: () => void }) {
  const { data, mutate } = api.monitors.detail(name);
  const monitor: MonitorType = data?.monitor;
  const runs = useMemo(
    () => (monitor?.history || []).slice(-RUNS_SHOWN).reverse(),
    [monitor],
  );
  const keys = useMemo(() => {
    const seen = new Set<string>();
    runs.forEach(run => run.checks.forEach(check => seen.add(checkKey(check))));
    return Array.from(seen);
  }, [runs]);

  if (!monitor) {
    return null;
  }

  return (
    <>
      <FlexContainer alignItems="center">
        <Button compact onClick={onBack}>All monitors</Button>
        <Spacing ml={2} />
        <Headline level={4}>{monitor.name}</Headline>
      </FlexContainer>
      <Spacing mt={1}>
        <Text muted small>
          Block {monitor.block_uuid} of pipeline {monitor.pipeline_uuid}; {monitor.runs} checked
          runs. Each column is a pipeline run, newest first; each row a check, with its value.
        </Text>
      </Spacing>
      <Spacing mt={1}>
        <SilenceControl monitor={monitor} onChanged={() => mutate()} />
      </Spacing>
      {runs.length === 0 ? (
        <Spacing mt={2}><Text muted>No run checked yet.</Text></Spacing>
      ) : (
        <Spacing mt={2} style={{ overflowX: 'auto' }}>
          <TableStyle>
            <thead>
              <tr>
                <th><Text bold small>Check</Text></th>
                {runs.map(run => (
                  <th key={run.pipeline_run_id}>
                    <Link
                      href={`/pipelines/${monitor.pipeline_uuid}/runs/${run.pipeline_run_id}`}
                      small
                    >
                      run {run.pipeline_run_id}
                    </Link>
                    <div><ResultText result={run.result} /></div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {keys.map(key => (
                <tr key={key}>
                  <td><Text monospace small>{key}</Text></td>
                  {runs.map(run => {
                    const check = run.checks.find(c => checkKey(c) === key);
                    return (
                      <td key={run.pipeline_run_id} title={check?.detail}>
                        {check && (
                          <CellStyle result={check.result}>
                            {check.value === null || typeof check.value === 'undefined'
                              ? check.result
                              : check.value}
                          </CellStyle>
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </TableStyle>
        </Spacing>
      )}
    </>
  );
}

// The project's monitors (monitors/<name>.yaml): their latest results, and the history of
// one monitor's checks across runs.
function Monitors() {
  const [selected, setSelected] = useState<string>(null);
  const { data, mutate } = api.monitors.list();
  const monitors: MonitorType[] = useMemo(() => data?.monitors || [], [data]);

  if (selected) {
    // The list is read again on the way back: a silence set in the detail shows there.
    return (
      <MonitorDetail
        name={selected}
        onBack={() => {
          setSelected(null);
          mutate();
        }}
      />
    );
  }

  return (
    <>
      <Text muted small>
        Monitors watch a block&apos;s output across pipeline runs: drift, missing values, row
        counts, freshness and model performance, with Evidently and NannyML as optional
        engines. Add one as monitors/&lt;name&gt;.yaml in the project.
      </Text>
      {monitors.length === 0 ? (
        <Spacing mt={2}><Text muted>The project has no monitors.</Text></Spacing>
      ) : (
        <Spacing mt={2}>
          <TableStyle>
            <thead>
              <tr>
                {['Monitor', 'Pipeline / block', 'Latest', 'Checked', 'Runs'].map(label => (
                  <th key={label}><Text bold small>{label}</Text></th>
                ))}
              </tr>
            </thead>
            <tbody>
              {monitors.map(monitor => {
                const failing = (monitor.latest?.checks || []).filter(
                  check => ['fail', 'warn'].includes(check.result),
                );
                return (
                  <tr key={monitor.name}>
                    <td>
                      <Link onClick={() => setSelected(monitor.name)} small>
                        {monitor.name}
                      </Link>
                    </td>
                    <td>
                      <Text small>{monitor.pipeline_uuid} / {monitor.block_uuid}</Text>
                    </td>
                    <td>
                      {monitor.error
                        ? <Text danger small>{monitor.error}</Text>
                        : monitor.latest
                          ? (
                            <>
                              <ResultText result={monitor.latest.result} />
                              {failing.length > 0 && (
                                <Text muted small>
                                  {failing.slice(0, 3).map(checkKey).join(', ')}
                                  {failing.length > 3 ? ` and ${failing.length - 3} more` : ''}
                                </Text>
                              )}
                            </>
                          )
                          : <Text muted small>not run yet</Text>}
                      {monitor.silence && (
                        <Text muted small>
                          silenced until {displayTimestamp(
                            monitor.silence.until, shouldDisplayLocalTimezone(),
                          )}
                        </Text>
                      )}
                    </td>
                    <td>
                      <Text muted small>
                        {monitor.latest?.checked_at
                          ? displayTimestamp(monitor.latest.checked_at, shouldDisplayLocalTimezone())
                          : ''}
                      </Text>
                    </td>
                    <td><Text small>{monitor.runs || 0}</Text></td>
                  </tr>
                );
              })}
            </tbody>
          </TableStyle>
        </Spacing>
      )}
    </>
  );
}

export default Monitors;
