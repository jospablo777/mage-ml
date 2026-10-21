import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import BlockType, { BlockTypeEnum } from '@interfaces/BlockType';
import Button from '@oracle/elements/Button';
import Select from '@oracle/elements/Inputs/Select';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import { publishStreamBlocks } from './badges';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

// A stream on the local engine, from the editor: start it (naming its sinks), see what it
// does (state, rates, lag, totals, the engine's log), pause, resume and stop it; and check
// that its source and sinks answer before starting it.

export type ConnectionCheckType = {
  facts: { label: string; value: string }[];
  kind: string;
  pipeline: string;
  problems: string[];
  result: 'passed' | 'warning' | 'failed' | 'not_checked';
  role: string;
  summary: string;
};

export type ConnectionsType = {
  checks?: ConnectionCheckType[];
  error?: string;
  notes?: string[];
};

export type StreamRunBlockType = {
  batches: number;
  block: string;
  errors: number;
  messages_in: number;
  messages_out: number;
  p50_ms: number;
  p95_ms: number;
};

export type StreamRunType = {
  alerts?: number;
  batches?: number;
  blocks?: StreamRunBlockType[];
  connections?: ConnectionsType;
  dead_lettered_messages?: number;
  error?: string;
  exit_code?: number;
  lag?: { [partition: string]: number };
  lag_history?: number[];
  last_error?: string;
  logs?: string[];
  messages_in?: number;
  messages_out?: number;
  notes?: string[];
  paused?: boolean;
  pipeline_uuid: string;
  rate_in?: number[];
  rate_out?: number[];
  retries?: number;
  running?: boolean;
  state?: string;
  tap?: StreamTapType;
};

export type StreamTapType = {
  error?: string;
  messages?: { partition: number; position?: number; value: any }[];
  point?: string;
  points?: string[];
  redacted?: { [path: string]: number };
};

const PanelStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  margin-bottom: ${UNIT * 1.5}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const NumbersStyle = styled.div`
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: repeat(auto-fill, minmax(${UNIT * 13}px, 1fr));
`;

const BlocksStyle = styled.div`
  display: grid;
  gap: ${UNIT * 0.5}px ${UNIT * 1.5}px;
  grid-template-columns: minmax(0, 1fr) repeat(5, auto);
  overflow-x: auto;
`;

// Long block names end with an ellipsis; the full name is the tooltip.
const NameStyle = styled.div`
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  p {
    display: inline;
  }
`;

const FactsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 0.25}px ${UNIT * 1.5}px;
  grid-template-columns: auto minmax(0, 1fr);
`;

const LogStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  margin: 0;
  max-height: ${UNIT * 50}px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-word;
`;

function body(response: any) {
  return response?.data || response;
}

function last(series?: number[]): number {
  return series?.length ? series[series.length - 1] : 0;
}

export function sinksOf(blocks: BlockType[]): string[] {
  return (blocks || [])
    .filter(({ type }) => BlockTypeEnum.DATA_EXPORTER === type)
    .map(({ uuid }) => uuid);
}

// One line per series over the last minute; the scale is the larger series' maximum.
function Sparkline({ label, series }: {
  label: string;
  series: { color: string; values: number[] }[];
}) {
  const width = 240;
  const height = 40;
  const max = Math.max(1, ...series.flatMap(({ values }) => values || []));
  return (
    <div>
      <Text muted xsmall>{label}</Text>
      <svg height={height} viewBox={`0 0 ${width} ${height}`} width="100%">
        {series.map(({ color, values }) => {
          const points = (values || []).map((value, index) => {
            const x = values.length > 1 ? (index / (values.length - 1)) * width : 0;
            const y = height - 2 - (value / max) * (height - 4);
            return `${x.toFixed(1)},${y.toFixed(1)}`;
          });
          return points.length ? (
            <polyline fill="none" key={color} points={points.join(' ')} stroke={color}
              strokeWidth={1.5} />
          ) : null;
        })}
      </svg>
    </div>
  );
}

// The blocks the status already names, offered as tap points before the tap answers.
function sinkFeeders(run: StreamRunType): string[] {
  return (run?.blocks || []).map(({ block }) => block);
}

function headline(run: StreamRunType): string {
  if (!run || !run.running) {
    if (run?.state === 'exited') {
      return `Stopped on its own: the engine exited with code ${run.exit_code}`;
    }
    return run?.state === 'stopped' ? 'Stopped' : 'Not running';
  }
  const lag = Object.values(run.lag || {}).reduce((sum, value) => sum + value, 0);
  const state = run.paused ? 'Paused' : (run.state || 'running')
    .replace(/^./, c => c.toUpperCase());
  return `${state} · ${last(run.rate_in).toLocaleString()} in/s · `
    + `${last(run.rate_out).toLocaleString()} out/s · lag ${lag.toLocaleString()}`;
}

const RESULT_WORDS = {
  failed: 'Failed',
  not_checked: 'Not checked',
  passed: 'Passed',
  warning: 'Look at this',
};

// The point first: how many connections failed or need a look.
export function connectionsHeadline(checks: ConnectionCheckType[]): string {
  const count = (result: string) => checks.filter(c => c.result === result).length;
  const failed = count('failed');
  const warnings = count('warning');
  if (failed) {
    return `${failed} of ${checks.length} connections failed: the stream cannot start`;
  }
  if (warnings) {
    return `Every connection answered; ${warnings} to look at`;
  }
  return checks.length ? 'Every connection answered' : 'No connections to check';
}

function ConnectionChecks({ connections }: { connections: ConnectionsType }) {
  if (connections.error) {
    return <LogStyle><Text danger small>{connections.error}</Text></LogStyle>;
  }
  const checks = connections.checks || [];
  return (
    <>
      <Text bold>{connectionsHeadline(checks)}</Text>
      {checks.map(check => (
        <Spacing key={`${check.role}-${check.kind}`} mt={2}>
          <FlexContainer alignItems="center">
            <Text bold small>{check.role}</Text>
            <Spacing ml={1}><Text monospace muted small>{check.kind}</Text></Spacing>
            <Spacing ml={1}>
              <Text
                danger={check.result === 'failed'}
                muted={check.result === 'not_checked'}
                small
                success={check.result === 'passed'}
                warning={check.result === 'warning'}
              >
                {RESULT_WORDS[check.result] || check.result}
              </Text>
            </Spacing>
          </FlexContainer>
          <Text small>{check.summary}</Text>
          {check.problems.map(problem => (
            <Text danger={check.result === 'failed'} key={problem} small
              warning={check.result !== 'failed'}>
              {problem}
            </Text>
          ))}
          {!!check.facts.length && (
            <Spacing mt={1}>
              <FactsStyle>
                {check.facts.map(({ label, value }) => [
                  <Text key={`${label}-label`} muted small>{label}</Text>,
                  <Text key={`${label}-value`} monospace small>{value}</Text>,
                ])}
              </FactsStyle>
            </Spacing>
          )}
        </Spacing>
      ))}
    </>
  );
}

type StreamRunProps = {
  blocks: BlockType[];
  pipelineUUID: string;
};

function StreamRun({ blocks, pipelineUUID }: StreamRunProps) {
  const [run, setRun] = useState<StreamRunType>(null);
  const [busy, setBusy] = useState<boolean>(false);
  const [error, setError] = useState<string>(null);
  const timer = useRef<ReturnType<typeof setTimeout>>(null);
  // The tap point whose last messages the panel shows; none until chosen.
  const [tapPoint, setTapPoint] = useState<string>(null);
  const sinks = useMemo(() => sinksOf(blocks), [blocks]);
  const [connections, setConnections] = useState<ConnectionsType>(null);
  const [checking, setChecking] = useState<boolean>(false);

  // The engine checks the source and sinks with the settings a development run uses; the
  // check only reads.
  const checkConnections = useCallback(async () => {
    setChecking(true);
    try {
      const response = await api.stream_runs.pipelines.detailAsync(
        pipelineUUID, 'current', { connections: 1 },
      );
      const found = body(response);
      setConnections(found?.stream_run?.connections
        || { error: found?.error?.message || 'The check did not answer.' });
    } finally {
      setChecking(false);
    }
  }, [pipelineUUID]);

  const load = useCallback(async () => {
    const response = await api.stream_runs.pipelines.detailAsync(
      pipelineUUID, 'current', tapPoint ? { tap: tapPoint } : {},
    );
    const found = body(response)?.stream_run;
    if (found) {
      setRun(found);
      // The graph shows each block's numbers while the stream runs.
      publishStreamBlocks(pipelineUUID, found.running ? (found.blocks || []) : []);
    }
    return found;
  }, [pipelineUUID, tapPoint]);

  // Every second while the engine runs.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const found = await load();
      if (!cancelled && found?.running) {
        timer.current = setTimeout(tick, 1000);
      }
    };
    tick();
    return () => {
      cancelled = true;
      clearTimeout(timer.current);
    };
  }, [load, run?.running]);

  const act = useCallback(async (request: () => Promise<any>) => {
    setBusy(true);
    setError(null);
    try {
      const result = body(await request());
      if (result?.error) {
        setError(result.error.message);
      } else if (result?.stream_run) {
        setRun(result.stream_run);
      }
    } finally {
      setBusy(false);
    }
  }, []);

  const start = useCallback(() => {
    // The sources read with development positions; the sinks are real (SAFETY.md section 3).
    if (sinks.length >= 1 && typeof window !== 'undefined' && !window.confirm(
      'The stream reads with a development consumer group, so production keeps its position. '
        + `It writes to the sinks of ${sinks.join(', ')}. Start it?`,
    )) {
      return;
    }
    act(() => api.stream_runs.pipelines.useCreate(pipelineUUID)({
      stream_run: { confirmed_sinks: sinks },
    }));
  }, [act, pipelineUUID, sinks]);

  const control = useCallback((action: 'pause' | 'resume') => act(
    () => api.stream_runs.pipelines.useUpdate(pipelineUUID, 'current')({
      stream_run: { action },
    }),
  ), [act, pipelineUUID]);

  const stop = useCallback(() => act(
    () => api.stream_runs.pipelines.useDelete(pipelineUUID, 'current')(),
  ), [act, pipelineUUID]);

  const running = !!run?.running;
  const numbers = [
    ['Messages in', run?.messages_in],
    ['Messages out', run?.messages_out],
    ['Batches', run?.batches],
    ['Retries', run?.retries],
    ['Dead-lettered', run?.dead_lettered_messages],
    ['Alerts', run?.alerts],
  ];

  return (
    <Spacing p={2}>
      <PanelStyle>
        <Text bold large>{headline(run)}</Text>
        <Spacing mt={1}>
          <FlexContainer alignItems="center" flexWrap="wrap">
            {!running && (
              <Button disabled={busy} loading={busy} onClick={start} primary>
                Start on the local engine
              </Button>
            )}
            {running && (
              <Button disabled={busy} onClick={() => control(run.paused ? 'resume' : 'pause')}>
                {run.paused ? 'Resume' : 'Pause'}
              </Button>
            )}
            {running && (
              <Spacing ml={1}>
                <Button disabled={busy} onClick={stop}>Stop</Button>
              </Spacing>
            )}
            <Spacing ml={1}>
              <Button disabled={checking} loading={checking} onClick={checkConnections}>
                Check connections
              </Button>
            </Spacing>
          </FlexContainer>
        </Spacing>
        {error && (
          <Spacing mt={1}>
            <LogStyle><Text danger small>{error}</Text></LogStyle>
          </Spacing>
        )}
        {(run?.notes || []).map(note => (
          <Spacing key={note} mt={1}>
            <Text muted small>{note}</Text>
          </Spacing>
        ))}
        {run?.last_error && (
          <Spacing mt={1}>
            <LogStyle><Text danger small>{run.last_error}</Text></LogStyle>
          </Spacing>
        )}
      </PanelStyle>

      {connections && (
        <PanelStyle>
          <ConnectionChecks connections={connections} />
        </PanelStyle>
      )}

      {(running || !!run?.messages_in) && (
        <PanelStyle>
          <Sparkline
            label="Messages per second, last minute"
            series={[
              { color: dark.accent.info, values: run?.rate_in },
              { color: dark.accent.positive, values: run?.rate_out },
            ]}
          />
          <Text xsmall>
            <span style={{ color: dark.accent.info }}>read</span>
            {' · '}
            <span style={{ color: dark.accent.positive }}>written</span>
          </Text>
          <Spacing mt={1} />
          <Sparkline
            label="Messages behind the end of the source, last minute"
            series={[{ color: dark.accent.warning, values: run?.lag_history }]}
          />
          <Spacing mt={2} />
          <NumbersStyle>
            {numbers.map(([label, value]) => (
              <div key={label as string}>
                <Text muted xsmall>{label}</Text>
                <Text bold>{(value as number || 0).toLocaleString()}</Text>
              </div>
            ))}
          </NumbersStyle>
        </PanelStyle>
      )}

      {!!run?.blocks?.length && (
        <PanelStyle>
          <Text bold small>Blocks</Text>
          <Spacing mt={1} />
          <BlocksStyle>
            {['Block', 'In', 'Out', 'p50', 'p95', 'Errors'].map(label => (
              <Text key={label} muted xsmall>{label}</Text>
            ))}
            {run.blocks.map(block => [
              <NameStyle key={`${block.block}-name`} title={block.block}>
                <Text monospace small>{block.block}</Text>
              </NameStyle>,
              <Text key={`${block.block}-in`} small>{block.messages_in.toLocaleString()}</Text>,
              <Text key={`${block.block}-out`} small>
                {block.messages_out.toLocaleString()}
              </Text>,
              <Text key={`${block.block}-p50`} small>{block.p50_ms.toFixed(1)} ms</Text>,
              <Text key={`${block.block}-p95`} small>{block.p95_ms.toFixed(1)} ms</Text>,
              <Text danger={block.errors > 0} key={`${block.block}-errors`} small>
                {block.errors.toLocaleString()}
              </Text>,
            ])}
          </BlocksStyle>
        </PanelStyle>
      )}

      {running && (
        <PanelStyle>
          <FlexContainer alignItems="center" justifyContent="space-between">
            <Text bold small>Tap: the last messages at a block</Text>
            <Select
              aria-label="Tap point"
              compact
              onChange={e => setTapPoint(e.target.value || null)}
              value={tapPoint || ''}
            >
              <option value="">Off</option>
              {(run?.tap?.points?.length ? run.tap.points : ['source', ...sinkFeeders(run)])
                .map(point => <option key={point} value={point}>{point}</option>)}
            </Select>
          </FlexContainer>
          {tapPoint && run?.tap?.error && (
            <Spacing mt={1}><Text danger small>{run.tap.error}</Text></Spacing>
          )}
          {tapPoint && !run?.tap?.error && (
            <Spacing mt={1}>
              {!run?.tap?.messages?.length && <Text muted small>No message yet.</Text>}
              {!!Object.keys(run?.tap?.redacted || {}).length && (
                <Text muted small>
                  Shown with {Object.keys(run.tap.redacted).join(', ')} redacted.
                </Text>
              )}
              <LogStyle>
                {[...(run?.tap?.messages || [])].reverse()
                  .map(m => `${m.partition}:${m.position ?? ''}  ${
                    typeof m.value === 'string' ? m.value : JSON.stringify(m.value)
                  }`).join('\n')}
              </LogStyle>
            </Spacing>
          )}
        </PanelStyle>
      )}

      {!!run?.logs?.length && (
        <PanelStyle>
          <Text bold small>Engine log</Text>
          <Spacing mt={1} />
          <LogStyle>{run.logs.join('\n')}</LogStyle>
        </PanelStyle>
      )}
    </Spacing>
  );
}

export default StreamRun;
