import { useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

type ChangesType = {
  added: string[];
  changed: string[];
  removed: string[];
};

type ComparisonType = {
  code: ChangesType & { diffs?: { [path: string]: string } };
  environment: {
    added: string[];
    changed: { [name: string]: [string, string] };
    removed: string[];
    runtime: { [key: string]: [string, string] };
  };
  outputs: {
    block_uuid: string;
    metrics?: { [metric: string]: [number, number] };
    result: string;
  }[];
  runs: [number, number];
  same_code: boolean;
  same_environment: boolean;
  variables: ChangesType;
};

type ReproductionType = {
  finished_at?: number;
  log?: string;
  report?: {
    blocks?: { block_uuid: string; detail?: string; result: string }[];
    code_restored?: boolean;
    reproduced_run_id?: number;
  };
  started_at?: number;
  status: 'error' | 'failed' | 'passed' | 'running' | 'stopped';
};

type MlflowRunType = {
  experiment_id: string;
  metrics?: { [name: string]: number };
  model_versions?: { name: string; version: string }[];
  params?: number;
  run_id: string;
  run_name?: string;
  status?: string;
  url?: string;
};

type ReleaseType = {
  alias: string;
  champion?: string;
  decision: 'fail' | 'hold' | 'pass';
  error?: string;
  model: string;
  rules: {
    champion?: number;
    metric: string;
    reasons: string[];
    result: string;
    value?: number;
  }[];
  status: string;
  version: string;
};

type RunRecordType = {
  captured_at?: string;
  captures?: number;
  code?: { digest: string; files: number; truncated: boolean };
  code_changed_between_starts?: boolean;
  comparison?: ComparisonType;
  comparison_error?: string;
  experiments?: {
    [blockUUID: string]: { runs: MlflowRunType[]; tracking_uri?: string };
  };
  releases?: { [blockUUID: string]: ReleaseType[] };
  environment?: {
    mage?: string;
    packages?: number;
    pipeline_environment?: string;
    platform?: string;
    python?: string;
  };
  git?: { branch?: string; commit: string; dirty: boolean };
  reproduction?: ReproductionType;
  variables?: string[];
};

const PreStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 18px;
  margin: 0;
  padding: ${UNIT}px;
  white-space: pre-wrap;
  word-break: break-word;

  ${props => `
    background-color: ${(props.theme.background || dark.background).codeArea};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    border-radius: ${UNIT / 2}px;
    color: ${(props.theme.content || dark.content).default};
  `}
`;

function Row({ label, children }: { children: any; label: string }) {
  return (
    <FlexContainer alignItems="flex-start">
      <div style={{ minWidth: 140 }}>
        <Text muted small>{label}</Text>
      </div>
      <Text small>{children}</Text>
    </FlexContainer>
  );
}

function seconds(reproduction: ReproductionType): string {
  if (!reproduction?.started_at) {
    return '';
  }
  const end = reproduction.finished_at || Date.now() / 1000;
  return `${Math.max(0, Math.round(end - reproduction.started_at))} s`;
}

function Comparison({ comparison }: { comparison: ComparisonType }) {
  const { code, environment, outputs, variables } = comparison;
  const changedVariables = [...variables.changed, ...variables.added, ...variables.removed];
  const packages = Object.entries(environment.changed || {});

  return (
    <Spacing mt={1}>
      <Row label="Code">
        {comparison.same_code
          ? 'the same'
          : ['changed', 'added', 'removed']
            .filter(kind => code[kind]?.length)
            .map(kind => `${kind}: ${code[kind].join(', ')}`)
            .join('; ')}
      </Row>
      <Row label="Environment">
        {comparison.same_environment
          ? 'the same'
          : [
            ...Object.entries(environment.runtime || {})
              .map(([key, [before, after]]) => `${key} ${before} → ${after}`),
            ...packages.map(([name, [before, after]]) => `${name} ${before} → ${after}`),
            environment.added?.length ? `added ${environment.added.join(', ')}` : null,
            environment.removed?.length ? `removed ${environment.removed.join(', ')}` : null,
          ].filter(Boolean).join('; ')}
      </Row>
      <Row label="Variables">
        {changedVariables.length ? `changed: ${changedVariables.join(', ')}` : 'the same'}
      </Row>
      {outputs.map(({ block_uuid: blockUUID, metrics, result }) => (
        <Row key={blockUUID} label={`Output of ${blockUUID}`}>
          <Text
            danger={result === 'differs'}
            inline
            muted={result === 'not recorded'}
            small
            success={result === 'same'}
          >
            {result}
          </Text>
          {Object.entries(metrics || {}).map(([metric, [before, after]]) => (
            <Text key={metric} monospace small>
              {metric}: {String(before ?? 'none')} → {String(after ?? 'none')}
            </Text>
          ))}
        </Row>
      ))}
      {Object.entries(code.diffs || {}).map(([path, diff]) => (
        <Spacing key={path} mt={1}>
          <PreStyle>{diff}</PreStyle>
        </Spacing>
      ))}
    </Spacing>
  );
}

type RunRecordProps = {
  pipelineRunId: number;
};

// What a pipeline run ran with (run_records.py), a comparison with another run, and a
// reproduction of the run with its recorded code.
function RunRecord({ pipelineRunId }: RunRecordProps) {
  const [showError] = useError(null, {}, [], { uuid: 'PipelineDetail/RunRecord' });
  const [compareInput, setCompareInput] = useState<string>('');
  const [compareWith, setCompareWith] = useState<string>(null);
  const [confirming, setConfirming] = useState<boolean>(false);

  const { data, mutate } = api.run_records.detail(
    pipelineRunId,
    compareWith ? { compare_with: compareWith } : {},
    {
      refreshInterval: (latest: { run_record?: RunRecordType }) => (
        latest?.run_record?.reproduction?.status === 'running' ? 1500 : 0
      ),
    },
  );
  const record: RunRecordType = data?.run_record;
  const reproduction = record?.reproduction;
  const running = reproduction?.status === 'running';

  const [approved, setApproved] = useState<{ [key: string]: string }>({});
  const [release, { isLoading: isReleasing }] = useMutation(
    api.model_releases.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ model_release: updated }) => setApproved(prev => ({
          ...prev,
          [updated.model]: updated.champion,
        })),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const [start, { isLoading: isStarting }] = useMutation(
    api.run_records.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => {
          setConfirming(false);
          mutate();
        },
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  if (!record) {
    return null;
  }

  return (
    <>
      <Headline level={5}>Run record</Headline>
      {!record.code ? (
        <Spacing mt={1}>
          <Text muted small>
            This run has no run record. Runs started before run records existed have none.
          </Text>
        </Spacing>
      ) : (
        <Spacing mt={1}>
          <Row label="Code">
            {record.code.files} files, {record.code.digest.slice(0, 12)}
            {record.code.truncated ? ' (too many files to keep a copy)' : ''}
          </Row>
          {record.git && (
            <Row label="Git">
              {record.git.commit.slice(0, 10)}
              {record.git.branch ? ` on ${record.git.branch}` : ''}
              {record.git.dirty ? ', with uncommitted changes' : ''}
            </Row>
          )}
          <Row label="Environment">
            Python {record.environment?.python}, Mage {record.environment?.mage},{' '}
            {record.environment?.packages} packages, {record.environment?.platform}
            {record.environment?.pipeline_environment
              ? `, pipeline environment ${record.environment.pipeline_environment.slice(0, 12)}`
              : ''}
          </Row>
          <Row label="Variables">
            {record.variables?.length ? record.variables.join(', ') : 'none'}
          </Row>
          {Object.entries(record.experiments || {}).map(([blockUUID, tracked]) => (
            <Spacing key={blockUUID} mt={1}>
              <Text muted small>MLflow runs of {blockUUID}</Text>
              {tracked.runs.map(run => (
                <Row key={run.run_id} label={run.run_name || run.run_id.slice(0, 8)}>
                  {run.status?.toLowerCase()}
                  {Object.entries(run.metrics || {}).slice(0, 12)
                    .map(([name, value]) => `, ${name} ${value}`).join('')}
                  {run.model_versions?.length
                    ? `, registered ${run.model_versions
                      .map(({ name, version }) => `${name} v${version}`).join(', ')}`
                    : ''}
                  {run.url && (
                    <>
                      {' '}
                      <a href={run.url} rel="noopener noreferrer" target="_blank">
                        open in MLflow
                      </a>
                    </>
                  )}
                </Row>
              ))}
            </Spacing>
          ))}
          {Object.entries(record.releases || {}).map(([blockUUID, evaluations]) => (
            <Spacing key={blockUUID} mt={1}>
              {evaluations.filter(e => e.decision).map(evaluation => (
                <Spacing key={`${evaluation.model}-${evaluation.version}`} mt={1}>
                  <Text small>
                    Release check of {evaluation.model} version {evaluation.version}:{' '}
                    <Text
                      danger={evaluation.decision === 'fail'}
                      inline
                      small
                      success={evaluation.decision === 'pass'}
                      warning={evaluation.decision === 'hold'}
                    >
                      {evaluation.decision}
                    </Text>
                    {' '}({approved[evaluation.model] === evaluation.version
                      ? 'promoted'
                      : evaluation.status}; {evaluation.alias} was{' '}
                    {evaluation.champion || 'none'})
                  </Text>
                  {evaluation.rules.map(rule => (
                    <Row key={rule.metric} label={rule.metric}>
                      {String(rule.value ?? 'missing')}: {rule.result}
                      {rule.reasons.length ? `; ${rule.reasons.join(', ')}` : ''}
                    </Row>
                  ))}
                  {evaluation.decision === 'pass'
                    && evaluation.status === 'awaiting approval'
                    && approved[evaluation.model] !== evaluation.version && (
                    <Spacing mt={1}>
                      <Button
                        compact
                        loading={isReleasing}
                        // @ts-ignore
                        onClick={() => release({
                          model_release: {
                            action: 'promote',
                            expected_champion: evaluation.champion,
                            model: evaluation.model,
                            version: evaluation.version,
                          },
                        })}
                        primary
                      >
                        Approve promotion to {evaluation.alias}
                      </Button>
                    </Spacing>
                  )}
                </Spacing>
              ))}
            </Spacing>
          ))}
          {record.code_changed_between_starts && (
            <Text small warning>
              The run was started again with other code; the record shows the latest start.
            </Text>
          )}

          <Spacing mt={2}>
            <FlexContainer alignItems="center">
              <TextInput
                compact
                label="Compare with run id"
                monospace
                onChange={e => setCompareInput(e.target.value)}
                type="number"
                value={compareInput}
              />
              <Spacing ml={1} />
              <Button
                compact
                disabled={!compareInput}
                onClick={() => setCompareWith(compareInput)}
              >
                Compare
              </Button>
            </FlexContainer>
            {record.comparison_error && (
              <Spacing mt={1}>
                <Text danger small>{record.comparison_error}</Text>
              </Spacing>
            )}
            {record.comparison && <Comparison comparison={record.comparison} />}
          </Spacing>

          <Spacing mt={2}>
            <Text muted small>
              Reproduce runs the pipeline again with the code, variables and execution date
              of this run, in a new run, and compares every block&apos;s outputs with this
              run&apos;s. Exporters write again.
            </Text>
            <Spacing mt={1}>
              <FlexContainer alignItems="center">
                {!confirming && !running && (
                  <Button
                    compact
                    disabled={record.code.truncated}
                    onClick={() => setConfirming(true)}
                  >
                    Reproduce
                  </Button>
                )}
                {confirming && !running && (
                  <>
                    <Text small warning>
                      Run the pipeline again, exporters included?&nbsp;&nbsp;
                    </Text>
                    <Button
                      compact
                      loading={isStarting}
                      // @ts-ignore
                      onClick={() => start({ run_record: { pipeline_run_id: pipelineRunId } })}
                      primary
                    >
                      Run it
                    </Button>
                    <Spacing ml={1} />
                    <Button compact onClick={() => setConfirming(false)}>
                      Cancel
                    </Button>
                  </>
                )}
                {running && (
                  <>
                    <Text small>Reproducing, {seconds(reproduction)}&nbsp;&nbsp;</Text>
                    <Button
                      compact
                      onClick={() => api.run_records.deleteAsync(pipelineRunId)
                        .then(() => mutate())}
                    >
                      Stop
                    </Button>
                  </>
                )}
              </FlexContainer>
            </Spacing>
            {reproduction?.status === 'passed' && (
              <Spacing mt={1}>
                <Text small success>
                  Reproduced as run {reproduction.report?.reproduced_run_id}: every block gave
                  the same outputs ({seconds(reproduction)}).
                </Text>
              </Spacing>
            )}
            {reproduction?.status === 'failed' && (
              <Spacing mt={1}>
                <Text danger small>Not reproduced; the blocks below say where.</Text>
              </Spacing>
            )}
            {reproduction?.status === 'error' && (
              <Spacing mt={1}>
                <Text danger small>The reproduction could not run; its log says why.</Text>
              </Spacing>
            )}
            {reproduction?.status === 'stopped' && (
              <Spacing mt={1}>
                <Text muted small>The reproduction was stopped.</Text>
              </Spacing>
            )}
            {reproduction?.report?.blocks?.map(block => (
              <Row key={block.block_uuid} label={block.block_uuid}>
                {block.result}{block.detail ? `. ${block.detail}` : ''}
              </Row>
            ))}
            {reproduction?.log && (
              <Spacing mt={1}>
                <PreStyle>{reproduction.log}</PreStyle>
              </Spacing>
            )}
          </Spacing>
        </Spacing>
      )}
    </>
  );
}

export default RunRecord;
