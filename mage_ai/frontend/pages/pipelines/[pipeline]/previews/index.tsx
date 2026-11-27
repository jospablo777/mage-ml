import { useEffect, useMemo, useState } from 'react';
import NextLink from 'next/link';
import styled from 'styled-components';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import ErrorsType from '@interfaces/ErrorsType';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Link from '@oracle/elements/Link';
import PipelineDetailPage from '@components/PipelineDetailPage';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { PADDING_UNITS, UNIT } from '@oracle/styles/units/spacing';
import { PageNameEnum } from '@components/PipelineDetailPage/constants';
import { onSuccess } from '@api/utils/response';

type DiffType = {
  added?: number;
  changed?: number;
  columns?: { [column: string]: number };
  complete: boolean;
  duplicate_keys?: { baseline: number; candidate: number };
  keys: string[];
  removed?: number;
  rows: { baseline: number; candidate: number };
  samples?: {
    added: { [key: string]: any }[];
    changed: { columns: string[]; key: { [key: string]: any } }[];
    removed: { [key: string]: any }[];
  };
  schema: {
    added: { name: string; type: string }[];
    changed: { from: string; name: string; to: string }[];
    removed: { name: string; type: string }[];
  };
  unchanged?: number;
};

type BlockPreviewType = {
  block_uuid: string;
  error?: string;
  outputs?: { [name: string]: { detail?: string; diff?: DiffType; result: string } };
  reason: string;
  status: 'run' | 'previewed' | 'failed' | 'not run' | 'unchanged';
};

export type ChangePreviewType = {
  baseline_run_id: number;
  blocks?: BlockPreviewType[];
  changed_files?: string[];
  created_at: string;
  error?: string;
  id: string;
  state: 'queued' | 'running' | 'done' | 'failed';
};

// The kinds of rows a comparison counts, in the bar's order.
const KINDS: { color: string; key: 'unchanged' | 'changed' | 'added' | 'removed' }[] = [
  { color: dark.content.muted, key: 'unchanged' },
  { color: dark.accent.warning, key: 'changed' },
  { color: dark.accent.positive, key: 'added' },
  { color: dark.accent.negative, key: 'removed' },
];

const BarStyle = styled.div`
  border-radius: ${UNIT / 2}px;
  display: flex;
  height: ${UNIT * 1.5}px;
  overflow: hidden;
  width: ${UNIT * 40}px;
`;

const SwatchStyle = styled.div<{ color: string }>`
  background: ${({ color }) => color};
  border-radius: 2px;
  height: ${UNIT}px;
  width: ${UNIT}px;
`;

function keyText(key: { [key: string]: any }): string {
  return Object.values(key).map(v => String(v)).join(', ');
}

// One output's comparison: its rows as a bar of unchanged, changed, added and removed.
function Comparison({ diff, name }: { diff: DiffType; name: string }) {
  const total = KINDS.reduce((sum, kind) => sum + (diff[kind.key] || 0), 0);
  const schema = [
    ...diff.schema.added.map(c => `+ ${c.name} (${c.type})`),
    ...diff.schema.removed.map(c => `- ${c.name} (${c.type})`),
    ...diff.schema.changed.map(c => `${c.name} ${c.from} to ${c.to}`),
  ];
  const changedColumns = Object.entries(diff.columns || {})
    .map(([column, count]) => `${column} ${count}`).join(', ');

  return (
    <Spacing mt={1}>
      <Text monospace small>{name}</Text>
      {schema.length > 0 && (
        <Text small>Schema: {schema.join(', ')}</Text>
      )}
      {diff.duplicate_keys && !diff.complete && (
        <Text small warning>
          {diff.keys.join(', ')} is not unique ({diff.duplicate_keys.baseline} repeated keys
          before, {diff.duplicate_keys.candidate} now); set preview_key to compare rows.
        </Text>
      )}
      {diff.complete && diff.keys.length > 0 && (
        <>
          <Spacing mt="4px">
            <BarStyle aria-label={`${name} rows`} role="img">
              {total > 0 && KINDS.map(kind => (diff[kind.key] || 0) > 0 && (
                <div
                  key={kind.key}
                  style={{
                    background: kind.color,
                    width: `${((diff[kind.key] || 0) / total) * 100}%`,
                  }}
                  title={`${diff[kind.key]} ${kind.key}`}
                />
              ))}
            </BarStyle>
          </Spacing>
          <FlexContainer alignItems="center">
            {KINDS.map(kind => (
              <Spacing key={kind.key} mr={2} mt="4px">
                <FlexContainer alignItems="center">
                  <SwatchStyle color={kind.color} />
                  <Spacing ml="4px">
                    <Text small>{diff[kind.key] || 0} {kind.key}</Text>
                  </Spacing>
                </FlexContainer>
              </Spacing>
            ))}
            <Text muted small>by {diff.keys.join(', ')}</Text>
          </FlexContainer>
          {changedColumns && <Text muted small>Changed values: {changedColumns}</Text>}
          {(['changed', 'added', 'removed'] as const).map(kind => {
            const samples = diff.samples?.[kind] || [];
            if (!samples.length) {
              return null;
            }
            return (
              <Text key={kind} monospace muted xsmall>
                {kind}: {samples.slice(0, 8).map(sample => kind === 'changed'
                  // @ts-ignore
                  ? `${keyText(sample.key)} (${sample.columns.join(', ')})`
                  : keyText(sample)).join('; ')}
                {samples.length > 8 ? '; ...' : ''}
              </Text>
            );
          })}
        </>
      )}
      {diff.complete && !diff.keys.length && (
        <Text small>
          {diff.added} rows only now, {diff.removed} only before (no key: compared as sets of
          rows; set preview_key to see changed rows).
        </Text>
      )}
      <Text muted xsmall>{diff.rows.baseline} rows before, {diff.rows.candidate} now</Text>
    </Spacing>
  );
}

// What the latest preview found, in one line.
function headline(preview: ChangePreviewType): string {
  if (preview.state === 'queued' || preview.state === 'running') {
    return `Previewing the current code against run ${preview.baseline_run_id}...`;
  }
  if (preview.state === 'failed') {
    return `The preview could not run: ${preview.error}`;
  }
  const blocks = preview.blocks || [];
  const failed = blocks.filter(b => b.status === 'failed');
  if (failed.length) {
    return `${failed.map(b => b.block_uuid).join(', ')} failed with the current code`;
  }
  const differs = blocks.filter(b => Object.values(b.outputs || {})
    .some(output => output.result !== 'same'));
  if (differs.length) {
    return `${differs.length} of ${blocks.length} blocks output different data than run `
      + `${preview.baseline_run_id}`;
  }
  if (blocks.every(b => b.status === 'unchanged')) {
    return `No block changed since run ${preview.baseline_run_id}`;
  }
  return `The changed blocks output the same data as run ${preview.baseline_run_id}`;
}

function PipelinePreviews({ pipeline }: { pipeline: { uuid: string } }) {
  const pipelineUUID = pipeline.uuid;
  const [errors, setErrors] = useState<ErrorsType>(null);
  const [selected, setSelected] = useState<string>(null);
  const { data: listed, mutate: refreshList } = api.change_previews.list(
    { pipeline_uuid: pipelineUUID },
  );
  const previews: ChangePreviewType[] = useMemo(() => listed?.change_previews || [], [listed]);
  const shownId = selected || previews[0]?.id;
  const { data, mutate } = api.change_previews.detail(
    shownId, { pipeline_uuid: pipelineUUID }, {
      refreshInterval: (latest: { change_preview?: ChangePreviewType }) => (
        ['queued', 'running'].includes(latest?.change_preview?.state) ? 2000 : 0
      ),
    },
  );
  const preview: ChangePreviewType = data?.change_preview;
  const running = preview && ['queued', 'running'].includes(preview.state);
  useEffect(() => {
    if (preview && !running) {
      refreshList();
    }
  }, [preview, refreshList, running]);

  const [start, { isLoading }] = useMutation(api.change_previews.useCreate(), {
    onSuccess: (response: any) => onSuccess(response, {
      callback: ({ change_preview: created }) => {
        setSelected(created.id);
        refreshList();
        mutate();
      },
      onErrorCallback: (response, errors) => setErrors({ errors, response }),
    }),
  });

  return (
    <PipelineDetailPage
      breadcrumbs={[{ label: () => 'Change previews' }]}
      errors={errors}
      pageName={PageNameEnum.PREVIEWS}
      pipeline={pipeline}
      setErrors={setErrors}
      title={({ name }) => `${name} change previews`}
      uuid={`${PageNameEnum.PREVIEWS}_${pipelineUUID}`}
    >
      <Spacing p={PADDING_UNITS}>
        <Headline level={4}>
          {preview ? headline(preview) : 'See what a change does to the data before it ships'}
        </Headline>
        <Spacing mt={1}>
          <Text default>
            The blocks that changed since the last completed run, and those after them, run
            again on that run&apos;s stored inputs; their outputs are compared with the
            run&apos;s. Exporters, sensors and SQL blocks never run, so nothing reaches a
            destination.
          </Text>
        </Spacing>
        <Spacing mt={2}>
          <Button
            disabled={running}
            loading={isLoading}
            onClick={() => start({ change_preview: { pipeline_uuid: pipelineUUID } } as any)}
            primary
          >
            Preview the current code
          </Button>
        </Spacing>

        {preview?.blocks?.length > 0 && (
          <Spacing mt={3}>
            {preview.blocks.map(block => (
              <Spacing key={block.block_uuid} mb={2}>
                <FlexContainer alignItems="center">
                  <Text bold monospace>{block.block_uuid}</Text>
                  <Spacing ml={1}>
                    <Text
                      danger={block.status === 'failed'}
                      muted={['unchanged', 'not run'].includes(block.status)}
                      small
                    >
                      {block.status}
                    </Text>
                  </Spacing>
                </FlexContainer>
                {(block.error || block.reason) && (
                  <Text danger={!!block.error} muted={!block.error} small>
                    {block.error || block.reason}
                  </Text>
                )}
                {Object.entries(block.outputs || {}).map(([name, output]) => output.diff
                  ? <Comparison diff={output.diff} key={name} name={name} />
                  : (
                    <Text key={name} small>
                      {name}: {output.result}{output.detail ? `, ${output.detail}` : ''}
                    </Text>
                  ))}
              </Spacing>
            ))}
          </Spacing>
        )}

        {previews.length > 1 && (
          <Spacing mt={3}>
            <Text bold>Earlier previews</Text>
            {previews.map(item => (
              <FlexContainer alignItems="center" key={item.id}>
                <Link
                  onClick={() => setSelected(item.id)}
                  preventDefault
                  small
                >
                  {item.created_at}
                </Link>
                <Spacing ml={1}>
                  <Text muted small>
                    against run {item.baseline_run_id}, {item.state}
                  </Text>
                </Spacing>
              </FlexContainer>
            ))}
          </Spacing>
        )}

        {preview && (
          <Spacing mt={2}>
            <NextLink
              as={`/pipelines/${pipelineUUID}/runs/${preview.baseline_run_id}`}
              href="/pipelines/[pipeline]/runs/[run]"
              legacyBehavior
              passHref
            >
              <Link small>Open run {preview.baseline_run_id}</Link>
            </NextLink>
          </Spacing>
        )}
      </Spacing>
    </PipelineDetailPage>
  );
}

PipelinePreviews.getInitialProps = async (ctx: any) => ({
  pipeline: { uuid: ctx.query.pipeline },
});

export default PrivateRoute(PipelinePreviews);
