import { useMemo, useState } from 'react';
import NextLink from 'next/link';
import styled from 'styled-components';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import ErrorsType from '@interfaces/ErrorsType';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import PipelineDetailPage from '@components/PipelineDetailPage';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';
import { PageNameEnum } from '@components/PipelineDetailPage/constants';
import { onSuccess } from '@api/utils/response';

type PartitionType = {
  completed_at?: string;
  partition: string;
  run_id?: number;
  stale_because: { kind: 'code' | 'upstream'; pipeline?: string }[];
  state: 'missing' | 'queued' | 'running' | 'failed' | 'stale' | 'done';
};

type PartitionsType = {
  counts: { [state: string]: number };
  key: string;
  kind: string;
  partitions: PartitionType[];
  // on_close, on_upstream, or none: run when asked (partition_schedule.py).
  schedule?: string;
  total: number;
  upstream?: string[];
};

// How the scheduler runs the partitions, in one sentence.
function scheduleText(found: PartitionsType): string {
  const after = (found.upstream || []).join(', ');
  if (found.schedule === 'on_close') {
    return 'Each partition runs when it closes.';
  }
  if (found.schedule === 'on_upstream') {
    return `Each partition runs once ${after}'s same partition is done.`;
  }
  return 'Partitions run when you ask.';
}

// Problems first, as the headline reads them.
const STATES: PartitionType['state'][] = ['failed', 'stale', 'missing', 'running', 'queued', 'done'];
const COLORS = {
  done: dark.background.success,
  failed: dark.accent.negative,
  missing: dark.accent.contentDefaultTransparent,
  queued: dark.accent.infoTransparent,
  running: dark.accent.info,
  stale: dark.accent.warning,
};
const CELL = 18;

const CellStyle = styled.div<{ state: PartitionType['state'] }>`
  background: ${({ state }) => COLORS[state]};
  border-radius: 3px;
  height: ${CELL}px;
  width: ${CELL}px;
`;

function why(partition: PartitionType): string {
  return partition.stale_because.map(reason => reason.kind === 'code'
    ? 'code changed'
    : `${reason.pipeline} ran again`).join('; ');
}

const WEEKDAYS = ['M', 'T', 'W', 'T', 'F', 'S', 'S'];

// Daily partitions as a calendar: a row per week from Monday, so columns are weekdays (the
// first week starts with blanks). Hourly ones in rows of a day (UTC); others in rows of 24.
function rows(found: PartitionsType): PartitionType[][] {
  const result: PartitionType[][] = [];
  found.partitions.forEach((partition, index) => {
    let startsRow = index % 24 === 0;
    if (found.kind === 'daily') {
      startsRow = new Date(`${partition.partition}T00:00:00Z`).getUTCDay() === 1;
    } else if (found.kind === 'hourly') {
      startsRow = partition.partition.endsWith('T00');
    }
    if (!result.length || startsRow) {
      result.push([]);
      if (found.kind === 'daily' && !result[0].length && !startsRow) {
        const weekday = (new Date(`${partition.partition}T00:00:00Z`).getUTCDay() + 6) % 7;
        for (let blank = 0; blank < weekday; blank += 1) {
          result[0].push(null);
        }
      }
    }
    result[result.length - 1].push(partition);
  });
  return result;
}

function PipelinePartitions({ pipeline }: { pipeline: { uuid: string } }) {
  const pipelineUUID = pipeline.uuid;
  const [errors, setErrors] = useState<ErrorsType>(null);
  const { data, mutate } = api.pipeline_partitions.detail(pipelineUUID, {}, {
    refreshInterval: (latest: { pipeline_partition?: PartitionsType }) => {
      const counts = latest?.pipeline_partition?.counts || {};
      return (counts.queued || counts.running) ? 5000 : 0;
    },
  });
  const found: PartitionsType = data?.pipeline_partition;
  const declared = !data?.error;

  const [act, { isLoading }] = useMutation(api.pipeline_partitions.useCreate(), {
    onSuccess: (response: any) => onSuccess(response, {
      callback: () => mutate(),
      onErrorCallback: (response, errors) => setErrors({ errors, response }),
    }),
  });

  const headline = useMemo(() => {
    if (!found) {
      return null;
    }
    const parts = STATES
      .filter(state => state !== 'done' && found.counts[state])
      .map(state => `${found.counts[state]} ${state}`);
    return parts.length
      ? `${parts.join(', ')} of ${found.total} partitions`
      : `All ${found.total} partitions done`;
  }, [found]);

  const actions = [
    ['run', 'missing', 'Run missing'],
    ['run', 'stale', 'Run stale'],
    ['run', 'failed', 'Run failed'],
    ['accept_code', 'stale', 'Accept current code'],
  ];

  return (
    <PipelineDetailPage
      breadcrumbs={[{ label: () => 'Partitions' }]}
      errors={errors}
      pageName={PageNameEnum.PARTITIONS}
      pipeline={pipeline}
      setErrors={setErrors}
      title={({ name }) => `${name} partitions`}
      uuid={`${PageNameEnum.PARTITIONS}_${pipelineUUID}`}
    >
      <Spacing p={PADDING_UNITS}>
        {!declared && (
          <>
            <Headline level={5}>This pipeline declares no partitions</Headline>
            <Spacing mt={1}>
              <Text default>
                Declare them in its metadata.yaml to run and track it one day, hour or region
                at a time, and rerun only the partitions whose code or upstream data changed:
              </Text>
              <Spacing mt={1}>
                <Text monospace small>{'partitions:'}</Text>
                <Text monospace small>{'  key: day'}</Text>
                <Text monospace small>{'  daily: {start: 2026-01-01, close_after: 2h}'}</Text>
              </Spacing>
            </Spacing>
          </>
        )}

        {found && (
          <>
            <Headline level={4}>{headline}</Headline>
            <Spacing mt={1}>
              <Text muted small>
                {found.kind === 'values' ? 'Values' : `${found.kind} partitions`} of
                {' '}{found.key}. {scheduleText(found)} Stale: the code changed since the
                partition ran, or an upstream pipeline&apos;s same partition ran after it.
              </Text>
            </Spacing>

            <Spacing mt={2}>
              <FlexContainer alignItems="center">
                {actions.map(([action, select, label]) => (
                  <Spacing key={label} mr={1}>
                    <Button
                      disabled={!found.counts[select]}
                      loading={isLoading}
                      onClick={() => act({
                        pipeline_partition: {
                          action,
                          pipeline_uuid: pipelineUUID,
                          select,
                        },
                      } as any)}
                      small
                    >
                      {label} ({found.counts[select] || 0})
                    </Button>
                  </Spacing>
                ))}
              </FlexContainer>
            </Spacing>

            <Spacing mt={2}>
              <FlexContainer alignItems="center">
                {STATES.map(state => (
                  <Spacing key={state} mr={2}>
                    <FlexContainer alignItems="center">
                      <CellStyle state={state} />
                      <Spacing ml={1}>
                        <Text small>{state} {found.counts[state] || 0}</Text>
                      </Spacing>
                    </FlexContainer>
                  </Spacing>
                ))}
              </FlexContainer>
            </Spacing>

            <Spacing mt={2}>
              {found.kind === 'daily' && (
                <FlexContainer>
                  <Spacing mr={1} style={{ width: 13 * 8 }} />
                  {WEEKDAYS.map((day, index) => (
                    <Spacing key={index} mb="2px" mr="2px" style={{ width: CELL }}>
                      <Text center monospace muted xsmall>{day}</Text>
                    </Spacing>
                  ))}
                </FlexContainer>
              )}
              {rows(found).map(row => (
                <FlexContainer key={row.find(Boolean).partition}>
                  <Spacing mr={1} style={{ width: 13 * 8 }}>
                    <Text monospace muted xsmall>{row.find(Boolean).partition}</Text>
                  </Spacing>
                  {row.map((partition, index) => {
                    if (!partition) {
                      return <Spacing key={`blank-${index}`} mr="2px" style={{ width: CELL }} />;
                    }
                    const title = `${partition.partition}: ${partition.state}`
                      + (why(partition) ? ` (${why(partition)})` : '');
                    const cell = <CellStyle state={partition.state} title={title} />;
                    return (
                      <Spacing key={partition.partition} mb="2px" mr="2px">
                        {partition.run_id ? (
                          <NextLink as={`/pipelines/${pipelineUUID}/runs/${partition.run_id}`}
                            href="/pipelines/[pipeline]/runs/[run]"
                            legacyBehavior
                            passHref
                          >
                            <a aria-label={title}>{cell}</a>
                          </NextLink>
                        ) : cell}
                      </Spacing>
                    );
                  })}
                </FlexContainer>
              ))}
            </Spacing>
          </>
        )}
      </Spacing>
    </PipelineDetailPage>
  );
}

PipelinePartitions.getInitialProps = async (ctx: any) => ({
  pipeline: { uuid: ctx.query.pipeline },
});

export default PrivateRoute(PipelinePartitions);
