import NextLink from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { useMutation } from 'react-query';
import { useRouter } from 'next/router';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import { ColumnAtlasOutput } from '@components/ColumnAtlas';
import {
  PARTITION_COLORS,
  PARTITION_STATES,
  PartitionStateType,
} from '@components/Partitions/states';
import { UNIT } from '@oracle/styles/units/spacing';

type FreshnessType = {
  age_seconds?: number;
  basis?: 'data' | 'published';
  max_age?: string;
  reason?: string;
  state: 'fresh' | 'stale' | 'unknown';
};

// The partitions of a partitioned pipeline's dataset: the gaps are ranges of consecutive
// partitions in one state, the latest listed; the strip is the latest partitions.
type CoverageType = {
  counts: { [state in PartitionStateType]: number };
  error?: string;
  gaps: { count: number; first: string; last: string; state: PartitionStateType }[];
  gaps_total: number;
  key: string;
  kind: string;
  strip: { partition: string; state: PartitionStateType }[];
  total: number;
};

export type DatasetType = {
  block: string;
  block_run_id?: number;
  checks: { failing: string[]; state: 'passing' | 'failing' | 'unknown' };
  columns?: { name: string; type: string }[];
  coverage?: CoverageType;
  data_through?: string;
  dependencies?: {
    downstream: { id: string; via: string }[];
    upstream: { id: string; via: string }[];
  };
  description?: string;
  freshness: FreshnessType;
  id: string;
  name: string;
  output: string;
  owner?: string;
  pipeline: string;
  pipeline_run_id?: number;
  published_at?: string;
  rows?: number;
  runs?: { at: string; block_run_id: number; pipeline_run_id: number; status: string }[];
  source?: string;
};

function ago(seconds: number): string {
  if (seconds < 3600) {
    return `${Math.round(seconds / 60)} min`;
  }
  if (seconds < 172800) {
    return `${Math.round(seconds / 3600)} h`;
  }
  return `${Math.round(seconds / 86400)} days`;
}

// Freshness in words: what it is measured on, and why it is unknown when it is.
export function freshnessText(freshness: FreshnessType): string {
  if (freshness.state === 'unknown') {
    return `Freshness unknown: ${freshness.reason}`;
  }
  const what = freshness.basis === 'data' ? 'Data' : 'Published';
  return `${freshness.state === 'fresh' ? 'Fresh' : 'Stale'}: ${what.toLowerCase()} `
    + `${ago(freshness.age_seconds)} old, ${freshness.max_age} allowed`;
}

function checksText(checks: DatasetType['checks']): string {
  if (checks.state === 'failing') {
    return `Checks failing: ${checks.failing.join('; ')}`;
  }
  return checks.state === 'passing' ? 'Checks passing' : 'Checks unknown: no completed run';
}

// The point first: how many partitions are done, and the other states by count.
export function coverageText(coverage: CoverageType): string {
  const others = PARTITION_STATES
    .filter(state => state !== 'done' && coverage.counts[state])
    .map(state => `${coverage.counts[state].toLocaleString()} ${state}`);
  return `${coverage.counts.done.toLocaleString()} of ${coverage.total.toLocaleString()} `
    + `partitions done${others.length ? `; ${others.join(', ')}` : ''}`;
}

function Coverage({ coverage, pipeline }: { coverage: CoverageType; pipeline: string }) {
  if (coverage.error) {
    return <Text danger small>Partitions: {coverage.error}</Text>;
  }
  return (
    <div aria-label="Partition coverage" role="group">
      <FlexContainer alignItems="center">
        <Text bold small>{coverageText(coverage)}</Text>
        <Spacing ml={1}>
          <NextLink
            as={`/pipelines/${pipeline}/partitions`}
            href="/pipelines/[pipeline]/partitions"
            legacyBehavior
            passHref
          >
            <Link small>Partitions</Link>
          </NextLink>
        </Spacing>
      </FlexContainer>
      <Spacing mt={1}>
        <FlexContainer flexWrap="wrap">
          {coverage.strip.map(cell => (
            <div
              key={cell.partition}
              style={{
                backgroundColor: PARTITION_COLORS[cell.state],
                borderRadius: 2,
                height: UNIT,
                marginBottom: 2,
                marginRight: 2,
                width: UNIT,
              }}
              title={`${coverage.key} ${cell.partition}: ${cell.state}`}
            />
          ))}
        </FlexContainer>
        {coverage.strip.length < coverage.total && (
          <Text muted xsmall>The latest {coverage.strip.length} partitions</Text>
        )}
      </Spacing>
      {coverage.gaps.map(gap => (
        <Text key={`${gap.state}-${gap.first}`} small>
          <span style={{ color: PARTITION_COLORS[gap.state] }}>■</span>
          {' '}
          {gap.state[0].toUpperCase() + gap.state.slice(1)}:{' '}
          {gap.count === 1 ? gap.first : `${gap.first} to ${gap.last} (${gap.count})`}
        </Text>
      ))}
      {coverage.gaps_total > coverage.gaps.length && (
        <Text muted small>
          The latest {coverage.gaps.length} of {coverage.gaps_total} gaps.
        </Text>
      )}
    </div>
  );
}

function when(text?: string): string {
  return text ? new Date(text).toLocaleString() : 'never';
}

const REGISTER_FIELDS: { key: string; label: string; placeholder: string }[] = [
  { key: 'id', label: 'Id', placeholder: 'daily_sales' },
  { key: 'pipeline', label: 'Pipeline', placeholder: 'sales_daily' },
  { key: 'block', label: 'Block', placeholder: 'aggregate_sales' },
  { key: 'owner', label: 'Owner', placeholder: 'ana@example.com' },
  { key: 'time_column', label: 'Time column', placeholder: 'day' },
  { key: 'max_age', label: 'Max age', placeholder: '1d' },
];

function Register({ draft, onDone }: { draft?: Partial<DatasetType>; onDone: (id: string) => void }) {
  const [entry, setEntry] = useState<{ [key: string]: string }>({});
  const [error, setError] = useState<string>(null);
  // Suggestions as you type: the project's pipelines, then the chosen pipeline's blocks.
  const { data: listed } = api.pipelines.list({ _limit: 1000 }, { revalidateOnFocus: false });
  const pipelineUUIDs: string[] = useMemo(
    () => (listed?.pipelines || []).map(pipeline => pipeline.uuid).sort(),
    [listed],
  );
  const { data: chosen } = api.pipelines.detail(
    pipelineUUIDs.includes(entry.pipeline) ? entry.pipeline : null,
  );
  const options: { [key: string]: string[] } = useMemo(() => ({
    block: (chosen?.pipeline?.blocks || []).map(block => block.uuid),
    pipeline: pipelineUUIDs,
  }), [chosen, pipelineUUIDs]);
  useEffect(() => {
    if (draft) {
      setEntry({ block: draft.block, id: draft.id, pipeline: draft.pipeline });
    }
  }, [draft]);
  const [register, { isLoading }] = useMutation(api.datasets.useCreate(), {
    onSuccess: (response: any) => {
      const failure = response?.data?.error;
      if (failure) {
        setError(failure.message);
        return;
      }
      setError(null);
      setEntry({});
      onDone(response.data.dataset.id);
    },
  });
  return (
    <div aria-label="Register a dataset" role="form">
      <FlexContainer flexWrap="wrap">
        {REGISTER_FIELDS.map(field => (
          <Spacing key={field.key} mb={1} mr={1} style={{ width: UNIT * 22 }}>
            <Text muted small>{field.label}</Text>
            <TextInput
              aria-label={`Dataset ${field.label.toLowerCase()}`}
              list={options[field.key] ? `dataset-${field.key}-options` : undefined}
              onChange={e => setEntry({ ...entry, [field.key]: e.target.value })}
              placeholder={field.placeholder}
              value={entry[field.key] || ''}
            />
            {options[field.key] && (
              <datalist id={`dataset-${field.key}-options`}>
                {options[field.key].map(value => <option key={value} value={value} />)}
              </datalist>
            )}
          </Spacing>
        ))}
      </FlexContainer>
      <Button
        loading={isLoading}
        onClick={() => register({
          dataset: Object.fromEntries(Object.entries(entry).filter(([, v]) => v)),
        } as any)}
      >
        Register the dataset
      </Button>
      {error && <Text danger small>{error}</Text>}
    </div>
  );
}

function Detail({ id, select }: { id: string; select: (id: string) => void }) {
  const { data } = api.datasets.detail(id);
  const dataset: DatasetType = data?.dataset;
  if (data?.error) {
    return <Text danger>{data.error.message}</Text>;
  }
  if (!dataset) {
    return <Text muted>Loading {id}...</Text>;
  }
  const dependencies = dataset.dependencies || { downstream: [], upstream: [] };

  return (
    <div aria-label={`Dataset ${dataset.id}`} role="region">
      <Headline level={4}>{dataset.name}</Headline>
      {dataset.description && <Text default>{dataset.description}</Text>}
      <Spacing mt={1}>
        <Text small>
          {dataset.owner ? `Owner ${dataset.owner}. ` : 'No owner. '}
          Made by block {dataset.block} of{' '}
          <NextLink
            as={`/pipelines/${dataset.pipeline}/edit`}
            href="/pipelines/[pipeline]/edit"
            legacyBehavior
            passHref
          >
            <Link small>{dataset.pipeline}</Link>
          </NextLink>
          , output {dataset.output}.
        </Text>
        <Text small>
          Published {when(dataset.published_at)}
          {dataset.pipeline_run_id ? ` by run ${dataset.pipeline_run_id}` : ''}
          {dataset.data_through ? `; data through ${when(dataset.data_through)}` : ''}
          {typeof dataset.rows === 'number' ? `; ${dataset.rows} rows` : ''}.
        </Text>
        <Text danger={dataset.freshness.state === 'stale'} small>
          {freshnessText(dataset.freshness)}
        </Text>
        <Text danger={dataset.checks.state === 'failing'} small>
          {checksText(dataset.checks)}
          {dataset.checks.state === 'failing' && (
            <>
              {' '}
              <NextLink href="/incidents" legacyBehavior passHref>
                <Link small>Open incidents</Link>
              </NextLink>
            </>
          )}
        </Text>
      </Spacing>

      {dataset.coverage && (
        <Spacing mt={2}>
          <Coverage coverage={dataset.coverage} pipeline={dataset.pipeline} />
        </Spacing>
      )}

      {(['upstream', 'downstream'] as const).map(direction => (
        <Spacing key={direction} mt={2}>
          <Text bold small>{direction === 'upstream' ? 'Reads' : 'Read by'}</Text>
          {dependencies[direction].length
            ? dependencies[direction].map(dep => (
              <FlexContainer key={dep.id}>
                <Link onClick={() => select(dep.id)} preventDefault small>{dep.id}</Link>
                <Spacing ml={1}><Text muted small>through {dep.via}</Text></Spacing>
              </FlexContainer>
            ))
            : <Text muted small>No registered dataset</Text>}
        </Spacing>
      ))}

      {dataset.columns?.length > 0 && (
        <Spacing mt={2}>
          <Text bold small>Columns</Text>
          <table aria-label={`Columns of ${dataset.id}`}>
            <tbody>
              {dataset.columns.map(column => (
                <tr key={column.name}>
                  <td><Text monospace small>{column.name}</Text></td>
                  <td><Spacing pl={2}><Text monospace muted small>{column.type}</Text></Spacing></td>
                </tr>
              ))}
            </tbody>
          </table>
        </Spacing>
      )}

      {dataset.runs?.length > 0 && (
        <Spacing mt={2}>
          <Text bold small>Recent runs of its block</Text>
          {dataset.runs.slice(0, 5).map(run => (
            <FlexContainer key={run.block_run_id}>
              <NextLink
                as={`/pipelines/${dataset.pipeline}/runs/${run.pipeline_run_id}`}
                href="/pipelines/[pipeline]/runs/[run]"
                legacyBehavior
                passHref
              >
                <Link small>Run {run.pipeline_run_id}</Link>
              </NextLink>
              <Spacing ml={1}>
                <Text danger={run.status === 'failed'} muted={run.status !== 'failed'} small>
                  {run.status}, {when(run.at)}
                </Text>
              </Spacing>
            </FlexContainer>
          ))}
        </Spacing>
      )}

      {dataset.block_run_id && (
        <Spacing mt={2}>
          <Text bold small>The published output</Text>
          <ColumnAtlasOutput
            fallback={<Text muted small>The output cannot be explored here.</Text>}
            source={{
              block_run_id: dataset.block_run_id,
              block_uuid: dataset.block,
              pipeline_uuid: dataset.pipeline,
              variable_uuid: dataset.output,
            }}
          />
        </Spacing>
      )}
    </div>
  );
}

// Datasets (ADR-0046): block outputs a project names, found by name, with their freshness,
// checks, owner, dependencies and output.
function Datasets() {
  const router = useRouter();
  const selected = typeof router.query.id === 'string' ? router.query.id : null;
  const [typed, setTyped] = useState('');
  const [query, setQuery] = useState('');
  useEffect(() => {
    const timer = setTimeout(() => setQuery(typed), 150);
    return () => clearTimeout(timer);
  }, [typed]);
  const { data, mutate } = api.datasets.list({ query });
  const found: DatasetType[] = useMemo(() => data?.datasets || [], [data]);
  const { data: drafted } = api.datasets.list({ drafts: 1 });
  const drafts: DatasetType[] = drafted?.datasets || [];
  const [draft, setDraft] = useState<Partial<DatasetType>>(null);

  const select = (id: string) => router.push(
    { pathname: '/datasets', query: id ? { id } : {} }, undefined, { shallow: true },
  );

  return (
    <FlexContainer>
      <div style={{ flex: 1, maxWidth: UNIT * 70, minWidth: UNIT * 50 }}>
        <TextInput
          aria-label="Search datasets"
          onChange={e => setTyped(e.target.value)}
          placeholder="Search by name, owner or pipeline"
          value={typed}
        />
        <Spacing mt={2}>
          {data && !found.length && (
            <Text muted>
              {query ? `No dataset matches ${query}.` : 'No datasets yet: register a block output below.'}
            </Text>
          )}
          {found.map(dataset => (
            <Spacing key={dataset.id} mb={2}>
              <Link onClick={() => select(dataset.id)} preventDefault>{dataset.name}</Link>
              <Text muted small>
                {dataset.owner || 'No owner'}, {dataset.pipeline}
                {typeof dataset.rows === 'number' ? `, ${dataset.rows} rows` : ''}
              </Text>
              <Text danger={dataset.freshness.state === 'stale'} small>
                {freshnessText(dataset.freshness)}
              </Text>
              <Text danger={dataset.checks.state === 'failing'} small>
                {checksText(dataset.checks)}
              </Text>
            </Spacing>
          ))}
        </Spacing>
        <Spacing mt={3}>
          <Headline level={5}>Register a block output</Headline>
          {drafts.length > 0 && (
            <Spacing mb={1}>
              <Text muted small>From global data products:</Text>
              {drafts.map(item => (
                <Link key={item.id} onClick={() => setDraft(item)} preventDefault small>
                  {item.name}
                </Link>
              ))}
            </Spacing>
          )}
          <Register draft={draft} onDone={(id) => { mutate(); select(id); }} />
        </Spacing>
      </div>
      <Spacing ml={4} style={{ flex: 2 }}>
        {selected
          ? <Detail id={selected} select={select} />
          : <Text muted>Choose a dataset to see its freshness, checks, dependencies and output.</Text>}
      </Spacing>
    </FlexContainer>
  );
}

export default Datasets;
