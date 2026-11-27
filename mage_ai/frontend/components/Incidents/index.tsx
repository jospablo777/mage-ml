import NextLink from 'next/link';
import { useState } from 'react';

import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import { MonoStyle, TableStyle } from '@components/Workers/shared';

type IncidentType = {
  blocks: { block_uuid: string; error_line?: string }[];
  failed_runs: number;
  latest_at?: string;
  latest_run_id: number;
  partition?: string;
  pipeline_uuid: string;
  recovered_by?: number;
};

const DAYS = [1, 7, 30];

function when(at?: string): string {
  if (!at) {
    return '';
  }
  const date = new Date(at.endsWith('Z') || at.includes('+') ? at : `${at}Z`);
  return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

// The pipelines with a failed run lately, still failing first (orchestration/incidents.py):
// what failed in the latest failure, and the run that recovered it, if any. Each row opens
// the failed run, whose page opens on what failed.
function Incidents() {
  const [days, setDays] = useState(7);
  const { data } = api.incidents.list({ days }, { refreshInterval: 30_000 });
  const incidents: IncidentType[] = data?.incidents;
  if (!incidents) {
    return <Spinner />;
  }
  const failing = incidents.filter(i => !i.recovered_by);
  const period = days === 1 ? 'the last day' : `the last ${days} days`;

  return (
    <>
      <FlexContainer alignItems="center" justifyContent="space-between">
        <Text bold xlarge>
          {failing.length
            ? `${failing.length} pipeline${failing.length === 1 ? '' : 's'} still failing`
            : incidents.length ? 'Every failing pipeline recovered' : `No failed runs in ${period}`}
        </Text>
        <Select
          aria-label="Period"
          compact
          onChange={e => setDays(Number(e.target.value))}
          value={days}
        >
          {DAYS.map(d => (
            <option key={d} value={d}>{d === 1 ? 'Last day' : `Last ${d} days`}</option>
          ))}
        </Select>
      </FlexContainer>
      <Text muted small>
        Pipelines with a failed run in {period}: the latest failure, what failed in it, and
        whether a later run completed.
      </Text>
      {incidents.length > 0 && (
        <Spacing mt={2}>
          <TableStyle aria-label="Incidents">
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>State</th>
                <th>Failed runs</th>
                <th>Latest failure</th>
                <th>What failed</th>
              </tr>
            </thead>
            <tbody>
              {incidents.map(incident => (
                <tr key={incident.pipeline_uuid}>
                  <td>
                    <NextLink
                      as={`/pipelines/${incident.pipeline_uuid}/runs`}
                      href="/pipelines/[pipeline]/runs"
                      passHref
                    >
                      <Link small>{incident.pipeline_uuid}</Link>
                    </NextLink>
                  </td>
                  <td>
                    {incident.recovered_by
                      ? <Text muted small>Recovered in run {incident.recovered_by}</Text>
                      : <Text danger small>Still failing</Text>}
                  </td>
                  <td><Text small>{incident.failed_runs}</Text></td>
                  <td>
                    <NextLink
                      as={`/pipelines/${incident.pipeline_uuid}/runs/${incident.latest_run_id}`}
                      href="/pipelines/[pipeline]/runs/[run]"
                      passHref
                    >
                      <Link small>
                        Run {incident.latest_run_id}
                        {incident.partition ? ` (${incident.partition})` : ''}
                      </Link>
                    </NextLink>
                    <Text muted xsmall>{when(incident.latest_at)}</Text>
                  </td>
                  <td>
                    {incident.blocks.length ? incident.blocks.map(block => (
                      <div key={block.block_uuid}>
                        <MonoStyle>{block.block_uuid}</MonoStyle>
                        {block.error_line && <Text muted small>{block.error_line}</Text>}
                      </div>
                    )) : <Text muted small>No block failed: cancelled or timed out</Text>}
                  </td>
                </tr>
              ))}
            </tbody>
          </TableStyle>
        </Spacing>
      )}
    </>
  );
}

export default Incidents;
