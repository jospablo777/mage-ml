import { useEffect, useState } from 'react';
import { useMutation } from 'react-query';

import BlockType from '@interfaces/BlockType';
import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Spacing from '@oracle/elements/Spacing';
import StreamRun from '@components/StreamRun';
import Text from '@oracle/elements/Text';
import api from '@api';

type SyncType = {
  destination: string;
  kind: string;
  pipeline_uuid: string;
  source: string;
  tables: string[];
};

type SyncTableType = {
  blocked?: string;
  key: string[];
  rows: number;
  synced: boolean;
  table: string;
};

const NAMES = { clickhouse: 'ClickHouse', mysql: 'MySQL', postgres: 'PostgreSQL' };

// The syncs made on this page, and tables added to one: the next start copies them, then
// follows them with the others.
function AddTables() {
  const [syncs, setSyncs] = useState<SyncType[]>(null);
  const [open, setOpen] = useState<{ pipeline_uuid: string; tables: SyncTableType[] }>(null);
  const [chosen, setChosen] = useState<{ [table: string]: boolean }>({});
  const [added, setAdded] = useState<{ added: string[]; blocks: BlockType[];
    pipeline_uuid: string; review: string[] }>(null);
  const [error, setError] = useState<string>(null);

  const [call, { isLoading }] = useMutation(api.ingestion_setups.useCreate(), {
    onSuccess: (response: any) => {
      const found = response?.data?.ingestion_setup;
      const failure = response?.data?.error;
      if (failure) {
        setError(failure.message || 'The request failed.');
        return;
      }
      setError(null);
      if (found.action === 'syncs') {
        setSyncs(found.syncs);
      } else if (found.action === 'sync_tables') {
        setOpen(found);
        setChosen({});
        setAdded(null);
      } else if (found.action === 'add_tables') {
        setAdded(found);
        setOpen(null);
        setSyncs(syncs.map(s => s.pipeline_uuid === found.pipeline_uuid
          ? { ...s, tables: [...s.tables, ...found.added] } : s));
      }
    },
  });

  useEffect(() => {
    call({ ingestion_setup: { action: 'syncs' } } as any);
  }, [call]);

  if (!syncs?.length) {
    return null;
  }
  const picked = Object.keys(chosen).filter(table => chosen[table]);

  return (
    <Spacing mb={4}>
      <Headline level={5}>Syncs made here</Headline>
      <Text muted small>
        Add a table to a sync: its next start copies the table, then follows it with the
        others. The tables synced already are not copied again.
      </Text>
      {error && <Spacing mt={1}><Text danger small>{error}</Text></Spacing>}
      {syncs.map(sync => (
        <div aria-label={`Sync ${sync.pipeline_uuid}`} key={sync.pipeline_uuid} role="group">
          <Spacing mt={1}>
            <FlexContainer alignItems="center">
              <Text bold>{sync.pipeline_uuid}</Text>
              <Spacing ml={1} mr={1}>
                <Text muted small>
                  {NAMES[sync.kind] || sync.kind} {sync.source} into
                  {' '}{NAMES[sync.destination] || sync.destination}, {sync.tables.length}
                  {' '}{sync.tables.length === 1 ? 'table' : 'tables'}
                </Text>
              </Spacing>
              <Button
                compact
                disabled={isLoading}
                onClick={() => call({
                  ingestion_setup: { action: 'sync_tables', pipeline_uuid: sync.pipeline_uuid },
                } as any)}
              >
                Add tables
              </Button>
            </FlexContainer>
          </Spacing>

          {open?.pipeline_uuid === sync.pipeline_uuid && (
            <Spacing ml={2} mt={1}>
              {open.tables.map(table => (
                <div aria-label={table.table} key={table.table} role="group">
                  <FlexContainer alignItems="center">
                    <Checkbox
                      checked={table.synced || !!chosen[table.table]}
                      disabled={table.synced || !!table.blocked}
                      label={table.table}
                      onClick={() => !table.synced && !table.blocked && setChosen({
                        ...chosen,
                        [table.table]: !chosen[table.table],
                      })}
                    />
                    <Spacing ml={1}>
                      <Text muted small>
                        {table.synced ? 'synced' : table.blocked
                          || `key ${table.key.join(', ')}, about ${table.rows} rows`}
                      </Text>
                    </Spacing>
                  </FlexContainer>
                </div>
              ))}
              <Spacing mt={1}>
                <Button
                  disabled={!picked.length}
                  loading={isLoading}
                  onClick={() => call({
                    ingestion_setup: {
                      action: 'add_tables', pipeline_uuid: sync.pipeline_uuid, tables: picked,
                    },
                  } as any)}
                  primary
                >
                  {picked.length > 1 ? `Add ${picked.length} tables` : 'Add the table'}
                </Button>
              </Spacing>
            </Spacing>
          )}

          {added?.pipeline_uuid === sync.pipeline_uuid && (
            <Spacing ml={2} mt={1}>
              <ul aria-label="What adding does">
                {added.review.map(line => <li key={line}><Text small>{line}</Text></li>)}
              </ul>
              <Text small>
                Restart the sync for the copy to start: stop it, then start it.
              </Text>
              <StreamRun blocks={added.blocks} pipelineUUID={added.pipeline_uuid} />
            </Spacing>
          )}
        </div>
      ))}
    </Spacing>
  );
}

export default AddTables;
