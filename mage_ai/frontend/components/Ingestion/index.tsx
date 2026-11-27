import NextLink from 'next/link';
import { useMutation } from 'react-query';
import { useState } from 'react';

import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Link from '@oracle/elements/Link';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import { UNIT } from '@oracle/styles/units/spacing';

type ConnectionType = {
  database: string;
  host: string;
  password: string;
  port: string;
  sslmode: string;
  user: string;
};

type CheckType = { detail: string; name: string; ok: boolean };

type TableType = {
  key: string[];
  owned: boolean;
  replica_identity: string;
  rows: number;
  table: string;
};

const EMPTY: ConnectionType = {
  database: '',
  host: '',
  password: '',
  port: '5432',
  sslmode: 'prefer',
  user: '',
};

const FIELDS: { key: keyof ConnectionType; label: string; secret?: boolean }[] = [
  { key: 'host', label: 'Host' },
  { key: 'port', label: 'Port' },
  { key: 'database', label: 'Database' },
  { key: 'user', label: 'User' },
  { key: 'password', label: 'Password', secret: true },
];

function ConnectionForm({
  connection,
  name,
  setConnection,
}: {
  connection: ConnectionType;
  name: string;
  setConnection: (connection: ConnectionType) => void;
}) {
  return (
    <FlexContainer flexWrap="wrap">
      {FIELDS.map(field => (
        <Spacing key={field.key} mb={1} mr={1} style={{ width: UNIT * 24 }}>
          <Text muted small>{field.label}</Text>
          <TextInput
            aria-label={`${name} ${field.label.toLowerCase()}`}
            onChange={e => setConnection({ ...connection, [field.key]: e.target.value })}
            type={field.secret ? 'password' : 'text'}
            value={connection[field.key]}
          />
        </Spacing>
      ))}
      <Spacing mb={1} mr={1} style={{ width: UNIT * 24 }}>
        <Text muted small>TLS</Text>
        <Select
          aria-label={`${name} TLS`}
          onChange={e => setConnection({ ...connection, sslmode: e.target.value })}
          value={connection.sslmode}
        >
          {['prefer', 'require', 'verify-full', 'disable'].map(mode => (
            <option key={mode} value={mode}>{mode}</option>
          ))}
        </Select>
      </Spacing>
    </FlexContainer>
  );
}

// Each check with its result in words, not only a color.
function Checks({ checks }: { checks: CheckType[] }) {
  return (
    <Spacing mt={1}>
      {checks.map(check => (
        <Text danger={!check.ok} key={check.name} small>
          {check.ok ? 'Ready' : 'Not ready'}: {check.name}. {check.detail}
        </Text>
      ))}
    </Spacing>
  );
}

function Step({ children, title }: { children: any; title: string }) {
  return (
    <Spacing mb={4}>
      <Headline level={5}>{title}</Headline>
      <Spacing mt={1}>{children}</Spacing>
    </Spacing>
  );
}

// Guided ingestion (ADR-0045): a PostgreSQL database's tables copied into another and kept
// in step, set up after checks and a review, as a streaming pipeline the project keeps.
function Ingestion() {
  const [source, setSource] = useState<ConnectionType>(EMPTY);
  const [destination, setDestination] = useState<ConnectionType>(EMPTY);
  const [sameServer, setSameServer] = useState(true);
  const [sourceFound, setSourceFound] = useState<{ checks: CheckType[]; ready: boolean;
    tables: TableType[] }>(null);
  const [destinationFound, setDestinationFound] = useState<{ checks: CheckType[];
    ready: boolean }>(null);
  const [chosen, setChosen] = useState<{ [table: string]: boolean }>({});
  const [sample, setSample] = useState<{ columns: string[]; rows: string[][]; table: string }>(
    null,
  );
  const [schema, setSchema] = useState('public');
  const [prefix, setPrefix] = useState('copy_');
  const [deletes, setDeletes] = useState('delete');
  const [name, setName] = useState('');
  const [review, setReview] = useState<string[]>(null);
  const [created, setCreated] = useState<string>(null);
  const [error, setError] = useState<string>(null);

  const target = sameServer ? source : destination;
  const tables = (sourceFound?.tables || []).filter(t => chosen[t.table]);

  const [call, { isLoading }] = useMutation(api.ingestion_setups.useCreate(), {
    onSuccess: (response: any) => {
      const found = response?.data?.ingestion_setup;
      const failure = response?.data?.error;
      if (failure) {
        setError(failure.message || 'The request failed.');
        return;
      }
      setError(null);
      if (found.action === 'check_source') {
        setSourceFound(found);
      } else if (found.action === 'check_destination') {
        setDestinationFound(found);
      } else if (found.action === 'sample') {
        setSample(found);
      } else if (found.action === 'review') {
        setReview(found.review);
      } else if (found.action === 'create') {
        setReview(found.review);
        setCreated(found.pipeline_uuid);
      }
    },
  });

  const choices = { deletes, prefix, schema, tables };

  return (
    <div style={{ maxWidth: UNIT * 140 }}>
      <Headline level={4}>Copy a PostgreSQL database and keep it in step</Headline>
      <Spacing mb={3} mt={1}>
        <Text default>
          The tables you choose are copied first, then every insert, update and delete
          follows within seconds, by logical replication. The result is a streaming pipeline
          you can read, run from its Run tab and export as a service.
        </Text>
      </Spacing>

      {error && (
        <Spacing mb={2}><Text danger>{error}</Text></Spacing>
      )}

      <Step title="1. Source">
        <ConnectionForm connection={source} name="Source" setConnection={setSource} />
        <Button
          loading={isLoading}
          onClick={() => call({ ingestion_setup: { action: 'check_source', source } } as any)}
        >
          Check the source
        </Button>
        {sourceFound && <Checks checks={sourceFound.checks} />}
      </Step>

      {sourceFound && (
        <Step title="2. Tables">
          <Text muted small>
            A table copies by its primary key: rows upsert and delete by it. Tables without one
            cannot be chosen.
          </Text>
          {sourceFound.tables.map(table => (
            <div aria-label={table.table} key={table.table} role="group">
              <FlexContainer alignItems="center">
                <Checkbox
                checked={!!chosen[table.table]}
                disabled={!table.key.length}
                label={table.table}
                onClick={() => table.key.length && setChosen({
                  ...chosen,
                  [table.table]: !chosen[table.table],
                })}
              />
                <Spacing ml={1}>
                  <Text muted small>
                    {table.key.length
                    ? `key ${table.key.join(', ')}, about ${table.rows} rows`
                    : 'no primary key'}
                  </Text>
                </Spacing>
                <Spacing ml={1}>
                  <Link
                  onClick={() => call({
                    ingestion_setup: { action: 'sample', source, table: table.table },
                  } as any)}
                  preventDefault
                  small
                >
                    Sample
                  </Link>
                </Spacing>
              </FlexContainer>
            </div>
          ))}
          {sample && (
            <Spacing mt={1}>
              <Text small>First rows of {sample.table}</Text>
              <table aria-label={`Sample of ${sample.table}`}>
                <thead>
                  <tr>{sample.columns.map(c => <th key={c}><Text small>{c}</Text></th>)}</tr>
                </thead>
                <tbody>
                  {sample.rows.map((row, index) => (
                     
                    <tr key={index}>
                      {row.map((value, column) => (
                         
                        <td key={column}>
                          <Text monospace muted={value === null} small>
                            {value === null ? 'null' : value}
                          </Text>
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </Spacing>
          )}
        </Step>
      )}

      {tables.length > 0 && (
        <Step title="3. Destination">
          <Checkbox
            checked={sameServer}
            label="The same server as the source"
            onClick={() => setSameServer(!sameServer)}
          />
          {!sameServer && (
            <Spacing mt={1}>
              <ConnectionForm
                connection={destination}
                name="Destination"
                setConnection={setDestination}
              />
            </Spacing>
          )}
          <FlexContainer flexWrap="wrap">
            <Spacing mr={1} mt={1} style={{ width: UNIT * 24 }}>
              <Text muted small>Schema</Text>
              <TextInput
                aria-label="Destination schema"
                onChange={e => setSchema(e.target.value)}
                value={schema}
              />
            </Spacing>
            <Spacing mr={1} mt={1} style={{ width: UNIT * 24 }}>
              <Text muted small>Table names start with</Text>
              <TextInput
                aria-label="Table prefix"
                onChange={e => setPrefix(e.target.value)}
                value={prefix}
              />
            </Spacing>
            <Spacing mr={1} mt={1} style={{ width: UNIT * 32 }}>
              <Text muted small>A row deleted in the source</Text>
              <Select
                aria-label="Deletes"
                onChange={e => setDeletes(e.target.value)}
                value={deletes}
              >
                <option value="delete">is deleted in the copy</option>
                <option value="soft">stays, marked _mage_deleted_at</option>
              </Select>
            </Spacing>
          </FlexContainer>
          <Spacing mt={1}>
            <Button
              loading={isLoading}
              onClick={() => call({
                ingestion_setup: { action: 'check_destination', destination: target, schema },
              } as any)}
            >
              Check the destination
            </Button>
          </Spacing>
          {destinationFound && <Checks checks={destinationFound.checks} />}
        </Step>
      )}

      {destinationFound?.ready && (
        <Step title="4. Review">
          <Button
            loading={isLoading}
            onClick={() => call({ ingestion_setup: { action: 'review', ...choices } } as any)}
          >
            Review what the sync will do
          </Button>
          {review && (
            <Spacing mt={1}>
              <ul aria-label="What the sync does">
                {review.map(line => <li key={line}><Text small>{line}</Text></li>)}
              </ul>
            </Spacing>
          )}
        </Step>
      )}

      {review && destinationFound?.ready && (
        <Step title="5. Create">
          <FlexContainer alignItems="flex-end">
            <Spacing mr={1} style={{ width: UNIT * 32 }}>
              <Text muted small>Pipeline name</Text>
              <TextInput
                aria-label="Pipeline name"
                onChange={e => setName(e.target.value)}
                value={name}
              />
            </Spacing>
            <Button
              disabled={!name || !!created || !sourceFound?.ready}
              loading={isLoading}
              onClick={() => call({
                ingestion_setup: {
                  action: 'create', destination: target, name, source, ...choices,
                },
              } as any)}
              primary
            >
              Create the pipeline
            </Button>
          </FlexContainer>
          {!sourceFound?.ready && (
            <Text danger small>The source is not ready; fix its checks first.</Text>
          )}
          {created && (
            <Spacing mt={2}>
              <Text>
                Created {created}. The passwords are kept as project secrets.
                {' '}
                <NextLink
                  as={`/pipelines/${created}/edit`}
                  href="/pipelines/[pipeline]/edit"
                  legacyBehavior
                  passHref
                >
                  <Link>Open it and start it from its Run tab</Link>
                </NextLink>
              </Text>
            </Spacing>
          )}
        </Step>
      )}
    </div>
  );
}

export default Ingestion;
