import { Fragment, useMemo, useState } from 'react';
import styled from 'styled-components';
import { useRouter } from 'next/router';

import AuditEventType, { AuditOutcomeEnum } from '@interfaces/AuditEventType';
import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Paginate from '@components/shared/Paginate';
import PrivateRoute from '@components/shared/PrivateRoute';
import Select from '@oracle/elements/Inputs/Select';
import SettingsDashboard from '@components/settings/Dashboard';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { MetaQueryEnum } from '@api/constants';
import { PADDING_UNITS, UNIT } from '@oracle/styles/units/spacing';
import { SectionEnum, SectionItemEnum } from '@components/settings/Dashboard/constants';
import { dateFormatLong } from '@utils/date';
import { queryFromUrl, queryString } from '@utils/url';

const PAGE_SIZE = 50;
const DOWNLOAD_LIMIT = 10000;
const FILTERS = ['user_name', 'object_type', 'action', 'outcome'];

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.75}px ${UNIT}px;
    text-align: left;
    vertical-align: top;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }

  tbody tr {
    cursor: pointer;
  }
`;

const ChangesStyle = styled.pre`
  margin: 0;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-all;
`;

function outcomeColor(outcome: AuditOutcomeEnum) {
  return {
    danger: AuditOutcomeEnum.FAILED === outcome,
    success: AuditOutcomeEnum.SUCCEEDED === outcome,
    warning: AuditOutcomeEnum.REFUSED === outcome,
  };
}

function download(events: AuditEventType[]) {
  const blob = new Blob(
    [events.map(event => JSON.stringify(event)).join('\n')],
    { type: 'application/x-ndjson' },
  );
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `audit-log-${new Date().toISOString().slice(0, 19)}.jsonl`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function AuditLogPage() {
  const router = useRouter();
  const q = queryFromUrl();
  const page = Number(q?.page || 0);
  const filters = useMemo(
    () => Object.fromEntries(FILTERS.filter(name => q?.[name]).map(name => [name, q[name]])),
    [q],
  );
  const [draft, setDraft] = useState<{ [key: string]: string }>(filters);
  const [opened, setOpened] = useState<number>(null);

  const { data } = api.audit_events.list({
    ...filters,
    [MetaQueryEnum.LIMIT]: PAGE_SIZE,
    [MetaQueryEnum.OFFSET]: page * PAGE_SIZE,
  }, { revalidateOnFocus: false });
  const events: AuditEventType[] = useMemo(() => data?.audit_events || [], [data]);
  const total = data?.metadata?.count || 0;
  const [downloading, setDownloading] = useState(false);

  const go = (query) => router.push(
    '/settings/workspace/audit-log',
    `/settings/workspace/audit-log?${queryString(query)}`,
  );
  const apply = () => go(Object.fromEntries(
    Object.entries(draft).filter(([, value]) => value),
  ));

  return (
    <SettingsDashboard
      title="Audit log"
      uuidItemSelected={SectionItemEnum.AUDIT_LOG}
      uuidWorkspaceSelected={SectionEnum.USER_MANAGEMENT}
    >
      <Spacing p={PADDING_UNITS}>
        <Text bold large>
          {total} change{total === 1 ? '' : 's'} made through MageML
        </Text>
        <Text muted small>
          Every create, update and delete, refused and failed ones included. Secrets and
          passwords are never kept; code and other long values are kept as their length and
          hash. Saves of the same thing within a minute count as one.
        </Text>

        <Spacing mt={2}>
          <FlexContainer alignItems="center" flexWrap="wrap">
            <Spacing mb={1} mr={1}>
              <TextInput
                compact
                onChange={e => setDraft(prev => ({ ...prev, user_name: e.target.value }))}
                onKeyDown={e => e.key === 'Enter' && apply()}
                placeholder="User (email)"
                value={draft.user_name || ''}
              />
            </Spacing>
            <Spacing mb={1} mr={1}>
              <TextInput
                compact
                monospace
                onChange={e => setDraft(prev => ({ ...prev, object_type: e.target.value }))}
                onKeyDown={e => e.key === 'Enter' && apply()}
                placeholder="Resource (pipelines)"
                value={draft.object_type || ''}
              />
            </Spacing>
            <Spacing mb={1} mr={1}>
              <Select
                compact
                onChange={e => setDraft(prev => ({ ...prev, action: e.target.value }))}
                value={draft.action || ''}
              >
                <option value="">Any action</option>
                <option value="create">create</option>
                <option value="update">update</option>
                <option value="delete">delete</option>
              </Select>
            </Spacing>
            <Spacing mb={1} mr={1}>
              <Select
                compact
                onChange={e => setDraft(prev => ({ ...prev, outcome: e.target.value }))}
                value={draft.outcome || ''}
              >
                <option value="">Any outcome</option>
                <option value="succeeded">succeeded</option>
                <option value="refused">refused</option>
                <option value="failed">failed</option>
              </Select>
            </Spacing>
            <Spacing mb={1} mr={1}>
              <Button compact onClick={apply} primary>Filter</Button>
            </Spacing>
            <Spacing mb={1}>
              <Button
                compact
                loading={downloading}
                onClick={() => {
                  setDownloading(true);
                  api.audit_events.listAsync({
                    ...filters,
                    [MetaQueryEnum.LIMIT]: DOWNLOAD_LIMIT,
                  }).then((response) => {
                    download(response?.audit_events || []);
                  }).finally(() => setDownloading(false));
                }}
              >
                Download JSON Lines
              </Button>
            </Spacing>
          </FlexContainer>
        </Spacing>
      </Spacing>

      <Spacing px={PADDING_UNITS}>
        <TableStyle>
          <thead>
            <tr>
              {['When', 'User', 'Action', 'What', 'Outcome', 'From'].map(label => (
                <th key={label}><Text bold muted small>{label}</Text></th>
              ))}
            </tr>
          </thead>
          <tbody>
            {events.map(event => (
              <Fragment key={event.id}>
                <tr onClick={() => setOpened(opened === event.id ? null : event.id)}>
                  <td>
                    <Text monospace small>{dateFormatLong(event.created_at)}</Text>
                    {event.count > 1 && (
                      <Text muted xsmall>
                        {event.count} times, until {dateFormatLong(event.last_at)}
                      </Text>
                    )}
                  </td>
                  <td><Text small>{event.user_name || 'not signed in'}</Text></td>
                  <td><Text monospace small>{event.action}</Text></td>
                  <td>
                    <Text monospace small>
                      {event.parent ? `${event.parent}/${event.parent_id}/` : ''}
                      {event.object_type}
                      {event.object_id ? `/${event.object_id}` : ''}
                    </Text>
                  </td>
                  <td><Text small {...outcomeColor(event.outcome)}>{event.outcome}</Text></td>
                  <td><Text monospace muted small>{event.source_ip || ''}</Text></td>
                </tr>
                {opened === event.id && (
                  <tr>
                    <td colSpan={6}>
                      {event.error && <Text danger small>{event.error}</Text>}
                      <ChangesStyle>
                        <Text monospace small>
                          {event.changes
                            ? JSON.stringify(event.changes, null, 2)
                            : 'Nothing written.'}
                        </Text>
                      </ChangesStyle>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </TableStyle>
      </Spacing>

      <Spacing p={2}>
        <Paginate
          maxPages={9}
          onUpdate={p => go({ ...filters, page: Math.max(0, Number(p)) })}
          page={page}
          totalPages={Math.ceil(total / PAGE_SIZE)}
        />
      </Spacing>
    </SettingsDashboard>
  );
}

AuditLogPage.getInitialProps = async () => ({});

export default PrivateRoute(AuditLogPage);
