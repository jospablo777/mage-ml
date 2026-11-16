import styled from 'styled-components';
import { useMemo, useState } from 'react';

import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import FlexContainer from '@oracle/components/FlexContainer';
import PrivateRoute from '@components/shared/PrivateRoute';
import Select from '@oracle/elements/Inputs/Select';
import SettingsDashboard from '@components/settings/Dashboard';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { PADDING_UNITS, UNIT } from '@oracle/styles/units/spacing';
import { SECTION_ITEM_UUID_IMPORT_EXPORT, SECTION_UUID_WORKSPACE } from
  '@components/settings/Dashboard/constants';

type PlanType = {
  conflicts: string[];
  files: { action: string; path: string; status: string }[];
  mageml_version?: string;
  mode: string;
  pipelines: { action: string; exists: boolean; name: string; uuid: string }[];
};

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.5}px ${UNIT}px ${UNIT * 0.5}px 0;
    text-align: left;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

function saveBase64(content: string, fileName: string) {
  const binary = atob(content);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  const url = URL.createObjectURL(new Blob([bytes], { type: 'application/zip' }));
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}

function readBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result).split(',')[1] || '');
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

async function post(payload) {
  const response = await api.project_archives.useCreate()(payload);

  return response?.data;
}

function ImportExportPage() {
  const { data } = api.pipelines.list({ _limit: 1000 }, { revalidateOnFocus: false });
  const pipelines = useMemo(() => data?.pipelines || [], [data]);
  const [selected, setSelected] = useState<{ [uuid: string]: boolean }>({});
  const [exporting, setExporting] = useState(false);
  const [content, setContent] = useState<string>(null);
  const [fileName, setFileName] = useState<string>(null);
  const [mode, setMode] = useState('skip');
  const [plan, setPlan] = useState<PlanType>(null);
  const [message, setMessage] = useState<{ error?: boolean; text: string }>(null);
  const [busy, setBusy] = useState(false);

  const chosen = Object.keys(selected).filter(uuid => selected[uuid]);

  const planFor = async (archive: string, nextMode: string) => {
    setBusy(true);
    setMessage(null);
    try {
      const result = await post({ project_archive: { action: 'plan', content: archive, mode: nextMode } });
      if (result?.error) {
        setPlan(null);
        setMessage({ error: true, text: result.error.message });
      } else {
        setPlan(result?.project_archive?.plan);
      }
    } finally {
      setBusy(false);
    }
  };

  const changes = (plan?.files || []).filter(file => file.action !== 'keep');

  return (
    <SettingsDashboard
      title="Import and export"
      uuidItemSelected={SECTION_ITEM_UUID_IMPORT_EXPORT}
      uuidWorkspaceSelected={SECTION_UUID_WORKSPACE}
    >
      <Spacing p={PADDING_UNITS}>
        <Text bold large>Export pipelines</Text>
        <Text muted small>
          One zip with each pipeline&apos;s folder (settings, triggers, README), its blocks&apos;
          code and the data contracts they use. io_config.yaml and secrets are not included.
        </Text>
        <Spacing mt={1}>
          {pipelines.map(({ uuid }) => (
            <Spacing key={uuid} mb={0.5}>
              <Checkbox
                checked={!!selected[uuid]}
                label={uuid}
                onClick={() => setSelected(prev => ({ ...prev, [uuid]: !prev[uuid] }))}
              />
            </Spacing>
          ))}
        </Spacing>
        <Spacing mt={1}>
          <Button
            loading={exporting}
            onClick={async () => {
              setExporting(true);
              try {
                const result = await post({
                  project_archive: { action: 'export', pipeline_uuids: chosen },
                });
                const archive = result?.project_archive;
                if (archive?.content) {
                  saveBase64(archive.content, archive.file_name);
                }
              } finally {
                setExporting(false);
              }
            }}
            primary
          >
            {chosen.length ? `Export ${chosen.length} pipeline${chosen.length === 1 ? '' : 's'}`
              : 'Export every pipeline'}
          </Button>
        </Spacing>

        <Spacing mt={5}>
          <Text bold large>Import pipelines</Text>
          <Text muted small>
            Choose a zip made by an export. You see what would change before anything is
            written.
          </Text>
          <Spacing mt={1}>
            <FlexContainer alignItems="center">
              <input
                accept=".zip"
                aria-label="Archive to import"
                onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (!file) {
                    return;
                  }
                  const archive = await readBase64(file);
                  setFileName(file.name);
                  setContent(archive);
                  planFor(archive, mode);
                }}
                type="file"
              />
              <Spacing ml={2}>
                <Select
                  compact
                  onChange={(e) => {
                    setMode(e.target.value);
                    if (content) {
                      planFor(content, e.target.value);
                    }
                  }}
                  value={mode}
                >
                  <option value="skip">Keep pipelines this project has</option>
                  <option value="replace">Replace them</option>
                </Select>
              </Spacing>
            </FlexContainer>
          </Spacing>

          {message && (
            <Spacing mt={1}>
              <Text danger={message.error} small success={!message.error}>{message.text}</Text>
            </Spacing>
          )}

          {plan && (
            <Spacing mt={2}>
              <Text bold>
                {fileName}: {plan.pipelines.length} pipeline
                {plan.pipelines.length === 1 ? '' : 's'}, {changes.length} file
                {changes.length === 1 ? '' : 's'} to write
              </Text>
              <TableStyle>
                <tbody>
                  {plan.pipelines.map(pipeline => (
                    <tr key={pipeline.uuid}>
                      <td><Text monospace small>{pipeline.uuid}</Text></td>
                      <td>
                        <Text small>
                          {{
                            create: 'created',
                            replace: 'replaced',
                            skip: 'kept as it is in this project',
                          }[pipeline.action]}
                        </Text>
                      </td>
                    </tr>
                  ))}
                  {changes.map(file => (
                    <tr key={file.path}>
                      <td><Text monospace muted small>{file.path}</Text></td>
                      <td>
                        <Text danger={file.action === 'conflict'} small>
                          {file.action === 'conflict'
                            ? 'different here, used by other pipelines'
                            : `${file.status === 'new' ? 'new' : 'overwritten'}`}
                        </Text>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </TableStyle>
              {plan.conflicts.length > 0 && (
                <Spacing mt={1}>
                  <Text danger small>
                    This project has different versions of {plan.conflicts.length} file
                    {plan.conflicts.length === 1 ? '' : 's'} its pipelines may run. Choose
                    Replace them to overwrite.
                  </Text>
                </Spacing>
              )}
              <Spacing mt={2}>
                <Button
                  disabled={busy || plan.conflicts.length > 0 || changes.length === 0}
                  loading={busy}
                  onClick={async () => {
                    setBusy(true);
                    try {
                      const result = await post({
                        project_archive: { action: 'import', content, mode },
                      });
                      if (result?.error) {
                        setMessage({ error: true, text: result.error.message });
                      } else {
                        const written = result?.project_archive?.written || [];
                        setMessage({ text: `Imported: ${written.length} files written.` });
                        setPlan(null);
                      }
                    } finally {
                      setBusy(false);
                    }
                  }}
                  primary
                >
                  Import
                </Button>
              </Spacing>
            </Spacing>
          )}
        </Spacing>
      </Spacing>
    </SettingsDashboard>
  );
}

ImportExportPage.getInitialProps = async () => ({});

export default PrivateRoute(ImportExportPage);
