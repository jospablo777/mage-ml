import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import dark from '@oracle/styles/themes/dark';
import { AlertTriangle, Check, Close } from '@oracle/icons';
import { BORDER_RADIUS, BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import {
  CommandStyle,
  ServiceExportType,
  ServiceModelType,
  describeModelVersion,
  modelProblem,
} from '@components/PipelineServiceExport';
import { FONT_FAMILY_MEDIUM } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

export type ModelSource = 'alias' | 'version' | 'uri';

// A model row as it is edited: a registered model with an alias or a version, or any URI.
// Only complete rows go into the service.
export type ModelRowType = {
  key: number;
  name: string;
  ref: string;
  registered: string;
  source: ModelSource;
  uri: string;
};

const NAME_PATTERN = /^[A-Za-z0-9._-]+$/;
const SOURCES: { label: string; placeholder: string; value: ModelSource }[] = [
  { label: 'Alias', placeholder: 'champion', value: 'alias' },
  { label: 'Version', placeholder: '10', value: 'version' },
  { label: 'URI', placeholder: 'runs:/<run id>/model', value: 'uri' },
];
const SHOWN_TAGS = 4;

let nextKey = 1;

export function newModelRow(): ModelRowType {
  nextKey += 1;

  return { key: nextKey, name: '', ref: '', registered: '', source: 'alias', uri: '' };
}

export function rowFromModel(model: { name: string; uri: string }): ModelRowType {
  const row = { ...newModelRow(), name: model.name };
  const alias = model.uri.match(/^models:\/([^/@]+)@(.+)$/);
  const version = model.uri.match(/^models:\/([^/@]+)\/(\d+)$/);
  if (alias) {
    return { ...row, ref: alias[2], registered: alias[1], source: 'alias' };
  }
  if (version) {
    return { ...row, ref: version[2], registered: version[1], source: 'version' };
  }

  return { ...row, source: 'uri', uri: model.uri };
}

export function modelName(row: ModelRowType): string {
  return row.name.trim() || (row.source === 'uri' ? '' : row.registered.trim());
}

export function modelUri(row: ModelRowType): string {
  if (row.source === 'uri') {
    return row.uri.trim();
  }
  const registered = row.registered.trim();
  const ref = row.ref.trim();
  if (!registered || !ref) {
    return '';
  }

  return row.source === 'alias' ? `models:/${registered}@${ref}` : `models:/${registered}/${ref}`;
}

export function rowProblem(row: ModelRowType, rows: ModelRowType[]): string {
  const name = modelName(row);
  if (!modelUri(row)) {
    if (row.source === 'uri') {
      return 'Enter the URI to download.';
    }
    return `Enter the registered model and its ${row.source}.`;
  }
  if (row.source === 'version' && !/^\d+$/.test(row.ref.trim())) {
    return 'A version is a number, such as 10.';
  }
  if (!name) {
    return 'Name it: blocks load it by this name.';
  }
  if (!NAME_PATTERN.test(name)) {
    return 'The name uses letters, digits, ".", "_" and "-".';
  }
  if (rows.filter(r => modelName(r) === name).length > 1) {
    return `Two models are named ${name}.`;
  }

  return null;
}

// The models of the rows the service can embed.
// Registry addresses MLflow reads: a server, or a database or folder it uses directly.
const REGISTRY_SCHEMES = /^(https?|databricks(-uc)?|uc|sqlite|postgresql|mysql|mssql|file):/i;

export function registryProblem(value: string): string {
  const url = (value || '').trim();
  if (!url) {
    return null;
  }
  if (/\s/.test(url)) {
    return 'The address has a space in it.';
  }
  if (!REGISTRY_SCHEMES.test(url)) {
    return 'Start with http:// or https://, such as http://mlflow:5000.';
  }
  if (/^https?:\/\/(:|\/|$)/i.test(url)) {
    return 'Add the host after http://, such as http://mlflow:5000.';
  }

  return null;
}

export function modelsOf(rows: ModelRowType[]): { name: string; uri: string }[] {
  return rows
    .filter(row => !rowProblem(row, rows))
    .map(row => ({ name: modelName(row), uri: modelUri(row) }));
}

const RowStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}

  & + & {
    margin-top: ${UNIT}px;
  }
`;

const FieldsStyle = styled.div`
  align-items: end;
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: minmax(0, 1.4fr) auto minmax(0, 1fr) minmax(0, 1fr) auto;

  @media (max-width: 1200px) {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  }
`;

const LabelStyle = styled.div`
  margin-bottom: ${UNIT * 0.5}px;
`;

const SegmentStyle = styled.button<{ selected: boolean }>`
  cursor: pointer;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 13px;
  line-height: ${UNIT * 4}px;
  padding: 0 ${UNIT * 1.5}px;

  ${props => `
    background-color: ${props.selected
      ? (props.theme.interactive || dark.interactive).defaultBackgroundTransparent
      : 'transparent'};
    border: 1px solid ${props.selected
      ? (props.theme.content || dark.content).muted
      : (props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}

  &:first-child {
    border-radius: ${BORDER_RADIUS_SMALL}px 0 0 ${BORDER_RADIUS_SMALL}px;
  }

  &:last-child {
    border-radius: 0 ${BORDER_RADIUS_SMALL}px ${BORDER_RADIUS_SMALL}px 0;
  }

  & + & {
    margin-left: -1px;
  }
`;

const RemoveStyle = styled.button`
  background: none;
  border: none;
  cursor: pointer;
  height: ${UNIT * 4}px;
  padding: 0 ${UNIT * 0.5}px;
`;

const StatusStyle = styled.div`
  align-items: flex-start;
  display: flex;
  gap: ${UNIT}px;
  margin-top: ${UNIT * 1.25}px;
`;

const EmptyStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 2}px;

  ${props => `
    border: 1px dashed ${(props.theme.borders || dark.borders).light};
  `}
`;

function Label({ children }: { children: any }) {
  return <LabelStyle><Text muted small>{children}</Text></LabelStyle>;
}

function RowStatus({
  checking,
  model,
  pending,
  problem,
  registryProblem,
  row,
}: {
  checking: boolean;
  model: ServiceModelType;
  pending: string;
  problem: string;
  registryProblem: { action: string; reason: string };
  row: ModelRowType;
}) {
  if (problem) {
    return <StatusStyle><Text muted small>{problem}</Text></StatusStyle>;
  }
  if (checking || pending) {
    return (
      <StatusStyle>
        <Spinner inverted small />
        <Text muted small>Checking the registry</Text>
      </StatusStyle>
    );
  }
  if (registryProblem) {
    return (
      <StatusStyle>
        <AlertTriangle danger size={UNIT * 2} />
        <Text danger small>{registryProblem.reason}</Text>
      </StatusStyle>
    );
  }
  if (!model) {
    return null;
  }
  const tags = Object.entries(model.tags || {});

  return (
    <StatusStyle>
      <Check size={UNIT * 2} success />
      <div>
        <Text small>
          {model.version
            ? `${row.source === 'alias' ? `${row.ref.trim()} is ` : ''}${describeModelVersion(model)}`
            : 'Downloaded from this URI when you export'}
        </Text>
        {model.feature_service && (
          <Text muted small>
            Trained on the Feast feature service {model.feature_service}
          </Text>
        )}
        {tags.length >= 1 && (
          <Text monospace muted xsmall>
            {tags.slice(0, SHOWN_TAGS).map(([key, value]) => `${key}=${value}`).join('  ')}
            {tags.length > SHOWN_TAGS ? `  +${tags.length - SHOWN_TAGS} more` : ''}
          </Text>
        )}
      </div>
    </StatusStyle>
  );
}

// Models pulled from a registry at export and kept in the image: blocks load the embedded
// copy, so the service needs no registry at run time and runs the version it was exported
// with.
function ModelsSection({
  check,
  checkedFor,
  checking,
  registry,
  rows,
  setRegistry,
  setRows,
}: {
  check: ServiceExportType;
  // The models the check was asked about.
  checkedFor: { name: string; uri: string }[];
  checking: boolean;
  registry: string;
  rows: ModelRowType[];
  setRegistry: (value: string) => void;
  setRows: (rows: ModelRowType[]) => void;
}) {
  const checked = check?.models || [];
  const problems = check?.problems || [];
  const names = rows.map(modelName);
  const detected = checked.filter(m => !names.includes(m.name));
  const updateRow = (key: number, values: Partial<ModelRowType>) => setRows(
    rows.map(row => (row.key === key ? { ...row, ...values } : row)),
  );
  const example = modelsOf(rows)[0];

  return (
    <>
      <Label>MLflow tracking server</Label>
      <TextInput
        aria-label="MLflow tracking server"
        compact
        fullWidth
        monospace
        onChange={e => setRegistry(e.target.value)}
        placeholder="http://mlflow.ml.svc:5000"
        value={registry || ''}
      />
      {registryProblem(registry) && (
        <Spacing mt={0.5}>
          <Text danger small>{registryProblem(registry)}</Text>
        </Spacing>
      )}
      <Spacing mt={0.5}>
        <Text muted xsmall>
          Empty: the server&apos;s MLFLOW_TRACKING_URI. Credentials, such as
          MLFLOW_TRACKING_TOKEN, come from the server&apos;s environment and never go in the
          image.
        </Text>
      </Spacing>

      <Spacing mt={2}>
        {!rows.length && (
          <EmptyStyle>
            <Text>No models chosen.</Text>
            <Spacing mt={0.5}>
              <Text muted small>
                Add the model your blocks score with, such as the champion of a registered
                model. Models the code loads with load_model(name, uri=...) and literal
                arguments are found and embedded without this.
              </Text>
            </Spacing>
            <Spacing mt={1.5}>
              <Button compact onClick={() => setRows([newModelRow()])}>Add a model</Button>
            </Spacing>
          </EmptyStyle>
        )}

        {rows.map((row) => {
          const problem = rowProblem(row, rows);
          const name = modelName(row);
          const uri = modelUri(row);
          const source = SOURCES.find(s => s.value === row.source);
          const registryProblem = problems
            .map(modelProblem)
            .find(p => p && p.model === name);
          // The check answers for the models it was asked about; a row changed since waits.
          const pending = !problem && check
            && !(checkedFor || []).some(m => m.name === name && m.uri === uri) ? 'pending' : null;

          return (
            <RowStyle key={row.key}>
              <FieldsStyle>
                {row.source === 'uri' ? (
                  <div>
                    <Label>URI</Label>
                    <TextInput
                      compact
                      fullWidth
                      monospace
                      onChange={e => updateRow(row.key, { uri: e.target.value })}
                      placeholder="runs:/<run id>/model, s3://..., https://..."
                      value={row.uri}
                    />
                  </div>
                ) : (
                  <div>
                    <Label>Registered model</Label>
                    <TextInput
                      compact
                      fullWidth
                      monospace
                      onChange={e => updateRow(row.key, { registered: e.target.value })}
                      placeholder="churn-classifier"
                      value={row.registered}
                    />
                  </div>
                )}
                <div>
                  <Label>Pick by</Label>
                  <div aria-label="Pick the model by" role="radiogroup" style={{ display: 'flex' }}>
                    {SOURCES.map(option => (
                      <SegmentStyle
                        aria-checked={row.source === option.value}
                        key={option.value}
                        onClick={() => updateRow(row.key, { source: option.value })}
                        role="radio"
                        selected={row.source === option.value}
                        type="button"
                      >
                        {option.label}
                      </SegmentStyle>
                    ))}
                  </div>
                </div>
                {row.source !== 'uri' ? (
                  <div>
                    <Label>{source.label}</Label>
                    <TextInput
                      compact
                      fullWidth
                      monospace
                      onChange={e => updateRow(row.key, { ref: e.target.value })}
                      placeholder={source.placeholder}
                      value={row.ref}
                    />
                  </div>
                ) : <div />}
                <div>
                  <Label>Name in the code</Label>
                  <TextInput
                    compact
                    fullWidth
                    monospace
                    onChange={e => updateRow(row.key, { name: e.target.value })}
                    placeholder={row.registered.trim() || 'churn'}
                    value={row.name}
                  />
                </div>
                <RemoveStyle
                  aria-label={`Remove ${name || 'model'}`}
                  onClick={() => setRows(rows.filter(r => r.key !== row.key))}
                  type="button"
                >
                  <Close muted size={UNIT * 1.5} />
                </RemoveStyle>
              </FieldsStyle>
              {uri && (
                <Spacing mt={1}>
                  <Text monospace muted xsmall>{uri}</Text>
                </Spacing>
              )}
              <RowStatus
                checking={checking && !problem}
                model={checked.find(m => m.name === name)}
                pending={pending}
                problem={problem}
                registryProblem={registryProblem}
                row={row}
              />
            </RowStyle>
          );
        })}

        {detected.length >= 1 && (
          <Spacing mt={2}>
            <Text bold small>Found in the code</Text>
            {detected.map(model => (
              <Spacing key={model.name} mt={0.75}>
                <Text monospace small>{model.name}</Text>
                <Text muted xsmall>
                  {[model.uri, describeModelVersion(model)].filter(Boolean).join(' · ')}
                </Text>
              </Spacing>
            ))}
          </Spacing>
        )}
      </Spacing>

      {example && (
        <Spacing mt={2}>
          <Text muted small>
            In a block, load it by its name. MageML downloads it from the registry; the
            service uses the embedded copy and ignores the uri. flavor=&apos;native&apos; gives
            the framework&apos;s own model, such as CatBoost with predict_proba.
          </Text>
          <Spacing mt={1}>
            <CommandStyle>
              {[
                'from mage_ai.pipeline_services.models import (',
                '    feature_service, load_model, model_metadata,',
                ')',
                '',
                `URI = '${example.uri}'`,
                `model = load_model('${example.name}', uri=URI, flavor='native')`,
                `info = model_metadata('${example.name}', uri=URI)  # version, run, tags, params, metrics`,
                `service = feature_service('${example.name}', uri=URI)  # its feast.feature_service tag`,
              ].join('\n')}
            </CommandStyle>
          </Spacing>
        </Spacing>
      )}
    </>
  );
}

export default ModelsSection;
