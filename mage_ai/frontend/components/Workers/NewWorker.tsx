import styled from 'styled-components';
import { useEffect, useMemo, useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import ErrorCallout from './ErrorCallout';
import FlexContainer from '@oracle/components/FlexContainer';
import PackageEditor from './PackageEditor';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { CardStyle, FieldLabel, MonoStyle } from './shared';
import { FONT_FAMILY_MEDIUM } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { WorkerTemplateType, WorkerType } from './types';
import { onSuccess } from '@api/utils/response';

export const PYTHON_VERSIONS = ['3.11', '3.12', '3.13', '3.14'];
// Folder names the templates suggest: not the package's own name, which a worker may need.
const SUGGESTED_NAMES = {
  empty: 'tools',
  feast: 'features',
  'ml-inference': 'inference',
  mlflow: 'models',
  polars: 'polars-tools',
};

const TemplatesStyle = styled.div`
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: repeat(auto-fill, minmax(${UNIT * 26}px, 1fr));
`;

const TemplateStyle = styled.button<{ selected: boolean }>`
  border-radius: ${BORDER_RADIUS}px;
  cursor: pointer;
  padding: ${UNIT * 1.5}px;
  text-align: left;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    border: 1px solid ${props.selected
      ? (props.theme.accent || dark.accent).sky
      : (props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}

  &:focus-visible {
    outline: 2px solid ${props => (props.theme.accent || dark.accent).sky};
  }
`;

const TitleStyle = styled.div`
  font-family: ${FONT_FAMILY_MEDIUM};
  margin-bottom: ${UNIT / 2}px;
`;

const FieldsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 30}px, 1fr));
`;

// The folder name a worker can have: python_workers/<name>/ (environments.new_worker).
export function normalizedName(value: string): string {
  return (value || '').trim().toLowerCase().replace(/[_.\s]+/g, '-');
}

export function nameProblem(name: string, workers: WorkerType[]): string {
  if (!name) {
    return 'Name the worker.';
  }
  if (!/^[a-z0-9][a-z0-9-]*$/.test(name)) {
    return 'Use letters, digits and "-".';
  }
  if (name === 'mage') {
    return '"mage" names MageML\'s own Python.';
  }
  if (workers.some(w => w.name === name)) {
    return `A worker named ${name} exists already.`;
  }
  return null;
}

// Adds a worker: a template's packages to start from, its name and Python. uv locks them
// before anything is written; then the worker prepares.
function NewWorker({ onCancel, onCreated, workers }: {
  onCancel: () => void;
  onCreated: (name: string) => void;
  workers: WorkerType[];
}) {
  const { data } = api.python_worker_templates.list({}, { revalidateOnFocus: false });
  const templates: WorkerTemplateType[] = useMemo(
    () => data?.python_worker_templates || [],
    [data],
  );
  const [template, setTemplate] = useState<WorkerTemplateType>(null);
  const [name, setName] = useState('');
  const [label, setLabel] = useState('');
  const [packages, setPackages] = useState<string[]>([]);
  const [python, setPython] = useState('');
  const [error, setError] = useState<string>(null);

  const choose = (chosen: WorkerTemplateType) => {
    setTemplate(chosen);
    setName(SUGGESTED_NAMES[chosen.id] || chosen.id);
    setLabel(chosen.label);
    setPackages(chosen.dependencies);
    setError(null);
  };
  useEffect(() => {
    if (!template && templates.length) {
      choose(templates.find(t => t.id === 'empty') || templates[0]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [templates]);

  const folder = normalizedName(name);
  const problem = nameProblem(folder, workers);
  const [create, { isLoading }] = useMutation(
    api.python_workers.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker: created }) => onCreated(created?.name),
        onErrorCallback: response => setError(
          response?.error?.message || 'The worker was not added.',
        ),
      }),
    },
  );

  return (
    <CardStyle>
      <Text bold large>New worker</Text>
      <Text muted small>
        A Python environment of its own, for blocks that need packages MageML&apos;s Python
        does not have, or other versions of them. uv locks the packages first: if they do not
        fit together, nothing is written.
      </Text>

      <Spacing mt={2}>
        <Text bold small>Start from</Text>
        <Spacing mt={1}>
          <TemplatesStyle aria-label="Start from" role="radiogroup">
            {templates.map(item => (
              <TemplateStyle
                aria-checked={template?.id === item.id}
                key={item.id}
                onClick={() => choose(item)}
                role="radio"
                selected={template?.id === item.id}
                type="button"
              >
                <TitleStyle>{item.id === 'empty' ? 'Empty' : item.label}</TitleStyle>
                <Text muted small>{item.description}</Text>
              </TemplateStyle>
            ))}
          </TemplatesStyle>
        </Spacing>
      </Spacing>

      <Spacing mt={3}>
        <FieldsStyle>
          <div>
            <FieldLabel>Name</FieldLabel>
            <TextInput
              aria-label="Name"
              meta={{ error: name ? problem : null, touched: true }}
              monospace
              onChange={e => setName(e.target.value)}
              value={name}
            />
            <Spacing mt={1}>
              <Text muted xsmall>
                Its folder, <MonoStyle>python_workers/{folder || '<name>'}/</MonoStyle>, and
                the name blocks select it by.
              </Text>
            </Spacing>
          </div>
          <div>
            <FieldLabel>Display name</FieldLabel>
            <TextInput
              aria-label="Display name"
              onChange={e => setLabel(e.target.value)}
              value={label}
            />
            <Spacing mt={1}>
              <Text muted xsmall>How the editor shows it, such as &quot;Feast worker&quot;.</Text>
            </Spacing>
          </div>
          <div>
            <FieldLabel>Python</FieldLabel>
            <Select
              aria-label="Python"
              onChange={e => setPython(e.target.value)}
              value={python}
            >
              <option value="">Same as MageML</option>
              {PYTHON_VERSIONS.map(version => (
                <option key={version} value={version}>Python {version}</option>
              ))}
            </Select>
            <Spacing mt={1}>
              <Text muted xsmall>uv installs another Python when the worker needs one.</Text>
            </Spacing>
          </div>
        </FieldsStyle>
      </Spacing>

      <Spacing mt={3}>
        <Text bold small>Packages</Text>
        <Text muted xsmall>
          Tables reach the worker as Arrow: keep pyarrow, with pandas or Polars.
        </Text>
        <Spacing mt={1}>
          <PackageEditor onChange={setPackages} value={packages} />
        </Spacing>
      </Spacing>

      {error && (
        <Spacing mt={2}>
          <ErrorCallout message={error} title="The worker was not added" />
        </Spacing>
      )}

      <Spacing mt={2}>
        <FlexContainer alignItems="center">
          <Button
            disabled={!!problem || isLoading}
            loading={isLoading}
            onClick={() => {
              setError(null);
              // @ts-ignore
              create({ python_worker: {
                dependencies: packages,
                label: label || undefined,
                name: folder,
                python: python || undefined,
                template: template?.id || 'empty',
              } });
            }}
            primary
          >
            Create and prepare
          </Button>
          <Spacing ml={1} />
          <Button onClick={onCancel}>Cancel</Button>
          {isLoading && (
            <>
              <Spacing ml={2} />
              <Text muted small>uv is locking the packages...</Text>
            </>
          )}
        </FlexContainer>
      </Spacing>
    </CardStyle>
  );
}

export default NewWorker;
