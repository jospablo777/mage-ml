import { useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import { onSuccess } from '@api/utils/response';

// The templates of mage_ai/data_preparation/environments.py WORKER_TEMPLATES.
const TEMPLATES = [
  { label: 'Feast worker', name: 'feast', packages: 'Feast, pandas 2', value: 'feast' },
  { label: 'MLflow worker', name: 'mlflow', packages: 'MLflow, pandas', value: 'mlflow' },
  {
    label: 'ML inference worker',
    name: 'inference',
    packages: 'scikit-learn, ONNX Runtime, joblib, pandas',
    value: 'ml-inference',
  },
  { label: 'Polars worker', name: 'polars', packages: 'Polars', value: 'polars' },
  { label: 'Python worker', name: 'tools', packages: 'pyarrow only; add your own', value: 'empty' },
];

// Adds a Python worker from a template: written to python_workers/<name>/, locked with uv
// and prepared. onAdded gets its name once the prepare started.
function NewPythonWorker({ onAdded }: { onAdded: (name: string) => void }) {
  const [open, setOpen] = useState(false);
  const [template, setTemplate] = useState(TEMPLATES[0]);
  const [name, setName] = useState(TEMPLATES[0].name);
  const [label, setLabel] = useState(TEMPLATES[0].label);
  const [error, setError] = useState<string>(null);
  const [add, { isLoading }] = useMutation(
    api.python_workers.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker: added }) => {
          setOpen(false);
          setError(null);
          onAdded(added?.name);
        },
        onErrorCallback: response => setError(
          response?.error?.message || 'The worker was not added.',
        ),
      }),
    },
  );

  if (!open) {
    return (
      <Spacing mt={1}>
        <Link onClick={() => setOpen(true)} small>Add a Python worker</Link>
      </Spacing>
    );
  }

  return (
    <Spacing mt={2}>
      <Text bold>Add a Python worker</Text>
      <Spacing mt={1}>
        <FlexContainer alignItems="flex-end" flexWrap="wrap">
          <Select
            aria-label="Template"
            compact
            labelFixed="Template"
            onChange={(e) => {
              const chosen = TEMPLATES.find(t => t.value === e.target.value);
              setTemplate(chosen);
              setName(chosen.name);
              setLabel(chosen.label);
            }}
            value={template.value}
          >
            {TEMPLATES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
          </Select>
          <Spacing ml={1} />
          <TextInput
            aria-label="Name in YAML"
            compact
            labelFixed="Name in YAML"
            monospace
            onChange={e => setName(e.target.value)}
            value={name}
          />
          <Spacing ml={1} />
          <TextInput
            aria-label="Name shown"
            compact
            labelFixed="Name shown"
            onChange={e => setLabel(e.target.value)}
            value={label}
          />
        </FlexContainer>
      </Spacing>
      <Spacing mt={1}>
        <Text muted small>
          Starts with {template.packages}. It is written to python_workers/{name || '<name>'}/,
          locked with uv and prepared; commit its pyproject.toml and uv.lock. Edit the
          packages there and run uv lock to change them.
        </Text>
      </Spacing>
      {error && <Text danger small>{error}</Text>}
      <Spacing mt={1}>
        <FlexContainer alignItems="center">
          <Button
            compact
            disabled={!name}
            loading={isLoading}
            onClick={() => {
              setError(null);
              // @ts-ignore
              add({ python_worker: { label, name, template: template.value } });
            }}
            primary
          >
            Add and prepare
          </Button>
          <Spacing ml={1} />
          <Button compact onClick={() => setOpen(false)}>Cancel</Button>
        </FlexContainer>
      </Spacing>
    </Spacing>
  );
}

export default NewPythonWorker;
