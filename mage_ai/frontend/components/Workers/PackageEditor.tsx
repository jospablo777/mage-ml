import styled from 'styled-components';
import { useState } from 'react';

import Button from '@oracle/elements/Button';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { Close } from '@oracle/icons';
import { MonoStyle } from './shared';
import { UNIT } from '@oracle/styles/units/spacing';

// A requirement as people write it: name, extras, version: "feast[redis]>=0.50".
export type RequirementType = { extras: string; name: string; spec: string };

const REQUIREMENT = /^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(\[[^\]]*\])?\s*(.*)$/;
const LATEST = '';
const CUSTOM = 'custom';

export function parseRequirement(text: string): RequirementType {
  const match = (text || '').trim().match(REQUIREMENT);
  if (!match) {
    return null;
  }
  return { extras: match[2] || '', name: match[1], spec: (match[3] || '').trim() };
}

export function requirementText({ extras, name, spec }: RequirementType): string {
  return `${name}${extras}${spec}`;
}

function sameName(a: string, b: string): boolean {
  const normal = (value: string) => value.toLowerCase().replace(/[-_.]+/g, '-');
  return normal(a) === normal(b);
}

// Names people type that are not the package to install: an import name (cv2), a
// placeholder on PyPI that fails (sklearn, pytorch), or another name for it.
export const PACKAGE_NAMES = {
  bs4: 'beautifulsoup4',
  cv2: 'opencv-python',
  dateutil: 'python-dateutil',
  dotenv: 'python-dotenv',
  opencv: 'opencv-python',
  pil: 'pillow',
  pytorch: 'torch',
  skimage: 'scikit-image',
  sklearn: 'scikit-learn',
  tf: 'tensorflow',
  yaml: 'pyyaml',
};
// R packages people reach for in a Python worker: R workers are a separate kind.
const R_PACKAGES = ['caret', 'dplyr', 'ggplot2', 'tidyverse', 'data.table', 'tidymodels'];

// "mlflow==2.8 pandas scikit-learn>=1.5": a requirement per word; commas inside a version
// range stay, a comma between packages goes. Known other names become the package's.
export function parseMany(text: string): {
  notes: string[];
  problems: string[];
  requirements: RequirementType[];
} {
  const notes = [];
  const problems = [];
  const requirements = [];
  (text || '').split(/\s+/).map(word => word.replace(/^,+|,+$/g, '')).filter(Boolean)
    .forEach((word) => {
      const parsed = parseRequirement(word);
      if (!parsed) {
        problems.push(`"${word}" is not a package.`);
        return;
      }
      const lower = parsed.name.toLowerCase();
      if (R_PACKAGES.includes(lower)) {
        problems.push(`${parsed.name} is an R package; a Python worker installs Python `
          + 'packages.');
        return;
      }
      const renamed = PACKAGE_NAMES[lower];
      if (renamed) {
        notes.push(`${parsed.name} is installed as ${renamed}.`);
        parsed.name = renamed;
      }
      requirements.push(parsed);
    });
  return { notes, problems, requirements };
}

const RowStyle = styled.div`
  align-items: center;
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: minmax(${UNIT * 16}px, 1fr) minmax(${UNIT * 24}px, 1.4fr) auto;
  padding: ${UNIT}px 0;

  ${props => `border-top: 1px solid ${(props.theme.borders || dark.borders).light};`}
`;

const RemoveStyle = styled.button`
  background: none;
  border: none;
  cursor: pointer;
  padding: ${UNIT / 2}px;
`;

// One package: its name, its version (Latest, a release, or a range) and whether the
// package index knows it.
function PackageRow({ onChange, onRemove, requirement }: {
  onChange: (requirement: RequirementType) => void;
  onRemove: () => void;
  requirement: RequirementType;
}) {
  const { data } = api.python_packages.detail(requirement.name, {}, {
    revalidateOnFocus: false,
  });
  const found = data?.python_package;
  const missing = data?.error?.message;
  const versions: string[] = found?.versions || [];
  const pinned = requirement.spec.startsWith('==') ? requirement.spec.slice(2) : null;
  const [custom, setCustom] = useState(
    !!requirement.spec && !(pinned && !pinned.includes('*')),
  );
  const choice = custom ? CUSTOM : (pinned || LATEST);

  return (
    <RowStyle>
      <div>
        <MonoStyle><Text bold inline>{requirement.name}</Text>{requirement.extras}</MonoStyle>
        {missing && <Text danger xsmall>{missing}</Text>}
        {found && !missing && pinned && versions.length > 0 && !versions.includes(pinned) && (
          <Text warning xsmall>{pinned} is not a release of {found.name}.</Text>
        )}
      </div>
      <div>
        <Select
          aria-label={`Version of ${requirement.name}`}
          compact
          onChange={(e) => {
            const value = e.target.value;
            if (value === CUSTOM) {
              setCustom(true);
              return;
            }
            setCustom(false);
            onChange({ ...requirement, spec: value === LATEST ? '' : `==${value}` });
          }}
          value={choice}
        >
          <option value={LATEST}>
            {found?.latest ? `Latest (${found.latest} today)` : 'Latest'}
          </option>
          <option value={CUSTOM}>A version range...</option>
          {pinned && !versions.includes(pinned) && !custom && (
            <option value={pinned}>{pinned}</option>
          )}
          {versions.map(version => (
            <option key={version} value={version}>{version}</option>
          ))}
        </Select>
        {custom && (
          <Spacing mt={1}>
            <TextInput
              aria-label={`Version range of ${requirement.name}`}
              compact
              onChange={e => onChange({ ...requirement, spec: e.target.value.trim() })}
              placeholder=">=3.15,<4"
              value={requirement.spec}
            />
          </Spacing>
        )}
      </div>
      <RemoveStyle aria-label={`Remove ${requirement.name}`} onClick={onRemove} type="button">
        <Close muted size={UNIT * 1.5} />
      </RemoveStyle>
    </RowStyle>
  );
}

// A worker's packages: each with its version, Latest unless one is chosen. Adding takes
// what people type, several at once: "mlflow==2.8 pandas scikit-learn>=1.5".
function PackageEditor({ onChange, value }: {
  onChange: (requirements: string[]) => void;
  value: string[];
}) {
  const [adding, setAdding] = useState('');
  const [problems, setProblems] = useState<string[]>([]);
  const [notes, setNotes] = useState<string[]>([]);
  const requirements = value.map(parseRequirement).filter(Boolean);

  const add = () => {
    const parsed = parseMany(adding);
    setProblems(parsed.problems);
    setNotes(parsed.notes);
    if (!parsed.requirements.length) {
      return;
    }
    const next = [...requirements];
    parsed.requirements.forEach((requirement) => {
      const index = next.findIndex(r => sameName(r.name, requirement.name));
      if (index >= 0) {
        next[index] = requirement;
      } else {
        next.push(requirement);
      }
    });
    onChange(next.map(requirementText));
    setAdding('');
  };

  return (
    <div>
      <div style={{ alignItems: 'flex-end', display: 'flex', gap: UNIT }}>
        <div style={{ flex: 1 }}>
          <TextInput
            aria-label="Add packages"
            label="Add packages"
            monospace
            onChange={e => setAdding(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                add();
              }
            }}
            placeholder="xgboost catboost scikit-learn>=1.5 torch matplotlib"
            value={adding}
          />
        </div>
        <Button disabled={!adding.trim()} onClick={add}>Add</Button>
      </div>
      <Spacing mt={1}>
        <Text muted xsmall>
          Several at once, separated by spaces. Without a version a package gets the latest
          release when it is locked; pick a version or a range below to keep it there.
        </Text>
        {notes.map(note => <Text info key={note} xsmall>{note}</Text>)}
        {problems.map(problem => <Text danger key={problem} xsmall>{problem}</Text>)}
      </Spacing>

      <Spacing mt={2}>
        {requirements.length === 0 && (
          <Text muted small>No packages yet: add the ones its blocks import.</Text>
        )}
        {requirements.map((requirement, index) => (
          <PackageRow
            key={requirement.name}
            onChange={(changed) => {
              const next = [...requirements];
              next[index] = changed;
              onChange(next.map(requirementText));
            }}
            onRemove={() => onChange(
              requirements.filter((_, i) => i !== index).map(requirementText))}
            requirement={requirement}
          />
        ))}
      </Spacing>
    </div>
  );
}

export default PackageEditor;
