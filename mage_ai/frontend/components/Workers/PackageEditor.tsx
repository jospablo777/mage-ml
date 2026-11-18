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
import { FieldLabel, MonoStyle } from './shared';
import { HeldPackageType } from './types';
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

// A package's name as uv.lock and the package index write it: feast_SDK is feast-sdk.
export function normalName(value: string): string {
  return (value || '').toLowerCase().replace(/[-_.]+/g, '-');
}

function sameName(a: string, b: string): boolean {
  return normalName(a) === normalName(b);
}

function monthOf(iso: string): string {
  const date = iso ? new Date(iso) : null;
  return date && !Number.isNaN(date.getTime())
    ? date.toLocaleDateString('en', { month: 'long', year: 'numeric' }) : null;
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

// One package: its name, its version (Latest, a release, or a range), whether the package
// index knows it and, once locked, the version uv chose: below the newest release when the
// newest does not fit the other packages.
function PackageRow({ held, locked, onChange, onRemove, requirement }: {
  held?: HeldPackageType;
  locked?: string;
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
        {held?.state === 'held' && !requirement.spec && (
          <Spacing mt={0.5}>
            <Text warning xsmall>
              Held back at {held.locked}
              {monthOf(held.locked_uploaded) ? ` (${monthOf(held.locked_uploaded)})` : ''}
              {held.behind ? `, ${held.behind} releases behind ${held.latest}` : `; ${held.latest} is out`}
            </Text>
            <Text muted xsmall>{held.reason}</Text>
            {held.uv_reason && (
              <details>
                <summary><Text inline muted xsmall>uv&apos;s explanation</Text></summary>
                <Text muted xsmall>{held.uv_reason}</Text>
              </details>
            )}
            <Spacing mt={0.5}>
              <Button
                compact
                onClick={() => onChange({ ...requirement, spec: `==${held.locked}` })}
                small
              >
                Keep {held.locked}
              </Button>
            </Spacing>
          </Spacing>
        )}
        {held?.state === 'behind' && !requirement.spec && (
          <Text muted xsmall>{held.locked} locked; {held.reason}</Text>
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
            {/* uv picks the newest release that fits the other packages: once locked, say
                which one it was. */}
            {locked
              ? `Newest that fits (${locked} locked)`
              : found?.latest ? `Newest that fits (${found.latest} today)` : 'Newest that fits'}
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
function PackageEditor({ held, locked, onChange, value }: {
  // By normalName: the packages held below their newest release, and each locked version.
  held?: { [name: string]: HeldPackageType };
  locked?: { [name: string]: string };
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
          <FieldLabel>Add packages</FieldLabel>
          <TextInput
            aria-label="Add packages"
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
          Several at once, separated by spaces. Without a version a package gets the newest
          release that fits the other packages; pick a version or a range below to keep it
          there.
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
            held={held?.[normalName(requirement.name)]}
            key={requirement.name}
            locked={locked?.[normalName(requirement.name)]}
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
