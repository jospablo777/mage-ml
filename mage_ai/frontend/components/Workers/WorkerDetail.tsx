import NextLink from 'next/link';
import styled from 'styled-components';
import { useEffect, useMemo, useState } from 'react';
import { useMutation } from 'react-query';
import { useRouter } from 'next/router';

import Button from '@oracle/elements/Button';
import ButtonTabs from '@oracle/components/Tabs/ButtonTabs';
import Checkbox from '@oracle/elements/Checkbox';
import ErrorCallout from './ErrorCallout';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import BuiltinWorker from './BuiltinWorker';
import OlderReleases from './OlderReleases';
import PackageEditor, { normalName } from './PackageEditor';
import WorkerFiles from './WorkerFiles';
import PythonWorkerStatus from '@components/PipelineDetail/Settings/PythonWorker';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import {
  BarStyle,
  FieldLabel,
  CardStyle,
  ChangesList,
  MonoStyle,
  PillStyle,
  TableStyle,
  formatBytes,
  plural,
  securityState,
  usageText,
  workerState,
} from './shared';
import {
  HeldPackageType,
  LockedPackageType,
  WorkerAuditType,
  WorkerHeldPackagesType,
  WorkerType,
  WorkerUpdateType,
} from './types';
import { PYTHON_VERSIONS } from './NewWorker';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { timeAgo } from '@utils/models/compilation';

const POLL_MS = 1500;

const HeaderStyle = styled.div`
  align-items: flex-start;
  display: flex;
  flex-wrap: wrap;
  gap: ${UNIT * 2}px;
  justify-content: space-between;
`;

const PillsStyle = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: ${UNIT}px;
`;

const FieldsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 30}px, 1fr));
`;

function sameList(a: string[], b: string[]): boolean {
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

// What a preview of an update or a fix would change, with Apply; nothing changes before.
function UpdatePreview({ onApplied, onClose, preview }: {
  onApplied: () => void;
  onClose: () => void;
  preview: WorkerUpdateType;
}) {
  const [error, setError] = useState<string>(null);
  const [apply, { isLoading }] = useMutation(
    api.python_worker_updates.useUpdate(preview.id),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => onApplied(),
        onErrorCallback: response => setError(response?.error?.message || 'Not applied.'),
      }),
    },
  );

  return (
    <CardStyle>
      <Text bold>
        {preview.changes.length
          ? `${plural(preview.changes.length, 'package')} would change`
          : (preview.message || 'Nothing to change.')}
      </Text>
      {preview.changes.length > 0 && (
        <>
          <Text muted small>
            Nothing has changed yet. Apply writes the new uv.lock and prepares the worker;
            run the pipelines that use it afterwards.
          </Text>
          <Spacing mt={2}><ChangesList changes={preview.changes} /></Spacing>
        </>
      )}
      {(preview.held || []).map(held => (
        <Spacing key={held.package} mt={2}>
          <ErrorCallout
            message={held.reason}
            title={`${held.package} stays: ${held.to} does not fit the other packages' ranges`}
          />
        </Spacing>
      ))}
      {error && <Spacing mt={2}><ErrorCallout message={error} title="Not applied" /></Spacing>}
      <Spacing mt={2}>
        <FlexContainer>
          {preview.id && (
            <>
              <Button
                loading={isLoading}
                // @ts-ignore
                onClick={() => apply({ python_worker_update: {} })}
                primary
              >
                Apply and prepare
              </Button>
              <Spacing ml={1} />
            </>
          )}
          <Button onClick={onClose}>{preview.id ? 'Cancel' : 'Close'}</Button>
        </FlexContainer>
      </Spacing>
    </CardStyle>
  );
}

// The packages a worker installs, largest first, with what it declares marked.
function InstalledPackages({ packages }: { packages: LockedPackageType[] }) {
  const [search, setSearch] = useState('');
  const largest = packages.reduce((max, p) => Math.max(max, p.bytes), 0) || 1;
  const shown = packages.filter(p => !search || p.name.includes(search.toLowerCase()));

  return (
    <>
      <FlexContainer alignItems="center" justifyContent="space-between">
        <div>
          <Text bold>Everything it installs</Text>
          <Text muted small>
            {plural(packages.length, 'package')} from its uv.lock: those it lists, and the
            ones they need. Largest first.
          </Text>
        </div>
        <TextInput
          aria-label="Find a package"
          compact
          onChange={e => setSearch(e.target.value)}
          placeholder="Find a package"
          value={search}
        />
      </FlexContainer>
      <Spacing mt={2}>
        <TableStyle aria-label="Installed packages">
          <thead>
            <tr>
              <th>Package</th>
              <th>Version</th>
              <th style={{ width: '40%' }}>Size on this server</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {shown.map(item => (
              <tr key={`${item.name}-${item.version}`}>
                <td><MonoStyle>{item.name}</MonoStyle></td>
                <td><MonoStyle>{item.version}</MonoStyle></td>
                <td>
                  <FlexContainer alignItems="center">
                    <div style={{ flex: 1 }}><BarStyle share={item.bytes / largest} /></div>
                    <Spacing ml={1} />
                    <Text muted small>
                      {formatBytes(item.bytes)}{item.built ? ' · built from source' : ''}
                    </Text>
                  </FlexContainer>
                </td>
                <td>
                  {item.declared
                    ? <PillStyle tone="info">listed</PillStyle>
                    : <Text muted xsmall>needed by another</Text>}
                </td>
              </tr>
            ))}
          </tbody>
        </TableStyle>
      </Spacing>
    </>
  );
}

function SecurityTab({ onFix, worker }: { onFix: () => void; worker: WorkerType }) {
  const [round, setRound] = useState(0);
  const { data, isValidating } = api.python_worker_audits.detail(
    worker.name,
    round ? { refresh: 1 } : {},
    { revalidateOnFocus: false },
    { key: `python_worker_audits/${worker.name}/${round}` },
  );
  const report: WorkerAuditType = data?.python_worker_audit;
  const error = data?.error?.message;
  const checking = isValidating || (!data && !error);

  return (
    <>
      <CardStyle>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <div>
            {checking && <Text>Checking its packages against known vulnerabilities...</Text>}
            {!checking && report?.state === 'clean' && (
              <Text bold>No known vulnerabilities in its {plural(
                (worker.packages || []).length, 'package')}.</Text>
            )}
            {!checking && report?.state === 'vulnerable' && (
              <Text bold>
                {plural(report.packages.length, 'package')} with known vulnerabilities
                {report.fixable ? `; ${report.fixable} can be fixed` : ''}.
              </Text>
            )}
            {!checking && report?.state === 'unchecked' && (
              <Text bold>Not checked: {report.reason}</Text>
            )}
            <Text muted small>
              From uv audit, against the OSV database
              {report?.checked_at ? `, checked ${timeAgo(report.checked_at * 1000)}` : ''}.
            </Text>
          </div>
          <FlexContainer>
            <Button disabled={checking} onClick={() => setRound(round + 1)}>Check now</Button>
            {report?.fixable > 0 && (
              <>
                <Spacing ml={1} />
                <Button onClick={onFix} primary>Fix vulnerabilities</Button>
              </>
            )}
          </FlexContainer>
        </FlexContainer>
        {error && <Spacing mt={2}><ErrorCallout message={error} title="Not checked" /></Spacing>}
      </CardStyle>

      {(report?.packages || []).map(item => (
        <Spacing key={item.package} mt={2}>
          <CardStyle>
            <FlexContainer alignItems="center" justifyContent="space-between">
              <div>
                <MonoStyle><Text bold inline>{item.package}</Text> {item.version}</MonoStyle>
                <Text muted small>
                  {item.advisories.length} {item.advisories.length === 1 ? 'advisory' : 'advisories'}
                </Text>
              </div>
              {item.fix
                ? <PillStyle tone="positive">fixed in {item.fix}</PillStyle>
                : <PillStyle tone="warning">no fixed release yet</PillStyle>}
            </FlexContainer>
            {item.advisories.map(advisory => (
              <Spacing key={advisory.id} mt={1}>
                <Text small>
                  {advisory.link
                    ? <a href={advisory.link} rel="noreferrer" target="_blank">{advisory.id}</a>
                    : advisory.id}
                  {advisory.aliases.length ? ` (${advisory.aliases.join(', ')})` : ''}
                  {': '}{advisory.summary || 'No summary.'}
                </Text>
              </Spacing>
            ))}
          </CardStyle>
        </Spacing>
      ))}
    </>
  );
}

function UsedByTab({ worker }: { worker: WorkerType }) {
  if (!worker.used_by?.length) {
    return (
      <CardStyle>
        <Text>No block runs in it yet.</Text>
        <Text muted small>
          Choose it in a block&apos;s settings, or for all of a pipeline&apos;s Python blocks in
          the pipeline&apos;s settings, under Where Python blocks run.
        </Text>
      </CardStyle>
    );
  }
  return (
    <CardStyle>
      <TableStyle aria-label="Pipelines that use it">
        <thead>
          <tr><th>Pipeline</th><th>Blocks</th><th /></tr>
        </thead>
        <tbody>
          {worker.used_by.map(usage => (
            <tr key={usage.pipeline}>
              <td>
                <NextLink
                  as={`/pipelines/${usage.pipeline}/edit`}
                  href="/pipelines/[pipeline]/edit"
                  passHref
                >
                  <Link>{usage.name}</Link>
                </NextLink>
              </td>
              <td><MonoStyle>{usage.blocks.join(', ')}</MonoStyle></td>
              <td>
                {usage.default && <Text muted xsmall>the pipeline&apos;s default worker</Text>}
              </td>
            </tr>
          ))}
        </tbody>
      </TableStyle>
    </CardStyle>
  );
}

function SettingsTab({ onSaved, worker }: { onSaved: () => void; worker: WorkerType }) {
  const router = useRouter();
  const [label, setLabel] = useState(worker.label || '');
  const [description, setDescription] = useState(worker.description || '');
  const [python, setPython] = useState(worker.python || '');
  const [combine, setCombine] = useState(worker.combine !== false);
  const memoryText = (bytes?: number) => {
    if (!bytes) {
      return '';
    }
    const gib = bytes / 2 ** 30;
    return gib >= 1 ? `${Number(gib.toFixed(1))} GiB` : `${Math.round(bytes / 2 ** 20)} MiB`;
  };
  const [memory, setMemory] = useState(memoryText(worker.memory_limit));
  const [error, setError] = useState<string>(null);
  const [confirming, setConfirming] = useState(false);
  const [save, { isLoading }] = useMutation(
    api.python_workers.useUpdate(worker.name),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => {
          setError(null);
          onSaved();
        },
        onErrorCallback: response => setError(response?.error?.message || 'Not saved.'),
      }),
    },
  );
  const [remove, { isLoading: removing }] = useMutation(
    api.python_workers.useDelete(worker.name),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => router.push('/workers'),
        onErrorCallback: response => setError(response?.error?.message || 'Not deleted.'),
      }),
    },
  );
  const used = worker.used_by?.length > 0;
  const changed = label !== (worker.label || '') || description !== (worker.description || '')
    || python !== (worker.python || '') || combine !== (worker.combine !== false)
    || memory !== memoryText(worker.memory_limit);

  return (
    <>
      <CardStyle>
        <FieldsStyle>
          <div>
            <FieldLabel>Display name</FieldLabel>
            <TextInput
              aria-label="Display name"
              onChange={e => setLabel(e.target.value)}
              value={label}
            />
          </div>
          <div>
            <FieldLabel>Python</FieldLabel>
            <Select
              aria-label="Python"
              onChange={e => setPython(e.target.value)}
              value={python}
            >
              {[...new Set([...PYTHON_VERSIONS, worker.python].filter(Boolean))].sort().map(v => (
                <option key={v} value={v}>Python {v}</option>
              ))}
            </Select>
          </div>
        </FieldsStyle>
        <Spacing mt={2}>
          <FieldLabel>Description</FieldLabel>
          <TextInput
            aria-label="Description"
            onChange={e => setDescription(e.target.value)}
            value={description}
          />
        </Spacing>
        <Spacing mt={2}>
          <FieldLabel>Memory limit</FieldLabel>
          <TextInput
            aria-label="Memory limit"
            onChange={e => setMemory(e.target.value)}
            placeholder="No limit, such as 4 GiB"
            value={memory}
          />
          <Spacing mt={1}>
            <Text muted xsmall>
              A block whose processes use more is stopped, and says so, instead of taking the
              server&apos;s memory.
            </Text>
          </Spacing>
        </Spacing>
        <Spacing mt={2}>
          <Checkbox
            checked={combine}
            label="May be combined with other workers when their packages fit together"
            onClick={() => setCombine(!combine)}
          />
        </Spacing>
        <Spacing mt={1}>
          <Text muted xsmall>
            A new name or description needs no new prepare. Another Python locks and prepares
            the worker again.
          </Text>
        </Spacing>
        {error && <Spacing mt={2}><ErrorCallout message={error} title="Not saved" /></Spacing>}
        <Spacing mt={2}>
          <Button
            disabled={!changed}
            loading={isLoading}
            onClick={() => {
              const payload: any = { combine, description, label };
              if (memory !== memoryText(worker.memory_limit)) {
                payload.memory = memory;
              }
              if (python !== worker.python) {
                payload.python = python;
              }
              // @ts-ignore
              save({ python_worker: payload });
            }}
            primary
          >
            Save
          </Button>
        </Spacing>
      </CardStyle>

      <Spacing mt={3}>
        <CardStyle>
          <Text bold>Delete this worker</Text>
          <Text muted small>
            {used
              ? `Its blocks would have nowhere to run: ${usageText(worker.used_by)} use it. `
                + 'Choose another worker for them first.'
              : `Removes ${worker.definition}/ and its prepared environments on this server.`}
          </Text>
          <Spacing mt={2}>
            {!confirming && (
              <Button danger disabled={used} onClick={() => setConfirming(true)}>
                Delete worker
              </Button>
            )}
            {confirming && (
              <FlexContainer alignItems="center">
                <Text small>Delete {worker.label || worker.name} for good?</Text>
                <Spacing ml={2} />
                <Button danger loading={removing} onClick={() => remove()}>
                  Yes, delete it
                </Button>
                <Spacing ml={1} />
                <Button onClick={() => setConfirming(false)}>Keep it</Button>
              </FlexContainer>
            )}
          </Spacing>
        </CardStyle>
      </Spacing>
    </>
  );
}

const TABS = [
  { label: () => 'Packages', uuid: 'packages' },
  { label: () => 'pyproject.toml', uuid: 'files' },
  { label: () => 'Security', uuid: 'security' },
  { label: () => 'Used by', uuid: 'used_by' },
  { label: () => 'Settings', uuid: 'settings' },
];

// A worker's page: whether it can run blocks, its packages (chosen and installed), updates
// and fixes previewed before they apply, where it is used, and its settings.
function WorkerDetail({ name }: { name: string }) {
  const [tab, setTab] = useState(TABS[0].uuid);
  const { data, mutate } = api.python_workers.detail(name, {}, {
    refreshInterval: latest => latest?.python_worker?.job?.state === 'running' ? POLL_MS : 0,
    revalidateOnFocus: false,
  });
  const worker: WorkerType = data?.python_worker;
  const declared = useMemo(() => worker?.dependencies || [], [worker]);
  const [draft, setDraft] = useState<string[]>(null);
  const [saved, setSaved] = useState<WorkerType>(null);
  const [error, setError] = useState<string>(null);
  const [preview, setPreview] = useState<WorkerUpdateType>(null);
  const [previewError, setPreviewError] = useState<string>(null);
  const [confirm, setConfirm] = useState<HeldPackageType[]>(null);

  useEffect(() => {
    if (worker && draft === null) {
      setDraft(declared);
    }
  }, [declared, draft, worker]);

  // Asked again whenever its pyproject.toml or uv.lock changes: each is a new lock to check.
  const { data: dataHeld } = api.python_worker_held_packages.detail(
    worker?.locked ? name : null,
    {},
    { revalidateOnFocus: false },
    { key: `python_worker_held_packages/${name}/${worker?.digest}` },
  );
  const heldReport: WorkerHeldPackagesType = dataHeld?.python_worker_held_package;
  const held = useMemo(() => Object.fromEntries((heldReport?.packages || []).map(
    (p: HeldPackageType) => [normalName(p.package), p])), [heldReport]);
  const locked = useMemo(() => Object.fromEntries(
    ((worker?.packages || []) as LockedPackageType[]).map(p => [normalName(p.name), p.version]),
  ), [worker]);
  const heldCount = (heldReport?.packages || []).filter(p => p.state === 'held').length;

  const [savePackages, { isLoading: saving }] = useMutation(
    api.python_workers.useUpdate(name),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker: result }) => {
          setError(null);
          if (result?.confirm?.length) {
            // Nothing changed: the page asks whether to go on with the older releases.
            setConfirm(result.confirm);
            return;
          }
          setConfirm(null);
          setSaved(result);
          setDraft(result?.dependencies || null);
          mutate();
        },
        onErrorCallback: response => setError(response?.error?.message || 'Not saved.'),
      }),
    },
  );
  const [startPreview, { isLoading: previewing }] = useMutation(
    api.python_worker_updates.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ python_worker_update: result }) => {
          setPreviewError(null);
          setPreview(result);
        },
        onErrorCallback: response => setPreviewError(
          response?.error?.message || 'No preview.'),
      }),
    },
  );

  if (data?.error) {
    return <ErrorCallout message={data.error.message} title="This worker cannot be shown" />;
  }
  if (!worker) {
    return <Spinner />;
  }
  if (worker.builtin) {
    return <BuiltinWorker worker={worker} />;
  }

  const state = workerState(worker);
  const security = securityState(worker.security);
  const packages = (worker.packages || []) as LockedPackageType[];
  const dirty = draft !== null && !sameList(draft, declared);
  const askPreview = (fix: boolean) => {
    setPreview(null);
    setTab('packages');
    // @ts-ignore
    startPreview({ python_worker_update: { fix_vulnerabilities: fix, worker: name } });
  };

  return (
    <>
      <NextLink href="/workers" passHref><Link small>Workers</Link></NextLink>
      <Spacing mt={1} />
      <HeaderStyle>
        <div>
          <Text bold xlarge>{worker.label || worker.name}</Text>
          <MonoStyle><Text inline muted>{worker.definition}/</Text></MonoStyle>
          {worker.description && <Text default>{worker.description}</Text>}
          <Spacing mt={1} />
          <PillsStyle>
            <PillStyle title={state.hint} tone={state.tone}>{state.label}</PillStyle>
            <PillStyle tone={security.tone}>{security.label}</PillStyle>
            {heldCount > 0 && (
              <PillStyle
                onClick={() => setTab('packages')}
                role="button"
                style={{ cursor: 'pointer' }}
                title="Locked below their newest release, which does not fit the other packages"
                tone="warning"
              >
                {plural(heldCount, 'package')} held back
              </PillStyle>
            )}
            <PillStyle tone="muted">Python {worker.python}</PillStyle>
            <PillStyle tone="muted">{formatBytes(worker.size_bytes)}</PillStyle>
            <PillStyle tone="muted">{usageText(worker.used_by)}</PillStyle>
          </PillsStyle>
        </div>
        <FlexContainer>
          <Button disabled={!worker.locked} loading={previewing} onClick={() => askPreview(false)}>
            Check for updates
          </Button>
        </FlexContainer>
      </HeaderStyle>

      {(!worker.prepared || worker.job?.state === 'running' || worker.job?.problems?.length > 0) && (
        <Spacing mt={2}>
          <CardStyle>
            <Text bold>{state.label}: {state.hint}</Text>
            <PythonWorkerStatus compact key={worker.name} onPrepared={() => mutate()}
              worker={worker as any} />
          </CardStyle>
        </Spacing>
      )}

      {previewError && (
        <Spacing mt={2}><ErrorCallout message={previewError} title="No preview" /></Spacing>
      )}
      {preview && (
        <Spacing mt={2}>
          <UpdatePreview
            onApplied={() => {
              setPreview(null);
              setDraft(null);
              mutate();
            }}
            onClose={() => setPreview(null)}
            preview={preview}
          />
        </Spacing>
      )}

      <Spacing mt={3}>
        <ButtonTabs
          onClickTab={({ uuid }) => setTab(uuid)}
          selectedTabUUID={tab}
          tabs={TABS.map(t => t.uuid === 'security' && worker.security?.state === 'vulnerable'
            ? { ...t, label: () => `Security (${worker.security.advisories})` } : t)}
          underlineStyle
          // File names keep their case.
          uppercase={false}
        />
      </Spacing>

      <Spacing mt={2}>
        {tab === 'packages' && (
          <>
            <CardStyle>
              <Text bold>Packages it lists</Text>
              <Text muted small>
                What its blocks import, with a version each. Saving locks them in a copy
                first: if they do not fit together nothing changes.
              </Text>
              <Spacing mt={2}>
                <PackageEditor
                  held={held}
                  locked={locked}
                  onChange={(next) => {
                    // A question about other versions no longer applies.
                    setConfirm(null);
                    setDraft(next);
                  }}
                  value={draft || []}
                />
              </Spacing>
              {error && (
                <Spacing mt={2}><ErrorCallout message={error} title="Not saved" /></Spacing>
              )}
              {confirm && dirty && (
                <Spacing mt={2}>
                  <OlderReleases
                    accepting={saving}
                    // @ts-ignore
                    onAccept={() => savePackages({
                      python_worker: { accept_older: true, dependencies: draft },
                    })}
                    onCancel={() => setConfirm(null)}
                    packages={confirm}
                  />
                </Spacing>
              )}
              {saved?.changes?.length > 0 && !dirty && (
                <Spacing mt={2}>
                  <Text bold small>Saved: {plural(saved.changes.length, 'package')} changed</Text>
                  <ChangesList changes={saved.changes} />
                </Spacing>
              )}
              <Spacing mt={2}>
                <FlexContainer>
                  <Button
                    disabled={!dirty}
                    loading={saving}
                    // @ts-ignore
                    onClick={() => savePackages({ python_worker: { dependencies: draft } })}
                    primary
                  >
                    Save and prepare
                  </Button>
                  {dirty && (
                    <>
                      <Spacing ml={1} />
                      <Button onClick={() => setDraft(declared)}>Discard changes</Button>
                    </>
                  )}
                  {saving && (
                    <>
                      <Spacing ml={2} />
                      <Text muted small>uv is locking the packages...</Text>
                    </>
                  )}
                </FlexContainer>
              </Spacing>
            </CardStyle>
            <Spacing mt={3}>
              <CardStyle>
                <InstalledPackages packages={packages} />
              </CardStyle>
            </Spacing>
          </>
        )}
        {tab === 'security' && <SecurityTab onFix={() => askPreview(true)} worker={worker} />}
        {tab === 'used_by' && <UsedByTab worker={worker} />}
        {tab === 'files' && (
          // Rebuilt when the file changes elsewhere (the package list, an update).
          <WorkerFiles key={(worker as any).pyproject} onSaved={() => {
            setDraft(null);
            mutate();
          }} worker={worker} />
        )}
        {tab === 'settings' && (
          <SettingsTab key={JSON.stringify([worker.label, worker.description, worker.python,
            worker.memory_limit])}
            onSaved={() => mutate()} worker={worker} />
        )}
      </Spacing>
    </>
  );
}

export default WorkerDetail;
