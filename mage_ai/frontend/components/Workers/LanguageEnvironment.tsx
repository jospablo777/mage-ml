import NextLink from 'next/link';
import styled from 'styled-components';
import { useEffect, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import { useRouter } from 'next/router';

import Button from '@oracle/elements/Button';
import CodeEditor from '@components/CodeEditor';
import CopyToClipboard from '@oracle/components/CopyToClipboard';
import ErrorCallout from './ErrorCallout';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import {
  CardStyle,
  MonoStyle,
  PillStyle,
  TableStyle,
  ToneType,
  plural,
  usageText,
} from './shared';
import { LanguageEnvironmentType } from './types';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { reloadLanguageEnvironments } from '@utils/models/blockEnvironment';

const CodeStyle = styled.pre`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 1.7;
  margin: 0;
  overflow-x: auto;
  padding: ${UNIT * 1.5}px;
  white-space: pre;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const ColumnsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 40}px, 1fr));
`;

// What each language's environment is and the tool that checks a change.
const ABOUT = {
  r: {
    check: 'rv',
    guide: 'https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/r-setup.md',
    text: 'R blocks run with this rv environment: the packages rproject.toml lists, '
      + 'installed as rv.lock pins them.',
    tools: { rig: 'rig', rscript: 'Rscript', rv: 'rv' },
  },
  rust: {
    check: 'cargo',
    guide: 'https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/rust-setup.md',
    text: 'Rust blocks build in this Cargo workspace and can use each crate under '
      + '[workspace.dependencies]; Cargo.lock pins their versions.',
    tools: { cargo: 'cargo', rustc: 'rustc' },
  },
};

// Which blocks run in it, said before the text about its files.
function whoRuns(environment: LanguageEnvironmentType): string {
  const language = environment.language === 'r' ? 'R' : 'Rust';
  if (environment.environment === 'default') {
    return `Every ${language} block runs here unless it or its pipeline chooses a named `
      + 'environment.';
  }
  const key = environment.language === 'r' ? 'r_environment' : 'rust_environment';
  return `${language} blocks run here when they choose ${environment.environment} (in the `
    + `block header, or ${key}: ${environment.environment} in its configuration), or when `
    + `their pipeline does (environment: {${environment.language}: ${environment.environment}}).`;
}

// The pipelines whose blocks run in it, each linked to its editor.
function UsedBy({ environment }: { environment: LanguageEnvironmentType }) {
  const pipelines = environment.pipelines || [];
  return (
    <CardStyle>
      <Text bold>Used by</Text>
      {!pipelines.length && (
        <Text muted small>No pipeline&apos;s blocks choose it yet.</Text>
      )}
      {pipelines.length > 0 && (
        <Spacing mt={1}>
          <TableStyle aria-label="Pipelines that use it">
            <thead>
              <tr><th>Pipeline</th><th>Blocks</th><th /></tr>
            </thead>
            <tbody>
              {pipelines.map(usage => (
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
                    {usage.default && (
                      <Text muted xsmall>the pipeline&apos;s choice for its blocks</Text>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </TableStyle>
        </Spacing>
      )}
    </CardStyle>
  );
}

// Removing a named environment: refused while blocks use it, confirmed once.
function Remove({ environment }: { environment: LanguageEnvironmentType }) {
  const router = useRouter();
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string>(null);
  const [remove, { isLoading }] = useMutation(
    api.language_environments.useDelete(environment.id),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => reloadLanguageEnvironments().then(() => router.push('/workers')),
        onErrorCallback: response => setError(response?.error?.message || 'Not removed.'),
      }),
    },
  );
  const used = (environment.pipelines || []).length > 0;
  return (
    <CardStyle>
      <Text bold>Remove this environment</Text>
      <Text muted small>
        {used
          ? `${usageText(environment.pipelines)} use it. Choose another environment for them `
            + 'first.'
          : `Removes ${environment.definition}/ with its lock and installed library.`}
      </Text>
      {error && <Spacing mt={2}><ErrorCallout message={error} title="Not removed" /></Spacing>}
      <Spacing mt={2}>
        {!confirming && (
          <Button danger disabled={used} onClick={() => setConfirming(true)}>
            Remove environment
          </Button>
        )}
        {confirming && (
          <FlexContainer alignItems="center">
            <Text small>Remove {environment.label} for good?</Text>
            <Spacing ml={2} />
            <Button danger loading={isLoading} onClick={() => remove()}>
              Yes, remove it
            </Button>
            <Spacing ml={1} />
            <Button onClick={() => setConfirming(false)}>Keep it</Button>
          </FlexContainer>
        )}
      </Spacing>
    </CardStyle>
  );
}

export function languageState(environment: LanguageEnvironmentType): {
  label: string;
  tone: ToneType;
} {
  if (environment.state === 'none') {
    return { label: 'Not set up', tone: 'muted' };
  }
  if (environment.state === 'problems') {
    return { label: 'Needs attention', tone: 'warning' };
  }
  // The list reads the files only; the tools say whether blocks can run (the detail).
  if (!environment.tools) {
    return { label: 'Set up', tone: 'info' };
  }
  return { label: 'Ready', tone: 'positive' };
}

// The R or Rust environment of the project: its tools, packages and blocks, what stops
// them, and its rproject.toml or Cargo.toml, saved once rv or cargo resolves it in a copy.
function LanguageEnvironment({ language }: { language: string }) {
  const { data, mutate } = api.language_environments.detail(language, {}, {
    revalidateOnFocus: false,
  });
  const environment: LanguageEnvironmentType = data?.language_environment;
  const [text, setText] = useState<string>(null);
  const [error, setError] = useState<string>(null);
  const [saved, setSaved] = useState<boolean>(false);
  const [synced, setSynced] = useState<boolean>(false);
  // The action the running update is: the result's message depends on it. A ref too: the
  // update's callback was made with the render before the click, which held the action before.
  const [action, setAction] = useState<'add' | 'init' | 'save' | 'sync'>(null);
  const actionRef = useRef<'add' | 'init' | 'save' | 'sync'>(null);
  // Packages or crates to add, and the R snapshot date or Rust toolchain to set.
  const [adding, setAdding] = useState('');
  const [pin, setPin] = useState('');
  const [added, setAdded] = useState<boolean>(false);
  useEffect(() => {
    if (environment && text === null) {
      setText(environment.manifest_text || '');
    }
  }, [environment, text]);

  const [update, { isLoading }] = useMutation(
    api.language_environments.useUpdate(language),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ language_environment: result }) => {
          setError(null);
          const done = actionRef.current;
          setSaved(done === 'save');
          setSynced(done === 'sync');
          setAdded(done === 'add');
          if (done === 'add') {
            setAdding('');
            setPin('');
          }
          setText(result.manifest_text || '');
          mutate();
        },
        onErrorCallback: (response) => {
          setSaved(false);
          setSynced(false);
          setError(response?.error?.message || 'Not saved.');
        },
      }),
    },
  );

  if (data?.error) {
    return <ErrorCallout message={data.error.message} title="Not found" />;
  }
  if (!environment) {
    return (
      <FlexContainer alignItems="center">
        <Spinner />
        <Spacing ml={1} />
        <Text muted>Asking the tools</Text>
      </FlexContainer>
    );
  }

  const about = ABOUT[environment.language];
  const state = languageState(environment);
  const dirty = text !== null && text !== (environment.manifest_text || '');
  const commands = (environment.commands || []).join('\n');
  const run = (next: 'add' | 'init' | 'save' | 'sync') => {
    setAction(next);
    actionRef.current = next;
    const pinned = environment.language === 'r' ? { snapshot: pin.trim() || null }
      : { toolchain: pin.trim() || null };
    const payload = {
      add: { packages: adding, ...pinned },
      init: { init: true },
      save: { manifest_text: text },
      sync: { sync: true },
    };
    // @ts-ignore
    update({ language_environment: payload[next] });
  };
  const save = () => run('save');

  return (
    <>
      <NextLink href="/workers" passHref><Link small>Workers</Link></NextLink>
      <Spacing mt={1} />
      <Text bold xlarge>{environment.label}</Text>
      <div style={{ maxWidth: UNIT * 90 }}>
        <Text default>
          {whoRuns(environment)} {about.text}{' '}
          <Link href={about.guide} inline openNewWindow>Setup guide</Link>
        </Text>
      </div>
      <Spacing mt={1} />
      <FlexContainer alignItems="center">
        <PillStyle tone={state.tone}>{state.label}</PillStyle>
        {environment.version && (
          <>
            <Spacing ml={1} />
            <PillStyle tone="muted">
              {environment.language === 'r' ? 'R' : 'Rust'} {environment.version}
            </PillStyle>
          </>
        )}
        <Spacing ml={1} />
        <PillStyle tone="muted">{plural(environment.block_count, 'block')}</PillStyle>
      </FlexContainer>

      {environment.problems?.length > 0 && (
        <Spacing mt={3}>
          <CardStyle role="status">
            <Text bold warning>What stops {environment.label} blocks</Text>
            {environment.problems.map(problem => (
              <Spacing key={problem} mt={1}><Text default>{problem}</Text></Spacing>
            ))}
            {environment.language === 'r' && environment.exists && (
              <Spacing mt={2}>
                <Button
                  loading={isLoading}
                  onClick={() => run('sync')}
                >
                  Sync the packages
                </Button>
              </Spacing>
            )}
          </CardStyle>
        </Spacing>
      )}

      {synced && !environment.problems?.length && (
        <Spacing mt={3}><Text bold small>Synced: the library matches rv.lock.</Text></Spacing>
      )}
      {error && action === 'sync' && (
        <Spacing mt={3}>
          <ErrorCallout message={error} title="Not synced" tool="rv" />
        </Spacing>
      )}

      <Spacing mt={3}>
        <ColumnsStyle>
          <CardStyle>
            <Text bold>Tools</Text>
            <Spacing mt={1}>
              <TableStyle aria-label="Tools">
                <tbody>
                  {Object.entries(about.tools).map(([key, name]) => (
                    <tr key={key}>
                      <td><MonoStyle>{name}</MonoStyle></td>
                      <td>
                        <Text muted={!environment.tools?.[key]} small>
                          {environment.tools?.[key] || 'Not found'}
                        </Text>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </TableStyle>
            </Spacing>
          </CardStyle>
          <CardStyle>
            <FlexContainer alignItems="center" justifyContent="space-between">
              <Text bold>{environment.exists ? 'Change it in a terminal' : 'Set it up in a terminal'}</Text>
              <CopyToClipboard copiedText={commands} linkText="Copy" small withCopyIcon />
            </FlexContainer>
            <Spacing mt={1} />
            <CodeStyle aria-label="Commands">{commands}</CodeStyle>
          </CardStyle>
        </ColumnsStyle>
      </Spacing>

      {!environment.exists && environment.language === 'r' && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold>No R environment yet</Text>
            <Text muted small>
              R blocks then use the R library of the Rscript they run with. `mage r init`
              creates one in r/ with the tidyverse and what R blocks exchange data with, which
              takes a minute or two; commit rproject.toml and rv.lock.
            </Text>
          </CardStyle>
        </Spacing>
      )}

      {!environment.exists && environment.language === 'rust' && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold>No Rust environment yet</Text>
            <Text muted small>
              It is created in rust/ with the crates Rust blocks use most, and the first Rust
              block creates it too. Commit Cargo.toml, Cargo.lock and rust-toolchain.toml.
            </Text>
            <Spacing mt={1.5} />
            <Button
              loading={isLoading}
              onClick={() => run('init')}
              primary
            >
              Set up the Rust environment
            </Button>
            {error && action === 'init' && (
              <Spacing mt={2}>
                <ErrorCallout message={error} title="Not set up" tool={about.check} />
              </Spacing>
            )}
          </CardStyle>
        </Spacing>
      )}

      {environment.exists && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold>
              {environment.language === 'r' ? 'Packages' : 'Crates'}
            </Text>
            <Text muted small>
              {environment.package_count} listed, {environment.locked_count} locked with what
              they depend on.
            </Text>
            <Spacing mt={1.5}>
              <TableStyle aria-label="Packages">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>{environment.language === 'r' ? 'From' : 'Asks for'}</th>
                    <th>Locked</th>
                  </tr>
                </thead>
                <tbody>
                  {environment.packages.map(p => (
                    <tr key={p.name}>
                      <td><MonoStyle>{p.name}</MonoStyle></td>
                      <td>
                        <Text muted={!(p.requirement || p.source)} small>
                          {p.requirement || p.source || 'First repository'}
                        </Text>
                      </td>
                      <td>
                        {p.locked
                          ? <MonoStyle>{p.locked}</MonoStyle>
                          : (
                            <Text muted small>
                              {environment.language === 'r' ? 'At the next sync' : 'At the next build'}
                            </Text>
                          )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </TableStyle>
            </Spacing>
            <Spacing mt={2}>
              <FlexContainer alignItems="center" flexWrap="wrap">
                <TextInput
                  aria-label={environment.language === 'r' ? 'Packages to add' : 'Crates to add'}
                  monospace
                  onChange={e => setAdding(e.target.value)}
                  placeholder={environment.language === 'r'
                    ? 'xgboost, tidyverse/dplyr@v1.1.4'
                    : 'ndarray@0.16, polars@0.55[lazy,sql]'}
                  value={adding}
                />
                <Spacing ml={1} />
                <TextInput
                  aria-label={environment.language === 'r' ? 'CRAN snapshot date'
                    : 'Rust toolchain'}
                  monospace
                  onChange={e => setPin(e.target.value)}
                  placeholder={environment.language === 'r' ? 'Snapshot: 2026-10-12'
                    : 'Toolchain: 1.98.0'}
                  value={pin}
                />
                <Spacing ml={1} />
                <Button
                  disabled={!adding.trim() && !pin.trim()}
                  loading={isLoading && action === 'add'}
                  onClick={() => run('add')}
                >
                  {environment.language === 'r' ? 'Add packages' : 'Add crates'}
                </Button>
              </FlexContainer>
              <Text muted xsmall>
                {environment.language === 'r'
                  ? 'Added once rproject.toml resolves with them; Sync installs them.'
                  : 'Added once Cargo.toml resolves with them; without a version, the newest on '
                    + 'crates.io. They compile at the next build.'}
              </Text>
              {added && (
                <Text small success>
                  {environment.language === 'r' ? 'Added: Sync the packages to install them.'
                    : 'Added.'}
                </Text>
              )}
            </Spacing>
          </CardStyle>
        </Spacing>
      )}

      {environment.exists && text !== null && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold>
              <MonoStyle>{environment.definition}/{environment.manifest}</MonoStyle>
            </Text>
            <Text muted small>
              Saving resolves it with {about.check} in a copy first: if {about.check} cannot
              resolve it, nothing changes.
            </Text>
            <Spacing mt={2}>
              <CodeEditor
                fontLigatures={false}
                height={420}
                language="ini"
                onChange={(value) => {
                  setSaved(false);
                  setText(value);
                }}
                onSave={() => dirty && save()}
                uuid={`language-environment/${environment.id}`}
                value={text}
                width="100%"
              />
            </Spacing>
            {error && action === 'save' && (
              <Spacing mt={2}>
                <ErrorCallout message={error} title="Not saved" tool={about.check} />
              </Spacing>
            )}
            {saved && !dirty && (
              <Spacing mt={2}>
                <Text bold small>
                  Saved. {environment.language === 'r'
                    ? 'Sync the packages to install the change.'
                    : 'Rust blocks build with it at their next run.'}
                </Text>
              </Spacing>
            )}
            <Spacing mt={2}>
              <FlexContainer alignItems="center">
                <Button disabled={!dirty} loading={isLoading} onClick={save} primary>
                  Check and save
                </Button>
                {dirty && (
                  <>
                    <Spacing ml={1} />
                    <Button onClick={() => setText(environment.manifest_text || '')}>
                      Discard changes
                    </Button>
                  </>
                )}
              </FlexContainer>
            </Spacing>
          </CardStyle>
        </Spacing>
      )}

      <Spacing mt={3}>
        <UsedBy environment={environment} />
      </Spacing>

      {environment.blocks?.length > 0 && (
        <Spacing mt={3}>
          <CardStyle>
            <Text bold>Block files</Text>
            <Spacing mt={1}>
              {environment.blocks.map(block => (
                <Text key={block} small><MonoStyle>{block}</MonoStyle></Text>
              ))}
            </Spacing>
          </CardStyle>
        </Spacing>
      )}

      {environment.environment && environment.environment !== 'default' && (
        <Spacing mt={3}>
          <Remove environment={environment} />
        </Spacing>
      )}
    </>
  );
}

export default LanguageEnvironment;
