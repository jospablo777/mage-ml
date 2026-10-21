import { useEffect, useMemo, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import CopyToClipboard from '@oracle/components/CopyToClipboard';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { AlertTriangle, Check } from '@oracle/icons';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { FONT_FAMILY_REGULAR, MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import {
  PLATFORMS,
  PLATFORM_LABELS,
  REGISTRIES,
  platformProgress,
  shipCommands,
} from './registries';

const POLL_MS = 700;

const SelectStyle = styled.select`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${FONT_FAMILY_REGULAR};
  font-size: 13px;
  height: ${UNIT * 4.5}px;
  padding: 0 ${UNIT}px;
  width: 100%;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const ToggleStyle = styled.button<{ active: boolean }>`
  border-radius: ${UNIT * 2}px;
  cursor: pointer;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 26px;
  margin: 0 ${UNIT}px ${UNIT}px 0;
  padding: 0 ${UNIT * 1.5}px;

  ${props => `
    background-color: ${props.active
      ? (props.theme.interactive || dark.interactive).defaultBackgroundTransparent
      : 'transparent'};
    border: 1px solid ${props.active
      ? (props.theme.content || dark.content).muted
      : (props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const GridStyle = styled.div`
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 22}px, 1fr));
`;

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

const LogStyle = styled(CodeStyle)`
  max-height: ${UNIT * 40}px;
  overflow: auto;
  white-space: pre-wrap;
`;

// Choose where the image runs and the registry; get the commands, or build on the server.
function BuildAndShip({ name }: { name: string }) {
  // Where the image runs: the service's saved platforms, else most servers' (amd64). Not
  // this machine's by default: a laptop's arm64 image does not run on an amd64 server.
  const [platforms, setPlatforms] = useState<string[]>(['linux/amd64']);
  const { data: dataService, mutate: reloadService } = api.pipeline_services.detail(
    name, {}, { revalidateOnFocus: false },
  );
  const saved: string[] = dataService?.pipeline_service?.platforms || [];
  const savedKey = saved.join(',');
  useEffect(() => {
    if (saved.length) {
      setPlatforms(saved);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [savedKey]);
  const [savePlatforms, { isLoading: isSaving }] = useMutation(
    api.pipeline_services.useUpdate(name),
    { onSuccess: () => reloadService() },
  );
  const [registryKey, setRegistryKey] = useState<string>('none');
  const [values, setValues] = useState<{ [key: string]: string }>({});
  const [tag, setTag] = useState<string>('1.0');
  const [job, setJob] = useState<any>(null);
  const logRef = useRef<HTMLPreElement>(null);

  const registry = REGISTRIES[registryKey];
  const push = registryKey !== 'none';
  const image = registry.image(values, name, tag || 'latest');
  const { lines, problem: commandProblem } = useMemo(() => shipCommands({
    image,
    login: registry.login?.(values),
    platforms,
    push,
  }), [image, platforms, push, registry, values]);
  const problem = platforms.length ? commandProblem : 'Choose where the image runs.';

  const { data: dataStatus } = api.pipeline_service_builds.list({}, { revalidateOnFocus: false });
  const server = dataStatus?.pipeline_service_builds?.[0];

  const [startBuild, { isLoading: isStarting }] = useMutation(
    api.pipeline_service_builds.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_service_build: started }) => setJob(started),
        onErrorCallback: response => setJob({
          lines: [],
          problems: [response?.error?.message || 'The build did not start.'],
          state: 'failed',
        }),
      }),
    },
  );
  const running = job?.state === 'running';
  const { data: dataJob } = api.pipeline_service_builds.detail(
    running ? name : null,
    {},
    { refreshInterval: running ? POLL_MS : 0 },
  );
  useEffect(() => {
    const latest = dataJob?.pipeline_service_build;
    if (latest && running) {
      setJob(latest);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataJob]);
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [job?.lines?.length]);

  const togglePlatform = (platform: string) => setPlatforms(prev => (
    prev.includes(platform) ? prev.filter(p => p !== platform) : [...prev, platform]
  ));
  const native: string = server?.platform;
  const emulated = native ? platforms.filter(p => p !== native) : [];
  // The server's own platform alone builds with docker build; others with buildx.
  const needsBuildx = !server?.buildx && !(platforms.length === 1 && platforms[0] === native);
  const unsaved = [...platforms].sort().join(',') !== [...saved].sort().join(',');
  const progress = useMemo(() => platformProgress(job?.lines || []), [job?.lines]);

  return (
    <div>
      <Text muted small>Where will it run?</Text>
      <Spacing mt={0.5}>
        {PLATFORMS.map(platform => (
          <ToggleStyle
            active={platforms.includes(platform)}
            aria-label={platform}
            aria-pressed={platforms.includes(platform)}
            key={platform}
            onClick={() => togglePlatform(platform)}
            type="button"
          >
            {PLATFORM_LABELS[platform] || platform}
          </ToggleStyle>
        ))}
      </Spacing>
      <Text muted xsmall>
        Both build one image that runs on either.
        {native && ` This server builds ${native} natively`}
        {native && emulated.length >= 1
          ? `; ${emulated.join(' and ')} builds under emulation, several times slower. The `
            + 'CI workflow builds each platform on its own runner.'
          : native ? '.' : ''}
      </Text>
      {unsaved && platforms.length >= 1 && (
        <Spacing mt={0.5}>
          <FlexContainer alignItems="center">
            <Text muted xsmall>
              {saved.length
                ? `The service builds for ${saved.join(' and ')}.`
                : 'The service has no platforms saved; its builds follow this machine.'}
            </Text>
            <Spacing ml={1} />
            <Button
              compact
              loading={isSaving}
              // @ts-ignore
              onClick={() => savePlatforms({ pipeline_service: { platforms } })}
              small
            >
              Save as the service&apos;s platforms
            </Button>
          </FlexContainer>
        </Spacing>
      )}

      <Spacing mt={1.5}>
        <GridStyle>
          <div>
            <Text muted small>Registry</Text>
            <Spacing mt={0.5}>
              <SelectStyle
                aria-label="Registry"
                onChange={e => setRegistryKey(e.target.value)}
                value={registryKey}
              >
                {Object.entries(REGISTRIES).map(([key, r]) => (
                  <option key={key} value={key}>{r.label}</option>
                ))}
              </SelectStyle>
            </Spacing>
          </div>
          {registry.fields.map(field => (
            <div key={field.key}>
              <Text muted small>{field.label}</Text>
              <Spacing mt={0.5}>
                {field.options ? (
                  <SelectStyle
                    aria-label={field.label}
                    onChange={e => setValues(prev => ({ ...prev, [field.key]: e.target.value }))}
                    value={values[field.key] || field.placeholder}
                  >
                    {field.options.map(o => <option key={o} value={o}>{o}</option>)}
                  </SelectStyle>
                ) : (
                  <TextInput
                    compact
                    fullWidth
                    monospace
                    onChange={e => setValues(prev => ({ ...prev, [field.key]: e.target.value }))}
                    placeholder={field.placeholder}
                    value={values[field.key] || ''}
                  />
                )}
              </Spacing>
            </div>
          ))}
          <div>
            <Text muted small>Tag</Text>
            <Spacing mt={0.5}>
              <TextInput
                compact
                fullWidth
                monospace
                onChange={e => setTag(e.target.value.replace(/\s/g, ''))}
                placeholder="1.0"
                value={tag}
              />
            </Spacing>
          </div>
        </GridStyle>
      </Spacing>

      <Spacing mt={1.5}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Text muted small>
            Image <Text inline monospace small>{image}</Text>
          </Text>
          <CopyToClipboard copiedText={lines} linkText="Copy" small withCopyIcon />
        </FlexContainer>
        <Spacing mt={1}><CodeStyle>{lines}</CodeStyle></Spacing>
        {problem && (
          <Spacing mt={1}><Text small warning>{problem}</Text></Spacing>
        )}
        <Spacing mt={1}>
          <Text muted xsmall>
            Run these in the unzipped folder. In your own CI, the folder&apos;s
            .github/workflows/build-image.yml (or .tekton for IBM Cloud) builds both platforms
            and pushes on every commit.
          </Text>
        </Spacing>
      </Spacing>

      {server?.docker && (
        <Spacing mt={2}>
          <FlexContainer alignItems="center">
            <Button
              disabled={!!problem || running || needsBuildx}
              loading={running || isStarting}
              // @ts-ignore
              onClick={() => startBuild({
                pipeline_service_build: { image, name, platforms, push },
              })}
            >
              {push ? 'Build and push from the server' : 'Build on the server'}
            </Button>
            <Spacing ml={1.5} />
            <Text muted xsmall>
              {push
                ? 'Uses the server\'s docker, which must be logged in to the registry.'
                : 'Uses the server\'s docker.'}
              {needsBuildx ? ` Platforms other than ${native || 'its own'} need docker buildx.` : ''}
            </Text>
          </FlexContainer>
        </Spacing>
      )}
      {server && !server.docker && (
        <Spacing mt={1.5}>
          <Text muted xsmall>
            Building from here needs docker on the MageML server ({server.reason}); run the
            commands where Docker is.
          </Text>
        </Spacing>
      )}

      {job && (
        <Spacing mt={1.5}>
          <FlexContainer alignItems="center">
            {running && <Spinner inverted small />}
            {job.state === 'completed' && <Check size={UNIT * 2} success />}
            {job.state === 'failed' && <AlertTriangle danger size={UNIT * 2} />}
            <Spacing ml={1} />
            <Text bold danger={job.state === 'failed'}>
              {running && (job.push ? 'Building and pushing' : 'Building')}
              {job.state === 'completed' && (job.push ? `Pushed ${job.image}` : `Built ${job.image}`)}
              {job.state === 'failed' && 'The build stopped'}
            </Text>
          </FlexContainer>
          {(job.problems || []).map(p => (
            <Spacing key={p} mt={0.5}><Text small>{p}</Text></Spacing>
          ))}
          {Object.keys(progress).length >= 2 && (
            <Spacing mt={1}>
              {Object.entries(progress).map(([platform, { done, failed, total }]) => (
                <Text danger={failed} key={platform} monospace small>
                  {platform}: {failed ? `failed at step ${done} of ${total}` : `step ${done} of ${total}`}
                </Text>
              ))}
            </Spacing>
          )}
          {job.lines?.length >= 1 && (
            <Spacing mt={1}>
              <LogStyle ref={logRef}>{job.lines.join('\n')}</LogStyle>
            </Spacing>
          )}
        </Spacing>
      )}
    </div>
  );
}

export default BuildAndShip;
