import { useEffect, useMemo, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import BuildAndShip from './BuildAndShip';
import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import CopyToClipboard from '@oracle/components/CopyToClipboard';
import FlexContainer from '@oracle/components/FlexContainer';
import PipelineType from '@interfaces/PipelineType';
import ProgressBar from '@oracle/components/ProgressBar';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import useProject from '@utils/models/project/useProject';
import dark from '@oracle/styles/themes/dark';
import { AlertTriangle, Check, Close, CubeWithArrowDown } from '@oracle/icons';
import { BORDER_RADIUS, BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { FONT_FAMILY_MEDIUM, MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { initiateDownload } from '@utils/downloads';
import { onSuccess } from '@api/utils/response';

const DOCS = 'https://github.com/jospablo777/mage-ml/blob/master/docs-md/production';
export const GUIDE_URL = `${DOCS}/export-a-service.md`;
export const DEPLOY_URL = `${DOCS}/pipeline-services.md#deploy`;
const DEPLOY_TARGETS = ['Kubernetes', 'Cloud Run', 'Code Engine', 'Container Apps', 'ECS'];
const PREVIEW_DELAY_MS = 450;
const POLL_MS = 300;

export const LANGUAGE_NAMES = {
  python: 'Python',
  r: 'R',
  rust: 'Rust',
  sql: 'SQL',
};

export type ServiceExportBlock = {
  language: string;
  type: string;
  uuid: string;
};

export type ServiceExportTrigger = {
  active: boolean;
  kind: string;
  name: string;
  schedule?: string;
};

export type ServiceExportType = {
  download_token?: string;
  environment?: { name: string; required: boolean; secret: boolean }[];
  files?: string[];
  name: string;
  notes?: string[];
  packages?: { [name: string]: string };
  path?: string;
  pipelines?: {
    blocks: ServiceExportBlock[];
    max_concurrent_runs: number;
    triggers: ServiceExportTrigger[];
    uuid: string;
  }[];
  problems?: string[];
  progress?: number;
  stage?: string;
  state?: 'running' | 'completed' | 'failed';
};

// Where the shown progress may go in each stage: a stage reports when it starts, and the
// bar moves toward the next stage's start while it runs, never past it.
const STAGE_CEILINGS = [
  [0.6, 0.55],
  [0.98, 0.95],
];

const DialogStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  display: flex;
  flex-direction: column;
  max-height: 88vh;
  max-width: calc(100vw - ${UNIT * 4}px);
  overflow: hidden;
  width: ${UNIT * 124}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    box-shadow: ${(props.theme.shadow || dark.shadow).popup};
  `}
`;

const HeaderStyle = styled.div`
  padding: ${UNIT * 2.5}px ${UNIT * 3}px ${UNIT * 2}px;

  ${props => `
    border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const BodyStyle = styled.div`
  display: grid;
  gap: ${UNIT * 3}px;
  grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
  overflow: auto;
  padding: ${UNIT * 2.5}px ${UNIT * 3}px;

  @media (max-width: 900px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const FooterStyle = styled.div`
  padding: ${UNIT * 2}px ${UNIT * 3}px;

  ${props => `
    border-top: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const ListStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  max-height: ${UNIT * 46}px;
  min-height: ${UNIT * 20}px;
  overflow: auto;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

export const RowStyle = styled.div<{ selected: boolean }>`
  cursor: pointer;
  display: flex;
  gap: ${UNIT * 1.25}px;
  padding: ${UNIT}px ${UNIT * 1.5}px;

  ${props => `
    border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};
    background-color: ${props.selected
      ? (props.theme.interactive || dark.interactive).defaultBackgroundTransparent
      : 'transparent'};

    &:hover {
      background-color: ${(props.theme.interactive || dark.interactive).defaultBackgroundTransparent};
    }
  `}
`;

export const CardStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  padding: ${UNIT * 2}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

export const TilesStyle = styled.div`
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: repeat(4, minmax(0, 1fr));
`;

export const TileStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  padding: ${UNIT}px ${UNIT * 1.25}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
  `}
`;

export const NumberStyle = styled.div`
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 20px;
  line-height: 28px;
`;

export const PillStyle = styled.span`
  border-radius: ${UNIT * 1.5}px;
  display: inline-block;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 20px;
  margin: 0 ${UNIT / 2}px ${UNIT / 2}px 0;
  padding: 0 ${UNIT}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

export const CommandStyle = styled.pre`
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

const StepStyle = styled.div`
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: ${UNIT * 3.5}px minmax(0, 1fr);
  padding: ${UNIT * 1.5}px 0;
`;

const StepNumberStyle = styled.div`
  align-items: center;
  border-radius: 50%;
  display: flex;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 13px;
  height: ${UNIT * 3.5}px;
  justify-content: center;
  width: ${UNIT * 3.5}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

export const LinkButtonStyle = styled.button`
  background: none;
  border: none;
  cursor: pointer;
  font-size: 12px;
  padding: 0;
  text-decoration: underline;

  ${props => `
    color: ${(props.theme.content || dark.content).muted};
  `}

  &:disabled {
    cursor: default;
    opacity: 0.5;
    text-decoration: none;
  }
`;

export function defaultServiceName(pipelineUUIDs: string[], projectName?: string): string {
  const raw = pipelineUUIDs?.length === 1 ? pipelineUUIDs[0] : (projectName || 'pipelines');

  return slugServiceName(raw) || 'pipelines';
}

export function slugServiceName(name: string): string {
  return (name || '').toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-+|-+$/g, '');
}

export function runCommands(service: ServiceExportType): string {
  const hasEnvironment = service?.environment?.length >= 1;

  return [
    ...(hasEnvironment ? ['cp .env.example .env   # then fill in the variables'] : []),
    `docker run -d -p 8080:8080 -e MAGE_SERVICE_TOKEN=change-me \\`,
    `  -v ${service.name}-data:/var/lib/mage-service \\`,
    `  ${hasEnvironment ? '--env-file .env ' : ''}${service.name}:1.0`,
  ].join('\n');
}

export function callCommand(service: ServiceExportType): string {
  const pipelineUUID = service?.pipelines?.[0]?.uuid;

  return [
    `curl -X POST -H "Authorization: Bearer change-me" \\`,
    `  "http://localhost:8080/v1/pipelines/${pipelineUUID}/runs?wait=60"`,
  ].join('\n');
}

export function plural(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? '' : 's'}`;
}

export function languagesOf(blocks: { language?: string }[]): string[] {
  return Array.from(new Set((blocks || []).map(b => b.language).filter(Boolean)))
    .map(language => LANGUAGE_NAMES[language] || language);
}

export function ExportedSteps({ service }: { service: ServiceExportType }) {
  const run = runCommands(service);
  const call = callCommand(service);

  return (
    <>
      <StepStyle>
        <StepNumberStyle>1</StepNumberStyle>
        <div>
          <Text bold>Download the build context</Text>
          <Spacing mt={1}>
            <Button
              beforeIcon={<CubeWithArrowDown />}
              onClick={() => initiateDownload(service.download_token)}
              primary
            >
              Download {service.name}.zip
            </Button>
          </Spacing>
          {service.path && (
            <Spacing mt={1}>
              <Text muted small>
                Also on the server in <Text inline monospace small>{service.path}</Text>
              </Text>
            </Spacing>
          )}
        </div>
      </StepStyle>

      <StepStyle>
        <StepNumberStyle>2</StepNumberStyle>
        <div>
          <Text bold>Build and ship the image</Text>
          <Spacing mt={0.5}>
            <Text muted small>
              Unzip it and build for the chips you run on, here or in your CI; push it to your
              registry.
            </Text>
          </Spacing>
          <Spacing mt={1.5}><BuildAndShip name={service.name} /></Spacing>
        </div>
      </StepStyle>

      <StepStyle>
        <StepNumberStyle>3</StepNumberStyle>
        <div>
          <FlexContainer alignItems="center" justifyContent="space-between">
            <Text bold>Run it and start a run</Text>
            <CopyToClipboard copiedText={`${run}\n\n${call}`} linkText="Copy" small withCopyIcon />
          </FlexContainer>
          <Spacing mt={1}><CommandStyle>{run}</CommandStyle></Spacing>
          {service.environment?.length >= 1 && (
            <Spacing mt={1}>
              <Text muted small>Set in .env:</Text>
              <Spacing mt={0.5}>
                {service.environment.map(variable => (
                  <PillStyle key={variable.name}>
                    {variable.name}{variable.secret ? ' · secret' : ''}
                  </PillStyle>
                ))}
              </Spacing>
            </Spacing>
          )}
          <Spacing mt={1}><CommandStyle>{call}</CommandStyle></Spacing>
          <Spacing mt={1}>
            <Text muted small>
              Schedules run with MAGE_SERVICE_SCHEDULES=on.{' '}
              <Text inline monospace small>mage-console</Text> inside the container opens a
              live dashboard.
            </Text>
          </Spacing>
        </div>
      </StepStyle>

      <StepStyle>
        <StepNumberStyle>4</StepNumberStyle>
        <div>
          <Text bold>Deploy it</Text>
          <Spacing mt={1}>
            {DEPLOY_TARGETS.map(target => (
              <a href={DEPLOY_URL} key={target} rel="noreferrer" target="_blank">
                <PillStyle>{target}</PillStyle>
              </a>
            ))}
          </Spacing>
          <Spacing mt={0.5}>
            <Text muted small>
              The build context holds a Kubernetes manifest. Other platforms run the image as it
              is.
            </Text>
          </Spacing>
        </div>
      </StepStyle>
    </>
  );
}

type PipelineServiceExportProps = {
  name?: string;
  onClose: () => void;
  onExported?: (service: ServiceExportType) => void;
  pipelineUUIDs: string[];
};

function PipelineServiceExport({
  name: nameInit,
  onClose,
  onExported,
  pipelineUUIDs: pipelineUUIDsInit,
}: PipelineServiceExportProps) {
  const [selected, setSelected] = useState<string[]>(
    (pipelineUUIDsInit || []).filter(Boolean),
  );
  const [search, setSearch] = useState<string>('');
  const [name, setName] = useState<string>(nameInit || null);
  const [maxRuns, setMaxRuns] = useState<string>('');
  const [showAdvanced, setShowAdvanced] = useState<boolean>(false);
  const [showNotes, setShowNotes] = useState<boolean>(false);
  const [preview, setPreview] = useState<ServiceExportType>(null);
  const [previewing, setPreviewing] = useState<boolean>(false);
  const [job, setJob] = useState<ServiceExportType>(null);
  const [shownProgress, setShownProgress] = useState<number>(0);
  const [service, setService] = useState<ServiceExportType>(null);
  const previewRequest = useRef<number>(0);

  const { data: dataPipelines } = api.pipelines.list({ _limit: 1000, include_schedules: 1 }, {
    revalidateOnFocus: false,
  });
  // The pipelines the dialog opened with come first, so they show as chosen.
  const pipelines: PipelineType[] = useMemo(() => {
    const all: PipelineType[] = dataPipelines?.pipelines || [];
    const first = all.filter(p => pipelineUUIDsInit?.includes(p.uuid));
    const rest = all.filter(p => !pipelineUUIDsInit?.includes(p.uuid));

    return [...first, ...rest];
  }, [dataPipelines, pipelineUUIDsInit]);
  const pipelinesShown = useMemo(() => {
    const term = search.trim().toLowerCase();

    return term
      ? pipelines.filter(p => `${p.uuid} ${p.name || ''}`.toLowerCase().includes(term))
      : pipelines;
  }, [pipelines, search]);

  const { project } = useProject();
  const nameToUse = name ?? defaultServiceName(selected, project?.name);
  const slug = slugServiceName(nameToUse);
  const maxRunsValue = maxRuns ? Number(maxRuns) : null;

  const toggle = (uuid: string) => setSelected(prev => (
    prev.includes(uuid) ? prev.filter(u => u !== uuid) : [...prev, uuid]
  ));

  const [requestPreview] = useMutation(api.pipeline_service_exports.useCreate());
  const [startExport, { isLoading: isStarting }] = useMutation(
    api.pipeline_service_exports.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_service_export: started }) => {
          setShownProgress(started?.progress || 0.05);
          setJob(started);
        },
        onErrorCallback: (response) => {
          setJob({
            name: slug,
            problems: [response?.error?.message || 'The export did not start.'],
            state: 'failed',
          });
        },
      }),
    },
  );

  // Preview what the service would hold each time the choice settles.
  useEffect(() => {
    if (!selected.length || !slug) {
      setPreview(null);
      setPreviewing(false);
      return;
    }
    const request = previewRequest.current + 1;
    previewRequest.current = request;
    setPreviewing(true);
    const timeout = setTimeout(() => {
      // @ts-ignore
      requestPreview({
        pipeline_service_export: {
          max_runs: maxRunsValue,
          name: slug,
          pipeline_uuids: selected,
          preview: true,
        },
      }).then((response: any) => {
        if (previewRequest.current !== request) {
          return;
        }
        setPreviewing(false);
        const body = response?.data || response;
        if (body?.pipeline_service_export) {
          setPreview(body.pipeline_service_export);
        } else if (body?.error) {
          setPreview({ name: slug, problems: [body.error.message] });
        }
      }).catch(() => {
        if (previewRequest.current === request) {
          setPreviewing(false);
        }
      });
    }, PREVIEW_DELAY_MS);

    return () => clearTimeout(timeout);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected.join(','), slug, maxRunsValue]);

  const running = job?.state === 'running';

  // Follow a running export.
  const { data: dataJob } = api.pipeline_service_exports.detail(
    running ? job?.name : null,
    {},
    { refreshInterval: running ? POLL_MS : 0 },
  );
  useEffect(() => {
    const latest: ServiceExportType = dataJob?.pipeline_service_export;
    if (!latest || !running || latest.name !== job?.name) {
      return;
    }
    if (latest.state === 'completed') {
      setShownProgress(1);
      setTimeout(() => {
        setService(latest);
        setJob(null);
        onExported?.(latest);
      }, 350);
    } else {
      setJob(latest);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataJob]);

  // Move the bar toward the end of the current stage while it runs.
  useEffect(() => {
    if (!running) {
      return;
    }
    const interval = setInterval(() => {
      setShownProgress((prev) => {
        const reported = job?.progress || 0;
        const [, ceiling] = STAGE_CEILINGS.find(([end]) => reported < end) || [1, 0.99];
        const floor = Math.max(prev, reported);

        return floor + Math.max(0, ceiling - floor) * 0.06;
      });
    }, 120);

    return () => clearInterval(interval);
  }, [running, job?.progress]);

  const problems = preview?.problems || [];
  const ready = !!preview && !problems.length && !previewing;
  const canExport = selected.length >= 1 && !!slug && !running && !isStarting
    && !(problems.length && !previewing);

  const header = (
    <HeaderStyle>
      <FlexContainer alignItems="flex-start" justifyContent="space-between">
        <div>
          <Text bold large>
            {service ? `${service.name} is ready to build` : 'Export as a service'}
          </Text>
          <Spacing mt={0.5}>
            <Text muted small>
              {service
                ? 'One Docker image that runs the pipelines without MageML.'
                : 'Run pipelines outside MageML as one Docker image: an HTTP API to start runs, their schedules, run history, logs and metrics.'}
            </Text>
          </Spacing>
        </div>
        <Button iconOnly noBackground noBorder onClick={onClose} title="Close">
          <Close size={UNIT * 2} />
        </Button>
      </FlexContainer>
    </HeaderStyle>
  );

  if (service) {
    const blocks = (service.pipelines || []).reduce((acc, p) => acc + p.blocks.length, 0);
    const schedules = (service.pipelines || []).reduce(
      (acc, p) => acc + p.triggers.filter(t => t.kind === 'time' && t.active).length,
      0,
    );

    return (
      <DialogStyle>
        {header}

        <div style={{ overflow: 'auto', padding: `${UNIT}px ${UNIT * 3}px` }}>
          <Spacing py={1}>
            <FlexContainer alignItems="center">
              <Check size={UNIT * 2} success />
              <Spacing ml={1} />
              <Text>
                {[
                  plural(service.pipelines?.length || 0, 'pipeline'),
                  plural(blocks, 'block'),
                  plural(schedules, 'schedule'),
                  plural(Object.keys(service.packages || {}).length, 'Python package'),
                ].join(' · ')}
              </Text>
            </FlexContainer>
          </Spacing>
          <ExportedSteps service={service} />
        </div>

        <FooterStyle>
          <FlexContainer justifyContent="space-between">
            <Button onClick={() => setService(null)}>Change and export again</Button>
            <Button onClick={onClose} primary>Done</Button>
          </FlexContainer>
        </FooterStyle>
      </DialogStyle>
    );
  }

  const allShownSelected = pipelinesShown.length >= 1
    && pipelinesShown.every(p => selected.includes(p.uuid));
  const blocksTotal = (preview?.pipelines || []).reduce((acc, p) => acc + p.blocks.length, 0);
  const schedulesTotal = (preview?.pipelines || []).reduce(
    (acc, p) => acc + p.triggers.filter(t => t.kind === 'time' && t.active).length,
    0,
  );
  const previewLanguages = languagesOf(
    (preview?.pipelines || []).reduce((acc, p) => acc.concat(p.blocks), []),
  );

  let status = null;
  if (!selected.length) {
    status = <Text muted>Choose the pipelines this service runs.</Text>;
  } else if (previewing && !preview) {
    status = (
      <FlexContainer alignItems="center">
        <Spinner inverted small />
        <Spacing ml={1} />
        <Text muted>Checking the pipelines</Text>
      </FlexContainer>
    );
  } else if (problems.length) {
    status = (
      <FlexContainer alignItems="center">
        <AlertTriangle danger size={UNIT * 2} />
        <Spacing ml={1} />
        <Text bold danger>
          {plural(problems.length, 'problem')} to fix before exporting
        </Text>
      </FlexContainer>
    );
  } else if (ready) {
    status = (
      <FlexContainer alignItems="center">
        <Check size={UNIT * 2} success />
        <Spacing ml={1} />
        <Text bold>Ready to export</Text>
      </FlexContainer>
    );
  } else {
    status = (
      <FlexContainer alignItems="center">
        <Spinner inverted small />
        <Spacing ml={1} />
        <Text muted>Updating</Text>
      </FlexContainer>
    );
  }

  return (
    <DialogStyle>
      {header}

      <BodyStyle>
        <div>
          <FlexContainer alignItems="center" justifyContent="space-between">
            <Text bold>Pipelines</Text>
            <div style={{ alignItems: 'center', display: 'flex', gap: UNIT * 1.5 }}>
              <Text muted small>{selected.length} selected</Text>
              <LinkButtonStyle
                disabled={allShownSelected}
                onClick={() => setSelected(prev => Array.from(new Set([
                  ...prev,
                  ...pipelinesShown.map(p => p.uuid),
                ])))}
                type="button"
              >
                {search ? 'Select shown' : 'Select all'}
              </LinkButtonStyle>
              <LinkButtonStyle
                disabled={!selected.length}
                onClick={() => setSelected([])}
                type="button"
              >
                Clear
              </LinkButtonStyle>
            </div>
          </FlexContainer>

          <Spacing mt={1}>
            <TextInput
              compact
              fullWidth
              monospace
              onChange={e => setSearch(e.target.value)}
              placeholder="Search pipelines"
              value={search}
            />
          </Spacing>

          <Spacing mt={1}>
            <ListStyle>
              {!dataPipelines && (
                <Spacing p={2}><Text muted small>Loading pipelines</Text></Spacing>
              )}
              {dataPipelines && !pipelinesShown.length && (
                <Spacing p={2}><Text muted small>No pipeline matches.</Text></Spacing>
              )}
              {pipelinesShown.map((pipeline) => {
                const isSelected = selected.includes(pipeline.uuid);
                const languages = languagesOf(pipeline.blocks || []);
                // @ts-ignore
                const triggers = pipeline.schedules?.length || 0;
                const meta = [
                  plural(pipeline.blocks?.length || 0, 'block'),
                  languages.join(', '),
                  triggers ? plural(triggers, 'trigger') : null,
                  pipeline.type && pipeline.type !== 'python' ? pipeline.type : null,
                ].filter(Boolean).join(' · ');

                return (
                  <RowStyle
                    key={pipeline.uuid}
                    onClick={() => toggle(pipeline.uuid)}
                    selected={isSelected}
                  >
                    <div style={{ paddingTop: 2, pointerEvents: 'none' }}>
                      <Checkbox checked={isSelected} />
                    </div>
                    <div style={{ minWidth: 0 }}>
                      <Text monospace>{pipeline.uuid}</Text>
                      <Text muted small>{meta}</Text>
                    </div>
                  </RowStyle>
                );
              })}
            </ListStyle>
          </Spacing>
        </div>

        <div>
          <TextInput
            fullWidth
            label="Service name"
            monospace
            onChange={e => setName(e.target.value)}
            value={nameToUse}
          />
          <Spacing mt={0.5}>
            <Text muted xsmall>
              {slug && slug !== nameToUse
                ? `Exported as ${slug}: the image and folder name.`
                : 'The image and folder name.'}
            </Text>
          </Spacing>

          <Spacing mt={2}>
            <CardStyle>
              {status}

              {preview && !problems.length && (
                <>
                  <Spacing mt={2}>
                    <TilesStyle>
                      {[
                        ['Pipelines', preview.pipelines?.length || 0],
                        ['Blocks', blocksTotal],
                        ['Schedules', schedulesTotal],
                        ['Packages', Object.keys(preview.packages || {}).length],
                      ].map(([label, value]) => (
                        <TileStyle key={label as string}>
                          <NumberStyle>{value}</NumberStyle>
                          <Text muted xsmall>{label}</Text>
                        </TileStyle>
                      ))}
                    </TilesStyle>
                  </Spacing>

                  {previewLanguages.length >= 1 && (
                    <Spacing mt={2}>
                      <Text muted small>Languages</Text>
                      <Spacing mt={0.5}>
                        {previewLanguages.map(language => (
                          <PillStyle key={language}>{language}</PillStyle>
                        ))}
                      </Spacing>
                    </Spacing>
                  )}

                  <Spacing mt={1.5}>
                    <Text muted small>Environment variables to set</Text>
                    <Spacing mt={0.5}>
                      {preview.environment?.length
                        ? preview.environment.map(variable => (
                          <PillStyle key={variable.name}>
                            {variable.name}{variable.secret ? ' · secret' : ''}
                          </PillStyle>
                        ))
                        : <Text small>None</Text>}
                    </Spacing>
                  </Spacing>

                  {preview.notes?.length >= 1 && (
                    <Spacing mt={1.5}>
                      <LinkButtonStyle onClick={() => setShowNotes(prev => !prev)} type="button">
                        {showNotes ? 'Hide' : 'Show'} {plural(preview.notes.length, 'note')}
                      </LinkButtonStyle>
                      {showNotes && preview.notes.map(note => (
                        <Spacing key={note} mt={1}>
                          <Text muted small>{note}</Text>
                        </Spacing>
                      ))}
                    </Spacing>
                  )}
                </>
              )}

              {problems.length >= 1 && (
                <Spacing mt={1}>
                  {problems.map(problem => (
                    <Spacing key={problem} mt={1}>
                      <Text small>{problem}</Text>
                    </Spacing>
                  ))}
                </Spacing>
              )}
            </CardStyle>
          </Spacing>

          <Spacing mt={2}>
            <LinkButtonStyle onClick={() => setShowAdvanced(prev => !prev)} type="button">
              {showAdvanced ? 'Hide advanced options' : 'Advanced options'}
            </LinkButtonStyle>
            {showAdvanced && (
              <Spacing mt={1}>
                <TextInput
                  fullWidth
                  label="Runs at once, per pipeline"
                  monospace
                  onChange={e => setMaxRuns(e.target.value.replace(/[^0-9]/g, ''))}
                  placeholder="From each pipeline's settings"
                  value={maxRuns}
                />
              </Spacing>
            )}
          </Spacing>
        </div>
      </BodyStyle>

      <FooterStyle>
        {(running || job?.state === 'failed') && (
          <Spacing mb={2}>
            {running && (
              <>
                <FlexContainer justifyContent="space-between">
                  <Text small>{job?.stage}</Text>
                  <Text monospace muted small>{Math.round(shownProgress * 100)}%</Text>
                </FlexContainer>
                <Spacing mt={1}>
                  <ProgressBar progress={shownProgress * 100} />
                </Spacing>
              </>
            )}
            {job?.state === 'failed' && (
              <>
                <FlexContainer alignItems="center">
                  <AlertTriangle danger size={UNIT * 2} />
                  <Spacing ml={1} />
                  <Text bold danger>The export stopped</Text>
                </FlexContainer>
                {(job.problems || []).map(problem => (
                  <Spacing key={problem} mt={1}><Text small>{problem}</Text></Spacing>
                ))}
              </>
            )}
          </Spacing>
        )}

        <FlexContainer alignItems="center" justifyContent="space-between">
          <a href={GUIDE_URL} rel="noreferrer" target="_blank">
            <Text muted small>How services work</Text>
          </a>
          <FlexContainer>
            <Button onClick={onClose}>Cancel</Button>
            <Spacing ml={1} />
            <Button
              beforeIcon={<CubeWithArrowDown />}
              disabled={!canExport}
              loading={running || isStarting}
              // @ts-ignore
              onClick={() => startExport({
                pipeline_service_export: {
                  max_runs: maxRunsValue,
                  name: slug,
                  pipeline_uuids: selected,
                },
              })}
              primary
            >
              {selected.length > 1 ? `Export ${selected.length} pipelines` : 'Export service'}
            </Button>
          </FlexContainer>
        </FlexContainer>
      </FooterStyle>
    </DialogStyle>
  );
}

export default PipelineServiceExport;
