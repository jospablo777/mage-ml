import { useEffect, useMemo, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import CopyToClipboard from '@oracle/components/CopyToClipboard';
import FlexContainer from '@oracle/components/FlexContainer';
import Link from '@oracle/elements/Link';
import ModelsSection, {
  ModelRowType,
  modelsOf,
  newModelRow,
  registryProblem,
  rowFromModel,
  rowProblem,
} from './ModelsSection';
import PipelineType from '@interfaces/PipelineType';
import ProgressBar from '@oracle/components/ProgressBar';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import {
  AlertTriangle,
  Check,
  Close,
  CubeWithArrowDown,
  Info,
} from '@oracle/icons';
import { BORDER_RADIUS, BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import {
  CommandStyle,
  DEPLOY_URL,
  ExportedSteps,
  GUIDE_URL,
  LinkButtonStyle,
  NumberStyle,
  PillStyle,
  ProblemLines,
  ServiceExportType,
  TileStyle,
  TilesStyle,
  languagesOf,
  modelProblem,
  plural,
  slugServiceName,
} from '@components/PipelineServiceExport';
import {
  FONT_FAMILY_MEDIUM,
  FONT_FAMILY_REGULAR,
  MONO_FONT_FAMILY_REGULAR,
} from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { initiateDownload } from '@utils/downloads';
import { onSuccess } from '@api/utils/response';
import { timeAgo } from '@utils/models/compilation';
import { useError } from '@context/Error';

const CHECK_DELAY_MS = 500;
const POLL_MS = 300;

export type AfterRunType = {
  after: string;
  // The database trigger, which can be removed here; triggers only in triggers.yaml have none.
  id?: number;
  name?: string;
  run: string;
  when: 'completed' | 'failed' | 'any';
};

type AfterRunTriggerType = {
  id: number;
  name: string;
  pipeline_uuid: string;
  schedule_type: string;
  settings?: { after_pipeline?: string; after_status?: string };
  status: string;
};

export type ServiceDefinitionType = {
  description?: string;
  exported_at?: string;
  include: string[];
  max_runs?: number;
  model_registry?: string;
  models?: { name: string; uri: string }[];
  name: string;
  pipelines: string[];
};

type ExportListItem = {
  download_token: string;
  exported_at: string;
  name: string;
  pipelines: string[];
};

const WHEN_LABELS = {
  any: 'ends',
  completed: 'completes',
  failed: 'fails',
};

// The settings the service itself reads, from docs/production/pipeline-services.mdx.
const SERVICE_VARIABLES = [
  {
    name: 'MAGE_SERVICE_TOKEN',
    secret: true,
    what: 'Token for every API call. Without it the service makes one at start and prints it in its log.',
  },
  {
    name: 'MAGE_SERVICE_READ_TOKEN',
    secret: true,
    what: 'Optional read-only token, for dashboards and clients that only read.',
  },
  {
    name: 'MAGE_SERVICE_SCHEDULES',
    what: 'Set to on to run the schedules. Off by default, so a service and MageML never both run them.',
  },
  {
    name: 'MAGE_SERVICE_LOG_FORMAT',
    what: 'Set to json for log collectors.',
  },
  {
    name: 'MAGE_SERVICE_RETENTION_DAYS',
    what: 'Days runs, logs and outputs are kept. 30 by default.',
  },
  {
    name: 'MAGE_SERVICE_STRICT_ENV',
    what: 'Set to on to refuse to start while a variable the code requires is missing.',
  },
];

const SECRET_SOURCES = [
  ['Environment', '-e NAME=value, --env-file .env, Kubernetes secretKeyRef, ECS secrets'],
  ['A file per secret', 'NAME_FILE=/run/secrets/name sets NAME from the file'],
  ['A folder of files', 'MAGE_SERVICE_SECRETS_DIR=/run/secrets, one variable per file'],
  ['A secrets manager', 'MAGE_SERVICE_SECRETS with ibm:, aws:, aws-ssm: or file: references'],
];

const PageStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: ${UNIT * 34}px minmax(0, 1fr);
  min-height: calc(100vh - ${UNIT * 16}px);

  @media (max-width: 1000px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const RailStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  height: fit-content;
  overflow: hidden;
  position: sticky;
  top: ${UNIT * 2}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}

  @media (max-width: 1000px) {
    position: static;
  }
`;

const RailItemStyle = styled.button<{ selected: boolean }>`
  background: none;
  border: none;
  cursor: pointer;
  display: block;
  padding: ${UNIT * 1.25}px ${UNIT * 1.5}px;
  text-align: left;
  width: 100%;

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

const SectionStyle = styled.section`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 2.5}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const TwoColumnsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);

  @media (max-width: 1400px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const HeaderStyle = styled.div`
  align-items: flex-start;
  display: flex;
  flex-wrap: wrap;
  gap: ${UNIT * 2}px;
  justify-content: space-between;
`;

const NameInputStyle = styled.input`
  background: none;
  border: none;
  border-bottom: 1px dashed transparent;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 22px;
  line-height: 32px;
  outline: none;
  padding: 0;
  width: 100%;

  ${props => `
    color: ${(props.theme.content || dark.content).active || (props.theme.content || dark.content).default};

    &:hover, &:focus {
      border-bottom-color: ${(props.theme.borders || dark.borders).light};
    }
  `}
`;

const DescriptionInputStyle = styled.input`
  background: none;
  border: none;
  font-family: ${FONT_FAMILY_REGULAR};
  font-size: 14px;
  outline: none;
  padding: 0;
  width: 100%;

  ${props => `
    color: ${(props.theme.content || dark.content).muted};
  `}
`;

const StatusPillStyle = styled.span<{ tone: 'ok' | 'bad' | 'neutral' }>`
  align-items: center;
  border-radius: ${UNIT * 2}px;
  display: inline-flex;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 12px;
  gap: ${UNIT / 2}px;
  line-height: 24px;
  padding: 0 ${UNIT * 1.25}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${props.tone === 'bad'
      ? (props.theme.accent || dark.accent).negative
      : (props.theme.content || dark.content).default};
  `}
`;

const PipelineRowStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr) auto;
  padding: ${UNIT * 1.5}px 0;

  ${props => `
    border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};
  `}

  &:last-child {
    border-bottom: none;
  }

  @media (max-width: 760px) {
    grid-template-columns: minmax(0, 1fr) auto;
  }
`;

const StartChipStyle = styled.span`
  align-items: center;
  border-radius: ${UNIT * 1.5}px;
  display: inline-flex;
  font-family: ${FONT_FAMILY_REGULAR};
  font-size: 12px;
  gap: ${UNIT / 2}px;
  line-height: 22px;
  margin: 0 ${UNIT / 2}px ${UNIT / 2}px 0;
  padding: 0 ${UNIT}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const ChipCloseStyle = styled.button`
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  padding: 0;
`;

const SelectStyle = styled.select`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  height: ${UNIT * 3.5}px;
  padding: 0 ${UNIT}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const FlowStyle = styled.div`
  align-items: center;
  display: flex;
  flex-wrap: wrap;
  gap: ${UNIT}px;
  padding: ${UNIT / 2}px 0;
`;

const FlowNodeStyle = styled.span`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 26px;
  padding: 0 ${UNIT}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const CheckRowStyle = styled.div`
  display: grid;
  gap: ${UNIT * 1.25}px;
  grid-template-columns: ${UNIT * 2.5}px minmax(0, 1fr);
  padding: ${UNIT}px 0;
`;

const TableStyle = styled.table`
  border-collapse: collapse;
  width: 100%;

  td, th {
    padding: ${UNIT * 0.75}px ${UNIT}px ${UNIT * 0.75}px 0;
    text-align: left;
    vertical-align: top;
    ${props => `border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};`}
  }
`;

const PickerStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  margin-top: ${UNIT}px;
  max-height: ${UNIT * 36}px;
  overflow: auto;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const PickerRowStyle = styled.div`
  cursor: pointer;
  display: flex;
  gap: ${UNIT}px;
  padding: ${UNIT * 0.75}px ${UNIT * 1.25}px;

  ${props => `
    border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};

    &:hover {
      background-color: ${(props.theme.interactive || dark.interactive).defaultBackgroundTransparent};
    }
  `}
`;

const EmptyStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 4}px;

  ${props => `
    border: 1px dashed ${(props.theme.borders || dark.borders).light};
  `}
`;

const StepsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: repeat(3, minmax(0, 1fr));

  @media (max-width: 900px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const StepCardStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  padding: ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
  `}
`;

function emptyDefinition(name: string): ServiceDefinitionType {
  return {
    description: '',
    include: [],
    max_runs: null,
    model_registry: null,
    models: [],
    name,
    pipelines: [],
  };
}

function comparable(definition: ServiceDefinitionType) {
  if (!definition) {
    return '';
  }
  const {
    description,
    include,
    max_runs: maxRuns,
    model_registry: registry,
    models,
    name,
    pipelines,
  } = definition;

  return JSON.stringify([
    name,
    description || '',
    pipelines,
    maxRuns || null,
    include,
    (registry || '').trim() || null,
    models || [],
  ]);
}

function envTemplate(
  service: ServiceExportType,
  hasSchedules: boolean,
): string {
  const lines = [
    '# Settings of the service',
    'MAGE_SERVICE_TOKEN=change-me',
    ...(hasSchedules ? ['MAGE_SERVICE_SCHEDULES=on'] : []),
    'MAGE_SERVICE_LOG_FORMAT=json',
  ];
  if (service?.environment?.length) {
    lines.push('', '# What the pipelines read');
    service.environment.forEach(({ name, required, secret }) => {
      lines.push(`${name}=${secret ? '   # secret' : ''}${required && !secret ? '   # required' : ''}`);
    });
  }

  return lines.join('\n');
}

// Chains of after-run triggers as paths, for the run flow: a -> b -> c.
function flowPaths(afterRuns: AfterRunType[]): { node: string; when?: string }[][] {
  const next: { [uuid: string]: AfterRunType[] } = {};
  const started = new Set<string>();
  afterRuns.forEach((a) => {
    next[a.after] = [...(next[a.after] || []), a];
    started.add(a.run);
  });
  const roots = Object.keys(next).filter(uuid => !started.has(uuid));
  const paths = [];
  const walk = (uuid: string, path: { node: string; when?: string }[], seen: Set<string>) => {
    const outgoing = next[uuid] || [];
    if (!outgoing.length || seen.has(uuid)) {
      paths.push(path);
      return;
    }
    outgoing.forEach((a) => {
      walk(a.run, [...path, { node: a.run, when: a.when }], new Set([...seen, uuid]));
    });
  };
  roots.forEach(root => walk(root, [{ node: root }], new Set()));

  return paths;
}

function Section({
  children,
  right,
  subtitle,
  title,
}: {
  children: any;
  right?: any;
  subtitle?: string;
  title: string;
}) {
  return (
    <SectionStyle>
      <FlexContainer alignItems="flex-start" justifyContent="space-between">
        <div>
          <Text bold large>{title}</Text>
          {subtitle && (
            <Spacing mt={0.5}><Text muted small>{subtitle}</Text></Spacing>
          )}
        </div>
        {right && <div style={{ flexShrink: 0, marginLeft: UNIT * 2 }}>{right}</div>}
      </FlexContainer>
      <Spacing mt={2}>{children}</Spacing>
    </SectionStyle>
  );
}

function CheckRow({
  children,
  detail,
  tone,
  title,
}: {
  children?: any;
  detail?: string;
  tone: 'ok' | 'bad' | 'note';
  title: string;
}) {
  return (
    <CheckRowStyle>
      <div style={{ paddingTop: 2 }}>
        {tone === 'ok' && <Check size={UNIT * 2} success />}
        {tone === 'bad' && <AlertTriangle danger size={UNIT * 2} />}
        {tone === 'note' && <Info muted size={UNIT * 2} />}
      </div>
      <div>
        <Text bold={tone !== 'note'} danger={tone === 'bad'}>{title}</Text>
        {detail && <Text muted small>{detail}</Text>}
        {children}
      </div>
    </CheckRowStyle>
  );
}

function Expandable({ label, children }: { children: any; label: string }) {
  const [open, setOpen] = useState(false);

  return (
    <>
      <LinkButtonStyle onClick={() => setOpen(prev => !prev)} type="button">
        {open ? 'Hide' : label}
      </LinkButtonStyle>
      {open && <Spacing mt={1}>{children}</Spacing>}
    </>
  );
}

function Workbench({
  definition: saved,
  isNew,
  onDeleted,
  onExported,
  onSaved,
  pipelinesByUUID,
  pipelinesAll,
}: {
  definition: ServiceDefinitionType;
  isNew: boolean;
  onDeleted: () => void;
  onExported: () => void;
  onSaved: (definition: ServiceDefinitionType) => void;
  pipelinesAll: PipelineType[];
  pipelinesByUUID: { [uuid: string]: PipelineType };
}) {
  const [showError] = useError(null, {}, [], { uuid: 'Services/Workbench' });
  const [draft, setDraft] = useState<ServiceDefinitionType>(saved);
  const [check, setCheck] = useState<ServiceExportType>(null);
  // The latest check without problems: its triggers stay on the rows while a change, such
  // as a loop, makes the current check fail.
  const [lastGoodCheck, setLastGoodCheck] = useState<ServiceExportType>(null);
  const [checking, setChecking] = useState<boolean>(false);
  const [picking, setPicking] = useState<boolean>(false);
  const [pickSearch, setPickSearch] = useState<string>('');
  const [includeInput, setIncludeInput] = useState<string>('');
  const [job, setJob] = useState<ServiceExportType>(null);
  const [shownProgress, setShownProgress] = useState<number>(0);
  const [exported, setExported] = useState<ServiceExportType>(null);
  const [modelRows, setModelRows] = useState<ModelRowType[]>(
    () => (saved.models || []).map(rowFromModel),
  );
  const [checkedFor, setCheckedFor] = useState<{ name: string; uri: string }[]>([]);
  const checkRequest = useRef<number>(0);

  // After-run triggers are the pipelines' own triggers: MageML runs them, and so does the
  // exported service.
  const { data: dataTriggers, mutate: mutateTriggers } = api.pipeline_schedules.list(
    { 'schedule_type[]': 'after_run', _limit: 1000 },
    { revalidateOnFocus: true },
  );
  const afterRunTriggers: AfterRunTriggerType[] = useMemo(
    () => (dataTriggers?.pipeline_schedules || []).filter(t => (
      t.schedule_type === 'after_run'
        && t.status === 'active'
        && draft.pipelines.includes(t.pipeline_uuid)
    )),
    [dataTriggers, draft.pipelines],
  );
  const fromDatabase: AfterRunType[] = useMemo(() => afterRunTriggers.map(t => ({
    after: t.settings?.after_pipeline,
    id: t.id,
    name: t.name,
    run: t.pipeline_uuid,
    when: (t.settings?.after_status || 'completed') as AfterRunType['when'],
  })), [afterRunTriggers]);
  const afterRunsKey = fromDatabase.map(a => `${a.id}:${a.after}:${a.run}:${a.when}`).join(',');

  const [createAfterRun] = useMutation(
    ({ after, run, when }: AfterRunType) => api.pipeline_schedules.pipelines.useCreate(run)({
      pipeline_schedule: {
        name: `after_${after}`,
        schedule_type: 'after_run',
        settings: { after_pipeline: after, after_status: when },
        status: 'active',
      },
    }),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => mutateTriggers(),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );
  const [deleteAfterRun] = useMutation(
    (id: number) => api.pipeline_schedules.useDelete(id)(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => mutateTriggers(),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  // A refreshed list hands a new object for the same service: keep the export's result,
  // and take the saved settings only when they changed.
  useEffect(() => {
    setExported(null);
    setJob(null);
  }, [saved.name]);
  useEffect(() => {
    setDraft(saved);
    // Rows still being filled in are kept while they add nothing the saved service lacks.
    setModelRows(prev => (JSON.stringify(modelsOf(prev)) === JSON.stringify(saved.models || [])
      ? prev
      : (saved.models || []).map(rowFromModel)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [comparable(saved)]);

  const dirty = isNew || comparable(draft) !== comparable(saved);
  const slug = slugServiceName(draft.name);
  const update = (values: Partial<ServiceDefinitionType>) => setDraft(prev => ({
    ...prev,
    ...values,
  }));
  const setRows = (rows: ModelRowType[]) => {
    setModelRows(rows);
    update({ models: modelsOf(rows) });
  };

  const [requestCheck] = useMutation(api.pipeline_service_exports.useCreate());
  useEffect(() => {
    if (!draft.pipelines.length || !slug) {
      setCheck(null);
      setChecking(false);
      return;
    }
    const request = checkRequest.current + 1;
    checkRequest.current = request;
    setChecking(true);
    const timeout = setTimeout(() => {
      // @ts-ignore
      requestCheck({
        pipeline_service_export: {
          include: draft.include,
          max_runs: draft.max_runs || null,
          model_registry: (draft.model_registry || '').trim() || null,
          models: draft.models || [],
          name: slug,
          pipeline_uuids: draft.pipelines,
          preview: true,
        },
      }).then((response: any) => {
        if (checkRequest.current !== request) {
          return;
        }
        setChecking(false);
        setCheckedFor(draft.models || []);
        const body = response?.data || response;
        if (body?.pipeline_service_export) {
          setCheck(body.pipeline_service_export);
          if (!body.pipeline_service_export.problems?.length) {
            setLastGoodCheck(body.pipeline_service_export);
          }
        } else if (body?.error) {
          setCheck({ name: slug, problems: [body.error.message] });
        }
      }).catch(() => {
        if (checkRequest.current === request) {
          setChecking(false);
        }
      });
    }, CHECK_DELAY_MS);

    return () => clearTimeout(timeout);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [comparable({ ...draft, description: '' }), slug, afterRunsKey]);

  const payload = {
    description: draft.description,
    include: draft.include,
    max_runs: draft.max_runs || null,
    model_registry: (draft.model_registry || '').trim() || null,
    models: draft.models || [],
    name: slug,
    pipelines: draft.pipelines,
  };

  const [save, { isLoading: isSaving }] = useMutation(
    () => (isNew
      ? api.pipeline_services.useCreate()({ pipeline_service: payload })
      : api.pipeline_services.useUpdate(saved.name)({ pipeline_service: payload })),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_service: result }) => onSaved(result),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const [remove] = useMutation(
    () => api.pipeline_services.useDelete(saved.name)(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => onDeleted(),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const [startExport, { isLoading: isStarting }] = useMutation(
    api.pipeline_service_exports.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_service_export: started }) => {
          setShownProgress(started?.progress || 0.05);
          setJob(started);
        },
        onErrorCallback: (response) => setJob({
          name: slug,
          problems: [response?.error?.message || 'The export did not start.'],
          state: 'failed',
        }),
      }),
    },
  );

  const running = job?.state === 'running';
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
        setExported(latest);
        setJob(null);
        onExported();
      }, 300);
    } else {
      setJob(latest);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dataJob]);
  useEffect(() => {
    if (!running) {
      return;
    }
    const interval = setInterval(() => {
      setShownProgress((prev) => {
        const reported = job?.progress || 0;
        const ceiling = reported < 0.6 ? 0.55 : 0.95;
        const floor = Math.max(prev, reported);

        return floor + Math.max(0, ceiling - floor) * 0.06;
      });
    }, 120);

    return () => clearInterval(interval);
  }, [running, job?.progress]);

  const exportNow = async () => {
    if (dirty) {
      // @ts-ignore
      await save();
    }
    setExported(null);
    // @ts-ignore
    startExport({
      pipeline_service_export: {
        include: draft.include,
        max_runs: draft.max_runs || null,
        model_registry: (draft.model_registry || '').trim() || null,
        models: draft.models || [],
        name: slug,
        pipeline_uuids: draft.pipelines,
      },
    });
  };

  const problems = check?.problems || [];
  const checkPipelines = check?.pipelines || [];
  const triggersByPipeline = useMemo(() => (lastGoodCheck?.pipelines || []).reduce(
    (acc, p) => ({
      ...acc,
      [p.uuid]: p.triggers.filter(t => t.kind !== 'after_run'),
    }),
    {},
  ), [lastGoodCheck]);
  // The check lists what the service will run, triggers.yaml included; the database lists
  // the triggers that can be removed here.
  const afterRuns: AfterRunType[] = useMemo(() => {
    const merged: { [key: string]: AfterRunType } = {};
    (lastGoodCheck?.pipelines || []).forEach(p => p.triggers
      .filter(t => t.kind === 'after_run')
      .forEach((t) => {
        // @ts-ignore
        merged[`${p.uuid}:${t.name}`] = { after: t.after_pipeline, name: t.name, run: p.uuid, when: t.after_status };
      }));
    fromDatabase.forEach((t) => {
      merged[`${t.run}:${t.name}`] = t;
    });

    return Object.values(merged).filter(a => draft.pipelines.includes(a.run));
  }, [draft.pipelines, fromDatabase, lastGoodCheck]);
  const blocksTotal = checkPipelines.reduce((acc, p) => acc + p.blocks.length, 0);
  const guarded = checkPipelines.flatMap(p => p.blocks.filter(b => b.conditions?.length));
  const schedules = checkPipelines.reduce(
    (acc, p) => acc + p.triggers.filter(t => t.kind === 'time' && t.active).length,
    0,
  );
  const languages = languagesOf(checkPipelines.reduce((acc, p) => acc.concat(p.blocks), []));
  const paths = flowPaths(afterRuns);

  let tone: 'ok' | 'bad' | 'neutral' = 'neutral';
  // Unfinished settings would be dropped on save: they hold saving and exporting.
  const unfinished = registryProblem(draft.model_registry)
    ? 'Fix the tracking server address'
    : (modelRows.some(row => rowProblem(row, modelRows)) ? 'Finish or remove the model rows' : null);
  let statusText = 'Add pipelines';
  if (unfinished) {
    tone = 'bad';
    statusText = unfinished;
  } else if (draft.pipelines.length) {
    if (checking && !check) {
      statusText = 'Checking';
    } else if (problems.length) {
      tone = 'bad';
      statusText = plural(problems.length, 'problem');
    } else if (check) {
      tone = 'ok';
      statusText = checking ? 'Checking' : 'Ready to export';
    }
  }

  const candidates = pipelinesAll.filter(p => !draft.pipelines.includes(p.uuid)
    && (!pickSearch || p.uuid.toLowerCase().includes(pickSearch.toLowerCase())));

  // @ts-ignore
  const addAfterRun = (run: string, after: string, when: AfterRunType['when']) => createAfterRun({
    after,
    run,
    when,
  });

  return (
    <div>
      <HeaderStyle>
        <div style={{ flex: '1 1 360px', minWidth: 0 }}>
          <NameInputStyle
            aria-label="Service name"
            onChange={e => update({ name: e.target.value })}
            spellCheck={false}
            value={draft.name}
          />
          <DescriptionInputStyle
            aria-label="Description"
            onChange={e => update({ description: e.target.value })}
            placeholder="What this service does, for the people who run it"
            value={draft.description || ''}
          />
          {slug !== draft.name && slug && (
            <Text muted xsmall>Saved and exported as {slug}</Text>
          )}
        </div>
        <FlexContainer alignItems="center">
          <StatusPillStyle tone={tone}>
            {tone === 'ok' && !checking && <Check size={UNIT * 1.5} success />}
            {tone === 'bad' && <AlertTriangle danger size={UNIT * 1.5} />}
            {checking && <Spinner inverted size={UNIT * 1.5} />}
            {statusText}
          </StatusPillStyle>
          <Spacing ml={1.5} />
          {!isNew && (
            <>
              <Button
                compact
                noBackground
                onClick={() => {
                  if (typeof window !== 'undefined'
                    && window.confirm(`Delete service ${saved.name}? Its exports stay.`)) {
                    // @ts-ignore
                    remove();
                  }
                }}
              >
                Delete
              </Button>
              <Spacing ml={1} />
            </>
          )}
          <Button
            disabled={!dirty || !slug || !!unfinished}
            loading={isSaving}
            onClick={() => save()}
          >
            {dirty ? 'Save' : 'Saved'}
          </Button>
          <Spacing ml={1} />
          <Button
            beforeIcon={<CubeWithArrowDown />}
            disabled={!draft.pipelines.length || !slug || problems.length > 0 || running
              || !!unfinished}
            loading={running || isStarting}
            onClick={exportNow}
            primary
          >
            {dirty ? 'Save and export' : 'Export'}
          </Button>
        </FlexContainer>
      </HeaderStyle>

      {(running || job?.state === 'failed' || exported) && (
        <Spacing mt={2}>
          <SectionStyle>
            {running && (
              <>
                <FlexContainer justifyContent="space-between">
                  <Text>{job?.stage}</Text>
                  <Text monospace muted small>{Math.round(shownProgress * 100)}%</Text>
                </FlexContainer>
                <Spacing mt={1}><ProgressBar progress={shownProgress * 100} /></Spacing>
              </>
            )}
            {job?.state === 'failed' && (
              <CheckRow title="The export stopped" tone="bad">
                <ProblemLines problems={job.problems} />
              </CheckRow>
            )}
            {exported && (
              <>
                <FlexContainer alignItems="center" justifyContent="space-between">
                  <FlexContainer alignItems="center">
                    <Check size={UNIT * 2} success />
                    <Spacing ml={1} />
                    <Text bold large>{exported.name} is ready to build</Text>
                  </FlexContainer>
                  <LinkButtonStyle onClick={() => setExported(null)} type="button">
                    Hide
                  </LinkButtonStyle>
                </FlexContainer>
                <ExportedSteps service={exported} />
              </>
            )}
          </SectionStyle>
        </Spacing>
      )}

      <Spacing mt={2}>
        <Section
          right={(
            <Button compact onClick={() => setPicking(prev => !prev)}>
              {picking ? 'Done adding' : 'Add pipelines'}
            </Button>
          )}
          subtitle="Each pipeline keeps its triggers. Start one after another's runs to make a flow: it adds a trigger to the pipeline, which MageML runs too."
          title="Pipelines and how they start"
        >
          {picking && (
            <Spacing mb={2}>
              <TextInput
                compact
                fullWidth
                monospace
                onChange={e => setPickSearch(e.target.value)}
                placeholder="Search pipelines to add"
                value={pickSearch}
              />
              <PickerStyle>
                {!candidates.length && (
                  <Spacing p={1.5}><Text muted small>No other pipelines.</Text></Spacing>
                )}
                {candidates.map(p => (
                  <PickerRowStyle
                    key={p.uuid}
                    onClick={() => update({ pipelines: [...draft.pipelines, p.uuid] })}
                  >
                    <div style={{ pointerEvents: 'none' }}><Checkbox checked={false} /></div>
                    <div>
                      <Text monospace small>{p.uuid}</Text>
                      <Text muted xsmall>
                        {[
                          plural(p.blocks?.length || 0, 'block'),
                          languagesOf(p.blocks || []).join(', '),
                        ].filter(Boolean).join(' · ')}
                      </Text>
                    </div>
                  </PickerRowStyle>
                ))}
              </PickerStyle>
            </Spacing>
          )}

          {!draft.pipelines.length && (
            <EmptyStyle>
              <Text>A service runs one or more pipelines.</Text>
              <Spacing mt={0.5}>
                <Text muted small>
                  For example one that loads raw data and computes features, and one that
                  scores them with a model right after it.
                </Text>
              </Spacing>
              <Spacing mt={1.5}>
                <Button onClick={() => setPicking(true)} primary>Add pipelines</Button>
              </Spacing>
            </EmptyStyle>
          )}

          {draft.pipelines.map((uuid) => {
            const pipeline = pipelinesByUUID[uuid];
            const ownTriggers = triggersByPipeline[uuid] || [];
            const startsAfter = afterRuns.filter(a => a.run === uuid);
            const others = draft.pipelines.filter(p => p !== uuid);

            return (
              <PipelineRowStyle key={uuid}>
                <div style={{ minWidth: 0 }}>
                  <Link href={`/pipelines/${uuid}/edit`} monospace>{uuid}</Link>
                  <Text muted small>
                    {pipeline
                      ? [
                        plural(pipeline.blocks?.length || 0, 'block'),
                        languagesOf(pipeline.blocks || []).join(', '),
                      ].filter(Boolean).join(' · ')
                      : 'Not in the project'}
                  </Text>
                </div>
                <div>
                  {ownTriggers.map(t => (
                    <StartChipStyle key={t.name} title={t.name}>
                      {t.kind === 'time' ? `On schedule ${t.schedule}` : 'API'}
                      {t.active ? '' : ' (inactive)'}
                    </StartChipStyle>
                  ))}
                  {!ownTriggers.length && !startsAfter.length && lastGoodCheck && (
                    <StartChipStyle>API calls only</StartChipStyle>
                  )}
                  {startsAfter.map(a => (
                    <StartChipStyle
                      key={`${a.run}:${a.name}`}
                      title={a.id
                        ? 'A trigger of this pipeline: MageML runs it too'
                        : 'In the pipeline\'s triggers.yaml; edit it there'}
                    >
                      After {a.after} {WHEN_LABELS[a.when] || WHEN_LABELS.completed}
                      {!a.id && <Text inline muted xsmall>&nbsp;triggers.yaml</Text>}
                      {a.id && (
                      <ChipCloseStyle
                        aria-label={`Remove: after ${a.after}`}
                        onClick={() => {
                          if (typeof window === 'undefined' || window.confirm(
                            `Delete the trigger that starts ${uuid} after ${a.after}? `
                              + 'MageML stops running it too.',
                          )) {
                            // @ts-ignore
                            deleteAfterRun(a.id);
                          }
                        }}
                        type="button"
                      >
                        <Close muted size={UNIT * 1.25} />
                      </ChipCloseStyle>
                      )}
                    </StartChipStyle>
                  ))}
                  {others.length >= 1 && (
                    <StartAfter
                      onAdd={(after, when) => addAfterRun(uuid, after, when)}
                      options={others}
                    />
                  )}
                </div>
                <div>
                  <Button
                    compact
                    iconOnly
                    noBackground
                    onClick={() => update({
                      pipelines: draft.pipelines.filter(p => p !== uuid),
                    })}
                    title={`Remove ${uuid} from the service`}
                  >
                    <Close muted size={UNIT * 1.5} />
                  </Button>
                </div>
              </PipelineRowStyle>
            );
          })}

          {paths.length >= 1 && (
            <Spacing mt={2}>
              <Text muted small>Run flow</Text>
              {paths.map(path => (
                <FlowStyle key={path.map(p => p.node).join('>')}>
                  {path.map((step, idx) => (
                    <FlexContainer alignItems="center" key={`${step.node}-${idx}`}>
                      {idx > 0 && (
                        <Text muted small>
                          &nbsp;{WHEN_LABELS[step.when]} &rarr;&nbsp;
                        </Text>
                      )}
                      <FlowNodeStyle>{step.node}</FlowNodeStyle>
                    </FlexContainer>
                  ))}
                </FlowStyle>
              ))}
            </Spacing>
          )}
        </Section>
      </Spacing>

      <Spacing mt={2}>
        <TwoColumnsStyle>
          <Section
            subtitle="The same checks the export runs, updated as you change the service."
            title="Configuration check"
          >
            {!draft.pipelines.length && <Text muted>Add pipelines to check the service.</Text>}
            {draft.pipelines.length >= 1 && !check && checking && (
              <FlexContainer alignItems="center">
                <Spinner inverted small />
                <Spacing ml={1} />
                <Text muted>Checking the pipelines</Text>
              </FlexContainer>
            )}
            {problems.map((problem) => {
              const model = modelProblem(problem);

              return model ? (
                <CheckRow
                  detail={model.reason}
                  key={problem}
                  title={`The model ${model.model} could not be ${model.action}`}
                  tone="bad"
                />
              ) : <CheckRow key={problem} title={problem} tone="bad" />;
            })}
            {check && !problems.length && (
              <>
                <TilesStyle>
                  {[
                    ['Pipelines', checkPipelines.length],
                    ['Blocks', blocksTotal],
                    ['Schedules', schedules],
                    ['After-run', afterRuns.length],
                  ].map(([label, value]) => (
                    <TileStyle key={label as string}>
                      <NumberStyle>{value}</NumberStyle>
                      <Text muted xsmall>{label}</Text>
                    </TileStyle>
                  ))}
                </TilesStyle>
                <Spacing mt={1} />
                <CheckRow
                  detail={`${languages.join(', ')}: every block runs in the service.`}
                  title={`${plural(blocksTotal, 'block')} supported`}
                  tone="ok"
                />
                {guarded.length >= 1 && (
                  <CheckRow
                    detail={'When a condition returns False the block and the blocks after it '
                      + 'are skipped, and the run still completes, as in MageML.'}
                    title={`${plural(guarded.length, 'block')} ${guarded.length === 1 ? 'runs' : 'run'} only when ${guarded.length === 1 ? 'its condition holds' : 'their conditions hold'}`}
                    tone="ok"
                  >
                    <Expandable label="Show conditions">
                      {guarded.map(b => (
                        <Text key={b.uuid} monospace xsmall>
                          {b.uuid}: {b.conditions.join(', ')}
                        </Text>
                      ))}
                    </Expandable>
                  </CheckRow>
                )}
                <CheckRow
                  detail="Modules the blocks import and files they name, copied into the image."
                  title={plural(check.files?.length || 0, 'project file')}
                  tone="ok"
                >
                  {check.files?.length >= 1 && (
                    <Expandable label="Show files">
                      {check.files.map(f => <Text key={f} monospace xsmall>{f}</Text>)}
                    </Expandable>
                  )}
                </CheckRow>
                <CheckRow
                  detail="Only what the blocks import, pinned to the versions MageML runs with."
                  title={plural(Object.keys(check.packages || {}).length, 'Python package')}
                  tone="ok"
                >
                  {Object.keys(check.packages || {}).length >= 1 && (
                    <Expandable label="Show packages">
                      {Object.entries(check.packages).map(([n, v]) => (
                        <Text key={n} monospace xsmall>{n}=={v}</Text>
                      ))}
                    </Expandable>
                  )}
                </CheckRow>
                <CheckRow
                  detail={afterRuns.length
                    ? 'No loops: every flow ends.'
                    : 'Every pipeline also starts from the API: POST /v1/pipelines/<name>/runs.'}
                  title={schedules
                    ? `${plural(schedules, 'schedule')} ${schedules === 1 ? 'runs' : 'run'} when MAGE_SERVICE_SCHEDULES=on`
                    : 'Runs start from the API'}
                  tone="ok"
                />
                {(check.notes || []).map(note => (
                  <CheckRow key={note} title={note} tone="note" />
                ))}
              </>
            )}
          </Section>

          <Section
            right={check && !problems.length ? (
              <CopyToClipboard
                copiedText={envTemplate(check, schedules > 0)}
                linkText="Copy .env"
                small
                withCopyIcon
              />
            ) : null}
            subtitle="Found in io_config.yaml (env_var) and in the blocks (os.getenv, os.environ, mage_secret_var). Give them to the container; values are never exported."
            title="Secrets and environment variables"
          >
            <Text bold small>What the pipelines read</Text>
            <Spacing mt={0.5}>
              {!draft.pipelines.length && (
                <Text muted small>Add pipelines to see the variables they read.</Text>
              )}
              {draft.pipelines.length >= 1 && !check && checking && (
                <FlexContainer alignItems="center">
                  <Spinner inverted small />
                  <Spacing ml={1} />
                  <Text muted small>Checking the pipelines</Text>
                </FlexContainer>
              )}
              {draft.pipelines.length >= 1 && problems.length >= 1 && !check?.environment && (
                <Text muted small>
                  Fix the problems in the configuration check to see them.
                </Text>
              )}
              {draft.pipelines.length >= 1 && check?.environment && !check.environment.length && (
                <Text muted small>
                  Nothing: the pipelines read no environment variable.
                </Text>
              )}
              {check?.environment?.length >= 1 && (
                <TableStyle>
                  <tbody>
                    {check.environment.map(v => (
                      <tr key={v.name}>
                        <td><Text monospace small>{v.name}</Text></td>
                        <td>
                          <Text muted small>
                            {[v.secret ? 'secret' : null, v.required ? 'required' : 'optional']
                              .filter(Boolean).join(', ')}
                          </Text>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </TableStyle>
              )}
            </Spacing>

            <Spacing mt={2}>
              <Text bold small>What the service reads</Text>
              <Spacing mt={0.5}>
                <TableStyle>
                  <tbody>
                    {SERVICE_VARIABLES.map(v => (
                      <tr key={v.name}>
                        <td style={{ whiteSpace: 'nowrap', width: '1%' }}>
                          <Text monospace small>{v.name}</Text>
                          {v.secret && <Text muted xsmall>secret</Text>}
                        </td>
                        <td>
                          <Text muted small>
                            {v.name === 'MAGE_SERVICE_SCHEDULES' && schedules > 0
                              ? `Set to on: this service has ${plural(schedules, 'schedule')}.`
                              : v.what}
                          </Text>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </TableStyle>
              </Spacing>
            </Spacing>

            <Spacing mt={2}>
              <Text bold small>Where secrets can come from</Text>
              {SECRET_SOURCES.map(([title, how]) => (
                <Spacing key={title} mt={0.75}>
                  <Text small>{title}</Text>
                  <Text monospace muted xsmall>{how}</Text>
                </Spacing>
              ))}
            </Spacing>
          </Section>
        </TwoColumnsStyle>
      </Spacing>

      <Spacing mt={2}>
        <Section
          right={(
            <Button compact onClick={() => setRows([...modelRows, newModelRow()])}>
              Add a model
            </Button>
          )}
          subtitle="Pulled from the registry when you export and kept in the image: blocks load the embedded copy, so the service runs the exact version it was exported with and needs no registry at run time."
          title="Models in the image"
        >
          <ModelsSection
            check={check}
            checkedFor={checkedFor}
            checking={checking}
            registry={draft.model_registry}
            rows={modelRows}
            setRegistry={value => update({ model_registry: value })}
            setRows={setRows}
          />
        </Section>
      </Spacing>

      <Spacing mt={2}>
        <TwoColumnsStyle>
          <Section
            subtitle="The service keeps them in its data folder; give that folder a volume so they outlive the container."
            title="Keep run history, logs and metrics"
          >
            <CheckRow
              detail="Runs, block logs and outputs live in /var/lib/mage-service. Mount a volume there:"
              title="Run history and logs"
              tone="note"
            >
              <Spacing mt={1}>
                <CommandStyle>
                  {`docker run -v ${slug || 'service'}-data:/var/lib/mage-service ...`}
                </CommandStyle>
              </Spacing>
              <Spacing mt={1}>
                <Text muted small>
                  Kubernetes: deploy/kubernetes.yaml in the build context claims a volume. ECS:
                  an EFS volume. Cloud Run and Code Engine without a volume keep history as long
                  as the instance. MAGE_SERVICE_RETENTION_DAYS sets how long runs are kept.
                </Text>
              </Spacing>
            </CheckRow>
            <CheckRow
              detail="Every line goes to stdout. With MAGE_SERVICE_LOG_FORMAT=json, CloudWatch, Cloud Logging, Loki or IBM Log Analysis keep and search them."
              title="Logs in your platform"
              tone="note"
            />
            <CheckRow
              detail="GET /metrics serves Prometheus metrics without a token. Runs by pipeline and status come from the run history, so they outlive restarts with the volume; block durations and failures restart with the service, and Prometheus keeps their history."
              title="Metrics"
              tone="note"
            >
              <Spacing mt={1}>
                <CommandStyle>
                  {`scrape_configs:\n  - job_name: ${slug || 'service'}\n    static_configs:\n      - targets: ['${slug || 'service'}:8080']`}
                </CommandStyle>
              </Spacing>
            </CheckRow>
          </Section>

          <Section title="Settings">
            <Text bold small>Runs of one pipeline at the same time</Text>
            <Text muted small>
              The most runs of each pipeline the service runs together. A run that starts
              while that many are running waits in the queue and starts when one finishes.
              Raise it for pipelines called often through the API; keep 1 for pipelines that
              must not overlap, such as an incremental load that moves a cursor.
            </Text>
            <Spacing mt={1}>
              <TextInput
                aria-label="Runs of one pipeline at the same time"
                compact
                fullWidth
                monospace
                onChange={(e) => {
                  const value = e.target.value.replace(/[^0-9]/g, '');
                  update({ max_runs: value ? Number(value) : null });
                }}
                placeholder="Empty: each pipeline's own limit, 1 when it sets none"
                value={draft.max_runs ? String(draft.max_runs) : ''}
              />
            </Spacing>
            <Spacing mt={2}>
              <Text bold small>Extra files in the image</Text>
              <Text muted small>
                Files the blocks reach by a computed path, such as a model chosen at run time.
                Paths the blocks name are found without this.
              </Text>
              <Spacing mt={1}>
                {draft.include.map(path => (
                  <PillStyle key={path}>
                    {path}{' '}
                    <ChipCloseStyle
                      aria-label={`Remove ${path}`}
                      onClick={() => update({ include: draft.include.filter(p => p !== path) })}
                      type="button"
                    >
                      <Close muted size={UNIT * 1.25} />
                    </ChipCloseStyle>
                  </PillStyle>
                ))}
              </Spacing>
              <FlexContainer>
                <div style={{ flex: 1 }}>
                  <TextInput
                    compact
                    fullWidth
                    monospace
                    onChange={e => setIncludeInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && includeInput.trim()) {
                        update({ include: [...draft.include, includeInput.trim()] });
                        setIncludeInput('');
                      }
                    }}
                    placeholder="models/churn.onnx"
                    value={includeInput}
                  />
                </div>
                <Spacing ml={1} />
                <Button
                  compact
                  disabled={!includeInput.trim()}
                  onClick={() => {
                    update({ include: [...draft.include, includeInput.trim()] });
                    setIncludeInput('');
                  }}
                >
                  Add
                </Button>
              </FlexContainer>
            </Spacing>
            <Spacing mt={2}>
              <Text muted small>
                Saved in pipeline_services.yaml in the project, so it goes with your code. The
                CLI exports it too:
              </Text>
              <Spacing mt={1}>
                <CommandStyle>
                  {`mage export service . --service ${slug || 'name'} --out ./${slug || 'name'}`}
                </CommandStyle>
              </Spacing>
            </Spacing>
          </Section>
        </TwoColumnsStyle>
      </Spacing>
    </div>
  );
}

function StartAfter({
  onAdd,
  options,
}: {
  onAdd: (after: string, when: AfterRunType['when']) => void;
  options: string[];
}) {
  const [open, setOpen] = useState(false);
  const [after, setAfter] = useState<string>(options[0]);
  const [when, setWhen] = useState<AfterRunType['when']>('completed');

  useEffect(() => {
    if (!options.includes(after)) {
      setAfter(options[0]);
    }
  }, [after, options]);

  if (!open) {
    return (
      <LinkButtonStyle onClick={() => setOpen(true)} type="button">
        + Start after another pipeline
      </LinkButtonStyle>
    );
  }

  return (
    <FlexContainer alignItems="center" flexWrap="wrap">
      <Text muted small>After&nbsp;</Text>
      <SelectStyle aria-label="Pipeline to follow" onChange={e => setAfter(e.target.value)} value={after}>
        {options.map(o => <option key={o} value={o}>{o}</option>)}
      </SelectStyle>
      <Spacing ml={1} />
      <SelectStyle
        aria-label="Which endings"
        onChange={e => setWhen(e.target.value as AfterRunType['when'])}
        value={when}
      >
        <option value="completed">completes</option>
        <option value="failed">fails</option>
        <option value="any">ends either way</option>
      </SelectStyle>
      <Spacing ml={1} />
      <Button
        compact
        onClick={() => {
          onAdd(after, when);
          setOpen(false);
        }}
        primary
      >
        Add
      </Button>
      <Spacing ml={1} />
      <LinkButtonStyle onClick={() => setOpen(false)} type="button">Cancel</LinkButtonStyle>
    </FlexContainer>
  );
}

// Services saved in the project, and a workbench to check, change and export one.
function Services() {
  const { data, mutate } = api.pipeline_services.list({}, { revalidateOnFocus: false });
  const { data: dataExports, mutate: mutateExports } = api.pipeline_service_exports.list(
    {},
    { revalidateOnFocus: false },
  );
  const { data: dataPipelines } = api.pipelines.list(
    { _limit: 1000, include_schedules: 1 },
    { revalidateOnFocus: false },
  );
  const services: ServiceDefinitionType[] = useMemo(
    () => data?.pipeline_services || [],
    [data],
  );
  const exportsList: ExportListItem[] = useMemo(
    () => dataExports?.pipeline_service_exports || [],
    [dataExports],
  );
  const pipelinesAll: PipelineType[] = useMemo(
    () => dataPipelines?.pipelines || [],
    [dataPipelines],
  );
  const pipelinesByUUID = useMemo(
    () => pipelinesAll.reduce((acc, p) => ({ ...acc, [p.uuid]: p }), {}),
    [pipelinesAll],
  );
  const [selectedName, setSelectedName] = useState<string>(null);
  const [newDraft, setNewDraft] = useState<ServiceDefinitionType>(null);

  useEffect(() => {
    if (!selectedName && !newDraft && services.length) {
      setSelectedName(services[0].name);
    }
  }, [newDraft, selectedName, services]);

  const selected = newDraft || services.find(s => s.name === selectedName);
  const unsavedExports = exportsList.filter(e => !services.some(s => s.name === e.name));

  const startNew = () => {
    const taken = new Set(services.map(s => s.name));
    let name = 'new-service';
    let index = 2;
    while (taken.has(name)) {
      name = `new-service-${index}`;
      index += 1;
    }
    setNewDraft(emptyDefinition(name));
    setSelectedName(null);
  };

  return (
    <PageStyle>
      <div>
        <RailStyle>
          <Spacing p={1.5}>
            <FlexContainer alignItems="center" justifyContent="space-between">
              <Text bold>Services</Text>
              <Button compact onClick={startNew} primary>New service</Button>
            </FlexContainer>
          </Spacing>
          {newDraft && (
            <RailItemStyle selected type="button">
              <Text monospace>{newDraft.name}</Text>
              <Text muted xsmall>Not saved yet</Text>
            </RailItemStyle>
          )}
          {services.map(service => (
            <RailItemStyle
              key={service.name}
              onClick={() => {
                setNewDraft(null);
                setSelectedName(service.name);
              }}
              selected={!newDraft && service.name === selectedName}
              type="button"
            >
              <Text monospace>{service.name}</Text>
              <Text muted xsmall>
                {plural(service.pipelines.length, 'pipeline')}
                {' · '}
                {service.exported_at
                  ? `exported ${timeAgo(new Date(service.exported_at).getTime() / 1000)}`
                  : 'not exported'}
              </Text>
            </RailItemStyle>
          ))}
          {data && !services.length && !newDraft && (
            <Spacing p={1.5}><Text muted small>No saved services yet.</Text></Spacing>
          )}
          {unsavedExports.length >= 1 && (
            <Spacing p={1.5}>
              <Text muted xsmall>Exported from the pipelines list</Text>
              {unsavedExports.map(e => (
                <Spacing key={e.name} mt={1}>
                  <FlexContainer alignItems="center" justifyContent="space-between">
                    <div style={{ minWidth: 0 }}>
                      <Text monospace small>{e.name}</Text>
                      <Text muted xsmall>
                        {timeAgo(new Date(e.exported_at).getTime() / 1000)}
                      </Text>
                    </div>
                    <FlexContainer>
                      <LinkButtonStyle
                        onClick={() => initiateDownload(e.download_token)}
                        type="button"
                      >
                        .zip
                      </LinkButtonStyle>
                      <Spacing ml={1} />
                      <LinkButtonStyle
                        onClick={() => {
                          setNewDraft({ ...emptyDefinition(e.name), pipelines: e.pipelines });
                          setSelectedName(null);
                        }}
                        type="button"
                      >
                        Save
                      </LinkButtonStyle>
                    </FlexContainer>
                  </FlexContainer>
                </Spacing>
              ))}
            </Spacing>
          )}
        </RailStyle>
        <Spacing mt={1.5} px={0.5}>
          <a href={GUIDE_URL} rel="noreferrer" target="_blank">
            <Text muted small>How services work</Text>
          </a>
          <Spacing mt={0.5} />
          <a href={DEPLOY_URL} rel="noreferrer" target="_blank">
            <Text muted small>Where to deploy them</Text>
          </a>
        </Spacing>
      </div>

      <div>
        {!selected && data && (
          <EmptyStyle>
            <Text bold large>Package pipelines as a service</Text>
            <Spacing mt={1}>
              <Text muted>
                One Docker image that runs your pipelines without MageML: an HTTP API to start
                runs, their schedules, runs that start after other runs, run history, logs and
                metrics.
              </Text>
            </Spacing>
            <Spacing mt={3}>
              <StepsStyle>
                {[
                  ['1', 'Pick the pipelines', 'Features and scoring, ETL and its checks: any mix of Python, R, Rust and SQL.'],
                  ['2', 'Check it here', 'Blocks, packages, secrets and run flow, checked as you change them.'],
                  ['3', 'Export and deploy', 'Download the build context; run it on Kubernetes, Cloud Run, Code Engine, Container Apps or ECS.'],
                ].map(([number, title, detail]) => (
                  <StepCardStyle key={number}>
                    <NumberStyle>{number}</NumberStyle>
                    <Spacing mt={1}><Text bold>{title}</Text></Spacing>
                    <Spacing mt={0.5}><Text muted small>{detail}</Text></Spacing>
                  </StepCardStyle>
                ))}
              </StepsStyle>
            </Spacing>
            <Spacing mt={3}>
              <Button onClick={startNew} primary>New service</Button>
            </Spacing>
          </EmptyStyle>
        )}
        {selected && (
          <Workbench
            definition={selected}
            isNew={!!newDraft}
            key={newDraft ? `new-${newDraft.name}` : selected.name}
            onDeleted={() => {
              setSelectedName(null);
              mutate();
            }}
            onExported={() => {
              mutate();
              mutateExports();
            }}
            onSaved={(result) => {
              setNewDraft(null);
              setSelectedName(result.name);
              mutate();
              mutateExports();
            }}
            pipelinesAll={pipelinesAll}
            pipelinesByUUID={pipelinesByUUID}
          />
        )}
      </div>
    </PageStyle>
  );
}

export default Services;
