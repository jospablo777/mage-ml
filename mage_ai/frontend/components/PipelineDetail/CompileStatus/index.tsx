import { useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled, { ThemeContext } from 'styled-components';

import Button from '@oracle/elements/Button';
import ClickOutside from '@oracle/components/ClickOutside';
import FlexContainer from '@oracle/components/FlexContainer';
import KeyboardShortcutButton from '@oracle/elements/Button/KeyboardShortcutButton';
import PipelineType from '@interfaces/PipelineType';
import ProgressBar from '@oracle/components/ProgressBar';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import Tooltip from '@oracle/components/Tooltip';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import {
  AlertTriangle,
  Check,
  Info,
  Lightning,
} from '@oracle/icons';
import {
  CompileStatusType,
  CompilationUnitType,
  LANGUAGE_NAMES,
  PipelineCompilationType,
  compileStatus,
  polyglotHint,
  progressPercent,
  timeAgo,
} from '@utils/models/compilation';
import { FONT_FAMILY_MEDIUM } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

// Seconds the header keeps checking after a save, for the compilation the save starts
// (the server waits for saves to stop for 3 s before it compiles).
const POLL_AFTER_SAVE_SECONDS = 10;

const PanelStyle = styled.div`
  border-radius: ${UNIT}px;
  padding: ${UNIT * 2}px;
  position: absolute;
  right: 0;
  top: calc(100% + ${UNIT / 2}px);
  width: ${UNIT * 52}px;
  z-index: 100;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    box-shadow: ${(props.theme.shadow || dark.shadow).popup};
  `}
`;

const StripStyle = styled.div`
  border-radius: 0 0 ${UNIT / 2}px ${UNIT / 2}px;
  bottom: 0;
  height: 2px;
  left: 0;
  overflow: hidden;
  position: absolute;
  right: 0;
`;

const HintStyle = styled.div`
  border-radius: ${UNIT}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    background-color: ${(props.theme.interactive || dark.interactive).defaultBackgroundTransparent};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const LanguagePillStyle = styled.span`
  border-radius: ${UNIT * 1.5}px;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 12px;
  line-height: 20px;
  padding: 0 ${UNIT}px;
  white-space: nowrap;

  ${props => `
    background-color: ${(props.theme.interactive || dark.interactive).defaultBackground};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const SwitchStyle = styled.span`
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 12px;
  padding: 0 ${UNIT / 2}px;

  ${props => `
    color: ${(props.theme.accent || dark.accent).warning};
  `}
`;

const UnitRowStyle = styled.div`
  padding: ${UNIT * 0.75}px 0;

  ${props => `
    border-top: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const HEADER_LABELS: { [status in CompileStatusType]: string } = {
  compiling: 'Compiling',
  failed: 'Compile failed',
  never: 'Compile',
  nothing: 'Optimized',
  outdated: 'Compile',
  up_to_date: 'Compiled',
};

const UNIT_KINDS = {
  python_check: 'Python',
  r_check: 'R',
  r_environment: 'R environment',
  rust_block: 'Rust block',
  rust_chain: 'Rust chain',
};

const UNIT_STATES = {
  cached: 'up to date',
  checked: 'passed',
  compiled: 'built',
  compiling: 'building',
  failed: 'failed',
  pending: 'waiting',
};

const EXPLANATION = 'MageML optimizes the pipeline for production runs by compiling it. '
  + 'Compiling checks every Python and R block for errors, prepares the R environment and '
  + 'builds the Rust blocks. Blocks of the same language that follow each other run as one '
  + 'chain: consecutive Rust blocks build into one program whose queries Polars optimizes '
  + 'together, consecutive R blocks share one R session, and Python blocks share one '
  + 'process. SQL blocks run in the database as they are. Compiling starts on its own a '
  + 'few seconds after you save; Compile now runs it right away. Running a block in the '
  + 'editor is unchanged.';

function statusIcon(status: CompileStatusType) {
  if (status === 'compiling') {
    return <Spinner size={UNIT * 1.5} />;
  }
  if (status === 'failed') {
    return <AlertTriangle danger size={UNIT * 1.75} />;
  }
  return (
    <Lightning
      default={status === 'never'}
      size={UNIT * 1.75}
      success={status === 'up_to_date' || status === 'nothing'}
      warning={status === 'outdated'}
    />
  );
}

function headerLabel(status: CompileStatusType, compilation?: PipelineCompilationType): string {
  if (status === 'compiling' && compilation?.total) {
    return `Compiling ${Math.min(compilation.done + 1, compilation.total)}/${compilation.total}`;
  }
  return HEADER_LABELS[status];
}

function UnitRow({ unit }: { unit: CompilationUnitType }) {
  const done = ['cached', 'checked', 'compiled'].includes(unit.state);
  const warned = done && unit.warnings?.length > 0;

  return (
    <UnitRowStyle>
      <FlexContainer alignItems="center" justifyContent="space-between">
        <FlexContainer alignItems="center">
          {unit.state === 'compiling' && <Spinner size={UNIT * 1.25} />}
          {done && !warned && <Check size={UNIT * 1.5} success />}
          {warned && <AlertTriangle size={UNIT * 1.5} warning />}
          {unit.state === 'failed' && <AlertTriangle danger size={UNIT * 1.5} />}
          {unit.state === 'pending' && <Lightning muted size={UNIT * 1.5} />}
          <Spacing ml={1} />
          <div style={{ minWidth: 0 }}>
            <Text muted xsmall>{UNIT_KINDS[unit.kind] || unit.kind}</Text>
            <Text monospace small title={unit.label}>
              {unit.label}
            </Text>
          </div>
        </FlexContainer>
        <Spacing ml={1}>
          <Text
            danger={unit.state === 'failed'}
            muted={!done && unit.state !== 'failed'}
            noWrapping
            small
            success={done && !warned}
            warning={warned}
          >
            {warned
              ? `${unit.warnings.length} ${unit.warnings.length === 1 ? 'warning' : 'warnings'}`
              : UNIT_STATES[unit.state] || unit.state}
            {unit.seconds >= 0.1 && done ? ` · ${unit.seconds.toFixed(1)} s` : ''}
          </Text>
        </Spacing>
      </FlexContainer>
      {warned && (
        <Spacing mt={1}>
          {unit.warnings.map(warning => (
            <Text key={warning} preWrap warning xsmall>
              {warning}
            </Text>
          ))}
        </Spacing>
      )}
      {unit.error && (
        <Spacing mt={1}>
          <Text danger monospace preWrap xsmall>
            {unit.error}
          </Text>
        </Spacing>
      )}
    </UnitRowStyle>
  );
}

type CompileStatusProps = {
  pipeline: PipelineType;
};

// The editor header's compile control: the status of the pipeline's compilation, its
// progress, and a panel that explains it, lists what it builds and compiles on demand.
function CompileStatus({ pipeline }: CompileStatusProps) {
  const themeContext = useContext(ThemeContext);
  const [showError] = useError(null, {}, [], { uuid: 'CompileStatus' });
  const [open, setOpen] = useState(false);
  const refChip = useRef(null);
  const savedAt = useRef(0);
  const pipelineUUID = pipeline?.uuid;

  useEffect(() => {
    if (pipeline?.updated_at) {
      savedAt.current = Date.now();
    }
  }, [pipeline?.updated_at]);

  const { data, mutate } = api.pipeline_compilations.detail(pipelineUUID, {}, {
    refreshInterval: (latest: { pipeline_compilation?: PipelineCompilationType }) => {
      if (latest?.pipeline_compilation?.state === 'running') {
        return 1000;
      }
      return Date.now() - savedAt.current < POLL_AFTER_SAVE_SECONDS * 1000 ? 1500 : 0;
    },
    revalidateOnFocus: true,
  });
  const compilation: PipelineCompilationType = data?.pipeline_compilation;
  const status = compileStatus(compilation, pipeline?.last_compiled);
  const running = status === 'compiling';
  const percent = progressPercent(compilation);
  const summary = compilation?.summary;
  const hint = polyglotHint(summary);
  const last = compilation?.last_compiled;

  useEffect(() => {
    if (pipeline?.updated_at) {
      mutate();
    }
  }, [mutate, pipeline?.updated_at]);

  const [compile, { isLoading: isStarting }] = useMutation(
    api.pipeline_compilations.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => mutate(),
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const lastLine = useMemo(() => {
    if (status === 'nothing') {
      return 'Nothing to compile: this pipeline has no Python, R or Rust blocks.';
    }
    if (!last || last.state === 'never') {
      return 'Not compiled yet.';
    }
    const when = timeAgo(last.finished_at);
    if (last.state === 'failed') {
      return `Last compile failed ${when}.`;
    }
    return last.up_to_date
      ? `Last compiled ${when} · up to date`
      : `Last compiled ${when} · the code changed since`;
  }, [last, status]);

  const progressColor = (themeContext?.progress || dark.progress).positive;
  const languages = (summary?.languages || [])
    .filter(language => !['markdown', 'yaml'].includes(language))
    .map(language => LANGUAGE_NAMES[language] || language);

  const chainCount = summary?.chains?.length || 0;
  const switchCount = summary?.language_switches || 0;
  const summaryLine = [
    languages.length ? languages.join(', ') : null,
    `${chainCount} ${chainCount === 1 ? 'chain' : 'chains'}`,
    // The hint shows the switches when there are any.
    hint ? null : `${switchCount} language ${switchCount === 1 ? 'switch' : 'switches'}`,
  ].filter(Boolean).join(' · ');

  if (!pipelineUUID) {
    return null;
  }

  return (
    <div ref={refChip} style={{ position: 'relative' }}>
      <Tooltip
        appearBefore
        block
        label={open ? null : `${lastLine} Click for details.`}
        size={null}
        widthFitContent
      >
        <KeyboardShortcutButton
          beforeElement={statusIcon(status)}
          blackBorder
          compact
          inline
          onClick={() => setOpen(prev => !prev)}
          uuid="Pipeline/CompileStatus"
        >
          {headerLabel(status, compilation)}
        </KeyboardShortcutButton>
      </Tooltip>

      {running && (
        <StripStyle>
          <div
            style={{
              backgroundColor: progressColor,
              height: '100%',
              transition: 'width 400ms ease',
              width: `${Math.max(percent, 4)}%`,
            }}
          />
        </StripStyle>
      )}

      <ClickOutside
        additionalRef={refChip}
        disableEscape={!open}
        onClickOutside={() => setOpen(false)}
        open={open}
      >
        <PanelStyle>
          <FlexContainer alignItems="center" justifyContent="space-between">
            <FlexContainer alignItems="center">
              <Text bold large>Pipeline compilation</Text>
              <Spacing ml={1} />
              <Tooltip
                  appearBefore
                  description={EXPLANATION}
                  maxWidth={UNIT * 44}
                  size={null}
                >
                <Info muted size={UNIT * 1.75} />
              </Tooltip>
            </FlexContainer>
            <Button
                beforeIcon={!running && <Lightning size={UNIT * 1.5} />}
                compact
                disabled={running || status === 'nothing'}
                loading={isStarting}
                onClick={() => {
                  // @ts-ignore
                  compile({ pipeline_compilation: { pipeline_uuid: pipelineUUID } });
                }}
                primary
              >
              {running ? 'Compiling' : 'Compile now'}
            </Button>
          </FlexContainer>

          <Spacing mt={1}>
            <Text
                danger={status === 'failed'}
                muted={status !== 'failed' && status !== 'outdated'}
                small
                warning={status === 'outdated'}
              >
              {lastLine}
            </Text>
          </Spacing>

          {running && (
          <Spacing mt={2}>
            <ProgressBar progress={Math.max(percent, 4)} />
            <Spacing mt={1}>
              <Text muted small>
                {compilation?.current
                      ? `Building ${compilation.current}`
                      : 'Starting'}
                {compilation?.automatic ? ' · started by a save' : ''}
              </Text>
            </Spacing>
          </Spacing>
            )}

          {compilation?.units?.length > 0 && (
          <Spacing mt={2}>
            {compilation.units.map(unit => (
              <UnitRow key={`${unit.kind}:${unit.label}`} unit={unit} />
                ))}
          </Spacing>
            )}

          {summary && (
          <Spacing mt={2}>
            <Text muted small>
              {summaryLine}
            </Text>
          </Spacing>
            )}

          {summary && !summary.fusion && (
          <Spacing mt={1}>
            <Text small warning>
              Chains are turned off in this pipeline&apos;s settings, so production runs
              run each block alone. Turn on &quot;Run chains of blocks together&quot; for
              faster runs.
            </Text>
          </Spacing>
            )}

          {hint && (
          <Spacing mt={2}>
            <HintStyle>
              <Text bold>Single-language pipelines run faster</Text>
              <Spacing my={1}>
                <FlexContainer alignItems="center" flexWrap="wrap">
                  {(summary?.flow || []).map(({ blocks, language }, index) => (
                    <FlexContainer alignItems="center" key={`${language}:${index}`}>
                      {index > 0 && <SwitchStyle title="Language switch">→</SwitchStyle>}
                      <LanguagePillStyle>
                        {LANGUAGE_NAMES[language] || language}
                        {blocks > 1 ? ` ×${blocks}` : ''}
                      </LanguagePillStyle>
                    </FlexContainer>
                  ))}
                </FlexContainer>
              </Spacing>
              <Text muted small>{hint}</Text>
            </HintStyle>
          </Spacing>
            )}
        </PanelStyle>
      </ClickOutside>
    </div>
  );
}

export default CompileStatus;
