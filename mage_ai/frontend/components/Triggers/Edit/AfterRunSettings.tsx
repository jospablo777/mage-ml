import { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import Checkbox from '@oracle/elements/Checkbox';
import FlexContainer from '@oracle/components/FlexContainer';
import PipelineType from '@interfaces/PipelineType';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { AlertTriangle } from '@oracle/icons';
import {
  AFTER_RUN_ENDINGS,
  PipelineScheduleSettingsType,
} from '@interfaces/PipelineScheduleType';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { FONT_FAMILY_MEDIUM, MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

const LANGUAGE_NAMES = { python: 'Python', r: 'R', rust: 'Rust', sql: 'SQL' };
const ENDING_OPTIONS: { label: string; value: PipelineScheduleSettingsType['after_status'] }[] = [
  { label: 'Completes', value: 'completed' },
  { label: 'Fails', value: 'failed' },
  { label: 'Ends either way', value: 'any' },
];

const LayoutStyle = styled.div`
  display: grid;
  gap: ${UNIT * 3}px;
  grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);

  @media (max-width: 1100px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const ListStyle = styled.div`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  height: ${UNIT * 32}px;
  overflow: auto;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const RowStyle = styled.button<{ selected: boolean }>`
  background: none;
  border: none;
  cursor: pointer;
  display: flex;
  gap: ${UNIT * 1.25}px;
  padding: ${UNIT}px ${UNIT * 1.5}px;
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

const RadioStyle = styled.span<{ selected: boolean }>`
  border-radius: 50%;
  flex-shrink: 0;
  height: ${UNIT * 2}px;
  margin-top: 2px;
  width: ${UNIT * 2}px;

  ${props => `
    border: ${props.selected ? UNIT * 0.6 : 1}px solid ${props.selected
      ? (props.theme.interactive || dark.interactive).linkPrimary
      : (props.theme.content || dark.content).muted};
  `}
`;

const SegmentStyle = styled.button<{ selected: boolean }>`
  cursor: pointer;
  font-family: ${FONT_FAMILY_MEDIUM};
  font-size: 13px;
  line-height: ${UNIT * 4}px;
  padding: 0 ${UNIT * 1.75}px;

  ${props => `
    background-color: ${props.selected
      ? (props.theme.interactive || dark.interactive).defaultBackgroundTransparent
      : 'transparent'};
    border: 1px solid ${props.selected
      ? (props.theme.content || dark.content).muted
      : (props.theme.borders || dark.borders).light};
    color: ${(props.theme.content || dark.content).default};
  `}

  &:first-child {
    border-radius: ${BORDER_RADIUS_SMALL}px 0 0 ${BORDER_RADIUS_SMALL}px;
  }

  &:last-child {
    border-radius: 0 ${BORDER_RADIUS_SMALL}px ${BORDER_RADIUS_SMALL}px 0;
  }

  & + & {
    margin-left: -1px;
  }
`;

const FlowStyle = styled.div`
  align-items: center;
  border-radius: ${BORDER_RADIUS_SMALL}px;
  display: flex;
  flex-wrap: wrap;
  gap: ${UNIT}px;
  padding: ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).popup};
  `}
`;

const NodeStyle = styled.span<{ empty?: boolean }>`
  border-radius: ${BORDER_RADIUS_SMALL}px;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 13px;
  line-height: ${UNIT * 4}px;
  padding: 0 ${UNIT * 1.25}px;

  ${props => `
    border: 1px ${props.empty ? 'dashed' : 'solid'} ${(props.theme.borders || dark.borders).light};
    color: ${props.empty
      ? (props.theme.content || dark.content).muted
      : (props.theme.content || dark.content).default};
  `}
`;

type AfterRunSettingsProps = {
  activateOnSave?: boolean;
  onLoopChange?: (hasLoop: boolean) => void;
  pipelineUUID: string;
  setActivateOnSave?: (value: boolean) => void;
  setSettings: (update: (prev: PipelineScheduleSettingsType) => PipelineScheduleSettingsType) => void;
  settings: PipelineScheduleSettingsType;
  showActivate?: boolean;
};

function languagesOf(pipeline: PipelineType): string {
  return Array.from(new Set((pipeline.blocks || []).map(b => b.language).filter(Boolean)))
    .map(language => LANGUAGE_NAMES[language] || language)
    .join(', ');
}

// Choose the pipeline whose runs start this one and which of its endings do, see the flow
// it makes, and catch a loop before saving.
function AfterRunSettings({
  activateOnSave,
  onLoopChange,
  pipelineUUID,
  setActivateOnSave,
  setSettings,
  settings,
  showActivate,
}: AfterRunSettingsProps) {
  const [search, setSearch] = useState<string>('');
  const { data } = api.pipelines.list({ _limit: 1000 }, { revalidateOnFocus: false });
  const { data: dataTriggers } = api.pipeline_schedules.list(
    { 'schedule_type[]': 'after_run', _limit: 1000 },
    { revalidateOnFocus: false },
  );

  const selected = settings?.after_pipeline;
  const ending = settings?.after_status || 'completed';
  const others: PipelineType[] = useMemo(() => {
    const all = (data?.pipelines || []).filter(p => p.uuid !== pipelineUUID);
    const term = search.trim().toLowerCase();
    const shown = term ? all.filter(p => p.uuid.toLowerCase().includes(term)) : all;

    // The chosen pipeline stays on top, so it shows as chosen.
    return [
      ...shown.filter(p => p.uuid === selected),
      ...shown.filter(p => p.uuid !== selected),
    ];
  }, [data, pipelineUUID, search, selected]);

  // A loop: following the chosen pipeline's after-run triggers leads back to this one.
  const loop: string[] = useMemo(() => {
    if (!selected) {
      return null;
    }
    const followers: { [uuid: string]: string[] } = {};
    (dataTriggers?.pipeline_schedules || [])
      .filter(t => t.status === 'active' && t.settings?.after_pipeline)
      .forEach((t) => {
        const upstream = t.settings.after_pipeline;
        followers[upstream] = [...(followers[upstream] || []), t.pipeline_uuid];
      });
    const walk = (node: string, path: string[]): string[] => {
      if (node === selected) {
        return path;
      }
      for (const next of followers[node] || []) {
        if (!path.includes(next)) {
          const found = walk(next, [...path, next]);
          if (found) {
            return found;
          }
        }
      }
      return null;
    };
    const found = walk(pipelineUUID, [pipelineUUID]);

    return found ? [selected, ...found] : null;
  }, [dataTriggers, pipelineUUID, selected]);

  useEffect(() => {
    onLoopChange?.(!!loop);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [!!loop]);

  return (
    <LayoutStyle>
      <div>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Text bold>Pipeline</Text>
          <Text muted small>{others.length} to choose from</Text>
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
          <ListStyle role="radiogroup" aria-label="Pipeline to follow">
            {!data && <Spacing p={2}><Text muted small>Loading pipelines</Text></Spacing>}
            {data && !others.length && (
              <Spacing p={2}><Text muted small>No pipeline matches.</Text></Spacing>
            )}
            {others.map((pipeline) => {
              const isSelected = pipeline.uuid === selected;

              return (
                <RowStyle
                  aria-checked={isSelected}
                  key={pipeline.uuid}
                  onClick={() => setSettings(prev => ({ ...prev, after_pipeline: pipeline.uuid }))}
                  role="radio"
                  selected={isSelected}
                  type="button"
                >
                  <RadioStyle selected={isSelected} />
                  <div style={{ minWidth: 0 }}>
                    <Text monospace>{pipeline.uuid}</Text>
                    <Text muted small>
                      {[
                        `${pipeline.blocks?.length || 0} block${pipeline.blocks?.length === 1 ? '' : 's'}`,
                        languagesOf(pipeline),
                      ].filter(Boolean).join(' · ')}
                    </Text>
                  </div>
                </RowStyle>
              );
            })}
          </ListStyle>
        </Spacing>
      </div>

      <div>
        <Text bold>When its run</Text>
        <Spacing mt={1}>
          <div role="radiogroup" aria-label="Which endings start it">
            {ENDING_OPTIONS.map(option => (
              <SegmentStyle
                aria-checked={ending === option.value}
                key={option.value}
                onClick={() => setSettings(prev => ({ ...prev, after_status: option.value }))}
                role="radio"
                selected={ending === option.value}
                type="button"
              >
                {option.label}
              </SegmentStyle>
            ))}
          </div>
        </Spacing>

        <Spacing mt={3}>
          <Text bold>The flow</Text>
          <Spacing mt={1}>
            <FlowStyle>
              <NodeStyle empty={!selected}>{selected || 'choose a pipeline'}</NodeStyle>
              <Text muted small>{AFTER_RUN_ENDINGS[ending]} &rarr;</Text>
              <NodeStyle>{pipelineUUID}</NodeStyle>
            </FlowStyle>
          </Spacing>
          <Spacing mt={1}>
            <Text muted small>
              {selected
                ? `Each run of ${selected} starts one run of ${pipelineUUID}. The run gets `
                  + 'upstream_pipeline, upstream_pipeline_run_id and upstream_status as variables.'
                : 'Choose a pipeline to save the trigger.'}
            </Text>
          </Spacing>
        </Spacing>

        {loop && (
          <Spacing mt={2}>
            <FlexContainer alignItems="flex-start">
              <AlertTriangle danger size={UNIT * 2} />
              <Spacing ml={1} />
              <Text danger small>
                This makes a loop: {loop.join(' → ')}. Each run would start the next without
                end, so it cannot be saved. Choose another pipeline.
              </Text>
            </FlexContainer>
          </Spacing>
        )}

        {showActivate && (
          <Spacing mt={2}>
            <Checkbox
              checked={!!activateOnSave}
              label="Turn the trigger on when I save"
              onClick={() => setActivateOnSave?.(!activateOnSave)}
            />
          </Spacing>
        )}

        <Spacing mt={2}>
          <Text muted xsmall>
            MageML and the pipeline services exported from it both run this trigger.
          </Text>
        </Spacing>
      </div>
    </LayoutStyle>
  );
}

export default AfterRunSettings;
