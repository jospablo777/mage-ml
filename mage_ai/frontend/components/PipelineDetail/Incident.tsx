import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';

export type IncidentBlockType = {
  block_run_id: number;
  block_uuid: string;
  ended_at?: string;
  error?: string;
  // The line of the error that says what it was, such as KeyError: 'amount'.
  error_line?: string;
  log?: { content: string; first_line: number; total_lines: number };
  seconds?: number;
  started_at?: string;
  upstream: { [blockUUID: string]: string };
  usual: { runs: number; seconds?: number };
};

export type IncidentType = {
  blocks: IncidentBlockType[];
  not_completed: { block_uuid: string; status: string }[];
  recovery: {
    action: 'retry_from' | 'retry_incomplete';
    block_uuid?: string;
    blocks?: string[];
    label: string;
  }[];
};

const LogStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 18px;
  margin: 0;
  max-height: ${UNIT * 40}px;
  overflow: auto;
  padding: ${UNIT}px;
  white-space: pre-wrap;
  word-break: break-word;

  ${props => `
    background-color: ${(props.theme.background || dark.background).codeArea};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
    border-radius: ${UNIT / 2}px;
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const BarStyle = styled.div<{ width: number; muted?: boolean }>`
  border-radius: ${UNIT / 2}px;
  height: ${UNIT}px;
  min-width: 2px;

  ${props => `
    width: ${props.width}%;
    background-color: ${props.muted
      ? (props.theme.content || dark.content).muted
      : (props.theme.accent || dark.accent).negative};
  `}
`;

export function duration(seconds?: number): string {
  if (seconds === null || seconds === undefined) {
    return 'unknown';
  }
  if (seconds < 60) {
    return `${Math.round(seconds * 10) / 10}s`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ${Math.round(seconds % 60)}s`;
  }
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

// The block's time against its usual, as two bars on one scale.
function Timing({ block }: { block: IncidentBlockType }) {
  const { seconds, usual } = block;
  if (seconds === null || seconds === undefined) {
    return <Text muted small>Its start or end time was not recorded.</Text>;
  }
  if (!usual?.seconds) {
    return (
      <Text muted small>
        Ran {duration(seconds)}. No completed run of this block to compare with.
      </Text>
    );
  }
  const longest = Math.max(seconds, usual.seconds);
  const ratio = seconds / usual.seconds;
  return (
    <>
      <Text small>
        Ran {duration(seconds)}, {ratio >= 2 || ratio <= 0.5
          ? `${Math.round(ratio * 10) / 10} times its usual`
          : 'about its usual'} {duration(usual.seconds)} (median of the last {usual.runs} completed)
      </Text>
      <Spacing mt={1}>
        <FlexContainer alignItems="center">
          <Text monospace muted xsmall>this run&nbsp;</Text>
          <BarStyle width={(seconds / longest) * 80} />
        </FlexContainer>
        <FlexContainer alignItems="center">
          <Text monospace muted xsmall>usual&nbsp;&nbsp;&nbsp;&nbsp;</Text>
          <BarStyle muted width={(usual.seconds / longest) * 80} />
        </FlexContainer>
      </Spacing>
    </>
  );
}

type IncidentProps = {
  onRetry: (fromBlockUUID?: string) => void;
  pipelineRunId: number;
  retrying?: boolean;
};

// What a failed or cancelled run's page shows first: each failed block with its error,
// the end of its log, its time against its usual, its upstream blocks, and the ways to
// recover (orchestration/incidents.py).
function Incident({ onRetry, pipelineRunId, retrying }: IncidentProps) {
  const { data } = api.run_records.detail(pipelineRunId);
  const incident: IncidentType = data?.run_record?.incident;
  if (!incident || (!incident.blocks.length && !incident.not_completed.length)) {
    return null;
  }

  return (
    <div data-testid="incident">
      {incident.blocks.map(block => (
        <Spacing key={block.block_run_id} mb={3}>
          <Headline level={5}>
            {block.block_uuid} failed{block.error_line ? `: ${block.error_line}` : ''}
          </Headline>
          <Spacing mt={1}>
            <Timing block={block} />
          </Spacing>
          {Object.keys(block.upstream).length > 0 && (
            <Spacing mt={1}>
              <Text muted small>
                Upstream in this run: {Object.entries(block.upstream)
                  .map(([uuid, state]) => `${uuid} ${state || 'did not run'}`)
                  .join(', ')}
              </Text>
            </Spacing>
          )}
          {block.log ? (
            <Spacing mt={1}>
              <Text muted small>
                The end of its log, lines {block.log.first_line + 1} to {block.log.total_lines}
              </Text>
              <LogStyle>{block.log.content}</LogStyle>
            </Spacing>
          ) : (
            <Spacing mt={1}>
              <Text muted small>Its log is empty or was not found.</Text>
            </Spacing>
          )}
        </Spacing>
      ))}
      {incident.not_completed.length > 0 && (
        <Spacing mb={2}>
          <Text small>
            Did not complete: {incident.not_completed
              .map(({ block_uuid: uuid, status }) => `${uuid} (${status.replace('_', ' ')})`)
              .join(', ')}
          </Text>
        </Spacing>
      )}
      <FlexContainer flexWrap="wrap">
        {incident.recovery.map(action => (
          <Spacing key={`${action.action}-${action.block_uuid || ''}`} mb={1} mr={1}>
            <Button
              loading={retrying}
              onClick={() => onRetry(action.block_uuid)}
              outline
              primary={action.action === 'retry_from'}
            >
              {action.label}
            </Button>
          </Spacing>
        ))}
      </FlexContainer>
    </div>
  );
}

export default Incident;
