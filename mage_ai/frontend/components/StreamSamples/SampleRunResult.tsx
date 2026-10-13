import { useMemo, useState } from 'react';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { StreamSampleRunType } from '@interfaces/StreamSampleType';
import { UNIT } from '@oracle/styles/units/spacing';

const PAGE = 50;

const PanelStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  margin-top: ${UNIT}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const ColumnsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 1.5}px;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  margin-top: ${UNIT * 1.5}px;

  @media (max-width: 900px) {
    grid-template-columns: minmax(0, 1fr);
  }
`;

const MessageStyle = styled.div<{ highlighted?: boolean }>`
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: ${UNIT * 4}px minmax(0, 1fr) auto;
  padding: ${UNIT * 0.5}px ${UNIT * 0.75}px;

  ${props => `
    border-bottom: 1px solid ${(props.theme.borders || dark.borders).light};
  `}

  ${props => props.highlighted && `
    background-color: rgba(229, 72, 77, 0.18);
  `}
`;

const CodeStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 1.5;
  margin: 0;
  overflow-wrap: anywhere;
  white-space: pre-wrap;

  ${props => `
    color: ${(props.theme.content || dark.content).default};
  `}
`;

const ChipStyle = styled.span`
  border-radius: ${BORDER_RADIUS}px;
  display: inline-block;
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  margin: 0 ${UNIT * 0.5}px ${UNIT * 0.5}px 0;
  padding: ${UNIT * 0.25}px ${UNIT * 0.75}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).row};
    color: ${(props.theme.content || dark.content).default};
  `}
`;

function milliseconds(seconds: number): string {
  const ms = seconds * 1000;
  if (ms < 0.1) {
    return '<0.1 ms';
  }
  return ms < 10 ? `${ms.toFixed(1)} ms` : `${Math.round(ms)} ms`;
}

function compact(message: any): string {
  return JSON.stringify(message);
}

function Messages({
  highlight,
  messages,
  title,
}: {
  highlight?: number;
  messages: any[];
  title: string;
}) {
  const [shown, setShown] = useState<number>(PAGE);
  const visible = messages.slice(0, Math.max(shown, (highlight ?? -1) + 1));

  return (
    <div style={{ minWidth: 0 }}>
      <Text bold small>{title} ({messages.length.toLocaleString()})</Text>
      <Spacing mt={1} />
      {!messages.length && <Text muted small>No messages.</Text>}
      {visible.map((message, index) => (
        <MessageStyle highlighted={index === highlight} key={index}>
          <Text monospace muted xsmall>{index + 1}</Text>
          <CodeStyle>{compact(message)}</CodeStyle>
          {index === highlight ? <Text danger xsmall>fails</Text> : <span />}
        </MessageStyle>
      ))}
      {messages.length > visible.length && (
        <Spacing mt={1}>
          <Button compact onClick={() => setShown(visible.length + PAGE)} secondary>
            Show {Math.min(PAGE, messages.length - visible.length)} more
          </Button>
        </Spacing>
      )}
    </div>
  );
}

type SampleRunResultProps = {
  result: StreamSampleRunType;
};

// What a block made of a sample: input and output side by side, the time of each block, what
// the blocks printed, or the error with the first message that fails on its own.
function SampleRunResult({ result }: SampleRunResultProps) {
  const output = useMemo(
    () => result?.outputs?.[result.block_uuid] || [],
    [result],
  );
  const input = useMemo(() => {
    // The block's input is the output of the block before it, or the sample.
    const before = (result?.blocks || []).map(b => b.block_uuid).filter(u => u !== result.block_uuid);
    const previous = before[before.length - 1];
    return (previous && result?.outputs?.[previous]) || result?.messages || [];
  }, [result]);
  const failing = result?.failing_message;
  const logs = result?.logs || [];

  return (
    <PanelStyle>
      <FlexContainer alignItems="center" flexWrap="wrap" justifyContent="space-between">
        <Text bold>
          {result.error
            ? `Failed in ${result.error.block_uuid || 'a block'}`
            : `${result.input_messages.toLocaleString()} in → ${output.length.toLocaleString()} out`}
        </Text>
        <Text muted small>
          {result.sample ? `Sample ${result.sample} · ` : ''}{Math.round(result.seconds * 1000)} ms
        </Text>
      </FlexContainer>
      {!!result.blocks?.length && (
        <Spacing mt={1}>
          {result.blocks.map(({ block_uuid: uuid, seconds }) => (
            <ChipStyle key={uuid}>{uuid} · {milliseconds(seconds)}</ChipStyle>
          ))}
        </Spacing>
      )}

      {result.error && (
        <Spacing mt={1}>
          <Text danger>{result.error.message}</Text>
          {failing && (
            <Text small>
              Message {failing.index + 1} fails on its own; it is highlighted in the input.
            </Text>
          )}
          {result.error.details && (
            <Spacing mt={1}>
              <CodeStyle>{result.error.details}</CodeStyle>
            </Spacing>
          )}
        </Spacing>
      )}

      <ColumnsStyle>
        <Messages
          highlight={failing?.index}
          messages={input}
          title="Input"
        />
        {!result.error && <Messages messages={output} title="Output" />}
      </ColumnsStyle>

      {!!logs.length && (
        <Spacing mt={2}>
          <Text bold small>Printed</Text>
          <Spacing mt={1} />
          <CodeStyle>
            {logs.map(({ block_uuid: uuid, line }) => (uuid ? `${uuid}: ${line}` : line)).join('\n')}
          </CodeStyle>
        </Spacing>
      )}
    </PanelStyle>
  );
}

export default SampleRunResult;
