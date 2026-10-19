import { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { positionText, targetNoun } from './positions';

// A stream's dead letters: the messages a block failed on, with their errors; opened as a
// sample to see the failure again with Run on sample, or replayed to the source once fixed.

type DeadLetterType = {
  error?: string;
  offset: number;
  partition: number;
  timestamp?: number;
  value: any;
};

type DeadLettersType = {
  letters?: DeadLetterType[];
  redacted?: { [path: string]: number };
  kind?: string;
  source_kind?: string;
  source_topic?: string;
  topic?: string;
  total?: number;
};

const PanelStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  margin-bottom: ${UNIT * 1.5}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const LetterStyle = styled.div`
  padding: ${UNIT}px 0;

  ${props => `
    border-top: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const CodeStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
`;

function body(response: any) {
  return response?.data || response;
}

function when(timestamp?: number): string {
  return timestamp ? new Date(timestamp).toISOString().replace('T', ' ').slice(0, 19) : '';
}

type StreamDeadLettersProps = {
  openSamples?: () => void;
  pipelineUUID: string;
};

function StreamDeadLetters({ openSamples, pipelineUUID }: StreamDeadLettersProps) {
  const [found, setFound] = useState<DeadLettersType>(null);
  const [error, setError] = useState<string>(null);
  const [notice, setNotice] = useState<string>(null);
  const [busy, setBusy] = useState<boolean>(false);
  const [name, setName] = useState<string>('dead_letters');

  const load = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const result = body(await api.stream_dead_letters.pipelines.detailAsync(
        pipelineUUID, 'current',
      ));
      if (result?.error) {
        setError(result.error.message);
      } else {
        setFound(result?.stream_dead_letter);
      }
    } finally {
      setBusy(false);
    }
  }, [pipelineUUID]);

  useEffect(() => {
    load();
  }, [load]);

  const act = useCallback(async (payload: any, message: (result: any) => string) => {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const result = body(await api.stream_dead_letters.pipelines.useCreate(pipelineUUID)({
        stream_dead_letter: payload,
      }));
      if (result?.error) {
        setError(result.error.message);
      } else {
        setNotice(message(result?.stream_dead_letter));
      }
    } finally {
      setBusy(false);
    }
  }, [pipelineUUID]);

  const replay = useCallback(() => {
    // A production write: the user types the topic or subject it goes to.
    const typed = typeof window !== 'undefined' && window.prompt(
      `Replay these ${found?.letters?.length || 0} dead letters to the source. They are written `
        + `to ${found?.source_topic} as they were received. Type the `
        + `${targetNoun(found?.source_kind)} to confirm:`,
    );
    if (!typed) {
      return;
    }
    act({ action: 'replay', confirm_topic: typed },
      result => `Replayed ${result?.replayed} messages to ${result?.topic}.`);
  }, [act, found]);

  const letters = found?.letters || [];
  const redacted = Object.keys(found?.redacted || {});

  return (
    <Spacing p={2}>
      <PanelStyle>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Text bold large>
            {found
              ? `${(found.total || 0).toLocaleString()} dead letters in ${found.topic}`
              : 'Dead letters'}
          </Text>
          <Button compact disabled={busy} onClick={load}>Refresh</Button>
        </FlexContainer>
        {error && (
          <Spacing mt={1}>
            <CodeStyle><Text danger small>{error}</Text></CodeStyle>
          </Spacing>
        )}
        {notice && (
          <Spacing mt={1}>
            <Text small>{notice}</Text>
          </Spacing>
        )}
        {!!redacted.length && (
          <Spacing mt={1}>
            <Text muted small>Shown with {redacted.join(', ')} redacted.</Text>
          </Spacing>
        )}
        {!!letters.length && (
          <Spacing mt={2}>
            <FlexContainer alignItems="center" flexWrap="wrap">
              <TextInput
                compact
                onChange={e => setName(e.target.value)}
                placeholder="Sample name"
                value={name}
              />
              <Spacing ml={1} />
              <Button
                disabled={busy || !name}
                onClick={() => act({ action: 'sample', name }, (result) => {
                  openSamples?.();
                  return `Saved ${result?.sample?.message_count} messages as ${name}: Run on `
                    + 'sample shows the failure again.';
                })}
              >
                Open as sample
              </Button>
              {found?.source_topic && (
                <Spacing ml={1}>
                  <Button disabled={busy} onClick={replay}>
                    Replay to {found.source_topic}
                  </Button>
                </Spacing>
              )}
            </FlexContainer>
          </Spacing>
        )}
      </PanelStyle>

      {!!letters.length && (
        <PanelStyle>
          <Text muted small>Newest first ({letters.length.toLocaleString()} shown)</Text>
          {letters.map(letter => (
            <LetterStyle key={`${letter.partition}-${letter.offset}`}>
              <Text muted xsmall>
                {when(letter.timestamp)} · {positionText(letter, found?.kind)}
              </Text>
              {letter.error && (
                <CodeStyle><Text danger small>{letter.error}</Text></CodeStyle>
              )}
              <CodeStyle>
                {typeof letter.value === 'string' ? letter.value : JSON.stringify(letter.value)}
              </CodeStyle>
            </LetterStyle>
          ))}
        </PanelStyle>
      )}
    </Spacing>
  );
}

export default StreamDeadLetters;
