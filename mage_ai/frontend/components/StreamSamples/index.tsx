import { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import Checkbox from '@oracle/elements/Checkbox';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import StreamSampleType from '@interfaces/StreamSampleType';
import Text from '@oracle/elements/Text';
import TextArea from '@oracle/elements/Inputs/TextArea';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { UNIT } from '@oracle/styles/units/spacing';
import { parseMessages, useStreamSample } from '@utils/streamSamples';

const SectionStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  margin-bottom: ${UNIT * 1.5}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const RowStyle = styled.div<{ selected?: boolean }>`
  align-items: center;
  border-radius: ${BORDER_RADIUS}px;
  display: grid;
  gap: ${UNIT}px;
  grid-template-columns: minmax(0, 1fr) auto;
  padding: ${UNIT}px;

  ${props => props.selected && `
    background-color: ${(props.theme.background || dark.background).row};
    box-shadow: inset 0 0 0 1px ${(props.theme.interactive || dark.interactive).linkPrimary};
  `}
`;

const ToggleStyle = styled.div`
  display: inline-flex;
  gap: ${UNIT * 0.5}px;
`;

function body(response: any) {
  return response?.data || response;
}

function bytes(size: number): string {
  if (!size) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB'];
  const power = Math.min(Math.floor(Math.log(size) / Math.log(1024)), units.length - 1);
  return `${(size / 1024 ** power).toFixed(power ? 1 : 0)} ${units[power]}`;
}

type StreamSamplesProps = {
  pipelineUUID: string;
};

// The Samples panel of a streaming pipeline: capture messages from the source without moving
// its position, paste or edit them, and pick the sample "Run on sample" uses.
function StreamSamples({ pipelineUUID }: StreamSamplesProps) {
  const [selected, setSelected] = useStreamSample(pipelineUUID);
  const [samples, setSamples] = useState<StreamSampleType[]>(null);
  const [error, setError] = useState<string>(null);
  const [notice, setNotice] = useState<string>(null);
  const [busy, setBusy] = useState<boolean>(false);
  const [name, setName] = useState<string>('sample');
  const [count, setCount] = useState<number>(100);
  const [start, setStart] = useState<'latest' | 'earliest'>('latest');
  const [keepOutOfGit, setKeepOutOfGit] = useState<boolean>(true);
  const [pasted, setPasted] = useState<string>('');
  const [editing, setEditing] = useState<string>(null);
  const [editText, setEditText] = useState<string>('');

  const load = useCallback(async () => {
    const response = await api.stream_samples.pipelines.listAsync(pipelineUUID);
    const found = body(response)?.stream_samples || [];
    setSamples(found);
    if (!selected && found.length) {
      setSelected(found[0].name);
    }
  }, [pipelineUUID, selected, setSelected]);

  useEffect(() => {
     
    load();
  }, [load]);

  const save = useCallback(async (payload: any, message: (sample: StreamSampleType) => string) => {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.stream_samples.pipelines.useCreate(pipelineUUID)({
        stream_sample: payload,
      });
      const result = body(response);
      if (result?.error) {
        setError(result.error.message);
        return;
      }
      setNotice(message(result.stream_sample));
      setSelected(result.stream_sample.name);
      await load();
    } finally {
      setBusy(false);
    }
  }, [load, pipelineUUID, setSelected]);

  const capture = useCallback(() => save(
    { capture: true, count, keep_out_of_git: keepOutOfGit, name, start },
    sample => {
      const parts = Object.entries(sample.partitions || {})
        .map(([partition, read]) => `${partition}: ${read}`).join(', ');
      const redacted = Object.keys(sample.redacted || {});
      return `Captured ${sample.message_count} messages${parts ? ` (${parts})` : ''}.`
        + (redacted.length ? ` Redacted ${redacted.join(', ')}.` : '');
    },
  ), [count, keepOutOfGit, name, save, start]);

  const savePasted = useCallback(() => {
    let messages: any[];
    try {
      messages = parseMessages(pasted);
    } catch (parseError) {
      setError(parseError.message);
      return;
    }
     
    save({ keep_out_of_git: keepOutOfGit, messages, name }, sample => {
      setPasted('');
      return `Saved ${sample.message_count} messages as ${sample.name}.`;
    });
  }, [keepOutOfGit, name, pasted, save]);

  const openSample = useCallback(async (sample: string) => {
    if (editing === sample) {
      setEditing(null);
      return;
    }
    const response = await api.stream_samples.pipelines.detailAsync(pipelineUUID, sample);
    const messages = body(response)?.stream_sample?.messages || [];
    setEditText(messages.map(m => JSON.stringify(m)).join('\n'));
    setEditing(sample);
  }, [editing, pipelineUUID]);

  const saveEdit = useCallback(async () => {
    let messages: any[];
    try {
      messages = parseMessages(editText);
    } catch (parseError) {
      setError(parseError.message);
      return;
    }
    const response = await api.stream_samples.pipelines.useUpdate(pipelineUUID, editing)({
      stream_sample: { messages },
    });
    const result = body(response);
    if (result?.error) {
      setError(result.error.message);
      return;
    }
    setNotice(`Saved ${messages.length} messages in ${editing}.`);
    await load();
  }, [editText, editing, load, pipelineUUID]);

  const remove = useCallback(async (sample: string) => {
    if (typeof window !== 'undefined'
      && !window.confirm(`Delete sample ${sample}? Its messages are removed from the project.`)) {
      return;
    }
    await api.stream_samples.pipelines.useDelete(pipelineUUID, sample)();
    if (selected === sample) {
      setSelected(null);
    }
    if (editing === sample) {
      setEditing(null);
    }
    await load();
  }, [editing, load, pipelineUUID, selected, setSelected]);

  return (
    <Spacing p={2}>
      <Text bold large>Samples</Text>
      <Text muted small>
        Messages to run blocks on. A capture reads the source without its consumer group, so
        a running stream keeps its position.
      </Text>
      <Spacing mt={2} />

      <SectionStyle>
        <Text bold>Capture from the source</Text>
        <Spacing mt={1} />
        <FlexContainer alignItems="center" flexWrap="wrap">
          <TextInput
            aria-label="Sample name"
            compact
            monospace
            onChange={e => setName(e.target.value)}
            placeholder="Sample name"
            value={name}
          />
          <Spacing mr={1} />
          <TextInput
            aria-label="Messages to capture"
            compact
            monospace
            onChange={e => setCount(Number(e.target.value) || 0)}
            type="number"
            value={String(count)}
          />
          <Spacing mr={1} />
          <ToggleStyle>
            <Button
              compact
              onClick={() => setStart('latest')}
              primary={start === 'latest'}
              secondary={start !== 'latest'}
            >
              Newest
            </Button>
            <Button
              compact
              onClick={() => setStart('earliest')}
              primary={start === 'earliest'}
              secondary={start !== 'earliest'}
            >
              First
            </Button>
          </ToggleStyle>
        </FlexContainer>
        <Spacing mt={1} />
        <Checkbox
          checked={keepOutOfGit}
          label="Keep samples out of Git (they can hold real data)"
          onClick={() => setKeepOutOfGit(!keepOutOfGit)}
        />
        <Spacing mt={1} />
        <FlexContainer alignItems="center">
          <Button disabled={busy || !name || count < 1} loading={busy} onClick={capture} primary>
            Capture {count || ''} messages
          </Button>
        </FlexContainer>
      </SectionStyle>

      <SectionStyle>
        <Text bold>Or paste messages</Text>
        <Text muted small>JSON lines, or one JSON array. Saved under the name above.</Text>
        <Spacing mt={1} />
        <TextArea
          monospace
          onChange={e => setPasted(e.target.value)}
          placeholder={'{"order_id": 1, "amount": 12.5}\n{"order_id": 2, "amount": 7}'}
          rows={4}
          value={pasted}
        />
        <Spacing mt={1} />
        <Button disabled={busy || !pasted.trim() || !name} onClick={savePasted} secondary>
          Save as {name || 'sample'}
        </Button>
      </SectionStyle>

      {error && (
        <Spacing mb={2}><Text danger>{error}</Text></Spacing>
      )}
      {notice && !error && (
        <Spacing mb={2}><Text success>{notice}</Text></Spacing>
      )}

      <Text bold>Samples in this pipeline</Text>
      <Spacing mt={1} />
      {!samples && <Spinner inverted />}
      {samples && !samples.length && (
        <Text muted small>No samples yet: capture or paste some messages.</Text>
      )}
      {(samples || []).map(sample => (
        <div key={sample.name}>
          <RowStyle selected={sample.name === selected}>
            <div style={{ minWidth: 0 }}>
              <Text monospace>{sample.name}</Text>
              <Text muted xsmall>
                {sample.message_count?.toLocaleString()} messages · {bytes(sample.size_bytes)}
                {sample.name === selected ? ' · used by Run on sample' : ''}
              </Text>
            </div>
            <FlexContainer alignItems="center">
              {sample.name !== selected && (
                <Button compact onClick={() => setSelected(sample.name)} secondary>Use</Button>
              )}
              <Spacing mr={1} />
              <Button compact onClick={() => openSample(sample.name)} secondary>
                {editing === sample.name ? 'Close' : 'Edit'}
              </Button>
              <Spacing mr={1} />
              <Button compact danger onClick={() => remove(sample.name)}>Delete</Button>
            </FlexContainer>
          </RowStyle>
          {editing === sample.name && (
            <Spacing mb={1} mt={1}>
              <TextArea monospace onChange={e => setEditText(e.target.value)} rows={12} value={editText} />
              <Spacing mt={1} />
              <Button compact onClick={saveEdit} primary>Save messages</Button>
            </Spacing>
          )}
        </div>
      ))}
    </Spacing>
  );
}

export default StreamSamples;
