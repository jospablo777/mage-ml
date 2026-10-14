import React, {
  memo,
  useCallback,
  useDeferredValue,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import Ansi from 'ansi-to-react';

import Button from '@oracle/elements/Button';
import Flex from '@oracle/components/Flex';
import FlexContainer from '@oracle/components/FlexContainer';
import KernelOutputType, {
  DataTypeEnum,
  DATA_TYPE_TEXTLIKE,
} from '@interfaces/KernelOutputType';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import ToggleSwitch from '@oracle/elements/Inputs/ToggleSwitch';
import { Close, PlayButton } from '@oracle/icons';
import {
  ContainerStyle as CodeBlockStyle,
  OutputRowStyle,
} from '@components/CodeBlock/CodeOutput/index.style';
import {
  LOCAL_STORAGE_KEY_PIPELINE_EXECUTION_HIDDEN,
  set,
} from '@storage/localStorage';
import { OutputContainerStyle, OutputHeaderStyle } from './index.style';
import { UNIT } from '@oracle/styles/units/spacing';
import { removeKeyboardFocus } from '@context/shared/utils';

// Lines drawn at first; older ones come back on request, a page at a time. Nothing is
// dropped: the run's output stays complete (text is never hidden).
const LINES_SHOWN = 500;
const LINES_PAGE = 1000;
// How close to the bottom counts as following the output.
const FOLLOW_SLACK = 24;

type OutputLine = {
  data: string;
  // The message's position in the run's output, which only grows: a stable key, so a new
  // message renders only its own row.
  key: string;
  type: DataTypeEnum;
};

const OutputLineRow = memo(function OutputLineRow({
  data,
  first,
  index,
  last,
  type,
}: OutputLine & { first: boolean; index: number; last: boolean }) {
  if (DATA_TYPE_TEXTLIKE.includes(type)) {
    return (
      <OutputRowStyle first={first} last={last}>
        <Text monospace preWrap>
          <Ansi>{data}</Ansi>
        </Text>
      </OutputRowStyle>
    );
  }
  if (type === DataTypeEnum.IMAGE_PNG) {
    return (
      <div style={{ backgroundColor: 'white' }}>
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          alt={`Image ${index} from the pipeline's output`}
          src={`data:image/png;base64, ${data}`}
          style={{ height: 'auto', width: '100%' }}
        />
      </div>
    );
  }
  return null;
});

export type PipelineExecutionProps = {
  cancelPipeline: () => void;
  checkIfPipelineRunning: () => void;
  executePipeline: () => void;
  isPipelineExecuting: boolean;
  pipelineExecutionHidden: boolean;
  pipelineMessages: KernelOutputType[];
  setPipelineExecutionHidden: (pipelineExecutionHidden: boolean) => void;
};

function PipelineExecution({
  cancelPipeline,
  checkIfPipelineRunning,
  executePipeline,
  isPipelineExecuting,
  pipelineExecutionHidden,
  pipelineMessages,
  setPipelineExecutionHidden,
}: PipelineExecutionProps) {
  // Messages can arrive by the thousand in a stream: React renders them when it has time,
  // so typing and scrolling stay smooth.
  const messages = useDeferredValue(pipelineMessages);
  const lines: OutputLine[] = useMemo(() => {
    const out: OutputLine[] = [];
    (messages || []).forEach(({ data: dataInit, type }: KernelOutputType, index: number) => {
      const dataArray = (Array.isArray(dataInit) ? dataInit : [dataInit]).filter(d => d);
      dataArray.forEach((data, inner) => {
        out.push({ data: data as string, key: `${index}-${inner}`, type });
      });
    });
    return out;
  }, [messages]);
  // While following, the view shows the last `shown` lines. When the reader scrolls up, the
  // view freezes on the lines it had (like `less`), so nothing moves under the reader; new
  // lines are counted on the jump button, and scrolling back to the bottom follows again.
  const refOutput = useRef<HTMLDivElement>(null);
  const [shown, setShown] = useState<number>(LINES_SHOWN);
  const [frozen, setFrozen] = useState<{ end: number; start: number } | null>(null);
  const start = frozen ? frozen.start : Math.max(0, lines.length - shown);
  const end = frozen ? Math.min(frozen.end, lines.length) : lines.length;
  const visibleLines = useMemo(() => lines.slice(start, end), [lines, start, end]);
  const hiddenEarlier = start;
  const newLines = lines.length - end;

  const onScroll = useCallback(() => {
    const element = refOutput.current;
    if (!element) {
      return;
    }
    const atBottom = element.scrollHeight - element.scrollTop - element.clientHeight
      < FOLLOW_SLACK;
    if (atBottom && frozen) {
      // Keep the earlier lines the reader asked for.
      setShown(previous => Math.max(previous, frozen.end - frozen.start));
      setFrozen(null);
    } else if (!atBottom && !frozen) {
      setFrozen({ end, start });
    }
  }, [end, frozen, start]);
  useLayoutEffect(() => {
    const element = refOutput.current;
    if (!frozen && element) {
      element.scrollTop = element.scrollHeight;
    }
  }, [frozen, lines.length, pipelineExecutionHidden, shown]);
  const jumpToLatest = useCallback(() => setFrozen(null), []);
  // The reader's intent to leave the bottom freezes the view at once. Waiting for the scroll
  // event lost it while lines arrived: a render could move the view back to the bottom
  // between the reader's scroll and its event.
  const refRange = useRef({ end, start });
  refRange.current = { end, start };
  const freeze = useCallback(() => {
    setFrozen(previous => previous ?? { ...refRange.current });
  }, []);
  const onWheel = useCallback((event: React.WheelEvent) => {
    if (event.deltaY < 0) {
      freeze();
    }
  }, [freeze]);
  const onKeyDown = useCallback((event: React.KeyboardEvent) => {
    if (['ArrowUp', 'PageUp', 'Home'].includes(event.key)) {
      freeze();
    }
  }, [freeze]);
  const onPointerDown = useCallback(() => {
    // Dragging the scroll bar, selecting text, or a touch scroll.
    const element = refOutput.current;
    if (element && element.scrollHeight > element.clientHeight) {
      freeze();
    }
  }, [freeze]);

  // Earlier lines go above what the reader sees: keep their place.
  const refHeightBefore = useRef<number>(null);
  const showEarlier = useCallback(() => {
    if (frozen) {
      refHeightBefore.current = refOutput.current?.scrollHeight ?? null;
      setFrozen({ ...frozen, start: Math.max(0, frozen.start - LINES_PAGE) });
    } else {
      setShown(previous => previous + LINES_PAGE);
    }
  }, [frozen]);
  useLayoutEffect(() => {
    const element = refOutput.current;
    if (frozen && element && refHeightBefore.current !== null) {
      element.scrollTop += element.scrollHeight - refHeightBefore.current;
    }
    refHeightBefore.current = null;
  }, [frozen, start]);

  // A new run starts a new output.
  useLayoutEffect(() => {
    if (!pipelineMessages?.length) {
      setShown(LINES_SHOWN);
      setFrozen(null);
    }
  }, [pipelineMessages?.length]);

  // When the pipeline starts executing, the execution button gets disabled.
  // Disabled buttons are not focusable, so manually remove the focus here.
  const handleExecutePipeline = useCallback(() => {
    removeKeyboardFocus();
    executePipeline();
  }, [executePipeline]);
  const handleCancelPipeline = useCallback(() => {
    removeKeyboardFocus();
    cancelPipeline();
  }, [cancelPipeline]);

  const togglePipelineExecution = useCallback(() => {
    const val = !pipelineExecutionHidden;
    setPipelineExecutionHidden(val);
    set(LOCAL_STORAGE_KEY_PIPELINE_EXECUTION_HIDDEN, val);
  }, [
    pipelineExecutionHidden,
    setPipelineExecutionHidden,
  ]);

  return (
    <>
      <OutputHeaderStyle>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <Flex>
            <Button
              beforeIcon={<PlayButton inverted size={UNIT * 2}/>}
              compact={isPipelineExecuting}
              disabled={isPipelineExecuting}
              loading={isPipelineExecuting}
              onClick={handleExecutePipeline}
              success
            >
              <Text
                bold
                inverted
                noWrapping
                primary={false}
              >
                Execute pipeline
              </Text>
            </Button>
            <Spacing ml={1} />
            {isPipelineExecuting && (
              <>
                <Button
                  beforeIcon={<Close inverted size={UNIT * 2}/>}
                  onClick={handleCancelPipeline}
                  success
                >
                  <Text
                    bold
                    inverted
                    noWrapping
                    primary={false}
                  >
                    Cancel pipeline
                  </Text>
                </Button>
                <Spacing ml={1} />
              </>
            )}
            <Button
              onClick={checkIfPipelineRunning}
              secondary
            >
              <Text
                bold
                noWrapping
              >
                Running status
              </Text>
            </Button>
          </Flex>
          <Flex alignItems="center">
            <Spacing ml={1} />
            <Text>
              Hide
            </Text>
            <Spacing mr={1} />
            <ToggleSwitch
              checked={pipelineExecutionHidden}
              onCheck={togglePipelineExecution}
            />
          </Flex>
        </FlexContainer>
      </OutputHeaderStyle>


      {!pipelineExecutionHidden &&
        <>
          <Spacing mb={1} />
          {hiddenEarlier > 0 && (
            <Spacing mb={1}>
              <Button
                compact
                onClick={showEarlier}
                secondary
              >
                Show earlier output ({hiddenEarlier.toLocaleString()} line
                {hiddenEarlier === 1 ? '' : 's'})
              </Button>
            </Spacing>
          )}
          <div style={{ position: 'relative' }}>
            <OutputContainerStyle
              noScrollbarTrackBackground
              onKeyDown={onKeyDown}
              onPointerDown={onPointerDown}
              onScroll={onScroll}
              onTouchStart={onPointerDown}
              onWheel={onWheel}
              ref={refOutput}
              tabIndex={0}
            >
              <CodeBlockStyle
                executedAndIdle
                hasError={false}
                selected
              >
                {visibleLines.map((line, index) => (
                  <OutputLineRow
                    {...line}
                    first={index === 0}
                    index={start + index}
                    key={line.key}
                    last={index === visibleLines.length - 1}
                  />
                ))}
              </CodeBlockStyle>
            </OutputContainerStyle>
            {newLines > 0 && (
              <div style={{ bottom: UNIT, position: 'absolute', right: UNIT * 2 }}>
                <Button compact onClick={jumpToLatest} primary>
                  Jump to latest ({newLines.toLocaleString()} new line
                  {newLines === 1 ? '' : 's'})
                </Button>
              </div>
            )}
          </div>
        </>
      }
    </>
  );
}

export default PipelineExecution;
