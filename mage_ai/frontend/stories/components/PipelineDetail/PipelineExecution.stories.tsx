import React, { useEffect, useState } from 'react';
import { Meta, StoryFn } from '@storybook/nextjs-vite';

import KernelOutputType, { DataTypeEnum } from '@interfaces/KernelOutputType';
import PipelineExecution from '@components/PipelineDetail/PipelineExecution';
import ThemeBlock from '../../ThemeBlock';

export default {
  component: PipelineExecution,
  title: 'Components/PipelineDetail/PipelineExecution',
} as Meta;

// A streaming pipeline's output: 20 messages every 50 ms, up to `total`.
function StreamingOutput({ total }: { total: number }) {
  const [messages, setMessages] = useState<KernelOutputType[]>([]);
  useEffect(() => {
    const timer = setInterval(() => setMessages((previous) => {
      if (previous.length >= total) {
        clearInterval(timer);
        return previous;
      }
      const next = Array.from({ length: 20 }, (_, i) => ({
        data: `message ${previous.length + i + 1}: {"order_id": ${previous.length + i + 1}}`,
        type: DataTypeEnum.TEXT_PLAIN,
      }));
      return previous.concat(next);
    }), 50);
    return () => clearInterval(timer);
  }, [total]);
  const [hidden, setHidden] = useState<boolean>(false);

  return (
    <PipelineExecution
      cancelPipeline={() => null}
      checkIfPipelineRunning={() => null}
      executePipeline={() => null}
      isPipelineExecuting={messages.length < total}
      pipelineExecutionHidden={hidden}
      pipelineMessages={messages}
      setPipelineExecutionHidden={setHidden}
    />
  );
}

const Template: StoryFn<{ total: number }> = args => (
  <ThemeBlock darkOnly>
    <StreamingOutput {...args} />
  </ThemeBlock>
);

export const Streaming = Template.bind({});
Streaming.args = {
  total: 1200,
};
