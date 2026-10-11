import CompileRowButton from './CompileRowButton';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import { LastCompiledType, timeAgo } from '@utils/models/compilation';

type CompiledCellProps = {
  lastCompiled?: LastCompiledType;
  onCompiled: () => void;
  pipelineUUID: string;
  requestedAt?: number;
};

// The pipelines list's "Compiled" column: a Compile button, when the pipeline last compiled
// and whether its code changed since; a dash for pipelines with nothing to compile.
function CompiledCell({ lastCompiled, onCompiled, pipelineUUID, requestedAt }: CompiledCellProps) {
  if (!lastCompiled) {
    return <CompiledStatus />;
  }

  return (
    <FlexContainer alignItems="center">
      <CompileRowButton
        onCompiled={onCompiled}
        pipelineUUID={pipelineUUID}
        requestedAt={requestedAt}
      />
      <Spacing mr={1} />
      <CompiledStatus lastCompiled={lastCompiled} />
    </FlexContainer>
  );
}

function CompiledStatus({ lastCompiled }: { lastCompiled?: LastCompiledType }) {
  if (!lastCompiled) {
    return (
      <Text muted small title="No Python, R or Rust blocks: nothing to compile">
        &#8212;
      </Text>
    );
  }
  if (lastCompiled.state === 'never' || !lastCompiled.finished_at) {
    return (
      <Text muted small title="Compiles on its own after a save, or from Compile in the editor">
        not yet
      </Text>
    );
  }

  const when = timeAgo(lastCompiled.finished_at);
  const at = new Date(lastCompiled.finished_at * 1000).toLocaleString();
  if (lastCompiled.state === 'failed') {
    return (
      <Text danger small title={`Failed at ${at}; open the pipeline to see why`}>
        failed {when}
      </Text>
    );
  }

  return (
    <Text
      small
      success={lastCompiled.up_to_date}
      title={lastCompiled.up_to_date
        ? `Compiled at ${at}`
        : `Compiled at ${at}; the code changed since`}
      warning={!lastCompiled.up_to_date}
    >
      {when}{lastCompiled.up_to_date ? '' : ' · changed'}
    </Text>
  );
}

export default CompiledCell;
