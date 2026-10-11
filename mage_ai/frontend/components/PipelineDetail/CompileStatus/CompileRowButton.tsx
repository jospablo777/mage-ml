import { useEffect, useRef, useState } from 'react';
import { useMutation } from 'react-query';

import Button from '@oracle/elements/Button';
import Spinner from '@oracle/components/Spinner';
import api from '@api';
import { BORDER_RADIUS_SMALL } from '@oracle/styles/units/borders';
import { Lightning } from '@oracle/icons';
import { PipelineCompilationType } from '@utils/models/compilation';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

type CompileRowButtonProps = {
  disabled?: boolean;
  onCompiled: () => void;
  pipelineUUID: string;
  // Changes when something else asks to compile, such as the row's right-click menu.
  requestedAt?: number;
};

// The pipelines list's Compile action: compiles the pipeline, shows a spinner while it
// compiles and refreshes the list when it ends.
function CompileRowButton({
  disabled,
  onCompiled,
  pipelineUUID,
  requestedAt,
}: CompileRowButtonProps) {
  const [showError] = useError(null, {}, [], { uuid: `CompileRowButton/${pipelineUUID}` });
  const [running, setRunning] = useState(false);
  const mounted = useRef(true);

  useEffect(() => () => {
    mounted.current = false;
  }, []);

  const waitForEnd = async () => {
    for (let attempt = 0; attempt < 1800 && mounted.current; attempt += 1) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      try {
        const response = await api.pipeline_compilations.detailAsync(pipelineUUID);
        const compilation: PipelineCompilationType = response?.pipeline_compilation;
        if (compilation?.state !== 'running') {
          break;
        }
      } catch {
        break;
      }
    }
    if (mounted.current) {
      setRunning(false);
      onCompiled();
    }
  };

  const [compile] = useMutation(
    api.pipeline_compilations.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => waitForEnd(),
        onErrorCallback: (response, errors) => {
          setRunning(false);
          showError({ errors, response });
        },
      }),
    },
  );

  const start = () => {
    setRunning(true);
    // @ts-ignore
    compile({ pipeline_compilation: { pipeline_uuid: pipelineUUID } });
  };

  useEffect(() => {
    if (requestedAt && !disabled && !running) {
      start();
    }
    // Only a new request starts a compilation.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [requestedAt]);

  return (
    <Button
      borderRadius={`${BORDER_RADIUS_SMALL}px`}
      disabled={disabled || running}
      iconOnly
      noBackground
      noBorder
      onClick={(e) => {
        e?.stopPropagation?.();
        start();
      }}
      outline
      padding="4px"
      title={disabled
        ? 'Nothing to compile: no Python, R or Rust blocks'
        : running ? 'Compiling' : 'Compile for production runs'}
    >
      {running
        ? <Spinner size={2 * UNIT} />
        : <Lightning default={!disabled} muted={disabled} size={2 * UNIT} />}
    </Button>
  );
}

export default CompileRowButton;
