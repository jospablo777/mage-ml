import { useContext, useState } from 'react';
import { useMutation } from 'react-query';
import styled, { ThemeContext } from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { MONO_FONT_FAMILY_REGULAR } from '@oracle/styles/fonts/primary';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

type BlockResultType = {
  block_uuid: string;
  detail?: string;
  result: string;
  stage?: number;
};

type FusionVerificationType = {
  finished_at?: number;
  log?: string;
  pipeline_uuid: string;
  report?: {
    blocks?: BlockResultType[];
    first_difference?: BlockResultType;
    passed?: boolean;
    statuses?: { [mode: string]: string };
  };
  started_at?: number;
  status: 'error' | 'failed' | 'none' | 'passed' | 'running' | 'stopped';
};

const LogStyle = styled.pre`
  font-family: ${MONO_FONT_FAMILY_REGULAR};
  font-size: 12px;
  line-height: 18px;
  margin: 0;
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

const RESULT_LABELS = {
  close: 'same within rounding',
  differs: 'differs',
  failed: 'failed',
  'not compared': 'not compared',
  same: 'same',
};

function seconds(verification: FusionVerificationType): string {
  if (!verification?.started_at) {
    return '';
  }
  const end = verification.finished_at || Date.now() / 1000;
  return `${Math.max(0, Math.round(end - verification.started_at))} s`;
}

type FusionVerificationProps = {
  pipelineUUID: string;
};

// Runs `mage verify-fusion` for the pipeline and shows its log and per-block result.
function FusionVerification({ pipelineUUID }: FusionVerificationProps) {
  const themeContext = useContext(ThemeContext);
  const [showError] = useError(null, {}, [], { uuid: 'FusionVerification' });
  const [confirming, setConfirming] = useState(false);
  const { data, mutate } = api.fusion_verifications.detail(pipelineUUID, {}, {
    refreshInterval: (latest: { fusion_verification?: FusionVerificationType }) => (
      latest?.fusion_verification?.status === 'running' ? 1500 : 0
    ),
  });
  const verification: FusionVerificationType = data?.fusion_verification;
  const running = verification?.status === 'running';

  const [start, { isLoading: isStarting }] = useMutation(
    api.fusion_verifications.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => {
          setConfirming(false);
          mutate();
        },
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  const report = verification?.report;
  const difference = report?.first_difference;
  const borderColor = (themeContext?.borders || dark.borders).light;

  return (
    <>
      <Text muted small>
        Runs the pipeline once block by block and once with block fusion, from the same
        variables, and compares the stored output of every block. It names the first block
        whose output differs. Both runs execute the whole pipeline, so exporters write twice.
      </Text>

      <Spacing mt={1}>
        <FlexContainer alignItems="center">
          {!confirming && !running && (
            <Button compact onClick={() => setConfirming(true)}>
              Verify fusion
            </Button>
          )}
          {confirming && !running && (
            <>
              <Text small warning>
                Run the pipeline twice, exporters included?&nbsp;&nbsp;
              </Text>
              <Button
                compact
                loading={isStarting}
                // @ts-ignore
                onClick={() => start({ fusion_verification: { pipeline_uuid: pipelineUUID } })}
                primary
              >
                Run both
              </Button>
              <Spacing ml={1} />
              <Button compact onClick={() => setConfirming(false)}>
                Cancel
              </Button>
            </>
          )}
          {running && (
            <>
              <Text small>
                Verifying, {seconds(verification)}&nbsp;&nbsp;
              </Text>
              <Button
                compact
                onClick={() => api.fusion_verifications.deleteAsync(pipelineUUID).then(() => mutate())}
              >
                Stop
              </Button>
            </>
          )}
        </FlexContainer>
      </Spacing>

      {verification?.status === 'passed' && (
        <Spacing mt={1}>
          <Text small success>
            Fusion gives the same outputs as running block by block ({seconds(verification)}).
          </Text>
        </Spacing>
      )}
      {verification?.status === 'failed' && (
        <Spacing mt={1}>
          <Text danger small>
            {difference
              ? `First difference: ${difference.block_uuid}, ${RESULT_LABELS[difference.result]
                || difference.result}. ${difference.detail || ''}`
              : `A run did not complete: ${Object.entries(report?.statuses || {})
                .map(([mode, status]) => `${mode} ${status}`).join(', ')}.`}
          </Text>
        </Spacing>
      )}
      {verification?.status === 'error' && (
        <Spacing mt={1}>
          <Text danger small>
            The verification could not run; its log says why.
          </Text>
        </Spacing>
      )}
      {verification?.status === 'stopped' && (
        <Spacing mt={1}>
          <Text muted small>
            The verification was stopped.
          </Text>
        </Spacing>
      )}

      {report?.blocks?.length > 0 && (
        <Spacing mt={1}>
          {report.blocks.map(block => (
            <div
              key={block.block_uuid}
              style={{ borderBottom: `1px solid ${borderColor}`, padding: `${UNIT / 2}px 0` }}
            >
              <FlexContainer alignItems="center">
                <div style={{ minWidth: 260 }}>
                  <Text monospace small>{block.block_uuid}</Text>
                </div>
                <div style={{ minWidth: 70 }}>
                  <Text muted small>{block.stage ? `stage ${block.stage}` : ''}</Text>
                </div>
                <Text
                  danger={['differs', 'failed'].includes(block.result)}
                  small
                  success={block.result === 'same'}
                  warning={block.result === 'close'}
                >
                  {RESULT_LABELS[block.result] || block.result}
                </Text>
              </FlexContainer>
              {block.detail && (
                <Text muted preWrap small>{block.detail}</Text>
              )}
            </div>
          ))}
        </Spacing>
      )}

      {verification?.log && (
        <Spacing mt={1}>
          <LogStyle>{verification.log}</LogStyle>
        </Spacing>
      )}
    </>
  );
}

export default FusionVerification;
