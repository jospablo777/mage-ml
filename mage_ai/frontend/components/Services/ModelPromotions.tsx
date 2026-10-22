import { useState } from 'react';
import { useMutation } from 'react-query';
import styled from 'styled-components';

import Button from '@oracle/elements/Button';
import FlexContainer from '@oracle/components/FlexContainer';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { BORDER_RADIUS } from '@oracle/styles/units/borders';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';

type ChallengerType = { name: string; uri: string; version?: string };

type ModelStatusType = {
  built?: string;
  challengers: ChallengerType[];
  name: string;
  problem?: string;
  registry?: string;
  status: 'current' | 'moved' | 'not built';
  uri: string;
};

type PromotionType = {
  at: string;
  by: string;
  challenger?: string;
  from_version?: string;
  how: string;
  model: string;
  reason?: string;
  to_version: string;
};

type PromoteRequest = { challenger?: string; model: string };

const RowStyle = styled.div`
  border-radius: ${BORDER_RADIUS}px;
  padding: ${UNIT * 1.5}px;

  ${props => `
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const STATUS_TEXT = {
  current: 'The image has the version the registry names.',
  moved: 'The registry names another version than the image has.',
  'not built': 'No image was built with this model yet.',
};

// A service's models against the registry, and promoting a version: the moved alias's or
// a challenger's, recorded with the signed-in user. Building and shipping the image puts it
// in the service.
function ModelPromotions({ serviceName }: { serviceName: string }) {
  const { data, mutate } = api.pipeline_service_models.detail(
    serviceName, {}, { revalidateOnFocus: false },
  );
  const [reason, setReason] = useState<string>('');
  const [result, setResult] = useState<{ error?: string; promoted?: PromotionType }>(null);
  const state = data?.pipeline_service_model;
  const models: ModelStatusType[] = state?.models || [];
  const history: PromotionType[] = state?.promotions || [];

  const [promote, { isLoading }] = useMutation(
    (request: PromoteRequest) => api.pipeline_service_models.useCreate()({
      pipeline_service_model: {
        ...request,
        name: serviceName,
        reason: reason.trim() || undefined,
      },
    }),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_service_model: promoted }) => {
          setResult({ promoted: promoted?.promoted });
          setReason('');
          mutate();
        },
        onErrorCallback: (response) => setResult({
          error: response?.error?.message || 'The promotion did not go through.',
        }),
      }),
    },
  );

  if (!data) {
    return <Spinner inverted small />;
  }
  if (data?.error) {
    return <Text muted small>{data.error.message}</Text>;
  }
  if (!models.length) {
    return <Text muted small>The service embeds no models from a registry.</Text>;
  }

  return (
    <>
      {models.map(model => (
        <Spacing key={model.name} mb={1}>
          <RowStyle>
            <FlexContainer alignItems="center" justifyContent="space-between">
              <div>
                <Text bold monospace>{model.name}</Text>
                <Text muted small>
                  Image: {model.built ? `version ${model.built}` : 'not built'}.
                  Registry now: {model.registry ? `version ${model.registry}` : 'unknown'}.
                  {' '}{STATUS_TEXT[model.status]}
                </Text>
                {model.problem && <Text danger small>{model.problem}</Text>}
              </div>
              {model.status === 'moved' && (
                <Button
                  compact
                  disabled={isLoading}
                  onClick={() => promote({ model: model.name })}
                  primary
                >
                  Promote version {model.registry}
                </Button>
              )}
            </FlexContainer>
            {model.challengers.map(challenger => (
              <Spacing key={challenger.name} mt={1}>
                <FlexContainer alignItems="center" justifyContent="space-between">
                  <Text small>
                    Challenger <Text inline monospace small>{challenger.name}</Text>
                    {challenger.version ? `, version ${challenger.version}` : ''}
                  </Text>
                  <Button
                    compact
                    disabled={isLoading || !challenger.version}
                    onClick={() => promote({ challenger: challenger.name, model: model.name })}
                  >
                    Promote {challenger.name}
                  </Button>
                </FlexContainer>
              </Spacing>
            ))}
          </RowStyle>
        </Spacing>
      ))}

      <Spacing mt={1}>
        <TextInput
          label="Why (kept in promotions.yaml)"
          monospace={false}
          onChange={e => setReason(e.target.value)}
          value={reason}
        />
      </Spacing>

      {result?.promoted && (
        <Spacing mt={1}>
          <Text small success>
            {result.promoted.model}: version {result.promoted.to_version} promoted
            {result.promoted.from_version ? ` from version ${result.promoted.from_version}` : ''}.
            Build and ship the image to put it in the service.
          </Text>
        </Spacing>
      )}
      {result?.error && (
        <Spacing mt={1}><Text danger small>{result.error}</Text></Spacing>
      )}

      {history.length > 0 && (
        <Spacing mt={2}>
          <Text bold small>Last promotions</Text>
          {history.slice().reverse().map(record => (
            <Text key={`${record.at}-${record.model}`} muted small>
              {record.at} {record.by}: {record.model} {record.from_version || '?'} to
              {' '}{record.to_version} ({record.how}){record.reason ? `, ${record.reason}` : ''}
            </Text>
          ))}
        </Spacing>
      )}
    </>
  );
}

export default ModelPromotions;
