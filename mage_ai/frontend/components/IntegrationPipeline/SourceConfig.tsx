import React, { useEffect, useState } from 'react';
import { parse, stringify } from 'yaml';
import { useMutation } from 'react-query';

import BlockType, { BlockLanguageEnum } from '@interfaces/BlockType';
import Button from '@oracle/elements/Button';
import CodeEditor from '@components/CodeEditor';
import FlexContainer from '@oracle/components/FlexContainer';
import PipelineType from '@interfaces/PipelineType';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import usePrevious from '@utils/usePrevious';
import { CodeEditorStyle } from './index.style';
import { onSuccess } from '@api/utils/response';
import { singularize } from '@utils/string';

type SourceConfigProps = {
  api: string;
  block: BlockType;
  blockContent: any;
  onChangeCodeBlock: (type: string, uuid: string, value: string) => void;
  pipeline: PipelineType;
};

function SourceConfig({
  api: apiName,
  block,
  blockContent,
  onChangeCodeBlock,
  pipeline,
}: SourceConfigProps) {
  const [blockConfig, setBlockConfig] = useState(null);

  const blockConfigPrev = usePrevious(blockConfig);

  useEffect(() => {
    if (blockContent && !blockConfig && !blockConfigPrev) {
      setBlockConfig(stringify(blockContent?.config));
    }
  }, [blockConfig, blockConfigPrev, blockContent]);


  const [connected, setConnected] = useState<boolean>(null);
  const [error, setError] = useState<string>();
  // While the settings are being typed they are often not YAML yet ("host" before its
  // colon): the last valid settings are kept, and the problem is shown instead of thrown on
  // every keystroke.
  const [yamlError, setYamlError] = useState<string>(null);
  const [testConnection, { isLoading }] = useMutation(
    api[apiName].useCreate(),
    {
      onSuccess: (response: any) => onSuccess(
        response,
        {
          callback: (res) => {
            const key = singularize(apiName);
            setConnected(res?.[key]?.['success']);
            setError(res?.[key]?.['error_message']);
          },
          onErrorCallback: ({
            error: {
              errors,
              message,
            },
          }) => {
            console.log(errors, message);
          },
        },
      ),
    },
  );

  if (!block) {
    return null;
  }

  return (
    <>
      <CodeEditorStyle>
        <CodeEditor
          autoHeight
          language={BlockLanguageEnum.YAML}
          onChange={(val: string) => {
            setBlockConfig(val);
            let config;
            try {
              config = parse(val);
            } catch (err) {
              setYamlError(String(err?.message || err).split('\n')[0]);
              return;
            }
            setYamlError(null);
            onChangeCodeBlock(block.type, block.uuid, stringify({
              ...blockContent,
              config,
            }));
          }}
          tabSize={2}
          value={blockConfig || undefined}
          width="100%"
        />
      </CodeEditorStyle>
      <Spacing mt={1}>
        <FlexContainer alignItems="center">
          <Button
            disabled={!!yamlError}
            onClick={() => {
              // @ts-ignore
              testConnection({
                [singularize(apiName)]: {
                  action_type: 'test_connection',
                  config: blockConfig,
                  pipeline_uuid: pipeline.uuid,
                },
              });
              setError(null);
            }}
            small
            success
          >
            Test connection
          </Button>
          <Spacing ml={1}>
            {isLoading ? (
              <Spinner color="white" small/>
            ) : (
              <>
                {connected && (
                  <Text small success>
                    Connected successfully!
                  </Text>
                )}
                {connected === false && (
                  <Text small warning>
                    Failed to connect, see error below.
                  </Text>
                )}
              </>
            )}
          </Spacing>
        </FlexContainer>
        {yamlError && (
          <Spacing mt={1}>
            <Text muted small>
              Not valid YAML yet, so the settings are not saved: {yamlError}
            </Text>
          </Spacing>
        )}
        {error && (
          <Spacing mt={1}>
            <Text small warning>
              {error}
            </Text>
          </Spacing>
        )}
      </Spacing>
    </>
  );
}

export default SourceConfig;
