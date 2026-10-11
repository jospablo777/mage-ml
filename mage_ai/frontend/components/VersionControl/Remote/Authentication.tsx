import React, { useMemo } from 'react';
import { useMutation } from 'react-query';
import { useRouter } from 'next/router';

import BitbucketWithText from '@oracle/icons/custom/BitbucketWithText';
import Button from '@oracle/elements/Button';
import Divider from '@oracle/elements/Divider';
import Flex from '@oracle/components/Flex';
import FlexContainer from '@oracle/components/FlexContainer';
import GitBranchType from '@interfaces/GitBranchType';
import Headline from '@oracle/elements/Headline';
import OauthType, { OauthProviderEnum } from '@interfaces/OauthType';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import api from '@api';
import { DevOpsWithText, GitHubWithTextIcon, GitLabWithTextIcon } from '@oracle/icons';
import { UNIT, UNITS_BETWEEN_ITEMS_IN_SECTIONS, UNITS_BETWEEN_SECTIONS } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { queryFromUrl } from '@utils/url';
import { set } from '@storage/localStorage';

const PROVIDER_TO_ICON_MAPPING = {
  [OauthProviderEnum.AZURE_DEVOPS]: DevOpsWithText,
  [OauthProviderEnum.BITBUCKET]: BitbucketWithText,
  [OauthProviderEnum.GITHUB]: GitHubWithTextIcon,
  [OauthProviderEnum.GITLAB]: GitLabWithTextIcon,
};

type ProviderProps = {
  isLoadingCreateOauth: boolean;
  oauth: OauthType;
  provider: OauthProviderEnum | string;
  showError: (opts: any) => void;
};

type AuthenticationProps = {
  branch: GitBranchType;
  isLoadingCreateOauth: boolean;
  showError: (opts: any) => void;
};

function Provider({
  isLoadingCreateOauth,
  oauth,
  provider,
  showError,
}: ProviderProps) {
  const router = useRouter();

  const [updateOauth, { isLoading: isLoadingUpdateOauth }] = useMutation(
    api.oauths.useUpdate(provider),
    {
      onSuccess: (response: any) => onSuccess(
        response, {
          callback: () => window.location.href = window.location.href.split('?')[0],
          onErrorCallback: (response, errors) => showError({
            errors,
            response,
          }),
        },
      ),
    },
  );

  const Icon = PROVIDER_TO_ICON_MAPPING[provider];

  return (
    <FlexContainer justifyContent="space-between">
      <Flex>
        <Icon size={UNIT * 12} />
      </Flex>
      <Flex alignItems="center">
        {oauth?.authenticated ? (
          <Button
            loading={isLoadingUpdateOauth}
            // @ts-ignore
            onClick={() => updateOauth({
              oauth: {
                action_type: 'reset',
              },
            })}
            warning
          >
            Reset
          </Button>
        ) : (
          <Button
            loading={isLoadingCreateOauth}
            onClick={() => {
              const url = oauth?.url;
              const q = queryFromUrl(url);
              const state = q.state;
              set(state, oauth?.redirect_query_params || {});
              router.push(url);
            }}
            primary
          >
            Authenticate
          </Button>
        )}
      </Flex>
    </FlexContainer>
  );
}

function Authentication({
  branch,
  isLoadingCreateOauth,
  showError,
}: AuthenticationProps) {
  const { data: dataOauths } = api.oauths.list({
    type: 'git',
    redirect_uri: typeof window !== 'undefined' ? encodeURIComponent(window.location.href) : '',
  });

  const accessTokenExists = useMemo(() => branch?.access_token_exists, [branch]);

  // Only the Git providers configured with your own OAuth application.
  const providerMapping = useMemo(() => dataOauths?.oauths?.reduce(
    (acc, curr) => {
      acc[curr.provider] = curr;
      return acc;
    },
    {},
  ) || {}, [dataOauths]);

  const anyOauthAuthenticated = useMemo(
    () => Object.values(providerMapping).some((oauth) => oauth?.['authenticated']),
    [providerMapping],
  );

  return (
    <>
      <Headline>
        Authentication
      </Headline>

      <Spacing mt={UNITS_BETWEEN_ITEMS_IN_SECTIONS}>
        {accessTokenExists && !anyOauthAuthenticated && (
          <Spacing mb={2}>
            <Button
              disabled
            >
              Using access token from Git Settings
            </Button>
            <Spacing mt={1}>
              <Text muted>
                Some features may not work unless you authenticate with a Git client.
              </Text>
            </Spacing>
          </Spacing>
        )}

        <Spacing mb={UNITS_BETWEEN_SECTIONS}>
          <Spacing mb={UNITS_BETWEEN_ITEMS_IN_SECTIONS}>
            <Text muted>
              Authenticating with the apps below will allow you to easily pull, push, and create
              pull requests.
            </Text>
          </Spacing>
          <Spacing mb={UNITS_BETWEEN_ITEMS_IN_SECTIONS} style={{ maxWidth: '600px' }}>
            <Divider muted />
            {Object.keys(providerMapping).length === 0 && (
              <Spacing py={1}>
                <Text muted>
                  No Git app is set up. Use a personal access token or an SSH key in the Git
                  settings, or register your own OAuth application: for GitHub, set
                  GHE_HOSTNAME=https://github.com, GHE_CLIENT_ID and GHE_CLIENT_SECRET.
                </Text>
              </Spacing>
            )}
            {Object.entries(providerMapping || {}).map(([provider, oauth]) => (
              <>
                <Provider
                  isLoadingCreateOauth={isLoadingCreateOauth}
                  key={provider}
                  oauth={oauth}
                  provider={provider}
                  showError={showError}
                />
                <Divider muted />
              </>
            ))}
          </Spacing>
        </Spacing>
      </Spacing>
    </>
  );
}

export default Authentication;
