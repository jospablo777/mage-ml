import styled from 'styled-components';
import { useMutation } from 'react-query';
import { useRouter } from 'next/router';
import { useState } from 'react';

import Button from '@oracle/elements/Button';
import ErrorCallout from './ErrorCallout';
import FlexContainer from '@oracle/components/FlexContainer';
import Select from '@oracle/elements/Inputs/Select';
import Spacing from '@oracle/elements/Spacing';
import Text from '@oracle/elements/Text';
import TextInput from '@oracle/elements/Inputs/TextInput';
import api from '@api';
import { CardStyle, FieldLabel } from './shared';
import { UNIT } from '@oracle/styles/units/spacing';
import { onSuccess } from '@api/utils/response';
import { reloadLanguageEnvironments } from '@utils/models/blockEnvironment';

const FieldsStyle = styled.div`
  display: grid;
  gap: ${UNIT * 2}px;
  grid-template-columns: repeat(auto-fit, minmax(${UNIT * 24}px, 1fr));
`;

// block_environments.normalized: what a name may be, said before the server refuses it.
const NAME = /^[a-z0-9][a-z0-9_-]{0,63}$/;

// A named R or Rust environment (ADR-0040): a name, where its packages start from, and for R
// the R version. Creating writes files only; R packages install with Sync on its page.
function NewLanguageEnvironment({ onCancel }: { onCancel: () => void }) {
  const router = useRouter();
  const [language, setLanguage] = useState<'r' | 'rust'>('r');
  const [name, setName] = useState('');
  const [startFrom, setStartFrom] = useState<'default' | 'minimal'>('default');
  const [rVersion, setRVersion] = useState('');
  const [error, setError] = useState<string>(null);
  const normal = name.trim().toLowerCase();
  const invalid = !!normal && (!NAME.test(normal) || normal === 'default');

  const [create, { isLoading }] = useMutation(
    api.language_environments.useCreate(),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ language_environment: created }) => reloadLanguageEnvironments().then(
          () => router.push('/workers/languages/[language]', `/workers/languages/${created.id}`),
        ),
        onErrorCallback: response => setError(response?.error?.message || 'Not created.'),
      }),
    },
  );

  return (
    <CardStyle>
      <Text bold>New R or Rust environment</Text>
      <Text muted small>
        For blocks that need other package versions, another R version, or a heavy crate kept
        out of the other blocks&apos; builds. A block or a pipeline chooses it.
      </Text>
      <Spacing mt={2} />
      <FieldsStyle>
        <div>
          <FieldLabel>Language</FieldLabel>
          <Select
            aria-label="Language"
            onChange={e => setLanguage(e.target.value)}
            value={language}
          >
            <option value="r">R</option>
            <option value="rust">Rust</option>
          </Select>
        </div>
        <div>
          <FieldLabel>Name</FieldLabel>
          <TextInput
            aria-label="Name"
            meta={{ error: invalid ? 'Lowercase letters, digits, _ and -' : null, touched: true }}
            onChange={e => setName(e.target.value)}
            placeholder={language === 'r' ? 'models' : 'onnx'}
            value={name}
          />
        </div>
        <div>
          <FieldLabel>Start from</FieldLabel>
          <Select
            aria-label="Start from"
            onChange={e => setStartFrom(e.target.value)}
            value={startFrom}
          >
            <option value="default">
              The default environment&apos;s {language === 'r' ? 'packages' : 'crates'}
            </option>
            <option value="minimal">Only what blocks need</option>
          </Select>
        </div>
        {language === 'r' && (
          <div>
            <FieldLabel>R version</FieldLabel>
            <TextInput
              aria-label="R version"
              onChange={e => setRVersion(e.target.value)}
              placeholder="The default environment's"
              value={rVersion}
            />
          </div>
        )}
      </FieldsStyle>
      <Spacing mt={1.5}>
        <Text muted xsmall>
          {language === 'r'
            ? 'Its folder is r_environments/<name>; its packages install with Sync on its page. '
              + 'Another R version is installed with rig.'
            : 'Its folder is rust_environments/<name>; its crates compile at the first build of '
              + 'a block in it, into the target directory every environment shares.'}
        </Text>
      </Spacing>
      {error && <Spacing mt={2}><ErrorCallout message={error} title="Not created" /></Spacing>}
      <Spacing mt={2}>
        <FlexContainer alignItems="center">
          <Button
            disabled={!normal || invalid}
            loading={isLoading}
            onClick={() => {
              setError(null);
              // @ts-ignore
              create({
                language_environment: {
                  language,
                  name: normal,
                  r_version: language === 'r' ? rVersion.trim() || null : null,
                  start_from: startFrom,
                },
              });
            }}
            primary
          >
            Create environment
          </Button>
          <Spacing ml={1} />
          <Button onClick={onCancel}>Cancel</Button>
        </FlexContainer>
      </Spacing>
    </CardStyle>
  );
}

export default NewLanguageEnvironment;
