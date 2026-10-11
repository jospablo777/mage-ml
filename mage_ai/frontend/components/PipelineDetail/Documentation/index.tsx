import { useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useMutation } from 'react-query';
import styled, { ThemeContext } from 'styled-components';

import Button from '@oracle/elements/Button';
import CodeEditor from '@components/CodeEditor';
import FlexContainer from '@oracle/components/FlexContainer';
import Headline from '@oracle/elements/Headline';
import Markdown from '@oracle/components/Markdown';
import Spacing from '@oracle/elements/Spacing';
import Spinner from '@oracle/components/Spinner';
import Text from '@oracle/elements/Text';
import api from '@api';
import dark from '@oracle/styles/themes/dark';
import { API_KEY } from '@api/utils/fetcher';
import { UNIT } from '@oracle/styles/units/spacing';
import { buildUrl } from '@api/utils/url';
import { onSuccess } from '@api/utils/response';
import { useError } from '@context/Error';

export type PipelineDocumentationType = {
  exists: boolean;
  format?: 'markdown' | 'quarto';
  path?: string;
  pipeline_uuid: string;
  quarto_available: boolean;
  source: string;
  template: string;
  updated_at?: number;
};

const FORMAT_LABELS = {
  markdown: 'Markdown',
  quarto: 'Quarto',
};

const PaneStyle = styled.div`
  border-radius: ${UNIT}px;
  overflow: auto;
  padding: ${UNIT * 2}px;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};
  `}
`;

const ChoiceStyle = styled.button`
  border-radius: ${UNIT}px;
  cursor: pointer;
  flex: 1;
  padding: ${UNIT * 2}px;
  text-align: left;

  ${props => `
    background-color: ${(props.theme.background || dark.background).panel};
    border: 1px solid ${(props.theme.borders || dark.borders).light};

    &:hover, &:focus-visible {
      border-color: ${(props.theme.interactive || dark.interactive).hoverBorder};
    }
  `}
`;

const FrameStyle = styled.iframe`
  border: none;
  border-radius: ${UNIT}px;
  height: 100%;
  min-height: ${UNIT * 60}px;
  width: 100%;
  background-color: #FFFFFF;
`;

// A relative image path in the document, served from the pipeline's folder.
function imageUrl(pipelineUUID: string, src: string): string {
  if (!src || /^(?:[a-z]+:|\/\/|\/|#)/i.test(src)) {
    return src;
  }
  const path = src.replace(/^\.\//, '').split('/').map(encodeURIComponent).join('/');
  return buildUrl(
    'pipeline_documentations', pipelineUUID, 'images', path, { api_key: API_KEY },
  );
}

type DocumentationProps = {
  pipelineUUID: string;
};

// A pipeline's README: read it, create it from a template, edit it beside a live preview.
function Documentation({ pipelineUUID }: DocumentationProps) {
  const themeContext = useContext(ThemeContext);
  const [showError] = useError(null, {}, [], { uuid: 'PipelineDocumentation' });
  const { data, mutate } = api.pipeline_documentations.detail(pipelineUUID);
  const doc: PipelineDocumentationType = data?.pipeline_documentation;

  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState('');
  const [format, setFormat] = useState<'markdown' | 'quarto'>('markdown');
  const [frameLoading, setFrameLoading] = useState(true);

  const unsaved = editing && draft !== (doc?.exists ? doc.source : '');

  useEffect(() => {
    if (!unsaved) {
      return;
    }
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [unsaved]);

  const startEditing = useCallback((startFormat?: 'markdown' | 'quarto') => {
    setDraft(doc?.exists ? doc.source : doc?.template || '');
    setFormat(startFormat || doc?.format || 'markdown');
    setEditing(true);
  }, [doc]);

  const [save, { isLoading: isSaving }] = useMutation(
    api.pipeline_documentations.useUpdate(pipelineUUID),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: () => {
          setEditing(false);
          setFrameLoading(true);
          mutate();
        },
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );

  // An image (or a chart saved as PNG or SVG) goes to the pipeline folder's images/
  // directory, and a link to it is added to the end of the draft.
  const imageInput = useRef<HTMLInputElement>(null);
  const [uploadImage, { isLoading: isUploading }] = useMutation(
    api.pipeline_documentations.useUpdate(pipelineUUID),
    {
      onSuccess: (response: any) => onSuccess(response, {
        callback: ({ pipeline_documentation: saved }) => {
          const path = saved?.image_path;
          if (path) {
            const name = path.split('/').pop().replace(/\.[^.]+$/, '');
            setDraft(prev => `${prev.replace(/\s*$/, '')}\n\n![${name}](${path})\n`);
          }
        },
        onErrorCallback: (response, errors) => showError({ errors, response }),
      }),
    },
  );
  const addImage = useCallback((file: File) => {
    const reader = new FileReader();
    reader.onload = () => {
      const content = String(reader.result).split(',').pop();
      // @ts-ignore
      uploadImage({ pipeline_documentation: { image: { content, name: file.name } } });
    };
    reader.readAsDataURL(file);
  }, [uploadImage]);

  const saveDraft = useCallback((source?: string) => {
    // @ts-ignore
    save({ pipeline_documentation: { format, source: source ?? draft } });
  }, [draft, format, save]);

  const toImage = useCallback((src: string) => imageUrl(pipelineUUID, src), [pipelineUUID]);
  const renderedUrl = useMemo(() => buildUrl(
    'pipeline_documentations', pipelineUUID, 'rendered', null, {
      api_key: API_KEY,
      v: doc?.updated_at || 0,
    },
  ), [doc?.updated_at, pipelineUUID]);

  if (!doc) {
    return (
      <Spacing p={3}>
        <Spinner />
      </Spacing>
    );
  }

  const imagesHint = (
    <Text muted small>
      Add images and charts (PNG, JPEG, GIF, WebP or SVG) with Add image, or put them in
      this pipeline&apos;s folder, <Text inline monospace small>
        pipelines/{pipelineUUID}/
      </Text>, and show them with <Text inline monospace small>
        ![Description](image.png)
      </Text>. The file is saved as <Text inline monospace small>
        {format === 'quarto' ? 'README.qmd' : 'README.md'}
      </Text> and versions with the pipeline.
    </Text>
  );

  if (editing) {
    return (
      <Spacing p={3}>
        <FlexContainer alignItems="center" justifyContent="space-between">
          <FlexContainer alignItems="center">
            <Headline level={4}>Documentation</Headline>
            <Spacing ml={2} />
            {(['markdown', 'quarto'] as const).map(value => (
              <Spacing key={value} mr={1}>
                <Button
                  compact
                  onClick={() => setFormat(value)}
                  primary={format === value}
                  secondary={format !== value}
                >
                  {FORMAT_LABELS[value]}
                </Button>
              </Spacing>
            ))}
            {unsaved && (
              <Text muted small>
                Unsaved changes
              </Text>
            )}
          </FlexContainer>
          <FlexContainer alignItems="center">
            <input
              accept=".png,.jpg,.jpeg,.gif,.webp,.svg"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) {
                  addImage(file);
                }
                event.target.value = '';
              }}
              ref={imageInput}
              style={{ display: 'none' }}
              type="file"
            />
            <Button loading={isUploading} onClick={() => imageInput.current?.click()}>
              Add image
            </Button>
            <Spacing ml={1} />
            <Button
              onClick={() => {
                if (!unsaved || window.confirm('Discard your changes?')) {
                  setEditing(false);
                }
              }}
            >
              Cancel
            </Button>
            <Spacing ml={1} />
            <Button loading={isSaving} onClick={() => saveDraft()} primary>
              Save
            </Button>
          </FlexContainer>
        </FlexContainer>

        <Spacing mt={1}>{imagesHint}</Spacing>

        <Spacing mt={2}>
          <FlexContainer>
            <div style={{ flex: 1, minWidth: 0 }}>
              <PaneStyle style={{ padding: 0 }}>
                <CodeEditor
                  height={`calc(100vh - ${UNIT * 34}px)`}
                  language="markdown"
                  onChange={setDraft}
                  onSave={saveDraft}
                  padding={UNIT * 2}
                  showLineNumbers
                  uuid={`pipeline_documentation/${pipelineUUID}`}
                  value={draft}
                />
              </PaneStyle>
            </div>
            <Spacing ml={2} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <PaneStyle style={{ height: `calc(100vh - ${UNIT * 34}px)` }}>
                {format === 'quarto' && (
                  <Spacing mb={2}>
                    <Text muted small>
                      Preview as Markdown. Save to render it with Quarto.
                    </Text>
                  </Spacing>
                )}
                <Markdown imageSource={toImage}>{draft}</Markdown>
              </PaneStyle>
            </div>
          </FlexContainer>
        </Spacing>
      </Spacing>
    );
  }

  if (!doc.exists) {
    return (
      <Spacing p={3} style={{ maxWidth: UNIT * 100 }}>
        <Headline level={4}>Document this pipeline</Headline>
        <Spacing mt={1}>
          <Text default>
            A README beside the pipeline&apos;s code: what it does, its inputs and outputs, who
            owns it and how to operate it. It saves in the pipeline&apos;s folder and versions
            with it in Git.
          </Text>
        </Spacing>
        <Spacing mt={3}>
          <FlexContainer>
            <ChoiceStyle onClick={() => startEditing('markdown')} type="button">
              <Text bold>Write in Markdown</Text>
              <Spacing mt={1}>
                <Text muted small>
                  Recommended. Text, tables, links, code and images, shown as you type.
                </Text>
              </Spacing>
            </ChoiceStyle>
            <Spacing ml={2} />
            <ChoiceStyle onClick={() => startEditing('quarto')} type="button">
              <Text bold>Write in Quarto</Text>
              <Spacing mt={1}>
                <Text muted small>
                  {doc.quarto_available
                    ? 'For reports with Quarto features. Rendered by Quarto on save; code cells '
                      + 'are shown, not run.'
                    : 'Quarto is not installed on the server, so the document shows as '
                      + 'Markdown until it is.'}
                </Text>
              </Spacing>
            </ChoiceStyle>
          </FlexContainer>
        </Spacing>
      </Spacing>
    );
  }

  const showQuarto = doc.format === 'quarto' && doc.quarto_available;

  return (
    <Spacing p={3}>
      <FlexContainer alignItems="center" justifyContent="space-between">
        <FlexContainer alignItems="center">
          <Headline level={4}>Documentation</Headline>
          <Spacing ml={2} />
          <Text muted small>
            {FORMAT_LABELS[doc.format]} · {doc.path}
            {doc.updated_at
              ? ` · updated ${new Date(doc.updated_at * 1000).toLocaleString()}`
              : ''}
          </Text>
        </FlexContainer>
        <Button onClick={() => startEditing()} primary>
          Edit
        </Button>
      </FlexContainer>

      {doc.format === 'quarto' && !doc.quarto_available && (
        <Spacing mt={1}>
          <Text muted small>
            Shown as Markdown: Quarto is not installed on the server (or set MAGE_QUARTO to
            its path).
          </Text>
        </Spacing>
      )}

      <Spacing mt={2}>
        {showQuarto ? (
          <div style={{ position: 'relative' }}>
            {frameLoading && (
              <Spacing p={2}>
                <FlexContainer alignItems="center">
                  <Spinner small />
                  <Spacing ml={1} />
                  <Text muted small>Rendering with Quarto</Text>
                </FlexContainer>
              </Spacing>
            )}
            <FrameStyle
              onLoad={() => setFrameLoading(false)}
              sandbox=""
              src={renderedUrl}
              style={{
                borderColor: (themeContext?.borders || dark.borders).light,
                height: `calc(100vh - ${UNIT * 26}px)`,
              }}
              title={`Documentation of ${pipelineUUID}`}
            />
          </div>
        ) : (
          <PaneStyle style={{ maxWidth: UNIT * 120 }}>
            <Markdown imageSource={toImage}>{doc.source}</Markdown>
          </PaneStyle>
        )}
      </Spacing>
    </Spacing>
  );
}

export default Documentation;
