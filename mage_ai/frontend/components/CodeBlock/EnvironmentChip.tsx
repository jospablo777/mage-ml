import { useRef, useState } from 'react';

import BlockType, { BlockLanguageEnum } from '@interfaces/BlockType';
import FlyoutMenuWrapper from '@oracle/components/FlyoutMenu/FlyoutMenuWrapper';
import PipelineType from '@interfaces/PipelineType';
import Tooltip from '@oracle/components/Tooltip';
import api from '@api';
import { ChipStyle } from './WorkerChip';
import {
  DEFAULT_ENVIRONMENT,
  ENVIRONMENT_KEYS,
  blockLanguageEnvironment,
  environmentId,
} from '@utils/models/blockEnvironment';
import { LanguageEnvironmentType } from '@components/Workers/types';

// Where an R or Rust block runs, in its header, and the place to change it: the project's
// default environment, the pipeline's, or a named one (ADR-0040). A name the project does
// not have shows as a warning: the block fails with the names it has.
function EnvironmentChip({ block, onChange, pipeline }: {
  block: BlockType;
  onChange: (environment: string | null) => void;
  pipeline: PipelineType;
}) {
  const ref = useRef(null);
  const [open, setOpen] = useState(false);
  // One request for every block of the page: SWR shares it by its key.
  const { data } = api.language_environments.list({}, { revalidateOnFocus: false });
  const language = block.language === BlockLanguageEnum.R ? 'r' : 'rust';
  const languageLabel = language === 'r' ? 'R' : 'Rust';
  const environments: LanguageEnvironmentType[] = (data?.language_environments || [])
    .filter((e: LanguageEnvironmentType) => e.language === language);
  const named = environments.filter(e => e.environment !== DEFAULT_ENVIRONMENT);
  const chosen = blockLanguageEnvironment(block, pipeline);
  const own = String(block?.configuration?.[ENVIRONMENT_KEYS[block.language]] || '');
  const fallback = (language === 'r' ? pipeline?.environment?.r : pipeline?.environment?.rust)
    || '';
  const isDefault = chosen === DEFAULT_ENVIRONMENT;
  const missing = !isDefault && !!data && !named.some(e => e.environment === chosen);
  const text = isDefault ? `${languageLabel} default`
    : missing ? `${chosen} · missing` : chosen;
  const tip = missing
    ? `This block selects the ${languageLabel} environment ${chosen}, which the project does `
      + 'not have. Choose another, or create it on the Workers page.'
    : isDefault
      ? `Runs in the project's ${languageLabel} environment. Click to run it in a named one `
        + 'with other packages.'
      : `Runs in the ${languageLabel} environment ${chosen}. Click to choose another.`;

  // Not choosing is the default: an empty setting keeps metadata.yaml unchanged.
  const choose = (value: string | null) => {
    setOpen(false);
    if ((value || '') !== own) {
      onChange(value);
    }
  };
  const mark = (selected: boolean, label: string) => `${selected ? '✓ ' : ''}${label}`;
  const items = [
    { isGroupingTitle: true, label: () => 'Runs in', uuid: 'runs-in' },
    ...(fallback
      ? [{
        label: () => mark(!own, `The pipeline's: ${fallback}`),
        onClick: () => choose(null),
        uuid: 'pipeline',
      }]
      : []),
    {
      label: () => mark(fallback ? own === DEFAULT_ENVIRONMENT : !own,
        `The project's ${languageLabel} environment`),
      onClick: () => choose(fallback ? DEFAULT_ENVIRONMENT : null),
      uuid: DEFAULT_ENVIRONMENT,
    },
    ...named.map(e => ({
      label: () => mark(own === e.environment, e.environment),
      onClick: () => choose(e.environment),
      tag: e.state === 'problems' ? 'needs attention' : null,
      uuid: `environment-${e.environment}`,
    })),
    { isGroupingTitle: true, label: () => 'Environments', uuid: 'environments' },
    {
      label: () => named.length ? `Manage ${languageLabel} environments` : 'Add an environment',
      linkProps: {
        as: `/workers/languages/${environmentId(language, isDefault || missing ? null : chosen)}`,
        href: '/workers/languages/[language]',
      },
      uuid: 'manage',
    },
  ];

  return (
    <FlyoutMenuWrapper
      items={items}
      onClickCallback={() => setOpen(false)}
      onClickOutside={() => setOpen(false)}
      open={open}
      parentRef={ref}
      roundedStyle
      uuid={`environment/${block?.uuid}`}
    >
      <div ref={ref}>
        <Tooltip appearBefore label={tip} size={null} widthFitContent>
          <ChipStyle
            aria-expanded={open}
            aria-haspopup="menu"
            aria-label={`Runs in: ${text}`}
            builtin={isDefault}
            onClick={(e) => {
              e.stopPropagation();
              setOpen(!open);
            }}
            warning={missing}
          >
            {text}
            <span aria-hidden>▾</span>
          </ChipStyle>
        </Tooltip>
      </div>
    </FlyoutMenuWrapper>
  );
}

export default EnvironmentChip;
