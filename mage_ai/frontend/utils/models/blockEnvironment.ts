import { mutate } from 'swr';

import BlockType, { BlockLanguageEnum } from '@interfaces/BlockType';
import PipelineType from '@interfaces/PipelineType';
import { buildUrl } from '@api/utils/url';

export const DEFAULT_ENVIRONMENT = 'default';

// The block configuration key of each language's environment (block_environments.py).
export const ENVIRONMENT_KEYS = {
  [BlockLanguageEnum.R]: 'r_environment',
  [BlockLanguageEnum.RUST]: 'rust_environment',
};

export function hasLanguageEnvironment(block: BlockType): boolean {
  return [BlockLanguageEnum.R, BlockLanguageEnum.RUST].includes(block?.language);
}

// The R or Rust environment a block runs in (block_environments.selection): its own setting,
// else its pipeline's, else the project's default.
export function blockLanguageEnvironment(block: BlockType, pipeline?: PipelineType): string {
  if (!hasLanguageEnvironment(block)) {
    return null;
  }
  const own = block?.configuration?.[ENVIRONMENT_KEYS[block.language]];
  const fallback = block.language === BlockLanguageEnum.R
    ? pipeline?.environment?.r : pipeline?.environment?.rust;
  const chosen = String(own || fallback || DEFAULT_ENVIRONMENT).trim().toLowerCase();

  return chosen || DEFAULT_ENVIRONMENT;
}

// The id of an environment on the Workers page: r, rust, r.<name> or rust.<name>.
export function environmentId(language: string, environment: string): string {
  return !environment || environment === DEFAULT_ENVIRONMENT
    ? language : `${language}.${environment}`;
}

// Reloads every list of environments the page shows (the Workers page, the block headers'
// chips) after one is created or removed: a list read seconds before stays cached otherwise.
export function reloadLanguageEnvironments(): Promise<any> {
  return mutate(buildUrl('language_environments', null, null, null, {}));
}
