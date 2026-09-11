import type { StorybookConfig } from '@storybook/nextjs-vite';

import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

const config: StorybookConfig = {
  stories: ['../stories/**/*.stories.@(js|jsx|ts|tsx)'],
  staticDirs: ['../public'],
  addons: ['@storybook/addon-links', '@storybook/addon-docs'],
  viteFinal: async config => ({
    ...config,
    publicDir: false,
    resolve: {
      ...config.resolve,
      tsconfigPaths: true,
      alias: {
        ...config.resolve?.alias,
        path: require.resolve('path-browserify'),
      },
    },
  }),
  framework: {
    name: '@storybook/nextjs-vite',
    options: {},
  },
};

export default config;
