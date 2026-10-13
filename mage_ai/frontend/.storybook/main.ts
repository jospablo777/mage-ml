import type { StorybookConfig } from '@storybook/nextjs-vite';
import type { Plugin } from 'vite';

import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

// The Next.js framework points React at the copy Next bundles for the App Router (React 19).
// This app uses the Pages Router with the project's React, so stories render with that one,
// the same React users run. A late plugin's aliases take precedence over the framework's.
const projectReact: Plugin = {
  name: 'mage-project-react',
  enforce: 'post',
  config: () => ({
    resolve: {
      alias: [
        ['react', 'react'],
        ['react/jsx-runtime', 'react/jsx-runtime'],
        ['react/jsx-dev-runtime', 'react/jsx-dev-runtime'],
        ['react-dom', 'react-dom'],
        ['react-dom/client', 'react-dom/client'],
        ['react-dom/server', 'react-dom/server.browser'],
      ].map(([name, file]) => ({
        find: new RegExp(`^${name}$`),
        replacement: require.resolve(file),
      })),
    },
  }),
};

const config: StorybookConfig = {
  stories: ['../stories/**/*.stories.@(js|jsx|ts|tsx)'],
  staticDirs: ['../public'],
  addons: ['@storybook/addon-links', '@storybook/addon-docs'],
  viteFinal: async config => ({
    ...config,
    plugins: [...(config.plugins || []), projectReact],
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
