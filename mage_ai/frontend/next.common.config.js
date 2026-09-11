const path = require('node:path');
const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin');

module.exports = {
  output: process.env.NODE_ENV === 'production' ? 'export' : undefined,
  basePath: process.env.MAGE_FRONTEND_BASE_PATH || '',
  transpilePackages: ['monaco-editor'],
  compiler: { styledComponents: { ssr: true, displayName: true } },
  eslint: {
    ignoreDuringBuilds: true,
  },
  images: {
    unoptimized: true,
  },
  reactStrictMode: String(process.env.NEXT_PUBLIC_REACT_STRICT_MODE) !== '0',
  webpack: (config, options) => {
    // monaco-themes 0.4.8 omits its theme JSON directory from package exports.
    config.resolve.alias['monaco-themes/themes'] = path.resolve(
      path.dirname(require.resolve('monaco-themes')), '../themes',
    );
    if (!options?.isServer) {
      config.module.rules.push({
        loader: 'worker-loader',
        options: {
          filename: 'static/[contenthash].worker.js',
          publicPath: `${process.env.MAGE_FRONTEND_BASE_PATH || ''}/_next/`,
        },
        test: /\.worker\.ts$/,
      });

      config.plugins.push(
        new MonacoWebpackPlugin({
          languages: ['json', 'python', 'r', 'sql', 'typescript', 'yaml'],
        }),
      );
    }

    if (options?.dev) {
      if (parseInt(process.env.ONLY_V || 0) === 2) {
        console.log('Ignoring pages and components not in V2...');
        config.plugins.push(
          ...[
            new options.webpack.IgnorePlugin({
              // Ignore any file in `frontend/pages` directory not in `v2` subdirectory
              resourceRegExp: /^\.\/frontend\/pages\/(?!v2\/)/,
              // Apply this only for specific context, ensuring context is frontend
              contextRegExp: /frontend\/pages/,
            }),
            new options.webpack.IgnorePlugin({
              // Ignore any file in `frontend/components` directory not in v2 subdirectory or not `accessibleDiffViewer.js`
              resourceRegExp: /^\.\/frontend\/components\/(?!v2\/|accessibleDiffViewer\.js)/,
              // Apply this only for specific context, ensuring context is frontend
              contextRegExp: /frontend\/components/,
            }),
          ],
        );
      }
    }

    // Ignore files named mock.ts or mocks.ts
    config.plugins.push(
      new options.webpack.IgnorePlugin({
        resourceRegExp: /mock(s)?\.ts$/,
      })
    );

    return config;
  },
};
