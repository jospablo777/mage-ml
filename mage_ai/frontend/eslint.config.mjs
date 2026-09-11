import { FlatCompat } from '@eslint/eslintrc';
import { fileURLToPath } from 'node:url';
import legacy from './eslint.legacy.cjs';

const compat = new FlatCompat({ baseDirectory: fileURLToPath(new URL('.', import.meta.url)) });

export default [
  { ignores: ['.next/**', 'out/**', 'storybook-static/**', 'playwright-report/**', 'test-results/**'] },
  ...compat.config(legacy),
];
