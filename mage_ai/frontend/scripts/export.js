const { spawnSync } = require('node:child_process');
const { cpSync, existsSync, rmSync } = require('node:fs');
const path = require('node:path');

const frontend = path.resolve(__dirname, '..');
const basePath = process.argv.includes('--base-path');
const destination = path.resolve(
  frontend,
  '../server',
  basePath ? 'frontend_dist_base_path_template' : 'frontend_dist',
);
const result = spawnSync(process.execPath, [require.resolve('next/dist/bin/next'), 'build'], {
  cwd: frontend,
  env: {
    ...process.env,
    MAGE_FRONTEND_BASE_PATH: basePath ? '/CLOUD_NOTEBOOK_BASE_PATH_PLACEHOLDER_' : '',
  },
  stdio: 'inherit',
});
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status || 1);
const output = path.join(frontend, 'out');
if (!existsSync(path.join(output, 'index.html'))) {
  throw new Error('Static export did not produce index.html');
}
rmSync(destination, { recursive: true, force: true });
cpSync(output, destination, { recursive: true });
