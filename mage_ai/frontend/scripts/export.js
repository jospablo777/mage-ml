const { execFileSync, spawnSync } = require('node:child_process');
const { createHash } = require('node:crypto');
const { cpSync, existsSync, readFileSync, rmSync, writeFileSync } = require('node:fs');
const path = require('node:path');

// Files that do not change the build: tests, docs, Storybook. Keep in step with
// mage_ai/tests/server/test_frontend_builds.py, which checks both builds against the sources.
const NOT_BUILT = [/^tests\//, /\.md$/, /^playwright[^/]*$/, /^\.storybook\//, /\.stories\.tsx?$/];

// A digest of the tracked sources the build comes from: sha256 over "path\0sha256(content)\n"
// for each file, sorted by path.
function sourceDigest(root) {
  const listed = execFileSync(
    'git',
    ['ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '.'],
    { cwd: root, encoding: 'utf8' },
  );
  const files = listed
    .split('\0')
    .filter(file => file && !NOT_BUILT.some(pattern => pattern.test(file))
      && existsSync(path.join(root, file)))
    .sort();
  const digest = createHash('sha256');
  files.forEach((file) => {
    const content = createHash('sha256').update(readFileSync(path.join(root, file))).digest('hex');
    digest.update(`${file}\0${content}\n`);
  });
  return digest.digest('hex');
}

const frontend = path.resolve(__dirname, '..');
// `node scripts/export.js --digest` prints the digest of the current sources.
if (process.argv.includes('--digest')) {
  console.log(sourceDigest(frontend));
  process.exit(0);
}
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
writeFileSync(path.join(destination, 'source-digest.txt'), `${sourceDigest(frontend)}\n`);
