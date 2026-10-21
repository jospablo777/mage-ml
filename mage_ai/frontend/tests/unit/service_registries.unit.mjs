// Registries and docker commands of the export dialog's build step: `yarn test:unit`.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { REGISTRIES, platformProgress, shipCommands } from '../../components/PipelineServiceExport/registries.ts';

test('another registry, such as a local one, takes its host and path as typed', () => {
  const other = REGISTRIES.other;
  assert.equal(other.image({ path: 'localhost:5000' }, 'shop', '1.0'), 'localhost:5000/shop:1.0');
  assert.equal(
    other.image({ path: 'quay.io/team/' }, 'shop', '1.0'),
    'quay.io/team/shop:1.0',
  );
  assert.equal(other.login({ path: 'quay.io/team' }), 'docker login quay.io');
  assert.equal(other.login({ path: 'localhost:5000' }), undefined);
});

test('two platforms push with buildx, and without a registry they are a problem', () => {
  const pushed = shipCommands({
    image: 'localhost:5000/shop:1.0',
    platforms: ['linux/amd64', 'linux/arm64'],
    push: true,
  });
  assert.equal(
    pushed.lines,
    'docker buildx build --platform linux/amd64,linux/arm64 -t localhost:5000/shop:1.0 --push .',
  );
  assert.equal(pushed.problem, undefined);
  const local = shipCommands({ image: 'shop:1.0', platforms: ['linux/amd64', 'linux/arm64'], push: false });
  assert.match(local.problem, /need a registry/);
  assert.equal(
    shipCommands({ image: 'shop:1.0', platforms: [], push: false }).lines,
    'docker build -t shop:1.0 .',
  );
});

test('each platform of a buildx build has its own progress, and its own failure', () => {
  const progress = platformProgress([
    '#5 [linux/amd64 1/9] FROM docker.io/library/python:3.12-slim',
    '#9 [linux/arm64 3/9] RUN pip install -r requirements.txt',
    '#12 [linux/amd64 6/9] COPY . .',
    '#9 [linux/arm64 2/9] WORKDIR /app',
    'ERROR: failed to solve: process "/bin/sh -c pip install" did not complete',
    '#9 ERROR [linux/arm64 3/9] RUN pip install -r requirements.txt',
    'a line without a platform',
  ]);
  assert.deepEqual(progress, {
    'linux/amd64': { done: 6, failed: false, total: 9 },
    'linux/arm64': { done: 3, failed: true, total: 9 },
  });
  assert.deepEqual(platformProgress(undefined), {});
});

test('one platform builds and loads with buildx', () => {
  assert.equal(
    shipCommands({ image: 'shop:1.0', platforms: ['linux/amd64'], push: false }).lines,
    'docker buildx build --platform linux/amd64 -t shop:1.0 --load .',
  );
});
