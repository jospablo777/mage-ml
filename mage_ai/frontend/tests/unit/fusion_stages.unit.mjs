// Stages of block fusion in the dependency graph and the settings: `yarn test:unit`.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  fusionStagePositions,
  fusionStageText,
  fusionStages,
} from '../../utils/models/fusion.ts';

const pipeline = {
  block_fusion: 'chains',
  fusion_plan: [['alone'], ['load', 'clean', 'summarize'], ['export'], ['a', 'b']],
};

test('only chains are stages, numbered from 1', () => {
  assert.deepEqual(fusionStages(pipeline), [['load', 'clean', 'summarize'], ['a', 'b']]);
  assert.deepEqual(fusionStagePositions(pipeline).clean, { position: 2, size: 3, stage: 1 });
  assert.deepEqual(fusionStagePositions(pipeline).b, { position: 2, size: 2, stage: 2 });
  assert.equal(fusionStagePositions(pipeline).alone, undefined);
});

test('a pipeline without fusion has no stages', () => {
  assert.deepEqual(fusionStages({ ...pipeline, block_fusion: null }), []);
  assert.deepEqual(fusionStagePositions(undefined), {});
});

test('the stage text names the stage and the position', () => {
  assert.equal(fusionStageText({ position: 1, size: 3, stage: 2 }), 'stage 2 (1 of 3)');
  assert.equal(fusionStageText(undefined), '');
});
