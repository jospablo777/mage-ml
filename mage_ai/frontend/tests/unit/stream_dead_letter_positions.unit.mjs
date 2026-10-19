// Node runs positions.ts directly (type stripping): `npm run test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  positionText,
  targetNoun,
} from '../../components/StreamDeadLetters/positions.ts';

describe('dead letter positions', () => {
  test('Kafka letters show partition and offset, and replay to a topic', () => {
    assert.equal(positionText({ offset: 41, partition: 2 }, 'kafka'), 'partition 2 · offset 41');
    assert.equal(targetNoun('kafka'), 'topic');
  });

  test('NATS letters show their stream sequence, and replay to a subject', () => {
    assert.equal(positionText({ offset: 7, partition: 0 }, 'nats'), 'sequence 7');
    assert.equal(targetNoun('nats'), 'subject');
  });

  test('an older server without kinds reads as Kafka', () => {
    assert.equal(positionText({ offset: 1, partition: 0 }), 'partition 0 · offset 1');
    assert.equal(targetNoun(undefined), 'topic');
  });
});
