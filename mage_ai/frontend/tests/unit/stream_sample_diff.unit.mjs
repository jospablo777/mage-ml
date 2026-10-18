// Node runs diff.ts directly (type stripping): `npm run test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { fieldList, messageDiff } from '../../components/StreamSamples/diff.ts';

describe('messageDiff', () => {
  test('counts fields added, removed and changed per message', () => {
    const diff = messageDiff(
      [{ id: 1, amount: 10, raw: 'x' }, { id: 2, amount: 5, raw: 'y' }, { id: 3, amount: 1 }],
      [{ id: 1, amount: 10, score: 0.9 }, { id: 2, amount: 6, score: 0.1 }, { id: 3, amount: 1 }],
    );

    assert.equal(diff.paired, true);
    assert.deepEqual(diff.added, { score: 2 });
    assert.deepEqual(diff.removed, { raw: 2 });
    assert.deepEqual(diff.changed, { amount: 1 });
    assert.equal(diff.unchanged, 1);
  });

  test('a filter or a split does not pair messages', () => {
    const dropped = messageDiff([{ a: 1 }, { a: 2 }, { a: 3 }], [{ a: 2 }]);
    assert.equal(dropped.paired, false);
    assert.equal(dropped.dropped, 2);
    assert.equal(messageDiff([{ a: 1 }], [{ a: 1 }, { a: 1 }]).extra, 1);
  });

  test('values that are not objects compare whole', () => {
    const diff = messageDiff(['a', 'b'], ['a', 'B']);
    assert.deepEqual(diff.changed, { '(value)': 1 });
    assert.equal(diff.unchanged, 1);
  });

  test('fields are listed most frequent first', () => {
    assert.deepEqual(fieldList({ b: 1, a: 3, c: 1 }), ['a (3)', 'b (1)', 'c (1)']);
  });
});
