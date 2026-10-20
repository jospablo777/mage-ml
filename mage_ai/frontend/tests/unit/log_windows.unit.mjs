// Node runs windows.ts directly (type stripping): `yarn test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { lineCount, mergeWindows } from '../../components/Logs/windows.ts';

const lines = (from, to) => Array.from({ length: to - from }, (_, i) => `line ${from + i}\n`).join('');

describe('log windows', () => {
  test('a whole log has no gap', () => {
    const merged = mergeWindows('p', 'b.log', { content: lines(0, 3), start: 0 });
    assert.deepEqual(merged.gaps, []);
    assert.equal(merged.ranges[0].content, lines(0, 3));
  });

  test('the lines before the newest window are a gap to load', () => {
    const merged = mergeWindows('p', 'b.log', { content: lines(5000, 5003), start: 5000 });
    assert.deepEqual(merged.gaps, [{ beforeLine: 5000, lines: 5000, name: 'b.log', path: 'p' }]);
  });

  test('an earlier window joins the newest one and leaves the rest as a gap', () => {
    const merged = mergeWindows(
      'p', 'b.log', { content: lines(10, 15), start: 10 }, [{ content: lines(5, 10), start: 5 }],
    );
    assert.equal(merged.ranges.length, 1);
    assert.equal(merged.ranges[0].content, lines(5, 15));
    assert.deepEqual(merged.gaps, [{ beforeLine: 5, lines: 5, name: 'b.log', path: 'p' }]);
  });

  test('a window that moved on while the log grew leaves a gap in the middle', () => {
    // Lines 5 to 10 were loaded before; the newest window now starts at 20.
    const merged = mergeWindows(
      'p', 'b.log', { content: lines(20, 25), start: 20 }, [{ content: lines(5, 10), start: 5 }],
    );
    assert.equal(merged.ranges.length, 2);
    assert.deepEqual(merged.gaps.map(gap => [gap.beforeLine, gap.lines]), [[5, 5], [20, 10]]);
  });

  test('overlapping lines are kept once', () => {
    const merged = mergeWindows(
      'p', 'b.log', { content: lines(8, 12), start: 8 }, [{ content: lines(0, 10), start: 0 }],
    );
    assert.deepEqual(merged.gaps, []);
    assert.equal(merged.ranges[0].content, lines(0, 12));
    assert.equal(lineCount(merged.ranges[0].content), 12);
  });

  test('a last line without a newline counts', () => {
    assert.equal(lineCount('a\nb'), 2);
    assert.equal(lineCount(''), 0);
  });
});
