// Node runs logic.ts directly (type stripping): `yarn test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import {
  COLUMN_BLOCK,
  HEADER_HEIGHT,
  MAX_SCROLL_HEIGHT,
  PAGE_ROWS,
  ROW_HEIGHT,
  blocksFor,
  columnOffsets,
  columnWidth,
  filterError,
  filterLabel,
  formatCount,
  formatNumber,
  formatPercent,
  indexWidth,
  needsValue,
  nextSort,
  operatorsFor,
  pagesFor,
  rowGeometry,
  scrollTopFor,
  viewKey,
  visibleColumns,
} from '../../components/ColumnAtlas/logic.ts';

const integer = { dtype: 'Int64', exact_integer: true, id: 0, kind: 'numeric', name: 'id' };
const float = { dtype: 'Float64', exact_integer: false, id: 1, kind: 'numeric', name: 'score' };
const text = { dtype: 'String', id: 2, kind: 'string', name: 'city' };

describe('filters', () => {
  test('operators follow the column kind', () => {
    assert.ok(operatorsFor(float).includes('is_nan'));
    assert.ok(!operatorsFor(integer).includes('is_nan'));
    assert.ok(operatorsFor(text).includes('contains'));
    assert.deepEqual(operatorsFor({ dtype: 'Boolean', kind: 'boolean' }).slice(0, 2), ['is_true', 'is_false']);
    assert.deepEqual(operatorsFor({ dtype: 'Binary', kind: 'other' }), ['is_null', 'is_not_null']);
  });

  test('values are checked before a request', () => {
    assert.equal(filterError({ column_id: 0, op: 'eq', value: '9007199254740993' }, integer), null);
    assert.equal(filterError({ column_id: 0, op: 'eq', value: '1.5' }, integer), 'Enter a whole number');
    assert.equal(filterError({ column_id: 1, op: 'gt', value: 'abc' }, float), 'Enter a number');
    assert.equal(filterError({ column_id: 1, op: 'gt', value: ' ' }, float), 'Enter a value');
    assert.equal(filterError({ column_id: 1, high: '', op: 'between', value: '1' }, float), 'Enter a value');
    assert.equal(filterError({ column_id: 1, op: 'is_nan' }, float), null);
    assert.equal(filterError({ column_id: 2, op: 'gt', value: 'a' }, text), 'Choose a condition');
    assert.equal(needsValue('is_empty'), false);
    assert.equal(needsValue('starts_with'), true);
  });

  test('labels read as sentences', () => {
    assert.equal(filterLabel({ column_id: 1, high: '2', op: 'between', value: '1' }, float), 'score between 1 and 2');
    assert.equal(filterLabel({ column_id: 2, op: 'is_null' }, text), 'city is missing');
    assert.equal(filterLabel({ column_id: 7, op: 'gte', value: '3' }), 'column 7 ≥ 3');
  });

  test('view keys tell views apart', () => {
    const view = { filters: [{ column_id: 2, op: 'contains', value: 'a|b' }], sort: [] };
    assert.equal(viewKey(view), viewKey(structuredClone(view)));
    assert.notEqual(viewKey(view), viewKey({ ...view, sort: [{ column_id: 2, descending: false }] }));
  });
});

describe('sorting', () => {
  test('a click cycles ascending, descending and off', () => {
    let sort = nextSort([], 3, false);
    assert.deepEqual(sort, [{ column_id: 3, descending: false }]);
    sort = nextSort(sort, 3, false);
    assert.deepEqual(sort, [{ column_id: 3, descending: true }]);
    assert.deepEqual(nextSort(sort, 3, false), []);
  });

  test('a plain click replaces other keys, shift-click keeps them', () => {
    const sort = [{ column_id: 1, descending: false }];
    assert.deepEqual(nextSort(sort, 2, false), [{ column_id: 2, descending: false }]);
    const both = nextSort(sort, 2, true);
    assert.deepEqual(both, [{ column_id: 1, descending: false }, { column_id: 2, descending: false }]);
    assert.deepEqual(nextSort(both, 1, true), [
      { column_id: 1, descending: true },
      { column_id: 2, descending: false },
    ]);
    assert.deepEqual(nextSort(nextSort(both, 1, true), 1, true), [{ column_id: 2, descending: false }]);
  });
});

describe('layout', () => {
  test('widths stay within bounds', () => {
    assert.equal(columnWidth({ dtype: 'Boolean', kind: 'boolean', name: 'ok' }), 96);
    assert.equal(columnWidth({ dtype: 'String', kind: 'string', name: 'x'.repeat(500) }), 340);
    assert.equal(indexWidth(10), 56);
    assert.ok(indexWidth(1_000_000_000) > indexWidth(1000));
  });

  test('only the columns in view render, with overscan', () => {
    const offsets = columnOffsets(Array(100).fill(100), 50);
    assert.equal(offsets.length, 101);
    assert.equal(offsets[100], 10_050);
    assert.deepEqual(visibleColumns(offsets, 0, 350, 0), [0, 1, 2]);
    assert.deepEqual(visibleColumns(offsets, 0, 350), [0, 1, 2, 3, 4]);
    assert.deepEqual(visibleColumns(offsets, 1000, 350, 0), [10, 11, 12]);
    assert.deepEqual(visibleColumns(offsets, 1001, 350, 0), [10, 11, 12, 13]);
    assert.deepEqual(visibleColumns(offsets, 1_000_000, 350, 0), [99]);
    assert.deepEqual(visibleColumns([50], 0, 350), []);
  });

  test('small tables scroll one pixel per pixel', () => {
    const geometry = rowGeometry(1000, 400, 300);
    assert.equal(geometry.isScaled, false);
    assert.equal(geometry.firstRow, 10);
    assert.equal(geometry.top, HEADER_HEIGHT + 10 * ROW_HEIGHT);
    assert.equal(geometry.scrollHeight, 1000 * ROW_HEIGHT + HEADER_HEIGHT);
    assert.equal(scrollTopFor(10, 1000, 400), 300);
    assert.equal(rowGeometry(5, 400, 99_999).firstRow, 4);
    assert.equal(rowGeometry(0, 400, 0).firstRow, 0);
  });

  test('tall tables map the scroll range onto every row', () => {
    const total = 1_000_000_000;
    const viewport = 600;
    const top = rowGeometry(total, viewport, 0);
    assert.equal(top.isScaled, true);
    assert.equal(top.scrollHeight, MAX_SCROLL_HEIGHT);
    assert.equal(top.firstRow, 0);
    const bottom = rowGeometry(total, viewport, MAX_SCROLL_HEIGHT - viewport);
    const shown = Math.ceil((viewport - HEADER_HEIGHT) / ROW_HEIGHT);
    // The last row is on screen at the end of the scroll range.
    assert.ok(bottom.firstRow + shown >= total);
    assert.ok(bottom.firstRow < total);
    // Rows render from the top of the viewport, below the sticky header.
    assert.equal(bottom.top, MAX_SCROLL_HEIGHT - viewport + HEADER_HEIGHT);
    // Going to a row and reading the geometry back lands on that row.
    for (const row of [0, 1, 123_456_789, total / 2, total - 50]) {
      const scrollTop = scrollTopFor(row, total, viewport);
      assert.ok(Math.abs(rowGeometry(total, viewport, scrollTop).firstRow - row) <= 1, `row ${row}`);
    }
  });

  test('pages and column blocks cover the window', () => {
    assert.deepEqual(pagesFor(0, 20, 1000), [0]);
    assert.deepEqual(pagesFor(PAGE_ROWS - 5, 20, 1000), [0, 1]);
    assert.deepEqual(pagesFor(990, 20, 1000), [7]);
    assert.deepEqual(pagesFor(0, 20, 0), []);
    assert.deepEqual(blocksFor([0, 1, COLUMN_BLOCK - 1, COLUMN_BLOCK, 40]), [0, 1, 2]);
  });
});

describe('formatting', () => {
  test('counts, numbers and shares', () => {
    assert.equal(formatCount(1234567), '1,234,567');
    assert.equal(formatCount(null), '–');
    assert.equal(formatCount(Number.NaN), '–');
    assert.equal(formatNumber(3.14159265), '3.14159');
    assert.equal(formatNumber(0), '0');
    assert.equal(formatNumber(0.00001234), '1.234e-5');
    assert.equal(formatNumber(2e16), '2.000e+16');
    assert.equal(formatNumber('2024-01-31'), '2024-01-31');
    assert.equal(formatNumber(Number.POSITIVE_INFINITY), '–');
    assert.equal(formatPercent(1, 0), '0%');
    assert.equal(formatPercent(1, 10_000), '<0.1%');
    assert.equal(formatPercent(9_999, 10_000), '>99.9%');
    assert.equal(formatPercent(5, 100), '5.0%');
    assert.equal(formatPercent(50, 100), '50%');
    assert.equal(formatPercent(100, 100), '100%');
  });
});

describe('statistics sent as text', () => {
  test('decimals round, large integers and dates stay exact', () => {
    assert.equal(formatNumber('29.693176517'), '29.6932');
    assert.equal(formatNumber('1e-7'), '1.000e-7');
    assert.equal(formatNumber('-12'), '-12');
    assert.equal(formatNumber('4500'), '4,500');
    assert.equal(formatNumber('9007199254740993'), '9007199254740993');
    assert.equal(formatNumber('-170141183460469231731687303715884105728'), '-170141183460469231731687303715884105728');
    assert.equal(formatNumber('2024-01-31 12:00:00'), '2024-01-31 12:00:00');
    assert.equal(formatNumber('12:30:00'), '12:30:00');
    assert.equal(formatNumber('NaN'), 'NaN');
  });
});
