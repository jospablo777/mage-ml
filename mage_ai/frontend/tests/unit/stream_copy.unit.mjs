// Node runs copy.ts directly (type stripping): `npm run test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { copyHeadline, tableCopyWords } from '../../components/StreamRun/copy.ts';

describe('copyHeadline', () => {
  test('counts the tables done and the rows left by the estimates', () => {
    assert.equal(copyHeadline([
      { table: 'public.orders', estimate: 1200, copied: 1200, done: true },
      { table: 'public.lines', estimate: 5000, copied: 1500, done: false },
      { table: 'public.notes', estimate: null, copied: 0, done: false },
    ]), 'Initial copy: 1 of 3 tables done, about 3,500 rows left');
  });

  test('a table copied past its estimate leaves no negative rows', () => {
    assert.equal(copyHeadline([
      { table: 'public.orders', estimate: 100, copied: 150, done: false },
      { table: 'public.lines', estimate: 50, copied: 0, done: false },
    ]), 'Initial copy: 0 of 2 tables done, about 50 rows left');
  });

  test('once every table is done it says how many rows were copied', () => {
    assert.equal(copyHeadline([
      { table: 'public.orders', estimate: 2, copied: 2, done: true },
    ]), 'Initial copy done: 1 table, 2 rows');
  });
});

describe('tableCopyWords', () => {
  test('says done, waiting, or rows against the estimate', () => {
    assert.equal(tableCopyWords({ table: 't', estimate: 10, copied: 10, done: true }),
      'Done, 10 rows');
    assert.equal(tableCopyWords({ table: 't', estimate: 10, copied: 0, done: false }),
      'Waiting');
    assert.equal(tableCopyWords({ table: 't', estimate: 4000, copied: 1500, done: false }),
      '1,500 of about 4,000 rows');
  });

  test('without an estimate, or past it, only the rows read', () => {
    assert.equal(tableCopyWords({ table: 't', estimate: null, copied: 7, done: false }),
      '7 rows');
    assert.equal(tableCopyWords({ table: 't', estimate: 5, copied: 7, done: false }),
      '7 rows');
  });
});
