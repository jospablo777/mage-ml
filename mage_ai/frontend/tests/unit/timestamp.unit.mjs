// Node runs timestamp.ts directly (type stripping): `yarn test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { displayTimestamp } from '../../utils/timestamp.ts';

describe('displayTimestamp', () => {
  test('shows an ISO timestamp with an offset in UTC', () => {
    assert.equal(displayTimestamp('2026-10-11T01:08:42-06:00'), '2026-10-11 07:08:42 UTC');
  });

  test('takes milliseconds', () => {
    assert.equal(displayTimestamp(Date.UTC(2026, 9, 11, 7, 21, 23)), '2026-10-11 07:21:23 UTC');
  });

  test('shows local time without a zone when asked', () => {
    assert.doesNotMatch(displayTimestamp('2026-10-11T07:08:42Z', true), /UTC/);
  });

  test('leaves a value it cannot read as it is', () => {
    assert.equal(displayTimestamp('soon'), 'soon');
  });
});
