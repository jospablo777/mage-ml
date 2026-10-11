// Node runs html.ts directly (type stripping): `yarn test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { escapeHtml, preformattedHtml } from '../../utils/html.ts';

describe('escapeHtml', () => {
  test('a pipeline name with markup stays text', () => {
    assert.equal(
      escapeHtml('Pipeline <img src=x onerror="alert(1)"> already exists.'),
      'Pipeline &lt;img src=x onerror=&quot;alert(1)&quot;&gt; already exists.',
    );
  });

  test('escapes the ampersand first, once', () => {
    assert.equal(escapeHtml('a & b &lt;'), 'a &amp; b &amp;lt;');
  });
});

describe('preformattedHtml', () => {
  test('keeps indentation and escapes markup', () => {
    assert.equal(preformattedHtml('  File "<string>"'), '&nbsp;&nbsp;File&nbsp;&quot;&lt;string&gt;&quot;');
  });
});
