// Node runs sandboxedReport.ts directly (type stripping): `yarn test:unit`.
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';

import { sandboxedReport } from '../../components/PipelineDetail/sandboxedReport.ts';

describe('sandboxedReport', () => {
  test('runs the report in an iframe without allow-same-origin', () => {
    const page = sandboxedReport('<script>parent.document.cookie</script>', 'drift.html');
    const iframe = page.match(/<iframe[^>]*>/)[0];

    assert.match(iframe, /sandbox="allow-scripts allow-popups"/);
    assert.doesNotMatch(iframe, /allow-same-origin/);
    assert.doesNotMatch(page.replace(/srcdoc="[^"]*"/, ''), /<script/);
  });

  test('escapes the report so it cannot close the srcdoc attribute', () => {
    const page = sandboxedReport('"><script>alert(1)</script><p a=\'&amp;\'>', '<b>');

    assert.equal((page.match(/<iframe/g) || []).length, 1);
    assert.equal((page.match(/<script/g) || []).length, 0);
    assert.match(page, /srcdoc="&quot;&gt;&lt;script&gt;alert\(1\)&lt;\/script&gt;&lt;p a='&amp;amp;'&gt;"/);
    assert.match(page, /<title>&lt;b&gt;<\/title>/);
  });
});
