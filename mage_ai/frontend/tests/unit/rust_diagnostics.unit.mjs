// Compiler diagnostics of Rust blocks as editor markers: `yarn test:unit`.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { markerMessage, toMarkers } from '../../components/CodeBlock/rustDiagnostics.ts';

const monaco = { MarkerSeverity: { Error: 8, Hint: 1, Info: 2, Warning: 4 } };

test('errors and warnings become markers at their lines', () => {
  const markers = toMarkers([
    { column: 21, end_column: 27, end_line: 5, level: 'error', line: 5, message: 'mismatched types' },
    { column: 9, end_column: 15, end_line: 4, level: 'warning', line: 4, message: 'unused variable' },
    { level: 'failure-note', message: 'ignored' },
  ], monaco);
  assert.equal(markers.length, 2);
  assert.deepEqual(markers[0], {
    endColumn: 27,
    endLineNumber: 5,
    message: 'mismatched types',
    severity: 8,
    source: 'rustc',
    startColumn: 21,
    startLineNumber: 5,
  });
  assert.equal(markers[1].severity, 4);
});

test('a diagnostic without a position marks the first line, never an empty range', () => {
  const [marker] = toMarkers([{ level: 'error', message: 'cargo failed' }], monaco);
  assert.equal(marker.startLineNumber, 1);
  assert.equal(marker.startColumn, 1);
  assert.equal(marker.endColumn, 2);
  const [point] = toMarkers([{ column: 3, end_column: 3, level: 'error', line: 2, message: 'x' }], monaco);
  assert.equal(point.endColumn, 4);
});

test('the message carries the code, help and notes without color codes', () => {
  const message = markerMessage({
    code: 'E0308',
    level: 'error',
    message: 'mismatched types',
    rendered: '\u001b[1merror[E0308]\u001b[0m: mismatched types\n  |\n  = help: use `0.13` instead\n  = note: expected `f64`\n',
  });
  assert.equal(message, 'mismatched types [E0308]\nhelp: use `0.13` instead\nnote: expected `f64`');
});
