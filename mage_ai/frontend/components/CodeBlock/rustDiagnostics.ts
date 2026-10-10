// Compiler diagnostics of Rust blocks as Monaco markers. No imports, so
// `yarn test:unit` runs it with Node.
const ANSI = /\u001b\[[0-9;]*m/g;

export type RustDiagnostic = {
  code?: string | null;
  column?: number | null;
  end_column?: number | null;
  end_line?: number | null;
  level: string;
  line?: number | null;
  message: string;
  rendered?: string;
};

// The compiler's help and notes say how to fix the error; the hover shows them.
export function markerMessage(diagnostic: RustDiagnostic): string {
  const notes = (diagnostic.rendered || '')
    .replace(ANSI, '')
    .split('\n')
    .map(line => line.trim())
    .filter(line => /^(= )?(help|note):/.test(line))
    .map(line => line.replace(/^= /, ''));
  const code = diagnostic.code ? ` [${diagnostic.code}]` : '';
  return [`${diagnostic.message}${code}`, ...notes].join('\n');
}

export function toMarkers(diagnostics: RustDiagnostic[], monaco: any) {
  const severity = {
    error: monaco.MarkerSeverity.Error,
    help: monaco.MarkerSeverity.Hint,
    note: monaco.MarkerSeverity.Info,
    warning: monaco.MarkerSeverity.Warning,
  };
  return diagnostics
    .filter(diagnostic => diagnostic.level in severity)
    .map(diagnostic => {
      const line = diagnostic.line || 1;
      const column = diagnostic.column || 1;
      const endLine = diagnostic.end_line || line;
      let endColumn = diagnostic.end_column || column + 1;
      if (endLine === line && endColumn <= column) {
        endColumn = column + 1;
      }
      return {
        endColumn,
        endLineNumber: endLine,
        message: markerMessage(diagnostic),
        severity: severity[diagnostic.level],
        source: 'rustc',
        startColumn: column,
        startLineNumber: line,
      };
    });
}

