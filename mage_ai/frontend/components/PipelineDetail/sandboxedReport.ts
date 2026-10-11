// Monitor reports (Evidently, NannyML) are HTML with scripts. A blob URL has the app's
// origin, so the report runs in an iframe sandboxed without allow-same-origin: its
// scripts draw the charts but cannot read the app's cookies, storage or API.
export function sandboxedReport(html: string, title: string): string {
  const escape = (value: string) => value
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  return '<!doctype html><html><head><meta charset="utf-8">'
    + `<title>${escape(title)}</title>`
    + '<style>html,body,iframe{margin:0;border:0;width:100%;height:100%}</style>'
    + `</head><body><iframe sandbox="allow-scripts allow-popups" srcdoc="${escape(html)}"></iframe></body></html>`;
}
