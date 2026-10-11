// Text placed in HTML: error messages carry user input (pipeline and block names, values).
export function escapeHtml(text: string): string {
  return String(text ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// A line of a traceback as HTML that keeps its indentation.
export function preformattedHtml(text: string): string {
  return escapeHtml(text).replace(/ /g, '&nbsp;');
}
