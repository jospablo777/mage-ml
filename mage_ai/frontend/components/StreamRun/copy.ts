// The words for a stream source's initial copy; plain TypeScript, so Node can test it.

// A table of the source's initial copy: rows by the server's estimate (none when the table
// was never analyzed), rows read, and whether every sink took them all.
export type TableCopyType = {
  copied: number;
  done: boolean;
  estimate?: number;
  table: string;
};

// The point first: tables done, and the rows left by the estimates.
export function copyHeadline(copy: TableCopyType[]): string {
  const done = copy.filter(t => t.done).length;
  const rows = copy.reduce((sum, t) => sum + t.copied, 0);
  if (done === copy.length) {
    return `Initial copy done: ${copy.length.toLocaleString()} `
      + `${copy.length === 1 ? 'table' : 'tables'}, ${rows.toLocaleString()} rows`;
  }
  const left = copy.reduce((sum, t) => sum + Math.max((t.estimate || 0) - t.copied, 0), 0);
  return `Initial copy: ${done.toLocaleString()} of ${copy.length.toLocaleString()} tables `
    + `done, about ${left.toLocaleString()} rows left`;
}

export function tableCopyWords({ copied, done, estimate }: TableCopyType): string {
  if (done) {
    return `Done, ${copied.toLocaleString()} rows`;
  }
  if (!copied) {
    return 'Waiting';
  }
  return estimate && copied <= estimate
    ? `${copied.toLocaleString()} of about ${estimate.toLocaleString()} rows`
    : `${copied.toLocaleString()} rows`;
}
