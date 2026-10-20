// A log file arrives as a window of its lines (first_line, total_lines): the newest ones.
// Earlier windows the page loads are kept per file, and every range of lines not loaded
// shows as a gap the user can load: no line of a log is out of reach, and none is sent
// before it is asked for. Node runs this file directly in the unit tests.

export interface LoadedRange {
  // Index of the range's first line, 0-based over the whole log.
  start: number;
  // The lines, each ending with a newline.
  content: string;
}

export interface Gap {
  path: string;
  name: string;
  // The first line after the gap: the window to load ends before it.
  beforeLine: number;
  // Lines in the gap.
  lines: number;
}

export interface MergedLog {
  // The loaded lines in order, ranges joined; gaps hold the lines between them.
  ranges: LoadedRange[];
  gaps: Gap[];
}

export function lineCount(content: string): number {
  if (!content) {
    return 0;
  }
  const newlines = content.split('\n').length - 1;
  return content.endsWith('\n') ? newlines : newlines + 1;
}

function dropLines(content: string, count: number): string {
  if (count <= 0) {
    return content;
  }
  let at = -1;
  for (let i = 0; i < count; i++) {
    at = content.indexOf('\n', at + 1);
    if (at === -1) {
      return '';
    }
  }
  return content.slice(at + 1);
}

// The latest window of a log with the earlier ranges loaded for it: ranges in order with
// overlapping lines kept once, and the gaps between them and before the first.
export function mergeWindows(
  path: string,
  name: string,
  latest: LoadedRange,
  earlier: LoadedRange[] = [],
): MergedLog {
  const sorted = [...earlier, latest]
    .filter(range => lineCount(range.content) > 0 || range === latest)
    .sort((a, b) => a.start - b.start);
  const ranges: LoadedRange[] = [];
  for (const range of sorted) {
    const previous = ranges[ranges.length - 1];
    if (!previous) {
      ranges.push({ ...range });
      continue;
    }
    const previousEnd = previous.start + lineCount(previous.content);
    if (range.start <= previousEnd) {
      // Overlap or contact: the lines past the previous range's end join it.
      const rangeEnd = range.start + lineCount(range.content);
      if (rangeEnd > previousEnd) {
        previous.content += dropLines(range.content, previousEnd - range.start);
      }
    } else {
      ranges.push({ ...range });
    }
  }
  const gaps: Gap[] = [];
  let end = 0;
  for (const range of ranges) {
    if (range.start > end) {
      gaps.push({ beforeLine: range.start, lines: range.start - end, name, path });
    }
    end = range.start + lineCount(range.content);
  }
  return { gaps, ranges };
}
