// What a block did to the messages, field by field: Run on sample shows it above the
// input and output. Messages pair by position when the counts match.

export type FieldCounts = { [field: string]: number };

export type MessageDiff = {
  added: FieldCounts;
  changed: FieldCounts;
  dropped: number;
  extra: number;
  paired: boolean;
  removed: FieldCounts;
  unchanged: number;
};

function isRecord(value: any): boolean {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function same(a: any, b: any): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function count(counts: FieldCounts, field: string) {
  counts[field] = (counts[field] || 0) + 1;
}

export function messageDiff(input: any[], output: any[]): MessageDiff {
  const result: MessageDiff = {
    added: {},
    changed: {},
    dropped: Math.max(0, input.length - output.length),
    extra: Math.max(0, output.length - input.length),
    paired: input.length === output.length,
    removed: {},
    unchanged: 0,
  };
  if (!result.paired) {
    return result;
  }
  input.forEach((before, index) => {
    const after = output[index];
    if (!isRecord(before) || !isRecord(after)) {
      if (same(before, after)) {
        result.unchanged += 1;
      } else {
        count(result.changed, '(value)');
      }
      return;
    }
    let changed = false;
    Object.keys(after).forEach((field) => {
      if (!(field in before)) {
        count(result.added, field);
        changed = true;
      } else if (!same(before[field], after[field])) {
        count(result.changed, field);
        changed = true;
      }
    });
    Object.keys(before).forEach((field) => {
      if (!(field in after)) {
        count(result.removed, field);
        changed = true;
      }
    });
    if (!changed) {
      result.unchanged += 1;
    }
  });
  return result;
}

// "score (100)", most frequent first.
export function fieldList(counts: FieldCounts): string[] {
  return Object.entries(counts)
    .sort(([a, x], [b, y]) => y - x || a.localeCompare(b))
    .map(([field, n]) => `${field} (${n.toLocaleString()})`);
}
