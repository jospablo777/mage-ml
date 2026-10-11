// Pipeline compilation (mage_ai/orchestration/compilation.py): what a production run of a
// pipeline builds, built ahead.

export type CompilationUnitType = {
  block_uuids: string[];
  error?: string;
  kind: 'python_check' | 'r_check' | 'r_environment' | 'rust_block' | 'rust_chain';
  label: string;
  language: string;
  seconds: number;
  state: 'cached' | 'checked' | 'compiled' | 'compiling' | 'failed' | 'pending';
  // What may fail at run time without being certain to.
  warnings?: string[];
};

export type LastCompiledType = {
  finished_at?: number;
  state: 'completed' | 'failed' | 'never';
  units?: number;
  up_to_date: boolean;
};

export type CompilationSummaryType = {
  blocks: number;
  chains: { blocks: string[]; languages: string[] }[];
  // The languages in pipeline order, consecutive blocks of one language merged.
  flow?: { blocks: number; language: string }[];
  fusion: boolean;
  language_switches: number;
  languages: string[];
};

export type PipelineCompilationType = {
  automatic?: boolean;
  current?: string;
  done: number;
  finished_at?: number;
  id: string;
  last_compiled?: LastCompiledType;
  pipeline_uuid: string;
  started_at?: number;
  state: 'completed' | 'failed' | 'none' | 'nothing' | 'running';
  summary?: CompilationSummaryType;
  total: number;
  units: CompilationUnitType[];
};

export const LANGUAGE_NAMES = {
  markdown: 'Markdown',
  python: 'Python',
  r: 'R',
  rust: 'Rust',
  sql: 'SQL',
  yaml: 'YAML',
};

// "just now", "4 min ago", "3 h ago", "2 days ago" for a time in seconds since the epoch.
export function timeAgo(seconds?: number, now: number = Date.now() / 1000): string {
  if (!seconds) {
    return '';
  }
  const elapsed = Math.max(0, now - seconds);
  if (elapsed < 45) {
    return 'just now';
  }
  if (elapsed < 3600) {
    return `${Math.round(elapsed / 60)} min ago`;
  }
  if (elapsed < 86400) {
    return `${Math.round(elapsed / 3600)} h ago`;
  }
  const days = Math.round(elapsed / 86400);
  return `${days} ${days === 1 ? 'day' : 'days'} ago`;
}

export function progressPercent(compilation?: PipelineCompilationType): number {
  if (!compilation?.total) {
    return 0;
  }
  // A unit being built counts as half done so the bar moves while it builds.
  const compiling = compilation.units?.some(u => u.state === 'compiling') ? 0.5 : 0;
  return Math.min(100, Math.round(100 * (compilation.done + compiling) / compilation.total));
}

export type CompileStatusType = 'compiling' | 'failed' | 'nothing' | 'outdated' | 'never'
  | 'up_to_date';

// The status the header shows: a running compilation first, then the last finished one.
export function compileStatus(
  compilation?: PipelineCompilationType,
  last?: LastCompiledType,
): CompileStatusType {
  if (compilation?.state === 'running') {
    return 'compiling';
  }
  if (compilation?.state === 'nothing' || (compilation && !compilation.total && !last)) {
    return 'nothing';
  }
  const record = compilation?.last_compiled || last;
  if (!record || record.state === 'never') {
    return 'never';
  }
  if (record.state === 'failed') {
    return 'failed';
  }
  return record.up_to_date ? 'up_to_date' : 'outdated';
}

// Why a pipeline that switches language runs slower; null when it does not switch.
export function polyglotHint(summary?: CompilationSummaryType): string | null {
  if (!summary?.language_switches) {
    return null;
  }
  return 'At each arrow, the data is converted from one language to the other before the '
    + 'next block can read it. Written in one language, the blocks pass their data directly '
    + 'and compile into one optimized chain.';
}
