import dark from '@oracle/styles/themes/dark';

// A partition's states (orchestration/partition_state.py), problems first as headlines read
// them, and their colors on every page that shows partitions.
export type PartitionStateType = 'missing' | 'queued' | 'running' | 'failed' | 'stale' | 'done';

export const PARTITION_STATES: PartitionStateType[] = [
  'failed', 'stale', 'missing', 'running', 'queued', 'done',
];

export const PARTITION_COLORS: { [state in PartitionStateType]: string } = {
  done: dark.background.success,
  failed: dark.accent.negative,
  missing: dark.accent.contentDefaultTransparent,
  queued: dark.accent.infoTransparent,
  running: dark.accent.info,
  stale: dark.accent.warning,
};
