import { PythonWorkerJobType } from '@components/PipelineDetail/Settings/PythonWorker';

// The answers of /api/python_workers (data_preparation/python_workers.py).

export type WorkerUsageType = {
  blocks: string[];
  // True when the pipeline's own setting chose the worker for its Python blocks.
  default: boolean;
  name: string;
  pipeline: string;
};

export type WorkerSecurityType = {
  advisories?: number;
  checked_at?: number;
  fixable?: number;
  reason?: string;
  stale?: boolean;
  state: 'clean' | 'vulnerable' | 'unchecked';
};

export type LockedPackageType = {
  built: boolean;
  bytes: number;
  // True when the worker's pyproject.toml lists it; else another package needs it.
  declared: boolean;
  name: string;
  version: string;
};

export type PackageChangeType = {
  from?: string;
  package: string;
  to?: string;
};

export type WorkerType = {
  changes?: PackageChangeType[];
  combine?: boolean;
  definition?: string;
  dependencies?: string[];
  description?: string;
  job?: PythonWorkerJobType;
  label?: string;
  locked?: boolean;
  name: string;
  notes?: string[];
  // In the list, what the prepared environment holds; in the detail, the lock's packages.
  packages?: LockedPackageType[] | string[];
  prepared?: boolean;
  python?: string;
  security?: WorkerSecurityType;
  size_bytes?: number;
  used_by?: WorkerUsageType[];
};

export type AdvisoryType = {
  aliases: string[];
  fix_versions: string[];
  id: string;
  link?: string;
  summary: string;
};

export type VulnerablePackageType = {
  advisories: AdvisoryType[];
  fix?: string;
  package: string;
  unfixed: string[];
  version: string;
};

export type WorkerAuditType = {
  advisories?: number;
  checked_at?: number;
  fixable?: number;
  name: string;
  packages: VulnerablePackageType[];
  reason?: string;
  state: 'clean' | 'vulnerable' | 'unchecked';
};

export type WorkerUpdateType = {
  changes: PackageChangeType[];
  held?: { package: string; reason: string; to: string }[];
  id?: string;
  job?: PythonWorkerJobType;
  message?: string;
  name: string;
};

export type WorkerTemplateType = {
  dependencies: string[];
  description: string;
  id: string;
  label: string;
};
