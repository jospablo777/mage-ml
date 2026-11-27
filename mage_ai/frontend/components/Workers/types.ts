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
  // Why its pyproject.toml cannot be used; it runs no block until fixed.
  broken?: string;
  // MageML's own Python (name mage): shown read only, with how to change it.
  builtin?: boolean;
  commands?: string[];
  kind?: 'docker' | 'environment' | 'project' | 'uv_tool';
  main_packages?: { name: string; version: string }[];
  package_count?: number;
  prefix?: string;
  project?: string;
  changes?: PackageChangeType[];
  combine?: boolean;
  // A save that would lock these below their newest release: nothing changed yet.
  confirm?: HeldPackageType[];
  definition?: string;
  dependencies?: string[];
  description?: string;
  // Changes with its pyproject.toml and uv.lock.
  digest?: string;
  // The packages it lists that the last check found held back (held_packages).
  held_back?: string[];
  job?: PythonWorkerJobType;
  label?: string;
  locked?: boolean;
  // Its uv.lock cannot be read; saving the packages locks them anew.
  lock_problem?: string;
  // Bytes its processes may use; none without a limit.
  memory_limit?: number;
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
  label?: string;
};

// A listed package uv locked below its newest release (python_workers.held_back): held when
// the newest does not fit the other packages, behind when an update would bring it.
export type HeldPackageType = {
  // Releases newer than the locked one.
  behind?: number;
  latest: string;
  latest_uploaded?: string;
  locked: string;
  locked_uploaded?: string;
  package: string;
  // What to ask before a save that would lock it (python_workers._older_releases).
  question?: string;
  reason: string;
  state: 'held' | 'behind';
  uv_reason?: string;
};

export type WorkerHeldPackagesType = {
  checked_at?: number;
  name: string;
  packages: HeldPackageType[];
  reason?: string;
  state: 'held' | 'current' | 'unchecked';
};

// language_environments.py: an R or Rust environment, the language's default or a named one
// (ADR-0040), which the blocks that select it run with.
export type LanguagePackageType = {
  locked?: string;
  name: string;
  requirement?: string;
  source?: string;
};

export type LanguageEnvironmentType = {
  block_count: number;
  blocks: string[];
  commands?: string[];
  definition?: string;
  // default, or the name of a folder under r_environments/ or rust_environments/.
  environment: string;
  exists: boolean;
  // r or rust for the defaults, r.<name> or rust.<name> for named ones.
  id: string;
  label: string;
  language: 'r' | 'rust';
  locked_count?: number;
  manifest: string;
  manifest_text?: string;
  package_count: number;
  packages: LanguagePackageType[];
  // The pipelines whose blocks select it.
  pipelines?: WorkerUsageType[];
  problems?: string[];
  state: 'ready' | 'problems' | 'none';
  tools?: { [tool: string]: string | null };
  version?: string;
};
