# Fork audit

Date: 2026-09-10. Base commit: `2e16452c423d6eb9cd75cd978c9e7aeb245754f6`.

The connector dependency conflicts and frontend findings below still prevent a production release. The Python lockfile audit does not cover the complete container image.

## Changes

| Area | Correction |
| --- | --- |
| YAML | Use PyYAML's safe loaders; reject Python object construction tags. |
| Downloads | Reject expired or invalid tokens and paths that resolve outside the project; preserve binary file contents and close file handles on errors. |
| Kernels | Retain Jupyter's generated message signing key instead of configuring an empty key. |
| PostgreSQL | Propagate connection failures, release failed SSH tunnels, and bind tunnels to an ephemeral loopback port. Preserve libpq's direct connection port options. |
| SSH | Load RSA, ECDSA, and Ed25519 keys with current Paramiko APIs. Remove sshtunnel's dependency on the deleted DSA API. |
| Data types | Handle nullable pandas and Arrow numeric and boolean dtypes in column detection. |
| Model detection | Avoid calling scikit-learn's estimator tag API on ordinary values; preserve support for third-party model instances. |
| Great Expectations | Map positional arguments to the current expectation API and return validation results; preserve saved expectation metadata. |
| Optional clients | Update LangChain imports and Qdrant queries for the selected dependency versions. |
| Request state | Stop the database query cache when a request raises an exception. Parse DEBUG consistently, including `DEBUG=0`. |
| Packaging | Exclude local databases, environment files, and bytecode from distributions. Exclude development dependencies from the production sync. |
| CI | Pass changed filenames as subprocess arguments, run pytest, check installed dependency consistency, and audit Python and frontend dependencies. |
| Tests | Allocate a temporary project per test class. Teardown removes the directory allocated during setup and rejects checkout paths and symlinks. |
| Pipeline execution | Provide an awaited async entry point, reject nested synchronous execution before starting blocks, and flush logs on failure. Preserve spaces in project paths passed to external executors. |
| Failure and cancellation | Finish independent branches when `allow_blocks_to_fail` is enabled, retain the failed run status, and check cancellation before starting another block. |
| Conditions | Resolve conditional block updates through the conditional registry instead of the callback registry. |
| Triggers | Create one run per schedule when several matchers accept the same event. Use a monotonic polling deadline and bound each sleep by the remaining timeout. |
| Parquet reads | Apply filters, projection, offsets, and row limits; include every source in multipart reads; preserve sample settings. Move async dataset construction and batch reads to the shared thread pool. |

The old shared test teardown called `shutil.rmtree(get_variables_dir())` after tests had changed the global project path. That could delete the checkout. Cleanup no longer derives its target from mutable application settings. Git fixtures remain inside the allocated temporary directory.

## Dependency audit

The lock retains pandas 3, NumPy 2, Polars 1, and SQLAlchemy 2. Updated packages include urllib3, sqlparse, Azure Identity, Kafka, MongoDB, MySQL Connector, Redshift Connector, LangChain, LangSmith, Transformers, Jupyter, and pyodbc.

| Profile | Result |
| --- | --- |
| Initial Python lock, every extra | 59 advisory entries across 17 packages. |
| Updated Python lock, `all` and `integrations` | 380 packages checked; no published advisories reported. |
| Updated Python lock, every extra | Five advisory entries for Chroma 1.5.9, representing four distinct advisories. No fixed version reported. |
| Existing frontend production dependency graph | 142 distinct advisories: 5 critical, 74 high, 47 moderate, 16 low. |

Python results use pip-audit 2.10.1. The frontend result uses Yarn 1.22.22 with `--groups dependencies`; this includes build tools declared as production dependencies, such as Storybook. An advisory match does not establish that the affected code is reachable in the deployed application.

## Release blockers

1. **Connector installation bypasses the lock.** `mage_integrations/requirements.txt` pins Paramiko 3.5.1 and Requests 2.31.x, conflicting with the root project's patched versions. It also retains psycopg2 2.9.3, clickhouse-sqlalchemy 0.2.x, and Singer SDK 0.34.x. The Dockerfiles and backend CI install these requirements separately. The new `uv pip check` step fails on incompatible installed dependencies.

2. **Singer and dbt require incompatible jsonschema versions.** The referenced `mage-ai/singer-python` fork at `0540a699c0e2fd8ba64c3245b5fa9aa87ae0538c` requires jsonschema 4.17.0, while the patched dbt dependency set requires a newer version. Singer also pins old simplejson and backoff releases. Resolve these constraints in a maintained Singer package or isolate the connector runtime before combining it with dbt. Installing with `--no-deps` does not resolve the conflict.

3. **The frontend remains unpatched.** Its lock contains Next.js 12.3.4, Axios 0.27.2, and affected transitive dependencies. The production Python server serves exported static assets; several Next.js advisories concern server features. Review the affected paths, upgrade the frontend dependency graph, rebuild both asset exports, and run the browser tests. The existing bundles have not been rebuilt during this audit. The frontend audit job currently fails on these findings.

4. **Optional Chroma has unresolved advisories.** The auditor reports GHSA-f4j7-r4q5-qw2c, GHSA-36p7-vc44-83pf, GHSA-xph7-9rjv-w5fr, and GHSA-2wm9-hf6c-p5cr without a fixed release. Chroma is outside the `all` container profile. Do not include it in an approved profile until its use and remediation are resolved.

5. **Several container dependencies remain outside uv.lock.** These include Git installations of Singer, dbt-mysql, SQLGlot, and Dremio tooling, plus additional PyPI installations. Unpinned branches also prevent repeatable builds. The container dependency graph must be resolved and scanned after those installations, including native packages.

6. **dbt-clickhouse resolves to 1.9.3.** Version 1.10.0 caps dbt-adapters below the range required by the patched dbt stack. The lock therefore selects 1.9.3. Validate the ClickHouse pipelines in use before accepting this adapter change.

## Validation

Validation used macOS arm64, Python 3.12.11, and uv 0.11.29. The lockfile consistency check, installed Python dependency check, and changed-file Python lint checks passed. Source and wheel builds passed; the resulting archives excluded local database files.

The final backend run in a disposable checkout passed **5,304 tests and 109 subtests**, with **one failure, ten skips, and one collection error**. It includes the CLI, downloads, signed kernel execution, SSH, nullable dtypes, YAML, debug flags, query-cache cleanup, temporary-directory regressions, and the flow and reader cases below.

A separate run passed 20 PostgreSQL and directory-cleanup tests, including ten integration tests against PostgreSQL 16. Fourteen Great Expectations tests passed. A core-only environment constructed the Tornado application successfully.

The remaining scheduling test failure could not import the separately packaged `mage_integrations.sources`. SQL Server test collection requires the native unixODBC library, which is absent from this host. The lock now uses pyodbc 5.3.0; that package still requires the native library.

Python 3.11 and 3.13, Linux images, frontend exports, and browser tests have not been validated locally. A complete container build was unavailable because the local Docker storage was full; no existing Docker data was removed.

### Block, flow, and trigger verification

The focused execution and reader suite passed 32 tests and 12 subtests. Tests execute Python loader, transformer, and exporter blocks, persist their outputs, and check database run statuses. The cases cover:

- API triggers, overlapping event matchers, cron interval boundaries, and inactive schedules.
- Branched flows with nullable pandas 3 columns, with memory caching enabled and disabled.
- Async execution, dynamic fan-out and reduction, retries, failed conditions, and downstream suppression.
- Failed blocks with independent branches allowed to finish; cancellation before execution and between blocks.
- Polling deadlines, filtered and projected Parquet reads, offsets across row groups, multiple files, and async reader thread placement.

Cron tests replace worker dispatch and execute the resulting runs locally. They do not exercise an external worker service or a deployed multi-process scheduler.

A local Parquet measurement used 250,000 rows, 32 numeric columns, 16,384-row groups, and five timed reads after warm-up. Selecting two columns reduced materialized DataFrame data from 64 MB to 4 MB; median read time changed from 9.63 ms to 2.08 ms. A two-column batch scan with `limit=100` previously returned all 250,000 rows. It now returns 100 rows, reducing materialized Arrow data from 4,062,500 bytes to 1,626 bytes. These sizes describe returned data, not peak process memory; the timings depend on the local filesystem cache.

Reproduce the Python advisory check from the repository root:

```bash
uv export --locked --extra all --extra integrations --no-default-groups \
  --no-hashes --no-emit-project --output-file /tmp/mage-runtime-requirements.txt
uvx pip-audit==2.10.1 --disable-pip --no-deps \
  --requirement /tmp/mage-runtime-requirements.txt
```

Run backend tests in a disposable checkout with the required extras synchronized. The shared test classes now allocate temporary projects, but individual legacy tests also manipulate files and global application settings.
