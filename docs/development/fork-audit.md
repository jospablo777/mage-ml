# Fork audit

Date: 2026-09-10. Base commit: `0cb12047e`.

The production dependency profiles resolve without conflicts. The Linux production image builds and passes execution and browser checks. Native package advisories without reported fixes remain open.

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
| Packaging | Exclude local databases, environment files, bytecode, and frontend dependency directories from distributions. Build all workspace packages together. |
| CI | Pass changed filenames as subprocess arguments, run pytest, check installed dependency consistency, and audit Python and frontend dependencies. |
| Tests | Allocate a temporary project per test class. Teardown removes the directory allocated during setup and rejects checkout paths and symlinks. Startup stubs read the interpreter from the environment so a checkout path with spaces still runs them. |
| Pipeline execution | Provide an awaited async entry point, reject nested synchronous execution before starting blocks, and flush logs on failure. Preserve spaces in project paths passed to external executors. |
| Failure and cancellation | Finish independent branches when `allow_blocks_to_fail` is enabled, retain the failed run status, and check cancellation before starting another block. |
| Conditions | Resolve conditional block updates through the conditional registry instead of the callback registry. |
| Triggers | Create one run per schedule when several matchers accept the same event. Use a monotonic polling deadline and bound each sleep by the remaining timeout. |
| Parquet reads | Apply filters, projection, offsets, and row limits; include every source in multipart reads; preserve sample settings. Move async dataset construction and batch reads to the shared thread pool. |
| Runtime dependencies | Resolve Mage, connectors, and the Singer compatibility package in one workspace. Install Sparkmagic in a separate locked environment. |
| Frontend | Upgrade Next.js, React, Axios, charts, Markdown rendering, Storybook, and transitive dependencies. Build both static exports. |
| Server responsiveness | Await kernel output through a dedicated reader thread so the Jupyter subscriber does not block Tornado's event loop. |
| Startup | Stop on dependency installation errors, apply runtime constraints, preserve argument boundaries, and forward process signals. |
| Queues | Export the standard multiprocessing queue when the magic kernel is disabled or faster-fifo is unavailable. |
| URL prefixes | Rewrite font URLs in both formatted and minified CSS. |
| Metadata cache | Track file identity, nanosecond timestamps, and size. Invalidate pipeline metadata after synchronous and asynchronous saves. Remove the timestamp delay and correct memory accounting on cache replacement and clearing. |
| Container runtime | Use Debian 13 slim, apply native updates, and keep compilers in the build stage. Exclude nested databases and environment files. Use uv without pip's vulnerable bundled dependencies. |
| Container processes | Run Mage, the language server, and custom Spark under tini to forward termination signals and reap child processes. Retry connections closed before the HTTP server is ready. |
| Language server | Remove pyls-memestra, which aborted diagnostics for unsaved files. |
| Spark image | Start outside the installed PySpark directory so its pandas submodule cannot shadow pandas. |
| Scheduler coordination | Deny locks while a configured Redis is unreachable, reconnect on an interval, and release only the lock the caller still holds. |
| Workspace images | Resolve the workspace image from the server pod or `MAGE_CONTAINER_IMAGE`. The upstream image is no longer a default. |
| Database access | Replace `Query.get`, legacy since SQLAlchemy 2.0, with `Session.get`. Import the declarative base from `sqlalchemy.orm` and cap SQLAlchemy below 3. |
| Secrets | Create the secrets directory and the encryption key readable by their owner only, and keep the key written by whichever process created it first. |
| File copies | Replace `distutils` with `shutil` and with a local boolean parser. The module is absent from Python 3.12 and resolved only through setuptools. |
| Releases | Publish images to the GitHub container registry on tags, and attach the workspace distributions to the tagged release. |
| Dependency updates | Add Dependabot for the Python workspaces, the frontend, the actions, and the container base. |
| Network filesystem | Remove `nfs-common` and the Cloud Filestore mount from startup. The client served that one caller and brought six of the native advisories through libevent. |
| Inventory | Publish a CycloneDX SBOM for the production image as a CI artifact. |

The old shared test teardown called `shutil.rmtree(get_variables_dir())` after tests had changed the global project path. That could delete the checkout. Cleanup no longer derives its target from mutable application settings. Git fixtures remain inside the allocated temporary directory.

## Dependency audit

The lock retains pandas 3, NumPy 2, Polars 1, and SQLAlchemy 2. Updated packages include urllib3, sqlparse, Azure Identity, Kafka, MongoDB, MySQL Connector, Redshift Connector, LangChain, LangSmith, Transformers, Jupyter, and pyodbc.

| Profile | Result |
| --- | --- |
| Initial Python lock, every extra | 59 advisory entries across 17 packages. |
| Updated Python lock, `all` and `integrations` | 385 packages checked; no published advisories reported. |
| Separate Livy environment | 112 packages checked; no published advisories reported. |
| Language server and Kubernetes startup helpers | 46 and 21 packages checked; no published advisories reported. |
| Separate local Spark environment | Nine packages checked; no published advisories reported. |
| Optional Spark NLP Python environment | 115 packages checked; no published Python advisories reported. Local JVM execution and pandas conversion pass. |
| Updated frontend, including development tools | 1,204 dependencies checked; no published advisories reported. |
| Optional Chroma extra | Four distinct advisories without a reported fixed release. Excluded from the production profile. |

Python results use pip-audit 2.10.1. Frontend results use Yarn 1.22.22 without a dependency-group filter. These checks cover published package advisories, not native operating-system packages or application logic.

## Deployment constraints

Mage, `mage_integrations`, and `vendor/singer-python` share `uv.lock`. The Singer source comes from the Mage fork at commit `0540a699c0e2fd8ba64c3245b5fa9aa87ae0538c`; its local build metadata permits current jsonschema, simplejson, and backoff. Tests replace the removed `assertEquals` alias. Distribute the local Singer wheel with the other workspace wheels.

The language-server, Kubernetes startup, local Spark, and optional custom Spark images also use separate lockfiles under `runtimes`. The obsolete branch-testing Dockerfile was removed because it fetched upstream packages instead of building the fork.

The production Docker target installs `all` and `integrations` through `uv sync --locked`. Development and local Spark use targets in the same Dockerfile. Unlocked Git installations have been removed. Sparkmagic requires pandas 2, so its kernel runs from `/opt/mage-livy` using `runtimes/livy/uv.lock`; Mage retains pandas 3 in `/opt/mage`. Local Spark 4.2 jobs use `/opt/mage-spark`: the [Spark SQL dependency constraints](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html) require pandas below 3. Native PySpark is not installed into the Mage environment; use Livy for Mage Spark pipelines.

The dbt MySQL adapter is no longer installed because its constraints conflict with the patched dbt stack. MySQL source and destination connectors remain available. `dbt-clickhouse` remains at 1.9.3 because 1.10.0 caps dbt-adapters below the selected range. Validate office pipelines that depend on these adapters before deployment.

Optional Chroma remains outside `all`. Its unresolved findings are GHSA-f4j7-r4q5-qw2c, GHSA-36p7-vc44-83pf, GHSA-xph7-9rjv-w5fr, and GHSA-2wm9-hf6c-p5cr.

The initial Linux build failed while Docker's 98 GB filesystem had no free space. Approved removal of unused build cache reclaimed 51.3 GB. Existing containers and volumes were preserved. After validation, temporary audit containers and unused build cache were removed, leaving 52 GiB available. The production image now builds on Linux arm64. Moving from the full Debian 12 image to Debian 13 slim reduced the image from 4.30 GB to 3.16 GB.

The final Trivy scan reports no advisories for the installed Python, JavaScript, or uv components, and no native findings with a reported fixed version. It still reports **14 critical and 98 high native package entries**, covering **44 distinct advisories, five critical**. These counts include repeated advisories across packages built from the same source. They do not establish exploitability in Mage. The [advisory snapshot](container-advisories.json) records affected packages and versions. CI publishes these findings and rejects fixable high or critical vulnerabilities. The fork should not receive an unconditional production approval while these findings and office-specific deployment checks remain open.

Pip 26.2.1 bundles affected msgpack and setuptools versions. The production image omits pip and installs project requirements through uv with the exported runtime constraints. This avoids shipping those bundled copies; it does not suppress scanner findings.

## Validation

Validation used macOS arm64, Python 3.12.11, and uv 0.11.29. The lockfile consistency check, installed Python dependency check, and changed-file Python lint checks passed. Source and wheel builds passed; the resulting archives excluded local database files.

The full backend and connector run passed **5,516 tests and 135 subtests**, with **ten skips**, after the metadata cache corrections. It includes the previously failing integration scheduler test, SQL Server collection, Singer tests, queue fallbacks, startup argument handling, the nonblocking Jupyter subscriber, and file cache invalidation. Native unixODBC and libmagic were built in temporary directories for the macOS run.

Later regression tests cover the scheduler lock while Redis is unreachable, schedules skipped when the lock is denied, workspace image resolution, encryption key permissions, and primary key lookups on the base model. `mage_ai/tests/test_deployment_contracts.py` scans the repository for the library calls, image references, disabled publishing workflows, and uv version drift that produced these corrections.

Both Next.js static exports build on Linux, and the Storybook build passed on macOS. Eight chart browser tests passed. All six application browser tests pass against the production Linux container, covering authentication, pipeline creation and deletion, a triggered loader-transformer-exporter run, and navigation across the main pages. Login under `/office` passes without browser exceptions or failed asset requests, including fonts. Docker and CI give the frontend compiler a 4 GB heap after a build exceeded Node's default limit.

The installed Linux production environment passes **80 focused tests** for metadata caching, block execution, scheduler behavior, triggers, Parquet reads, SQL Server handling, queues, and signed Jupyter kernels. The test fixtures are mounted read-only; application imports use the installed package. The image includes Microsoft ODBC Driver 18 for SQL Server and passes libmagic and Livy import checks.

The final Mage and language-server images stop in 0.51 and 0.58 seconds with exit status 143 after SIGTERM. Previously, Docker force-killed Mage after ten seconds. CI checks startup and termination. This verifies signal delivery and process termination; draining active pipeline runs during deployment remains untested.

Source and wheel builds passed for all three workspace packages. Archive inspection found no database files, bytecode, or frontend node_modules files. The main environment and installed helper environments pass `uv pip check`. The Linux language-server, Kubernetes startup, and optional Spark NLP images build. The language server passes WebSocket initialization, unsaved-file syntax diagnostics, and protocol shutdown. The Spark image passes a local JVM job and pandas conversion. Spark NLP model loading and its bundled Java dependencies have not been validated or audited here.

A prior separate run passed ten integration tests against PostgreSQL 16. SQL Server validation here covers imports and unit tests, not a connection to a live SQL Server. Office-specific connector credentials and workloads were not available. CI later narrowed to Python 3.12 only.

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
  --no-hashes --no-emit-workspace --output-file /tmp/mage-runtime-requirements.txt
uvx pip-audit==2.10.1 --disable-pip --no-deps \
  --requirement /tmp/mage-runtime-requirements.txt
```

Run backend tests in a disposable checkout with the required extras synchronized. The shared test classes now allocate temporary projects, but individual legacy tests also manipulate files and global application settings.

## Later corrections: pandas 3 block data handling

Date: 2026-09-11.

Pipelines that load a dataframe, join several of them, and export to PostgreSQL hit
these on pandas 3 and numpy 2. Each one now has a regression test.

| Area | Correction |
| --- | --- |
| Block output rendering | Stop importing `SettingWithCopyWarning`, removed in pandas 3. The suppression is now a no-op when the class is absent. |
| Dataframe profiling | Correlate numeric columns only. `corr` raises on text columns, and the exception left every dataframe with a text column without statistics, insights or column types. |
| Dataframe profiling | Skip histograms for empty and all-null columns instead of subtracting `None` bounds. |
| Dataframe profiling | Select the scatter-plot categories with a list. A set is no longer a column indexer, which removed the overview for every frame with a datetime column. |
| Time series charts | Convert datetimes to epoch seconds through their own resolution. `Series.view` is gone, and dividing the integer form by a billion is only correct for nanosecond columns. |
| SQL export | Convert timedelta and period columns without `Series.view`. |
| SQL export | Choose the integer column width by range comparison. numpy 2 raises `OverflowError` when narrowing a Python integer that does not fit, which broke export for object and Arrow-backed integer columns. The same dead narrowing was removed from the DuckDB, MSSQL, MySQL and Oracle exporters. |
| PostgreSQL | Register adapters for numpy scalars. psycopg2 rejected numpy integers and booleans, and numpy floats bound as the literal text `np.float64(0.5)`. |
| PostgreSQL | Build insert rows with `itertuples`, and replace missing values only on that path. The COPY path writes the same bytes without widening every column to object. |
| Polars output | Build the preview rows with `rows()`. Going through numpy raised `DTypePromotionError` for a frame mixing datetimes with numbers. |
| Variable storage | Encode JSON before opening the file. A value that could not be encoded left a zero-byte variable behind, and the next block failed while reading it rather than where it was produced. |
| SQL blocks | Test upstream emptiness by length. Series, arrays and polars frames raise on a truth test. |
| Update badge | Compare versions with PEP 440 and check the distribution this build publishes. The badge compared strings against the upstream package, so a local version that is ahead always looked out of date. `MAGE_UPDATE_CHECK_PACKAGE` and `MAGE_UPDATE_CHECK_ENABLED` configure it. |
| Dependencies | Floor tornado at 6.5.8 and constrain it, for CVE-2026-82397, GHSA-wwv5-g3v4-889x and GHSA-8423-8fgw-73vq. It arrives through ipykernel, jupyter-client, jupyter-server and terminado. |

`mage_ai/tests/orchestration/test_block_data_handling.py` runs pipelines whose block
outputs match the shapes above: a cursor dictionary holding a timestamp, four frames
merged one to one, an empty frame used as a branch signal, a fitted model, and a
scored frame. `mage_ai/tests/test_stack_contracts.py` pins the pandas 3 and numpy 2
behaviours these corrections depend on.

The PostgreSQL round trips in `mage_ai/tests/io/test_postgres_integration.py` need a
server; they skip when `MAGE_TEST_POSTGRES_*` is unset.

The backend test matrix now builds Python 3.12 only, on Linux and on Windows. freezegun
moved from 1.2.2 to 1.5.5; the pinned release read `uuid._load_system_functions`, which
Python 3.13 removed, and that stopped collection for every module importing it.
`requires-python` still admits 3.11 through 3.13, so those interpreters are installable
but no longer built.
