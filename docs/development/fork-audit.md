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

The lock retains pandas 3, NumPy 2, Polars 2, and SQLAlchemy 2. Updated packages include urllib3, sqlparse, Azure Identity, Kafka, MongoDB, MySQL Connector, Redshift Connector, LangChain, LangSmith, Transformers, Jupyter, and pyodbc.

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
| PostgreSQL | Send insert rows in pages through `execute_values`. `executemany` issues one statement per row, so a batch cost one network round trip per row. A 10,000-row batch went from 10,000 statements to 10, and from 3.30s to 0.20s on a loopback connection. Remote databases gain far more, because the saving is round trips. `insert_page_size` sets the page, default 1000. |
| Polars output | Build the preview rows with `rows()`. Going through numpy raised `DTypePromotionError` for a frame mixing datetimes with numbers. |
| Variable storage | Encode JSON before opening the file. A value that could not be encoded left a zero-byte variable behind, and the next block failed while reading it rather than where it was produced. |
| Variable storage | Serialize and parse dict, list and ObjectId columns one column at a time. The row-wise `DataFrame.apply` rebuilt the whole frame and inferred every column again. Reading 300,000 rows with a dict column took 5.90s and peaked 1,001 MB above the starting RSS, and nullable integer and categorical columns came back as object and str. The same read takes 0.08s and 167 MB and keeps the written dtypes. Sample reads skip JSON columns cut from the sample file, which raised `KeyError`. |
| Variable storage | Take a shallow copy of a frame before replacing its columns for Parquet. Under copy-on-write the caller's frame stays unchanged, and the deep copy cost 321 MB of NumPy memory for a 383 MB frame. |
| SQL blocks | Test upstream emptiness by length. Series, arrays and polars frames raise on a truth test. |
| Update badge | Compare versions with PEP 440 and check the distribution this build publishes. The badge compared strings against the upstream package, so a local version that is ahead always looked out of date. `MAGE_UPDATE_CHECK_PACKAGE` and `MAGE_UPDATE_CHECK_ENABLED` configure it. |
| Dependencies | Floor tornado at 6.5.8 and constrain it, for CVE-2026-82397, GHSA-wwv5-g3v4-889x and GHSA-8423-8fgw-73vq. It arrives through ipykernel, jupyter-client, jupyter-server and terminado. |
| Polars 2 | Require Polars 2.0 and below 3. Mage code builds no LazyFrame, so the streaming engine default changes nothing in it. Lazy joins, `group_by` and `unpivot` in user blocks no longer keep row order unless they pass `maintain_order=True`; `POLARS_ENGINE_AFFINITY=in-memory` restores the 1.x engine. Parquet and Arrow map columns load as `Map`, and output previews show each map value as a dict. |
| API source | Name headerless CSV columns from `column_1`. Polars 2 starts at `column_0`, which would rename the columns of existing streams. The gzip branch passed `sepr` to `read_csv` and raised `TypeError`. |

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

## Dependency update, 2026-10-08

Base `ce912e7`. Pins and floors were raised in `pyproject.toml` and `mage_integrations/pyproject.toml`, then `uv lock --upgrade` updated 213 packages, added 9 and removed 13. `itsdangerous` and `cron-converter` left the dependencies; nothing imports them. `vendor/singer-python` needed no changes. `requires-python` is unchanged.

pip-audit 2.10.1 on the `all` and `integrations` profile found 26 known vulnerabilities in 7 packages on the previous lock (PyJWT 14, pymongo 3, tornado 3, urllib3 3, multidict 1, oauthlib 1, Werkzeug 1) and none on the new lock.

| Package | Before | After |
| --- | --- | --- |
| bcrypt | 4.0.1 | 5.0.0 |
| dbt-core | 1.11.15 | 1.12.5 (mashumaro 3.14 to 3.17) |
| GitPython | 3.1.62 | 3.2.0 |
| kubernetes | 33.1.0 | 36.0.3 |
| newrelic | 8.8.0 | 13.6.1 |
| oracledb | 2.4.1, and 1.3.1 on Python 3.11 | 26.0.1 |
| pendulum | 3.0.0, and 2.1.0 on Python 3.11 | 3.2.0 |
| psutil | 5.9.8 | 7.2.2 |
| PyGithub | 1.59.0 | 2.10.0 |
| PyJWT | 2.13.0 | 2.15.1 |
| python-dateutil | 2.8.2 | 2.9.0.post0 |
| redis | 5.0.8 | 8.1.0 |
| ruamel.yaml | 0.17.17 | 0.19.1 |
| scikit-learn | 1.7.2 | 1.9.1 |
| tornado | 6.5.8 | 6.5.10 |
| tzlocal | 4.2 | 5.4.4 |
| urllib3 | 2.7.0 | 2.8.0 |
| watchdog | 4.0.0 | 6.0.0 |

| Area | Change |
| --- | --- |
| Passwords | bcrypt 5 raises `ValueError` for input longer than 72 bytes, where bcrypt 4 hashed the first 72. `mage_ai/authentication/passwords.py` truncates to 72 bytes before hashing and verifying, so hashes made by bcrypt 4 keep verifying. |
| MongoDB source | tzlocal 5 returns `zoneinfo.ZoneInfo`, which has no `localize()`. The three call sites use `localize_naive`, which attaches the local zone to a naive datetime and raises `ValueError` for an aware one, as pytz did. A time inside a daylight saving transition of the server's zone now resolves to its first occurrence. Servers on UTC are unaffected. |
| Facebook Ads source | pendulum 3 has no `utcnow()`. The leads sync raised `AttributeError` on Python 3.12 and later before this update, and would have on 3.11 after it. It calls `pendulum.now("UTC")`. A test scans `mage_integrations` for `pendulum.<name>` references that pendulum does not provide. |
| Kernel processes | psutil 7 deprecates `Process.connections()`. The kernel process listing calls `net_connections()`. |
| Kubernetes executor | kubernetes 36 reads `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` when it creates a client configuration. In a pod with an outbound proxy, calls to the API server go through that proxy unless `NO_PROXY` lists the API server host. Two job manager tests patched `os.getenv` for the whole process, which made the client read `pod_name` as its proxy URL. They now set the two variables the job manager reads, and a test pins the proxy behavior. |
| GitPython test | The version guard required 3.1.x. It now requires at least 3.1.59, the fix for CVE-2026-78676. |

### Verification

| Run | Passed | Failed |
| --- | --- | --- |
| macOS, Python 3.11.15 | 5,684 | 2 |
| macOS, Python 3.12.11 | 5,684 | 2 |
| macOS, Python 3.13.14 | 5,684 | 2 |
| Linux arm64 container, Python 3.12, `python:3.12-slim-trixie` | 5,684 | 3 |

macOS skips the new file watcher test, described below. The two failures in every run are the publish workflow contracts in `test_deployment_contracts.py` (`KeyError: 'push'`), which fail on `ce912e7` too. The Linux run also fails `test_pipeline_triggers.py::test_create_endpoint_with_parent_pipeline`, which fails on Linux before the update as well and passes on macOS. The same container on the previous lock gave the same three failures. The scheduler lock integration tests pass against Redis 7.4.11 and 8.10.2 with redis-py 8.1.0 and 5.0.8.

New backend tests cover the changes above: `mage_ai/tests/authentication/test_passwords.py` (passwords over 72 bytes, hashes made by bcrypt 4), `mage_integrations/mage_integrations/tests/sources/mongodb/test_common.py` (naive and aware datetimes at the three converted call sites), `mage_integrations/mage_integrations/tests/test_dependency_contracts.py` (pendulum names), `mage_ai/tests/kernels/default/test_utils.py` (process listing without deprecation warnings), `mage_ai/tests/server/test_file_observer.py` (a real observer on `metadata.yaml`) and the proxy tests in `mage_ai/tests/services/k8s/test_job_manager.py`.

An in-place upgrade ran end to end. A project, its users and a loader-transformer pipeline were created on the previous lock and code (bcrypt 4.0.1, redis 5.0.8), and the pipeline ran once through an `@once` trigger. The server then restarted on the new lock with the same database. The default owner, a user with a 120-byte password and a user with a short password signed in; a wrong password was rejected. The output written before the upgrade read back through the API, and a new triggered run completed with one block run per block. Nullable `Int64`, `category` and UTC datetime columns reached the transformer with those dtypes.

The browser suite gained four tests in `mage_ai/frontend/tests`: a wrong password on the sign-in page, a shell command in the terminal (terminado 0.18), a block run in the notebook editor with its output table, and the output of a block run started by a trigger. The last two create their own pipeline through the API and need no network access. All ten browser tests passed twice in a row against the old and the new server.

The production frontend build and ESLint give the same results on Node 24.21 and Node 26.11, and `tsc --noEmit` passes on Node 26.11. Node 26 becomes LTS on 2026-10-28 and Node 24 stays supported until 2028-04-30, so CI, the Dockerfile and `engines` stay on Node 24.

### Held back by other packages

| Package | Resolved | Latest | Constraint |
| --- | --- | --- | --- |
| sqlalchemy | 2.0.54 | 2.1.4 | clickhouse-sqlalchemy 0.3.2 requires `sqlalchemy<2.1`. |
| kubernetes | 36.0.3 | 37.0.0 | kubernetes 37 requires `certifi>=2026.7.22`; dbt-snowflake requires `certifi<2025.4.26`. |
| redshift-connector | 2.1.17 | 2.2.0 | dbt-redshift 1.11.1 requires `redshift-connector<2.2`. |
| google-cloud-storage | 3.1.1 | 3.17.0 | dbt-bigquery requires `google-cloud-storage<3.2`. |
| protobuf | 6.33.6 | 7.36.2 | google-ads 30.0.0 requires `protobuf<7`. |

### Follow-ups

- Not updated, each needs a port or a deployment decision: deltalake 0.20.2 (the Delta Lake writer imports private APIs), stripe 5.5.0, facebook-business 22.0.2 and google-ads 30.0.0 (API versions retire on a schedule), elasticsearch 8.x (the 9.x client targets Elasticsearch 9 servers), gspread 5.x, stomp.py 8.x, kafka-python 2.x, mysql-connector-python 9.x, pinotdb 5.x, sentence-transformers 5.x and google-cloud-aiplatform 1.x.
- certifi stays at 2025.1.31 because dbt-snowflake requires `certifi<2025.4.26`, and requests and urllib3 verify HTTPS with that bundle. Setting `SSL_CERT_FILE` and `REQUESTS_CA_BUNDLE` to `/etc/ssl/certs/ca-certificates.crt` in the image would use the Debian bundle that `apt-get` keeps current.
- On macOS, FSEvents reports no events for a watch on a single file with watchdog 4 or 6. The server watches `metadata.yaml` directly, so editing it on macOS does not reload settings. Linux uses inotify and is unaffected.
- A dbt model outside any dbt project makes Mage call `dbt list --project-dir None`. dbt 1.12 raises while parsing that flag, and Mage falls back to a block without upstream dbt dependencies.
- In one browser run, the scheduler dispatched the last block of a run four minutes late, and logged two ticks in that period where ten-second ticks give about 24. Forty later browser tests on both versions did not reproduce it.
- The notebook shows "The kernel has restarted" when the first kernel usage poll returns a process ID while a block is running. The frontend code for it is unchanged here. It appeared in one of seven editor runs on the new version and in none of five on the old version.
- Faker is a runtime dependency, but only tests import it.
- Mage sends usage statistics to `api.mage.ai` by default.
