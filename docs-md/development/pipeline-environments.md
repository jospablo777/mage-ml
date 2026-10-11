# Pipeline environments

Status: implemented (`mage_ai/data_preparation/environments.py`) and tested
(`mage_ai/tests/data_preparation/test_environments.py`): notebook runs, a triggered run
through the scheduler, build reuse, errors, dynamic blocks refused, the settings update,
and the service export. The user guide is `docs/guides/pipelines/pipeline-environments.mdx`.

## Review of the starter

`do_not_commit/top_10_next_features/02_isolated_environments` proposes a Rust crate that
derives an environment identity from the lockfile, runtime, ABI, target, CPU features,
base image, sources, build tool and policy, grants one builder lease per environment in
a cache index, and publishes environments with verified file manifests. Measured against
what Mage needs:

- **The building and process launch were left out.** The crate verifies bytes it is
  given; dependency resolution, package installation, process isolation and the
  scheduler integration are listed as integration work. Those are the feature.
- **uv already does the hard parts.** It resolves, installs from a shared cache with
  hard links, builds relocatable venvs and selects Python versions. Reimplementing that
  in Rust would add no speed (uv is Rust) and a large surface.
- **Mage already has an out-of-process block runner.** The pipeline services worker
  (`mage_ai/pipeline_services/runtime/worker.py`) runs a block file with Mage's
  decorators and passes inputs and outputs as Arrow, JSON or pickle files with exact
  types. Running it with the environment's interpreter reuses that path and its
  type-fidelity tests.
- **File manifests and signed provenance** need a trusted build service, which a single
  Mage server does not have. The build is atomic instead (staging folder, rename), and
  `mage-environment.json` records what was installed.

So the implementation is a Python module that builds environments with uv and runs
blocks in them through the services worker. No Rust: the work is process orchestration
around uv, and the hot paths (resolution, installation, Arrow IO) are already native.

## Design

- **Identity.** A SHA-256 of the Python version, the requirement lines, the pinned
  exchange packages (pandas, pyarrow, Polars, NumPy at Mage's versions unless named), the
  platform and the uv version. The first 24 hex digits name the folder.
- **Build.** `uv venv --relocatable` into `<identity>.staging-<pid>`, `uv pip install`,
  `uv pip freeze` into the marker, then `os.replace` into place. A thread lock and an
  `fcntl` file lock make concurrent processes wait for one build. A folder without the
  marker is never used.
- **Run.** `Block._execute_block` routes supported Python blocks of a pipeline with an
  environment to `environments.run_block`. It writes the inputs, starts the worker under
  `mage_ai/shared/supervise.py` (the child dies with Mage), sends one request and a
  shutdown, and reads replies on a pipe passed as `MAGE_WORKER_REPLY_FD`, so the block's
  stdout stays its output. The outputs come back to Mage's normal storage. Tests run in
  the worker; `run_tests` replays their results instead of running them again.
- **Export.** `mage export service` builds the environment, pins its frozen packages over
  the versions it resolved from Mage's interpreter, and uses its Python version for the
  image. Pipelines with different environments cannot share a service.

## Remaining gaps

1. **Dynamic and replicated blocks** are refused; the worker returns one output list and
   does not split dynamic children.
2. **No cleanup.** Old environments stay in the cache folder until deleted.
3. **No lockfile.** Requirements are resolved when the environment is built; pin every
   version (or use `uv pip compile` output) for the same packages on every server.
4. **Windows** builds are serialized within one process only (no `fcntl`).
5. **Variables** that are not JSON-serializable do not reach the block.
6. **Kubernetes and ECS executors** run blocks in their own containers; the environment
   applies inside them only if the image has uv and the cache folder.
