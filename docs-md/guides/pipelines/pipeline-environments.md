# Pipeline environments

*Run a pipeline's Python blocks with their own package versions.*

## Overview

All pipelines in a MageML project share the packages installed on the server. A pipeline
that needs another version of a library (an older scikit-learn for a model trained with
it, a client pinned by a vendor) conflicts with the others.

A pipeline environment gives one pipeline its own Python packages. Its Python blocks
run in a virtual environment built from a requirements file. Mage builds the environment
the first time a block needs it and reuses it afterwards.

## Set it up

1. Put a requirements file in the pipeline's folder, `pipelines/<pipeline uuid>/`:

```text
   scikit-learn==1.5.2
   six==1.16.0
   ```

2. Open the pipeline's **Settings**, enter the file name under **Pipeline environment
   requirements** and save. To use a different Python version, enter it under **Pipeline
   environment Python version** (such as `3.11`); the server's version is the default.

The setting is saved in the pipeline's `metadata.yaml`:

```yaml
environment:
  requirements: requirements.txt   # relative to the pipeline's folder
  python: '3.11'                    # optional
```

`requirements` can also be a list of requirement lines in `metadata.yaml`.

Clear the requirements field and save to run the blocks with Mage's packages again.

## What runs in the environment

- Python data loaders, transformers, data exporters and custom blocks, with their `@test`
  functions. This applies everywhere a block runs: the notebook, triggers, backfills,
  retries and block fusion.
- Sensors, scratchpads, callbacks, conditionals, SQL, R and Rust blocks run as before.
- Dynamic and replicated blocks can't run in an environment yet. They fail with a
  message that says so.

A block receives its upstream outputs and keyword variables as it would in Mage. Tables
pass as Arrow files. Other values pass as JSON, or as pickle when JSON can't hold them.
Variables that can't be written as JSON (such as an open client object) are left out.
Everything the block prints shows in its output and logs while it runs.

## Package versions

pandas, pyarrow, Polars and NumPy carry tables between Mage and the block, so the
environment gets Mage's versions of them. A requirements file that names one of them
pins its own version instead. Pin a version Mage can read. A pandas or pyarrow much
older than Mage's may write outputs Mage reads differently.

Blocks can import Mage's helpers, such as `mage_ai.data_preparation.shared.secrets`, as
long as the environment has the packages those helpers import. Add them to the
requirements file when a block fails with a missing module.

## Builds and the cache

- [uv](https://docs.astral.sh/uv/) builds the environments. The Mage Docker image
  includes it; install it on other servers.
- Environments are stored in `~/.cache/mage/environments`. Set `MAGE_ENVIRONMENTS_DIR`
  to put them elsewhere, such as a volume that survives container restarts.
- Each environment's folder name comes from a hash of its Python version,
  requirements, pinned packages, platform and uv version. Changing the requirements
  builds a new environment, and the old one stays until you delete its folder.
- A build goes to a temporary folder and is moved into place only when it finishes, so
  a failed or interrupted build is never used. Two processes that need the same
  environment build it once (on Linux and macOS).
- A build that takes longer than 30 minutes fails. Set
  `MAGE_ENVIRONMENT_BUILD_TIMEOUT_SECONDS` to change the limit.
- `mage-environment.json` in each environment's folder lists every package it installed.

## Python workers

A pipeline environment gives every Python block of a pipeline the same packages. A
Python worker is a named environment that each block chooses: in one pipeline, a block
can load features with Feast, which needs pandas 2, while the blocks around it run with
MageML's pandas 3.

A worker is a [uv project](https://docs.astral.sh/uv/concepts/projects/) in
`python_workers/<name>/`, with its `pyproject.toml` and `uv.lock` in Git:

```toml
# python_workers/feast/pyproject.toml
[project]
name = "feast"
version = "0"
requires-python = ">=3.12"
dependencies = [
  "feast[redis]>=0.50",
  "pyarrow",
]

[tool.mageml.worker]
label = "Feast worker"                       # how the editor names it
description = "Online features from Feast; pandas 2"
```

```bash
uv lock --project python_workers/feast
mage workers prepare feast      # uv sync --locked into the environment cache
mage workers list               # each worker and whether it is prepared
```

Or start from a template, which writes the `pyproject.toml`, locks it and prepares it:

```bash
mage workers new feast --template feast --label "Feast worker"
```

| Template | Starts with |
| --- | --- |
| `feast` | Feast, pandas 2 |
| `mlflow` | MLflow, pandas |
| `ml-inference` | scikit-learn, ONNX Runtime, joblib, pandas |
| `polars` | Polars |
| `empty` | pyarrow only |

Every template has pyarrow: tables cross between workers as Arrow. The pipeline's
**Settings** has **Add a Python worker** with the same templates.

To move several blocks, or a whole chain, to a worker at once:

```bash
mage workers assign feast my_pipeline --blocks online_features,join_features
mage workers assign feast my_pipeline --chain online_features   # its whole chain
mage workers assign mage my_pipeline --blocks score             # MageML's own Python
```

In the editor, the pipeline's **Settings** has **Where Python blocks run**: the default
worker for the pipeline's Python blocks, prepared with **Prepare** (uv's output on the
page), and a table of every Python block with where it runs and whether that worker is
ready; a worker a block chose that is not prepared has its **Prepare** there too. Each
block's settings start with **Where this block runs**, and a block in a worker
shows it in its header (with "not prepared" while it would fail); the dependency graph
names it too. In `metadata.yaml`, a block selects its
worker in its configuration; a pipeline selects one for all its Python blocks, and a block
can still choose MageML's own Python with `mage`:

```yaml
environment:
  worker: feast                  # every Python block, unless it selects another
blocks:
- uuid: online_features
  configuration:
    python_worker: feast
- uuid: score
  configuration:
    python_worker: mage          # MageML's own Python
```

- The worker's name is its `[project].name`, compared without case, `-`, `_` or `.`;
  YAML uses it. `label` and `description` under `[tool.mageml.worker]` are what the editor
  shows ("Feast worker (feast)"); changing them does not prepare the worker again.
  `.python-version` in its folder sets the Python version; MageML's is the default.
- Prepare is explicit: a run never installs packages. A block whose worker is not
  prepared for its current `pyproject.toml` and `uv.lock` fails with the command to run,
  and a lock that does not match its project says to run `uv lock` again.
- Tables cross between a worker and MageML's Python as Arrow, so a worker that returns
  pandas frames needs pyarrow. A worker does not import MageML: blocks in it can't import
  `mage_ai` helpers unless the worker has their packages.
- **Combining workers.** When a pipeline uses two workers or more, its **Settings** says
  in one line whether they can run as one: with the same package versions (and how much
  less to install), if some packages change version (which ones), or not (uv's reason).
  **Show what each worker installs** shows each worker's size from its `uv.lock`, and
  **Combine into one worker** writes a worker with all their dependencies, moves the
  pipeline's blocks to it and prepares it; the old workers stay until you delete them.
  Blocks of one worker share one process and its memory, and a crash stops the blocks
  after it, so keep a worker apart (a GPU worker, a native library that must run alone)
  with `combine = false` under `[tool.mageml.worker]`.
  `POST /api/python_worker_combinations` with `{"workers": ["feast", "mlflow"]}` answers
  the same question.
- A service exports the workers its blocks use. The blocks without a worker (or, when
  every block has one, the worker most of them select) run in the image's Python, with
  that environment's packages; each other worker becomes a virtual environment of its own
  in the image, built from its `uv.lock`, and its blocks run there. Each worker needs a
  `uv.lock` and the image's Python version. A chain of Python blocks splits where the
  worker changes; in a stream, the blocks then run as one chain, each reading the block
  before it, as in a stream that mixes languages.

## Pipeline services

`mage export service` installs the environment's packages, at the versions the
environment has, in the service image, and uses the environment's Python version as the
image's. Pipelines exported into one service must share an environment. Export
pipelines with different environments as separate services.
