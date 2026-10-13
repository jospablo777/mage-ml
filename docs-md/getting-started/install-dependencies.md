# Install dependencies

*Install the Python packages your project needs with requirements.txt*

List the Python packages your project needs in the `requirements.txt` file at the root of
the project.

## Add packages

1. Open `requirements.txt` in the file browser, or in the editor of the **Terminal** page
   (`/home/src/[project_path]/requirements.txt` in the container image).
2. Add your packages and save the file:

```txt
   scikit-learn>=1.6
   xgboost>=3.0
   ```

## Install them

Install them into the environment MageML runs in. How depends on how you installed MageML
(see the [Quickstart](setup.md)).

### In a uv project

Add them to the project, which records them in `pyproject.toml` and `uv.lock`:

```bash
uv add --requirements my_project/requirements.txt
uv add scikit-learn   # or one package
```

### With uv tool

Reinstall the tool with the packages:

```bash
uv tool install mage-ml --index https://jospablo777.github.io/mage-ml/simple/ \
  --with-requirements my_project/requirements.txt --reinstall
```

### In your own environment

When MageML is installed in an environment of yours, with uv pip or pip, install into the
same one:

```bash
uv pip install --python .venv/bin/python --requirement my_project/requirements.txt
```
```bash
.venv/bin/python -m pip install --requirement my_project/requirements.txt
```

### In the container image

The image built from the repository's Dockerfile installs `requirements.txt` when the
container starts. It uses uv and constrains the install to the versions MageML ships
(the file named by `MAGE_RUNTIME_CONSTRAINTS`), so a requirement cannot replace pandas,
pyarrow or another package MageML depends on. Restart the container after you change
the file.

To install without a restart, run the same command on the **Terminal** page:

```bash
uv pip install --python "$(command -v python3)" \
  --constraint "$MAGE_RUNTIME_CONSTRAINTS" --requirement requirements.txt
```

The image has no `pip`; use `uv pip`. In the small image built from the package index, add
the packages to the `uv pip install` line of its Dockerfile and rebuild it.

### From a source checkout

Install into the environment that `uv sync` created:

```bash
uv pip install --requirement my_project/requirements.txt
```

`uv sync` makes the environment match the lock file exactly, so a later `uv sync` removes
these packages. Run MageML with `uv run --no-sync`, or sync with
`uv sync --inexact` to keep them.

## Packages for one pipeline

All pipelines in a project share the installed packages. When one pipeline needs other
versions of a library, give it its own environment: see
[Pipeline environments](../guides/pipelines/pipeline-environments.md).
