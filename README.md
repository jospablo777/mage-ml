# Mage ML

A fork of [Mage OSS](https://github.com/mage-ai/mage-ai) for building and running data pipelines. This fork updates Python dependencies, uses uv for package management, and maintains the `mage_ai` import namespace and `mage` command.

The runtime supports Python 3.11–3.13 and uses pandas 3, NumPy 2, Polars 1, and SQLAlchemy 2. Dependencies are declared in `pyproject.toml` and resolved in `uv.lock`.

## Install from source

```bash
git clone https://github.com/jospablo777/mage-ml.git
cd mage-ml
uv sync --locked --no-default-groups --extra postgres
uv run --no-sync mage init my_project
uv run --no-sync mage start my_project
```

Select additional integrations with `--extra`, for example `--extra s3` or `--extra dbt`. A subsequent `uv sync` removes extras that are not selected. Use `uv run --no-sync` when running an environment already synchronized with the required extras.

The `all` extra selects the container dependency profile. The `integrations` extra supplies connector dependencies; the separate `mage_integrations` package still requires its own installation.

## Development

```bash
make dev_env
make test
```

See [the development guide](README_dev.md) for extras, frontend development, and dependency changes. See [the fork audit](docs/development/fork-audit.md) for validation results and remaining release blockers.

## Documentation

The [upstream documentation](https://docs.mage.ai) describes pipeline configuration, scheduling, connectors, and the user interface. Upstream installation commands and container images install Mage OSS rather than this fork.

## License

[Apache License 2.0](LICENSE).
