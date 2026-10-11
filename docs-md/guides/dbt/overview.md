# Use dbt with MageML

*Orchestrate, schedule and monitor dbt models as part of your data pipelines.*

> [!NOTE]
> dbt needs the `dbt` extra. The Docker image built from the repository includes it. For a
> local install, add `--extra dbt` to `uv sync`.

<img src="https://c.tenor.com/gbLWPf5HCsYAAAAC/devastator-constructicons.gif" />

## What the dbt integration does

MageML lets you **build, schedule, and monitor dbt models** alongside the rest of your
pipelines, without Airflow or dbt Cloud.

<img src="https://user-images.githubusercontent.com/59450879/252510713-4fd2ed03-7867-4038-8b0e-24eb1c40c518.png" />

## Who it is for

The dbt integration is for analytics engineers and data engineers.

### Analytics engineers

If you transform raw data into analytics-ready datasets with dbt, MageML gives you:

- A **visual interface** to build and run dbt models
- The ability to **chain dbt steps** with Python or SQL blocks
- Easy scheduling and triggering for model runs
- **Live SQL previews** to speed up development

### Data engineers

If you build and orchestrate production pipelines, MageML lets you:

- Run dbt models as part of larger **end-to-end workflows**
- Replace complex orchestration tools like **Airflow**
- Run dbt in **Docker**, **Kubernetes**, or in the **cloud (AWS, GCP, Azure)**
- Monitor, debug, and get alerts on dbt runs with built-in **observability tools**

## Key Features

### Schedule dbt model runs

Set dbt models to run:
- On a schedule
- From an API call
- After upstream blocks finish

### Run specific dbt models and their dependencies

<img src="https://mage-ai.github.io/assets/dbt/add-dbt-model.gif" />

Use the visual interface to:
- Select and run individual dbt models
- Automatically include required dependencies

### Exclude Models When Running All

![](https://raw.githubusercontent.com/mage-ai/assets/main/dbt/add-dbt-models.gif)

You can exclude specific dbt models when running all of them.

### Chain dbt Models with Other Tasks

Build workflows where dbt models run after:
- Pulling data from APIs
- Completing a Python or SQL block
- Finishing a different pipeline

### Preview dbt model results as you write SQL

<img src="https://mage-ai.github.io/assets/dbt/dbt-preview.gif" />

You can **preview dbt model output as you write SQL**, without running the pipeline.

### Build dynamic dbt pipelines using flexible variable interpolation

Use variable interpolation such as:

- `{{ env_var('...') }}`
- `{{ variables('...') }}`
- `{{ mage_secret_var('...') }}`

### Automatically run dbt tests every time a pipeline runs

In a pipeline run, a dbt model block runs `dbt build`, which runs the model's tests. A failed
test fails the block. See [Running dbt tests](running-dbt-tests.md).

### Built-In Monitoring and Alerting

For each dbt run you can:
- Track model run history
- See logs and SQL output
- Get alerts when models fail

## Supported connectors

1. `dbt-bigquery`
1. `dbt-clickhouse`
1. `dbt-core`
1. `dbt-duckdb`
1. `dbt-postgres`
1. `dbt-redshift`
1. `dbt-snowflake`
1. `dbt-spark`
1. `dbt-sqlserver`
1. `dbt-synapse`
1. `dbt-trino`

## Tutorials

Set up and run your first dbt models:

- [How to set up dbt models and orchestrate runs](setup-dbt.md)
