# Features

![](https://user-images.githubusercontent.com/78053898/198751496-22d51f03-f09f-454c-8f98-409b5dbec8f9.svg)

**[Data pipeline management](../design/data-pipeline-management.md)**

</Card>

**[Notebook for building data pipelines](features.md#1-data-centric-editor)**

</Card>

**[Changelog](releases.md)**

</Card>

**[Roadmap](roadmap.md)**

</Card>

## What MageML adds

- [Rust blocks](../guides/blocks/rust-blocks.md): consecutive Rust blocks compile into one program.
- [R blocks](../guides/blocks/r-blocks.md): consecutive R blocks share one session.
- [Block fusion](../design/data-pipeline-management.md#block-fusion): production runs fuse chains of
  blocks, and pipelines compile ahead of their runs.
- [Pipeline services](../production/pipeline-services.md): export a pipeline as a Docker image
  with `mage export service`.
- [Pipeline documentation](../guides/pipelines/documentation.md): a README per pipeline in
  Markdown or Quarto.

## Data pipeline management

See more [details here](../design/data-pipeline-management.md).

![](https://mage-ai.github.io/assets/orchestration-overview.gif)

## Notebook for building data pipelines

### 1\. Data centric editor

An interactive coding experience designed for preparing data to train ML models.

Visualize the impact of your code every time you load, clean, and transform
data.

<img src="https://mage-ai.github.io/assets/data-centric-editor.png" alt="Data centric editor" />

### 2\. Production ready code

No more writing throw away code or trying to turn notebooks into scripts.

Each block (aka cell) in this editor is a modular file that can be tested,
reused, and chained together to create an executable data pipeline locally or in
any environment.

Read more about [blocks](../design/blocks.md) and how they work.

<img src="https://mage-ai.github.io/assets/data-pipeline.png" alt="Production ready code" />

Run your data pipeline end-to-end using the command line function:
`$ mage run [project] [pipeline]`

You can run your pipeline in production environments with the orchestration
tools

- [Airflow](../integrations/airflow/integrate-mage-airflow.md)

- [Prefect](../integrations/prefect.md)

### 3\. Extensible

Add new functionality directly in the source code: API endpoints
([Tornado](https://www.tornadoweb.org/en/stable/)), transformations (Python, PySpark, SQL,
R, Rust), and charts (using [React](https://reactjs.org/)).

<img src="https://mage-ai.github.io/assets/extensible-charts.gif" alt="Extensible charts" />
