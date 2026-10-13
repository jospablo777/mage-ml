# Frequently Asked Questions

*Frequently asked questions about MageML.*

<div align="center">
<img src="https://mage-ai.github.io/assets/notion-faq.png" />
</div>

<details><summary>true</summary>

MageML is an open-source platform for building, running and shipping data and machine
learning pipelines in Python, SQL, R and Rust. It is a fork of
[Mage](https://github.com/mage-ai/mage-ai) 0.9.79 and keeps its `mage_ai` package and `mage`
command.

</details>

<details><summary>true</summary>

MageML is built for ML engineers, data engineers and data scientists, but is not limited to those roles.

</details>

<details><summary>true</summary>

Install it from MageML's package index with uv:

```bash
uv add mage-ml --index mageml=https://jospablo777.github.io/mage-ml/simple/
```

pip, `uv tool`, `uvx` and Docker work too; see the [Quickstart](../getting-started/setup.md).
`pip install mage-ai` and the `mageai/mageai` image install upstream Mage, not MageML.

</details>

<details><summary>true</summary>

MageML is free and open source. You host it yourself.

</details>

<details><summary>true</summary>

The 4 [core design principles](../design/core-design-principles.md) are:

1. Easy developer experience

2. Engineering best practices built-in

3. Data is a first-class citizen

4. Scaling is made simple

Features:

1. Mix and match SQL, Python, R and Rust in data pipeline tasks.
2. UI/IDE for building and managing data pipelines.
3. Data centric: we designed and built a pipeline engine ONLY for moving and transforming data. This makes it possible for us to make datasets a 1st class citizen; enabling native features such as partitioning, versioning, backfilling, data validation, testing, and data quality monitoring.
4. Extensible: we designed and built the tool with developers in mind, so new functionality can be added to the source code.
5. Scalable: production runs fuse chains of blocks, and consecutive Rust blocks compile into one program ([block fusion](../design/data-pipeline-management.md#block-fusion)).
6. Production ready: when you build your data pipeline, it runs exactly the same in development as it does in production. A pipeline can also be exported as a standalone Docker image ([pipeline services](../production/pipeline-services.md)).
7. Modular: every block/cell you write is a standalone file that is interoperable; meaning it can be used in other pipelines or in other code bases.

</details>

<details><summary>true</summary>

MageML provides an interactive notebook with built-in engineering best practices for building pipelines.

MageML supports writing pipelines in Python, SQL, R and Rust.

MageML supports multiple types of pipelines natively such as:

- Standard batch pipelines
- Data integration pipelines
- Streaming pipelines
- Spark pipelines
- DBT pipelines

</details>

<details><summary>true</summary>

SQL, Python, R, Rust and PySpark. See [Rust blocks](../guides/blocks/rust-blocks.md) and [R blocks](../guides/blocks/r-blocks.md).

</details>

<details><summary>true</summary>

Yes. [Here](../integrations/compute/spark-pyspark.md) is a step-by-step tutorial to use MageML with Spark on EMR.

</details>

<details><summary>true</summary>

Sagemaker is used to train machine learning models and serve them via api.

MageML is an engine for running data pipelines that can move and transform data. That data can then be stored anywhere (e.g. S3) and used to train models in Sagemaker.

</details>

<details><summary>true</summary>

Databricks provides infrastructure to run Spark. They also provide notebooks that can run your code in Spark as well.

MageML can execute your code in a Spark cluster, managed by AWS, GCP, or even Databricks.

</details>

<details><summary>true</summary>

[Here is a doc](../observability/alerting/alerting-slack.md) to help you set up alerting for pipeline status updates in Slack.

</details>

<details><summary>true</summary>

Contributions are welcome. [Here is a doc](../contributing/overview.md) to get you started.

To request features, open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues).

</details>

*Can’t find what you’re looking for? Ask in [GitHub issues](https://github.com/jospablo777/mage-ml/issues).*
