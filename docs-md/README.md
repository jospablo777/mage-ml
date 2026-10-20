# MageML documentation

The MageML documentation as Markdown, for reading on GitHub. It is generated from the pages in [`docs/`](../docs) by `scripts/docs/export_markdown.py`; edit those pages, not these files. The same pages, with search, run as a site with `docs/docs.sh` (http://localhost:3333).

## Documentation

### Adventure starts here

- [MageML](introduction/overview.md)
- [Quickstart](getting-started/setup.md)
- [Tutorial: a pipeline in Python, Rust and R](getting-started/polyglot-pipeline.md)
- **Retail orders, in four languages**
  - [Tutorial: retail orders in Python](getting-started/retail-python.md)
  - [Tutorial: retail orders in R](getting-started/retail-r.md)
  - [Tutorial: retail orders in Rust](getting-started/retail-rust.md)
  - [Tutorial: retail orders in SQL on DuckDB](getting-started/retail-sql.md)
- [From pipeline to production](getting-started/pipeline-to-production.md)
- [Build an ML inference service](getting-started/inference-service.md)
- [Tutorial: ship a live service from the playground](getting-started/service-tutorial.md)
- [Playground: try MageML with sample data](getting-started/playground.md)
- [Machine learning pipeline tutorial](guides/train/train-model.md)
- [Core Design Principles](design/core-design-principles.md)
- **Abstractions**
  - [Abstractions](design/core-abstractions.md)
  - [Project Structure](design/abstractions/project-structure.md)

### Concepts

- [Pipeline Management](design/data-pipeline-management.md)
- [ColumnAtlas: explore block outputs](guides/blocks/column-atlas.md)
- **Blocks**
  - [Overview](design/blocks.md)
  - **Types**
    - [Callback blocks](design/blocks/callbacks.md)
    - [Conditional blocks](design/blocks/conditionals.md)
    - [Data exporter](design/blocks/data-exporter.md)
    - [Data loader](design/blocks/data-loader.md)
    - [Scratchpad](design/blocks/scratchpad.md)
    - [Sensor](design/blocks/sensor.md)
    - [Transformer](design/blocks/transformer.md)
  - [Dynamic blocks](design/blocks/dynamic-blocks.md)
  - **Resources**
    - **Data loader**
      - [Data loader utilities](design/data-loading.md)
      - [Data loader templates](development/blocks/data_loaders/templates.md)
    - **Transformer**
      - [Transformer templates](development/blocks/transformers/templates.md)
    - **Data exporter**
      - [Data exporter templates](development/blocks/data_exporters/templates.md)
    - **Sensors**
      - [Sensor templates](development/blocks/sensors/templates.md)
    - **Callbacks**
      - [Callback templates](development/blocks/callbacks/templates.md)
    - **Conditionals**
      - [Conditional templates](development/blocks/conditionals/templates.md)
- **Data integrations**
  - [Overview](data-integrations/overview.md)
  - [Configure data integration pipelines](data-integrations/configuration.md)
  - **Sources**
    - [Sources technical documentation](data-integrations/sources/overview.md)
    - [Airtable](data-integrations/sources/airtable.md)
    - [Amazon S3](data-integrations/sources/amazon_s3.md)
    - [Amplitude](data-integrations/sources/amplitude.md)
    - [API Source Configuration](data-integrations/sources/api.md)
    - [Azure Blob Storage](data-integrations/sources/azure_blob_storage.md)
    - [BigQuery](data-integrations/sources/bigquery.md)
    - [Chargebee](data-integrations/sources/chargebee.md)
    - [Commercetools](data-integrations/sources/commercetools.md)
    - [Couchbase](data-integrations/sources/couchbase.md)
    - [Datadog](data-integrations/sources/datadog.md)
    - [Doris](data-integrations/sources/doris.md)
    - [Dremio](data-integrations/sources/dremio.md)
    - [Amazon DynamoDB](data-integrations/sources/dynamodb.md)
    - [Facebook Ads](data-integrations/sources/facebook_ads.md)
    - [Freshdesk](data-integrations/sources/freshdesk.md)
    - [Front](data-integrations/sources/front.md)
    - [GitHub](data-integrations/sources/github.md)
    - [Google Ads](data-integrations/sources/google_ads.md)
    - [Google Analytics](data-integrations/sources/google_analytics.md)
    - [Google Cloud Storage](data-integrations/sources/google_cloud_storage.md)
    - [Google Search Console](data-integrations/sources/google_search_console.md)
    - [Google Sheets](data-integrations/sources/google_sheets.md)
    - [HubSpot](data-integrations/sources/hubspot.md)
    - [Intercom](data-integrations/sources/intercom.md)
    - [Knowi](data-integrations/sources/knowi.md)
    - [LinkedIn Ads](data-integrations/sources/linkedin_ads.md)
    - [Mode](data-integrations/sources/mode.md)
    - [Monday](data-integrations/sources/monday.md)
    - [MongoDB](data-integrations/sources/mongodb.md)
    - [MSSQL (Microsoft SQL Server)](data-integrations/sources/mssql.md)
    - [MySQL](data-integrations/sources/mysql.md)
    - [Outreach](data-integrations/sources/outreach.md)
    - [Paystack](data-integrations/sources/paystack.md)
    - [Pipedrive](data-integrations/sources/pipedrive.md)
    - [PostgreSQL](data-integrations/sources/postgresql.md)
    - [Postmark](data-integrations/sources/postmark.md)
    - [Power BI](data-integrations/sources/powerbi.md)
    - [Amazon Redshift](data-integrations/sources/redshift.md)
    - [Salesforce](data-integrations/sources/salesforce.md)
    - [SFTP](data-integrations/sources/sftp.md)
    - [Snowflake](data-integrations/sources/snowflake.md)
    - [Stripe](data-integrations/sources/stripe.md)
    - [Tableau](data-integrations/sources/tableau.md)
    - [Teradata](data-integrations/sources/teradata.md)
    - [Twitter Ads](data-integrations/sources/twitter_ads.md)
    - [Zendesk](data-integrations/sources/zendesk.md)
  - **Destinations**
    - [Destinations technical documentation](data-integrations/destinations/overview.md)
    - [Airtable](data-integrations/destinations/airtable.md)
    - [Amazon S3](data-integrations/destinations/amazon_s3.md)
    - [BigQuery](data-integrations/destinations/bigquery.md)
    - [ClickHouse](data-integrations/destinations/clickhouse.md)
    - [Delta Lake (Azure)](data-integrations/destinations/delta_lake_azure.md)
    - [Delta Lake (S3)](data-integrations/destinations/delta_lake_s3.md)
    - [Doris](data-integrations/destinations/doris.md)
    - [Elasticsearch](data-integrations/destinations/elasticsearch.md)
    - [Google Cloud Storage](data-integrations/destinations/google_cloud_storage.md)
    - [Kafka](data-integrations/destinations/kafka.md)
    - [MongoDB](data-integrations/destinations/mongodb.md)
    - [MSSQL (Microsoft SQL Server)](data-integrations/destinations/mssql.md)
    - [MySQL](data-integrations/destinations/mysql.md)
    - [OpenSearch](data-integrations/destinations/opensearch.md)
    - [OracleDB](data-integrations/destinations/oracledb.md)
    - [PostgreSQL](data-integrations/destinations/postgresql.md)
    - [Redshift](data-integrations/destinations/redshift.md)
    - [Salesforce](data-integrations/destinations/salesforce.md)
    - [Snowflake](data-integrations/destinations/snowflake.md)
    - [Teradata](data-integrations/destinations/teradata.md)
    - [Trino](data-integrations/destinations/trino.md)
  - [Using data integrations in batch pipelines (in Beta)](data-integrations/batch-pipelines.md)
- **Triggers**
  - [Pipeline Trigger Overview](orchestration/triggers/triggering-pipelines.md)
  - [Configure triggers in code](orchestration/triggers/configure-triggers-in-code.md)
  - [Trigger pipeline from a block](orchestration/triggers/trigger-pipeline.md)
  - [Triggering pipeline via API request](orchestration/triggers/trigger-pipeline-api.md)
  - [Schedules](orchestration/triggers/schedule-pipelines.md)
  - [Query trigger metadata from database](orchestration/triggers/database-query.md)
- **Pipeline runs**
  - [Pipeline Run](orchestration/pipeline-runs/overview.md)
  - [Retrying block runs from a pipeline run](orchestration/pipeline-runs/retrying-block-runs.md)
  - [Saving block output as CSV file](orchestration/pipeline-runs/saving-block-output-as-csv.md)
  - [Query pipeline run metadata from database](orchestration/pipeline-runs/database-query.md)
- **Backfills**
  - [Backfilling pipelines](orchestration/backfills/overview.md)
  - [Backfill guide](orchestration/backfills/guides.md)

### Project

- [Project setup](development/project/setup.md)
- [Get started with Git](development/git/setting-up-git.md)
- **Docker**
  - [Connect a local db in Docker](development/docker/connecting-a-database.md)
  - [Run docker image as non-root user](development/docker/non-root-user.md)
- **Variables**
  - [MageML variables](development/variables/overview.md)
  - [Accessing variables in MageML](development/variables/referencing-variables.md)
  - **Variable Types**
    - [MageML environment variables](development/variables/environment-variables.md)
    - [Pipeline variables and keyword arguments](getting-started/runtime-variable.md)
    - [MageML block variables](development/variables/block-variables.md)
    - [Secrets](development/variables/secrets.md)
- **Custom libraries**
  - [Install Python packages using `requirements.txt`](development/dependencies/requirements.md)
  - [Import your own Python code](development/dependencies/custom-files.md)
- [Database](development/databases/default.md)
- [IO Config](development/io_config.md)
- [IO Config Setup](development/io_config_setup.md)
- [Python kernels](getting-started/kernels.md)
- [Local timezone](development/project/local-timezone.md)
- [Configuring MageML settings backend](development/settings-backend.md)
- [Updating MageML](development/updating-mage.md)
- [CLI Commands Reference](development/cli-commands.md)
- [Project metadata.yaml](development/project/metadata.md)

### Contributing

- [Contributing](contributing/overview.md)
- [Configure your development environment](contributing/development-environment.md)
- [Adapt an existing source](contributing/data-integrations/adapt-existing-source.md)
- [Create a new source](contributing/data-integrations/add-new-source.md)
- [Create a new destination](contributing/data-integrations/add-new-destination.md)
- **Backend**
  - [Contributing to the backend server](contributing/backend/overview.md)
  - **IO classes**
    - [Adding an IO class](contributing/backend/io/adding-a-class.md)
  - **Streaming**
    - [Contributing](contributing/backend/streaming/sources-and-destinations.md)
  - **Testing**
    - [Testing](contributing/backend/testing/overview.md)
- **Frontend**
  - [Contributing to the front-end client](contributing/frontend/overview.md)
  - [How to add a chart](contributing/charts/how-to-add.md)
  - [Frontend testing](contributing/frontend/testing.md)
- **Documentation**
  - [Documentation](contributing/documentation/overview.md)
- [styling](contributing/styling.md)

### About

- [Code of Conduct](about/code-of-conduct.md)
- [Frequently Asked Questions](about/frequently-asked-questions.md)
- [Releases](about/releases.md)

## Development

### Quickstart

- [MageML Guides](guides/overview.md)
- [MageML tips & tricks](guides/tips-and-tricks.md)
- [Troubleshooting](development/troubleshooting.md)

### Developer experience

- [Writing data pipeline code](guides/developer-ux/code-editor.md)
- [Block dependency tree](guides/developer-ux/block-dependency-tree.md)
- [Browse all files](guides/developer-ux/file-browser.md)
- [Pipeline sidekick](guides/developer-ux/sidekick.md)

### Version control

- [Configure Git](guides/git/configure.md)
- [Version control application with Git](guides/data-sync/version-control.md)
- [One-way code sync using Git](guides/data-sync/git-sync.md)
- [Git integration for code version control](guides/data-sync/git-integration.md)

### Blocks

- [Reordering blocks](guides/blocks/reorder-blocks.md)
- [Reuse blocks](guides/blocks/reuse-blocks.md)
- [User-defined templates](guides/blocks/user-defined-templates.md)
- [Detaching blocks](guides/blocks/detach-blocks.md)
- [Dynamic blocks](guides/blocks/dynamic-blocks.md)
- [Native support for Polars](guides/blocks/polars.md)
- [Replicating blocks](guides/blocks/replicate-blocks.md)
- [Transformer blocks](guides/blocks/transformer-blocks.md)
- [Data contracts](guides/blocks/data-contracts.md)
- [Custom blocks](guides/blocks/custom-blocks.md)
- [Markdown blocks](guides/blocks/markdown-blocks.md)
- [SQL blocks](guides/blocks/sql-blocks.md)
- [Set up R](guides/blocks/r-setup.md)
- [R blocks](guides/blocks/r-blocks.md)
- [R packages with rv](guides/blocks/r-packages.md)
- [Set up Rust](guides/blocks/rust-setup.md)
- [Rust blocks](guides/blocks/rust-blocks.md)
- [Feast features](guides/blocks/feast-features.md)

### Pipelines

- [ETL pipeline tutorial](guides/load-api-data.md)
- [How to build a data integration pipeline](guides/data-integration-pipeline.md)
- **Amplitude**
  - [Amplitude](guides/amplitude/overview.md)
  - [Amplitude events in a warehouse](guides/amplitude/warehouse.md)
  - [Amplitude features in Python blocks](guides/amplitude/python-blocks.md)
  - [Amplitude events as they happen](guides/amplitude/webhook-stream.md)
  - [Amplitude events through Kafka](guides/amplitude/kafka.md)
- **Streaming**
  - [Overview](guides/streaming/introduction.md)
  - **Sources**
    - [ActiveMQ](guides/streaming/sources/activemq.md)
    - [Amazon SQS](guides/streaming/sources/amazon-sqs.md)
    - [Azure Event Hub](guides/streaming/sources/azure-event-hub.md)
    - [Google Cloud PubSub](guides/streaming/sources/google-cloud-pubsub.md)
    - [InfluxDB](guides/streaming/sources/influxdb.md)
    - [Kafka](guides/streaming/sources/kafka.md)
    - [Kinesis](guides/streaming/sources/kinesis.md)
    - [NATS.io - JetStream](guides/streaming/sources/nats.md)
    - [PostgreSQL (logical replication)](guides/streaming/sources/postgres.md)
    - [RabbitMQ](guides/streaming/sources/rabbitmq.md)
    - [MongoDB](guides/streaming/sources/mongodb.md)
  - **Destinations**
    - [ActiveMQ](guides/streaming/destinations/activemq.md)
    - [Amazon S3](guides/streaming/destinations/amazon-s3.md)
    - [BigQuery](guides/streaming/destinations/bigquery.md)
    - [ClickHouse](guides/streaming/destinations/clickhouse.md)
    - [DuckDB](guides/streaming/destinations/duckdb.md)
    - [Dummy](guides/streaming/destinations/dummy.md)
    - [Elasticsearch](guides/streaming/destinations/elasticsearch.md)
    - [Google Cloud PubSub](guides/streaming/destinations/google-cloud-pubsub.md)
    - [Google Cloud Storage](guides/streaming/destinations/google-cloud-storage.md)
    - [InfluxDB](guides/streaming/destinations/influxdb.md)
    - [Kafka](guides/streaming/destinations/kafka.md)
    - [Kinesis](guides/streaming/destinations/kinesis.md)
    - [MongoDB](guides/streaming/destinations/mongodb.md)
    - [Microsoft SQL Server](guides/streaming/destinations/mssql.md)
    - [MySQL](guides/streaming/destinations/mysql.md)
    - [NATS.io - JetStream](guides/streaming/destinations/nats.md)
    - [Opensearch](guides/streaming/destinations/opensearch.md)
    - [Oracle Database](guides/streaming/destinations/oracledb.md)
    - [Postgres](guides/streaming/destinations/postgres.md)
    - [RabbitMQ](guides/streaming/destinations/rabbitmq.md)
    - [Redshift](guides/streaming/destinations/redshift.md)
    - [Snowflake](guides/streaming/destinations/snowflake.md)
    - [Trino](guides/streaming/destinations/trino.md)
    - [Azure Data Lake](guides/streaming/destinations/azure_data_lake.md)
  - **Tutorials**
    - [Stream examples](guides/streaming/tutorials/stream-examples.md)
    - [Kafka to PostgreSQL](guides/streaming/tutorials/kafka-to-postgres.md)
    - [PostgreSQL changes to Kafka](guides/streaming/tutorials/postgres-changes-to-kafka.md)
    - [Kafka streaming pipeline](guides/streaming/tutorials/streaming-pipeline.md)
    - [Local stream to dbt transform](guides/streaming/tutorials/magic-devcontainer.md)
    - [RabbitMQ streaming pipeline](guides/streaming/tutorials/streaming-pipeline-rabbitmq.md)
- [Compile pipelines](guides/pipelines/compilation.md)
- [Pipeline shapes](guides/pipelines/pipeline-shapes.md)
- [Importing pipelines](guides/pipelines/importing-pipelines.md)
- [Pipeline environments](guides/pipelines/pipeline-environments.md)
- [Run records and reproducing runs](guides/pipelines/run-records.md)
- [Experiments with MLflow](guides/pipelines/experiments.md)
- [Model release checks](guides/pipelines/model-releases.md)
- [Model inference](guides/pipelines/model-inference.md)
- [Resources and shared limits](guides/pipelines/resources.md)
- [Monitoring block outputs](guides/pipelines/monitoring.md)
- [Community Examples](guides/community-examples.md)
- [Pipeline documentation](guides/pipelines/documentation.md)

### dbt

- [Use dbt with MageML](guides/dbt/overview.md)
- [Develop dbt in MageML](guides/dbt/developing-dbt-in-mage.md)
- [dbt quickstart](guides/dbt/setup-dbt.md)
- **Configuration**
  - [dbt sources and upstream dependencies](guides/dbt/sources.md)
  - [dbt connection profiles](guides/dbt/connection-profiles.md)
  - [dbt variable interpolation](guides/dbt/variable-interpolation.md)
  - [Run dbt-spark against a pySpark session.](guides/dbt/run-dbt-spark.md)
- **Models**
  - [Add an existing dbt project to MageML](guides/dbt/add-existing-dbt.md)
  - [Incremental models](guides/dbt/incremental-models.md)
  - [Run a single model](guides/dbt/run-single-model.md)
  - [Run multiple models](guides/dbt/run-selected-model.md)
- **Commands**
  - [Serve dbt docs in production](guides/dbt/docs.md)
  - [Run dbt snapshot](guides/dbt/snapshots.md)
  - [Running dbt tests](guides/dbt/running-dbt-tests.md)

## Production

### Deployment

- [Overview](production/ci-cd/overview.md)
- [Repository setup](production/ci-cd/local-cloud/repository-setup.md)
- [GitHub Actions](production/ci-cd/local-cloud/github-actions.md)
- [GitLab deployments](production/ci-cd/local-cloud/gitlab-ci-cd.md)
- [Buildkite deployments](production/ci-cd/local-cloud/buildkite.md)
- [Production Configuration](production/configuring-production-settings/overview.md)
- [Export pipelines as a service](production/export-a-service.md)
- [Stream services](production/stream-services.md)
- [Services in the app](production/services-page.md)
- [Pipeline services: run pipelines without Mage](production/pipeline-services.md)
- [Service console](production/service-console.md)
- [Pipeline performance](production/pipeline-performance.md)

### Cloud infrastructure

- [Architecture](production/deploying-to-cloud/architecture.md)
- **Infrastructure as code**
  - [Terraform](production/deploying-to-cloud/using-terraform.md)
  - [Helm](production/deploying-to-cloud/using-helm.md)
- **AWS**
  - [Deploy to AWS with Terraform](production/deploying-to-cloud/aws/setup.md)
  - [Terraform AWS resources](production/deploying-to-cloud/aws/resources.md)
  - [AWS ECS deployment architecture](production/deploying-to-cloud/aws/aws-ecs-architecture.md)
  - [AWS policies for Terraform apply](production/deploying-to-cloud/aws/terraform-apply-policy.md)
  - [AWS policies for Terraform destroy](production/deploying-to-cloud/aws/terraform-destroy-policy.md)
  - [EMR policies](production/deploying-to-cloud/aws/emr-policy.md)
  - [MageML and Native Cloud Environments](production/deploying-to-cloud/aws-without-terraform.md)
  - [Deploy to AWS ECS with AWS CodePipeline](production/deploying-to-cloud/aws/code-pipeline.md)
- **Azure**
  - [Deploy to Azure with Terraform](production/deploying-to-cloud/azure/setup.md)
  - [Terraform Azure resources](production/deploying-to-cloud/azure/resources.md)
- **DigitalOcean**
  - [Deploy to DigitalOcean with Terraform](production/deploying-to-cloud/digitalocean/setup.md)
- **GCP**
  - [Deploy to GCP with Terraform](production/deploying-to-cloud/gcp/setup.md)
  - [Terraform GCP resources](production/deploying-to-cloud/gcp/resources.md)
  - [Push Docker Image to GCP Artifact Registry](production/deploying-to-cloud/gcp/gcp-artifact-registry.md)

### Compute resources

- [Compute resources](production/configuring-production-settings/compute-resource.md)
- [Local python executor](production/executors/python.md)
- [Kubernetes executor](production/executors/k8.md)
- [PySpark executor](production/executors/spark.md)
- [AWS ECS executor](production/executors/aws.md)
- [Azure Container Instance executor](production/executors/azure.md)
- [GCP Cloud Run executor](production/executors/gcp.md)

### Secret manager

- [MageML’s built-in secret manager](production/deploying-to-cloud/secrets/mage.md)
- [AWS Secrets Manager](production/deploying-to-cloud/secrets/AWS.md)
- [Azure Key Vault](production/deploying-to-cloud/secrets/Azure.md)
- [GCP Secret Management](production/deploying-to-cloud/secrets/GCP.md)

### Authentication

- [User authentication](production/authentication/overview.md)
- [Sign in with Okta](production/authentication/okta.md)
- [Sign in with Microsoft Entra ID aka Active Directory](production/authentication/microsoft.md)
- [Sign in with Google](production/authentication/google.md)
- [Lightweight Directory Access Protocol](production/authentication/ldap.md)
- [Role-based access controls (RBAC)](production/authentication/permissions/overview.md)
- [Roles](production/authentication/permissions/roles.md)
- [Permissions](production/authentication/permissions/permissions.md)
- [Sign in with OIDC](production/authentication/oidc.md)

## Observability

### Monitoring

- [Monitoring](observability/monitoring.md)
- [Writing metrics to Datadog](observability/external/datadog.md)
- [Metaplane in MageML](observability/external/metaplane.md)
- [Monitoring with New Relic](observability/external/newrelic.md)
- [Monitoring with OpenTelemetry](observability/external/opentelemetry.md)
- [Monitoring with Prometheus](observability/external/prometheus.md)
- [Monitoring with Sentry](observability/external/sentry.md)
- [Customizable dashboards](observability/visualizations/dashboards.md)

### Alerting

- [Alerting status updates in Discord](observability/alerting/alerting-discord.md)
- [Alerting status updates in Email](observability/alerting/alerting-email.md)
- [Alerting status updates in Opsgenie](observability/alerting/alerting-opsgenie.md)
- [Alerting status updates in Slack](observability/alerting/alerting-slack.md)
- [Alerting status updates in Teams](observability/alerting/alerting-teams.md)
- [Alerting status updates in Telegram](observability/alerting/alerting-telegram.md)

### Logging

- [Logging](observability/logging.md)

### Testing

- [Data validation](observability/testing/data-validation.md)
- [Unit tests](observability/testing/unit-tests.md)

## Integrations

### Infrastructure

- **Cloud platforms**
  - [IBM Cloud](integrations/ibm-cloud.md)
- **Compute**
  - [Spark and PySpark](integrations/compute/spark-pyspark.md)
- **Storage**
  - [Algolia Integration in MageML](integrations/databases/Algolia.md)
  - [Athena](integrations/databases/Athena.md)
  - [Azure Blob Storage](integrations/databases/AzureBlobStorage.md)
  - [Backblaze B2](integrations/databases/BackblazeB2.md)
  - [BigQuery](integrations/databases/BigQuery.md)
  - [Chroma](integrations/databases/Chroma.md)
  - [ClickHouse](integrations/databases/ClickHouse.md)
  - [Druid](integrations/databases/Druid.md)
  - [DuckDB](integrations/databases/DuckDB.md)
  - [Google Cloud Storage](integrations/databases/GoogleCloudStorage.md)
  - [Google Sheets](integrations/databases/GoogleSheets.md)
  - [Microsoft SQL Server](integrations/databases/MicrosoftSQLServer.md)
  - [MongoDB](integrations/databases/MongoDB.md)
  - [MotherDuck](integrations/databases/MotherDuck.md)
  - [MySQL](integrations/databases/MySQL.md)
  - [OracleDB](integrations/databases/OracleDB.md)
  - [Pinot](integrations/databases/Pinot.md)
  - [PostgreSQL](integrations/databases/PostgreSQL.md)
  - [Qdrant](integrations/databases/Qdrant.md)
  - [Redshift](integrations/databases/Redshift.md)
  - [S3](integrations/databases/S3.md)
  - [Snowflake](integrations/databases/Snowflake.md)
  - [Spark](integrations/databases/Spark.md)
  - [Trino SQL blocks](integrations/databases/trino.md)
  - [Weaviate](integrations/databases/Weaviate.md)

### 3rd party

- [Using an external IDE](integrations/text-editor.md)
- **Orchestration**
  - **Airflow**
    - [Run MageML pipelines in Airflow](integrations/airflow/index.md)
    - [Integrate MageML into an existing Airflow project](integrations/airflow/integrate-mage-airflow.md)
  - [Run MageML pipelines in Prefect](integrations/prefect.md)
- **ETL**
  - [Airbyte in MageML](integrations/airbyte.md)
  - [Stitch in MageML](integrations/stitch.md)
- **Reverse ETL**
  - [Hightouch in MageML](integrations/hightouch.md)
- [dbt Cloud in MageML](integrations/dbt-cloud.md)
- [StrongDM](integrations/strongdm.md)

## Extensibility

### Environment-specific configuration

- [Global data products](extensibility/global-data-products/overview.md)

### API

- [API overview](extensibility/api-reference/overview.md)
- **Design**
  - [API resources](extensibility/api-reference/resources.md)
  - [API policies](extensibility/api-reference/policies.md)
  - [API presenters](extensibility/api-reference/presenters.md)
- **Backfills**
  - [Overview](extensibility/api-reference/backfills/overview.md)
  - [Read backfills](extensibility/api-reference/backfills/read-backfills.md)
  - [Create backfill](extensibility/api-reference/backfills/create-backfills.md)
  - [Update backfill](extensibility/api-reference/backfills/update-backfill.md)
  - [Delete backfill](extensibility/api-reference/backfills/delete-backfill.md)
- **Blocks**
  - [Overview](extensibility/api-reference/blocks/overview.md)
  - [Read block](extensibility/api-reference/blocks/read-block.md)
  - [Create block](extensibility/api-reference/blocks/create-block.md)
  - [Update block](extensibility/api-reference/blocks/update-block.md)
  - [Delete block](extensibility/api-reference/blocks/delete-block.md)
- **Logs**
  - [Overview](extensibility/api-reference/logs/overview.md)
  - [Read logs](extensibility/api-reference/logs/read-logs.md)
- **OAuth access tokens**
  - [Overview](extensibility/api-reference/oauth-access-tokens/overview.md)
  - [Read OAuth tokens](extensibility/api-reference/oauth-access-tokens/read-oauth-access-tokens.md)
- **Pipeline runs**
  - [Overview](extensibility/api-reference/pipeline-runs/overview.md)
  - [Read pipeline runs](extensibility/api-reference/pipeline-runs/read-pipeline-runs.md)
  - [Trigger pipeline](extensibility/api-reference/pipeline-runs/trigger-pipeline.md)
- **Pipeline schedules**
  - [Overview](extensibility/api-reference/pipeline-schedules/overview.md)
  - [Read pipeline schedules](extensibility/api-reference/pipeline-schedules/read-pipeline-schedules.md)
  - [Create pipeline schedule](extensibility/api-reference/pipeline-schedules/create-pipeline-schedule.md)
  - [Update schedule](extensibility/api-reference/pipeline-schedules/update-pipeline-schedule.md)
- **Pipelines**
  - [Overview](extensibility/api-reference/pipelines/overview.md)
  - [Read pipelines](extensibility/api-reference/pipelines/read-pipeline.md)
  - [Create pipeline](extensibility/api-reference/pipelines/create-pipeline.md)
  - [Update pipeline](extensibility/api-reference/pipelines/update-pipeline.md)
  - [Delete pipeline](extensibility/api-reference/pipelines/delete-pipeline.md)
- **Sessions**
  - [Overview](extensibility/api-reference/sessions/overview.md)
  - [Create session](extensibility/api-reference/sessions/create-session.md)

## Other pages

- [Features](about/features.md)
- [Roadmap](about/roadmap.md)
- [OracleDB](data-integrations/sources/oracledb.md)
- [1. Use uv for Python package management](decisions/0001-use-uv-for-python-package-management.md)
- [2. One native extension, named column-atlas-native](decisions/0002-one-native-extension-named-column-atlas-native.md)
- [Pipeline services: pipelines exported as standalone Docker services](design/pipeline-services.md)
- [Block fusion: blocks that run together without losing their boundaries](development/block-fusion.md)
- [CI and tests](development/ci.md)
- [ColumnAtlas](development/column-atlas.md)
- [Compilation study: how pipeline shapes, languages and failures behave](development/compilation-study.md)
- [Data contracts](development/data-contracts.md)
- [DuckDB with pandas and Polars](development/duckdb-integration.md)
- [Durable execution: block run attempts](development/durable-execution.md)
- [Efficient data exchange](development/efficient-data-exchange.md)
- [Evaluation and releases](development/evaluation-releases.md)
- [Experiments and the model registry](development/experiment-registry.md)
- [Pushing and pulling features with Feast](development/feast-integration.md)
- [Fork audit](development/fork-audit.md)
- [Monitoring](development/ml-monitoring.md)
- [MongoDB with pandas and Polars](development/mongodb-integration.md)
- [MySQL with pandas and Polars](development/mysql-integration.md)
- [Pipeline environments](development/pipeline-environments.md)
- [PostgreSQL data integrity with pandas and Polars](development/postgres-data-integrity.md)
- [R blocks](development/r-blocks.md)
- [Release notes](development/release-notes.md)
- [v0.1.0](development/releases/v0.1.0.md)
- [v0.1.1](development/releases/v0.1.1.md)
- [v0.2.0](development/releases/v0.2.0.md)
- [Releasing MageML](development/releasing.md)
- [Resource-aware scheduling](development/resource-scheduling.md)
- [Rust run engine for triggered runs](development/run-engine.md)
- [Run records](development/run-records.md)
- [Rust pipeline optimization](development/rust-pipeline-optimization.md)
- [S3 with pandas and Polars](development/s3-integration.md)
- [Scheduler soak test](development/scheduler-soak.md)
- [Trino with pandas and Polars](development/trino-integration.md)
- [Install dependencies](getting-started/install-dependencies.md)
- [Use a completed pipeline](guides/train/complete-project.md)
- [Compute management for Spark](integrations/compute/management.md)
- [Pipeline filtering and tagging](pipelines/pipeline-tagging.md)

