# Overview

*Data integration is the process of synchronizing data between two systems.*

<img src="https://www.radicalcompliance.com/wp-content/uploads/2021/02/data-meme.gif" alt="Data integrations" />

## Requirements

> [!WARNING]
> Data integration pipelines need the `mage-integrations` package. The image built from the
> repository's Dockerfile includes it. In a source checkout, add `--extra integrations` to the
> `uv sync` command you installed with.

## Use cases

### Syncing external data → internal systems

If you have data in 3rd party systems (e.g other SaaS tools) and you want that
data available in your database, data warehouse, or data lake, you can use MageML
to sync data between those systems.

### Syncing internal data → external systems (reverse)

Your internal database or data warehouse may contain application data generated
by your users or customers. This data can be valuable if used in conjunction
with 3rd party SaaS tools.

You can use MageML to sync data from your internal systems, and have that data
appear in various 3rd party SaaS tools.

---

## How to build a data integration pipeline

[Documentation here](../guides/data-integration-pipeline.md)

---

## Sources

A source is a system that you want to load data from and synchronize it into
another system. A source can be a 3rd party API, SaaS, database, data warehouse,
or a data lake.

Read more about Sources:

- [Technical documentation](sources/overview.md)
- [Add a new source](../contributing/data-integrations/add-new-source.md)

### Available sources

- [Airtable](sources/airtable.md)
- [Amazon S3](sources/amazon_s3.md)
- [Amplitude](sources/amplitude.md)
- [API](sources/api.md)
- [Azure Blob Storage](sources/azure_blob_storage.md)
- [BigQuery](sources/bigquery.md)
- [Chargebee](sources/chargebee.md)
- [Commercetools](sources/commercetools.md)
- [Couchbase](sources/couchbase.md)
- [Datadog](sources/datadog.md)
- [Doris](sources/doris.md)
- [Dremio](sources/dremio.md)
- [DynamoDB](sources/dynamodb.md)
- [Facebook Ads](sources/facebook_ads.md)
- [Freshdesk](sources/freshdesk.md)
- [Front](sources/front.md)
- [GitHub](sources/github.md)
- [Google Ads](sources/google_ads.md)
- [Google Analytics](sources/google_analytics.md)
- [Google Cloud Storage](sources/google_cloud_storage.md)
- [Google Search Console](sources/google_search_console.md)
- [Google Sheets](sources/google_sheets.md)
- [HubSpot](sources/hubspot.md)
- [Intercom](sources/intercom.md)
- [Knowi](sources/knowi.md)
- [LinkedIn Ads](sources/linkedin_ads.md)
- [Microsoft SQL Server](sources/mssql.md)
- [Mode](sources/mode.md)
- [Monday](sources/monday.md)
- [MongoDB](sources/mongodb.md)
- [MySQL](sources/mysql.md)
- [OracleDB](sources/oracledb.md)
- [Outreach](sources/outreach.md)
- [Paystack](sources/paystack.md)
- [Pipedrive](sources/pipedrive.md)
- [PostgreSQL](sources/postgresql.md)
- [Postmark](sources/postmark.md)
- [PowerBI](sources/powerbi.md)
- [Redshift (Amazon Web Services)](sources/redshift.md)
- [Salesforce](sources/salesforce.md)
- [SFTP](sources/sftp.md)
- [Snowflake](sources/snowflake.md)
- [Stripe](sources/stripe.md)
- [Tableau](sources/tableau.md)
- [Teradata](sources/teradata.md)
- [Twitter Ads](sources/twitter_ads.md)
- [Zendesk](sources/zendesk.md)

MageML follows the data engineering standard for data integrations called the
[Singer Spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

Sources (taps) developed by the open-source community can be adapted to MageML. Here is a
[short list of available ones](https://www.singer.io/#taps).

> [!NOTE]
> Don’t see a source you need?
>
> See [Add a new source](../contributing/data-integrations/add-new-source.md), or open an issue on
> GitHub.
>

[Open an issue on GitHub](https://github.com/jospablo777/mage-ml/issues)

---

## Destinations

A destination is a system that you want to export data into. A destination can
be a 3rd party API, SaaS, database, data warehouse, or a data lake.

Read more about destinations:

- [Technical documentation](destinations/overview.md)
- [Add a new destination](../contributing/data-integrations/add-new-destination.md)

### Available destinations

- [Airtable](destinations/airtable.md)
- [Amazon Web Services (AWS) Redshift](destinations/redshift.md)
- [Amazon Web Services (AWS) S3](destinations/amazon_s3.md)
- [BigQuery](destinations/bigquery.md)
- [Clickhouse](destinations/clickhouse.md)
- [Delta Lake (S3)](destinations/delta_lake_s3.md)
- [Delta Lake (Azure)](destinations/delta_lake_azure.md)
- [Doris](destinations/doris.md)
- [Elasticsearch](destinations/elasticsearch.md)
- [Google Cloud Storage](destinations/google_cloud_storage.md)
- [Kafka](destinations/kafka.md)
- [Microsoft SQL Server](destinations/mssql.md)
- [MongoDB](destinations/mongodb.md)
- [MySQL](destinations/mysql.md)
- [Opensearch](destinations/opensearch.md)
- [OracleDB](destinations/oracledb.md)
- [PostgreSQL](destinations/postgresql.md)
- [Salesforce](destinations/salesforce.md)
- [Snowflake](destinations/snowflake.md)
- [Teradata](destinations/teradata.md)
- [Trino](destinations/trino.md)

MageML follows the data engineering standard for data integrations called the
[Singer Spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

Destinations (targets) developed by the open-source community can be adapted to MageML.
Here is a [short list of available ones](https://www.singer.io/#targets).

> Don’t see a destination you need?
>
> Check out the next section on <b>Add a new destination</b> or tell us in
> [GitHub issues](https://github.com/jospablo777/mage-ml/issues) and we can help you build it.

[Open an issue on GitHub](https://github.com/jospablo777/mage-ml/issues)
