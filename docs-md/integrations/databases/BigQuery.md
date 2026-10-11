# BigQuery

<img src="https://www.vectorlogo.zone/logos/google_bigquery/google_bigquery-ar21.svg" alt="true" height="200" />

## Add credentials

1. Create a service account in your GCP project and download its JSON key.
2. Open `io_config.yaml` at the root of your project and enter one of these under the
   `default` key (or another profile):

```yaml
version: 0.1.1
default:
  # Option 1: path to the service account key file
  GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/path/to/your/service/account/key.json"

  # Option 2: the contents of the key
  # GOOGLE_SERVICE_ACC_KEY:
  #   type: service_account
  #   project_id: project-id
  #   private_key_id: key-id
  #   private_key: "-----BEGIN PRIVATE KEY-----\nyour_private_key\n-----END PRIVATE KEY-----"
  #   client_email: your_service_account_email
  #   auth_uri: "https://accounts.google.com/o/oauth2/auth"
  #   token_uri: "https://accounts.google.com/o/oauth2/token"
  #   auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
  #   client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/your_service_account_email"

  GOOGLE_LOCATION: US  # Optional, BigQuery only
```

If both are set, `GOOGLE_SERVICE_ACC_KEY` is used. The `io_config.yaml` that
`mage init` creates has a placeholder `GOOGLE_SERVICE_ACC_KEY`; delete it when you use the
file path. When the server runs in the container
image, put the key file inside the project folder (such as
`/home/src/default_repo/key.json`) so the path is valid in the container.

## Required permissions

```
bigquery.datasets.create
bigquery.datasets.get
bigquery.jobs.create
bigquery.readsessions.create
bigquery.readsessions.getData
bigquery.tables.create
```

If you're running queries in the existing BigQuery dataset, make sure your account also have "BigQuery Data Editor"
role for the BigQuery dataset.

## Using SQL blocks

1. Create a new pipeline or open an existing pipeline.
2. Add a data loader, transformer, or data exporter block.
3. Select `SQL`.
4. Under the `Connection` dropdown, select `BigQuery`.
5. Under the `Profile` dropdown, select `default` (or the profile you added
   credentials underneath).
6. In the `Database` field, enter the database name you want this block to
   save data to.
7. In the `Schema` field, enter the schema name you want this block
   to save data to.
8. Under the `Write policy` dropdown, select `Replace` or `Append` (please see
   [SQL blocks guide](../../guides/blocks/sql-blocks.md)
   for more information on write policies).
9. Enter in this test query: `SELECT 1`.
10. Run the block.

## Using Python blocks

1. Create a new pipeline or open an existing pipeline.
2. Add a data loader, transformer, or data exporter block (the code snippet
   below is for a data loader).
3. Select `Generic (no template)`.
4. Enter this code snippet (note: change the `config_profile` from `default` if
   you have a different profile):

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_big_query(**kwargs) -> DataFrame:
    query = 'SELECT 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    return BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
```

5. Run the block.

### Export a dataframe

Here is an example code snippet to export a dataframe to BigQuery:

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.
    """
    table_id = 'your-project.your_dataset.your_table_name'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
        overwrite_types=None, # Specify the column types to overwrite in a dictionary
    )
```

To overwrite a column type when running a python export block, simply specify the column name and type in the `overwrite_types` dict in data exporter config.
You can find the supported types in this [doc](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type). Here is the example code:
```python
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
        overwrite_types={'col_name': 'STRING'}, # Specify the column types to overwrite
    )
```

### Method arguments

| Field name              | Description                                                                                                                                            | Example values              |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| if_exists               | Specify resolution policy if table name already exists                                                                                                 | "fail"/"replace"/"append" (default: "replace") |
| overwrite_types         | Overwrite the column types. See [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema.FIELDS.type) | `{'column1': 'STRING', 'column2': 'INTEGER'}` |
| unique_conflict_method  | How to handle the conflict on unique constraints. Use 'UPDATE' for UPSERT (update existing rows, insert new ones) or 'IGNORE' to skip duplicates.    | 'UPDATE' or 'IGNORE' (default: None) |
| unique_constraints      | The unique constraints of the table. Used with unique_conflict_method for UPSERT operations.                                                           | ['col1', 'col2'] (default: None) |

**Example: Using UPSERT (UPDATE or INSERT)**

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to BigQuery with UPSERT support.
    Specify your configuration settings in 'io_config.yaml'.
    """
    table_id = 'your-project.your_dataset.your_table_name'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',  # Use 'append' with UPSERT
        unique_constraints=['user_id', 'email'],  # Columns that form unique constraint
        unique_conflict_method='UPDATE',  # Update existing rows, insert new ones
    )
```
