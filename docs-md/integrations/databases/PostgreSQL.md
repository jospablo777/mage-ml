# PostgreSQL

![](https://user-images.githubusercontent.com/78053898/198754399-3b594af0-cf84-41d3-8faf-e1f6e4c5182d.png)

## Add credentials

1. Create a new pipeline or open an existing pipeline.
2. Expand the left side of your screen to view the file browser.
3. Scroll down and click on a file named `io_config.yaml`.
4. Enter the following keys and values under the key named `default` (you can
   have multiple profiles, add it under whichever is relevant to you)

```yaml
version: 0.1.1
default:
  POSTGRES_DBNAME: ...
  POSTGRES_HOST: ...
  POSTGRES_PASSWORD: ...
  POSTGRES_PORT: ...
  POSTGRES_USER: ...
  POSTGRES_CONNECTION_METHOD: direct
  POSTGRES_SSH_HOST:
  POSTGRES_SSH_PORT: 22
  POSTGRES_SSH_USERNAME:
  POSTGRES_SSH_PASSWORD:
  POSTGRES_SSH_PKEY:
```

### SSL

`io_config.yaml` has no SSL keys for PostgreSQL. psycopg2 uses libpq, which reads
`PGSSLMODE` (such as `require` or `verify-full`), `PGSSLROOTCERT`, `PGSSLCERT` and
`PGSSLKEY` from the server's environment.

### SSH tunneling
If you want to connect to Postgres cluster with ssh tunnel, update the value of `POSTGRES_CONNECTION_METHOD` to `ssh_tunnel`, and enter the values for keys with prefix `POSTGRES_SSH`.

| Key | Description | Sample value
| --- | --- | --- |
| `POSTGRES_SSH_HOST` | The host of the intermediate bastion server. | `123.45.67.89` |
| `POSTGRES_SSH_PORT` | The port of the intermediate bastion server. Default value: 22 | `22` |
| `POSTGRES_SSH_USERNAME` | The username used to connect to the bastion server. | `username` |
| `POSTGRES_SSH_PASSWORD` | (Optional) The password used to connect to the bastion server. It should be set if you authenticate with the bastion server with password. | `password` |
| `POSTGRES_SSH_PKEY` | (Optional) The path to the private key used to connect to the bastion server. It should be set if you authenticate with the bastion server with private key. | `/path/to/private/key` |

<br />

## Using SQL block

1. Create a new pipeline or open an existing pipeline.
2. Add a data loader, transformer, or data exporter block.
3. Select `SQL`.
4. Under the `Connection` dropdown, select `PostgreSQL`.
5. Under the `Profile` dropdown, select `default` (or the profile you added
   credentials underneath).
6. In the `Schema` field, enter the schema name you want this block
   to save data to.
7. Under the `Write policy` dropdown, select `Replace` or `Append` (please see
   [SQL blocks guide](../../guides/blocks/sql-blocks.md) for more
   information on write policies).
8. Enter in this test query: `SELECT 1`.
9. Run the block.

<br />

## Using Python block

1. Create a new pipeline or open an existing pipeline.
2. Add a data loader, transformer, or data exporter block (the code snippet
   below is for a data loader).
3. Select `Generic (no template)`.
4. Enter this code snippet (note: change the `config_profile` from `default` if
   you have a different profile):

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_postgres(**kwargs) -> DataFrame:
    query = 'SELECT 1'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)
```

5. Run the block.

`loader.load(query, polars=True)` returns a Polars DataFrame with exact column types. It
reads the rows in PostgreSQL's binary `COPY` format on a connection of MageML's Rust client,
kept for the next loads: on 1 million rows of seven columns, 0.9 seconds instead of 2.0.
The column types and values are those of the psycopg2 load; columns MageML builds in Python
(`numeric` without precision, `interval`, `timetz`, some arrays) are built the same way. The
load goes through psycopg2 when the query has bound parameters, when the client's own
connection has a transaction open (the other connection would not see its changes), and when
the connection uses a setting the Rust client does not have, such as client certificates.
`MAGE_POSTGRES_BINARY_LOAD=off` turns it off.

### Export a dataframe

Here is an example code snippet to export a dataframe to Postgres:

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'your_schema_name'  # Specify the name of the schema to export data to
    table_name = 'your_table_name'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
            drop_table_on_replace=False,   # Whether to drop the table when "if_exists" param is set to "replace"
            allow_reserved_words=True,
            unique_conflict_method='UPDATE',
            unique_constraints=['col'],
        )

```

<br />

6. Custom types

To overwrite a column type when running a python export block, simply specify the column name and type in the `overwrite_types` dict in data exporter config

Here is an example code snippet:

```python
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'your_schema_name'  # Specify the name of the schema to export data to
    table_name = 'your_table_name'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    overwrite_types = {'column_name': 'VARCHAR(255)'}

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
            allow_reserved_words=True,
            unique_conflict_method='UPDATE',
            unique_constraints=['col'],
            overwrite_types=overwrite_types,
        )

```

### Method arguments

| Field name              | Description                                                                                                                                            | Example values              |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| allow_reserved_words    | Whether to allow using reserved words as column names.                                                                                                 | True/False (default: False) |
| auto_clean_name         | Whether to automatically clean the column name (replace the empty space with underscore, avoid using number as the prefix of the column name)          | True/False (default: True)  |
| case_sensitive          | Whether to support case sensitive columns                                                                                                              | True/False (default: False) |
| cascade_on_drop         | Whether to cascade the table drop.                                                                                                                     | True/False (default: False) |
| drop_table_on_replace   | Whether to drop the table when "if_exists" param is set to "replace".                                                                                  | True/False (default: False) |
| if_exists               | Specify resolution policy if table name already exists                                                                                                 | "fail"/"replace"/"append" (default: "replace") |
| overwrite_types         | Overwrite the column types                                                                                                                             | `{'column1': 'INTEGER', 'column2': 'VARCHAR'}` |
| unique_conflict_method  | How to handle the conflict on unique constraints.                                                                                                      | 'UPDATE' or 'IGNORE' (default: None) |
| unique_constraints      | The unique constraints of the table.                                                                                                                   | ['col1', 'col2'] (default: None) |

Rows are written with `COPY`. When `unique_conflict_method` and `unique_constraints` are
both set, rows go through `INSERT ... ON CONFLICT` instead.

`COPY` sends the rows in PostgreSQL's binary format, encoded in Rust, when every column of
the frame encodes to the type its table column has: the types MageML chooses for a new
table, and for an existing table the same types, any `numeric` for decimals and
`varchar(n)` for text. Otherwise, for example for JSON or a naive timestamp going into a
`timestamptz` column, the rows go as `COPY` text. Both write the same values, in the same
transaction. On 1 million rows of seven columns an export took about 1 second in binary and
7 seconds as text. `MAGE_POSTGRES_BINARY_COPY=off` sends text always.
