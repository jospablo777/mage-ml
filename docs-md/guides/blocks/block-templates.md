# Block templates

*The templates a new block can start from, by block type, family and language.*

A new block can start from a template: code that already reads from a source, writes to
one, or transforms its input, with the settings it reads named in its comments. The
editor's **All blocks** menu lists them by block type, language and family, such as
**Data loader** > **Python** > **Databases** > **PostgreSQL**.

Each template is a record in `mage_ai/data_preparation/templates/constants.py`: its block
type, language, family, name and the file it starts from. The menus come from the
records, and a test renders every record and checks that its code has the function its
block type runs, so a template in the menu always makes a block that runs.

## Coverage

The sources and transformations each language has a template for. "none yet" marks
where a language has no template for the family: start from the language's base template
there.

### Data loaders

| Family | Python | R | Rust |
| --- | --- | --- | --- |
| APIs | Airtable, Amplitude, API | API | API |
| Cloud storage | Amazon S3, Azure Blob Storage, Google Cloud Storage, IBM Cloud Object Storage | Amazon S3 | none yet |
| Data warehouses | Amazon Redshift, Google BigQuery, Snowflake | none yet | none yet |
| Databases | Druid, DuckDB, IBM Db2, MSSQL, MySQL, OracleDB, Pinot, PostgreSQL | DuckDB, MySQL, PostgreSQL, SQLite | PostgreSQL |
| Databases (NoSQL) | MongoDB | none yet | none yet |
| Delta Lake | Amazon S3, Azure Blob Storage, Google Cloud Storage | none yet | none yet |
| Files | Local file | Local file | Local file |
| Orchestration | Trigger pipeline | none yet | none yet |
| Search | Algolia | none yet | none yet |
| Spreadsheets | Google Sheets | none yet | none yet |
| Vector databases | Chroma, Qdrant, Weaviate | none yet | none yet |
| Other | Base template (generic) | none yet | none yet |

### Transformers

| Family | Python | R | Rust |
| --- | --- | --- | --- |
| Aggregate | none yet | Aggregate | Aggregate |
| Combine | none yet | Join | Join |
| Compute | none yet | none yet | Parallel computation |
| Data cleaning | none yet | Clean data | Clean data |
| Data warehouses | Amazon Redshift, Google BigQuery, Snowflake | none yet | none yet |
| Databases | PostgreSQL | none yet | none yet |
| Feature extraction | none yet | none yet | Model features |
| Machine learning | none yet | Model inference | ONNX model inference |
| Reshape | none yet | Reshape | none yet |
| Other | Base template (generic) | none yet | none yet |

### Data exporters

| Family | Python | R | Rust |
| --- | --- | --- | --- |
| APIs | Amplitude | API | API |
| Cloud storage | Amazon S3, Azure Blob Storage, Google Cloud Storage, IBM Cloud Object Storage | Amazon S3 | none yet |
| Data warehouses | Amazon Redshift, Google BigQuery, Snowflake | none yet | none yet |
| Databases | DuckDB, IBM Db2, MSSQL, MySQL, OracleDB, PostgreSQL | DuckDB, MySQL, PostgreSQL, SQLite | PostgreSQL |
| Databases (NoSQL) | MongoDB | none yet | none yet |
| Delta Lake | Amazon S3, Azure Blob Storage, Google Cloud Storage | none yet | none yet |
| Files | Local file | Local file | Local file |
| Orchestration | Trigger pipeline | none yet | none yet |
| Search | Algolia | none yet | none yet |
| Spreadsheets | Google Sheets | none yet | none yet |
| Vector databases | Chroma, Qdrant, Weaviate | none yet | none yet |
| Other | Base template (generic) | none yet | none yet |

### Sensors

| Family | Python | R | Rust |
| --- | --- | --- | --- |
| Cloud storage | Amazon S3, Google Cloud Storage | none yet | none yet |
| Data warehouses | Amazon Redshift, Google BigQuery, Snowflake | none yet | none yet |
| Databases | MySQL, PostgreSQL | none yet | none yet |
| Other | Base template (generic) | none yet | none yet |

SQL blocks start from the connection you choose: the query is yours. Streaming pipelines
have their own source and sink templates, listed in
[Streaming](../streaming/introduction.md).

## Add a template

1. Add the file under `mage_ai/data_preparation/templates/<block folder>/`: Python files
   in the folder itself, R files in `r/`, Rust files in `rust/`.
2. Add its record to `constants.py` with its family (`groups`). A Python source file
   (`data_loaders/<source>.py`, `data_exporters/<source>.py`, `sensors/<source>.py`) is
   listed by itself; give it a name and family in `PYTHON_SOURCES`.
3. Run `scripts/docs/block_templates.py` to update the table on this page.
