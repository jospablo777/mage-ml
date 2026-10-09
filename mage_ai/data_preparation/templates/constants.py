from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.io.base import DataSource
from mage_ai.shared.hash import index_by

GROUP_AGGREGATE = 'Aggregate'
GROUP_APIS = 'APIs'
GROUP_CLOUD_STORAGE = 'Cloud storage'
GROUP_COLUMN_ACTIONS = 'Column actions'
GROUP_COLUMN_REMOVAL = 'Column removal'
GROUP_DATABASES = 'Databases'
GROUP_DATABASES_NO_SQL = 'Databases (NoSQL)'
GROUP_DATA_CLEANING = 'Data cleaning'
GROUP_DATA_LAKES = 'Data lakes'
GROUP_DATA_WAREHOUSES = 'Data warehouses'
GROUP_DELTA_LAKE = 'Delta Lake'
GROUP_FEATURE_EXTRACTION = 'Feature extraction'
GROUP_FEATURE_SCALING = 'Feature scaling'
GROUP_FILES = 'Files'
GROUP_FORMATTING = 'Formatting'
GROUP_ORCHESTRATION = 'Orchestration'
GROUP_ROW_ACTIONS = 'Row actions'
GROUP_SHIFT = 'Shift'

TEMPLATES = [
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load a Table from Airtable App.',
        language=BlockLanguage.PYTHON,
        name='Airtable',
        path='data_loaders/airtable.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load a Delta Table from Amazon S3.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Amazon S3',
        path='data_loaders/deltalake/s3.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load a Delta Table from Azure Blob Storage.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Azure Blob Storage',
        path='data_loaders/deltalake/azure_blob_storage.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load a Delta Table from Google Cloud Storage.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Google Cloud Storage',
        path='data_loaders/deltalake/gcs.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load data from MongoDB.',
        groups=[GROUP_DATABASES_NO_SQL],
        language=BlockLanguage.PYTHON,
        name='MongoDB',
        path='data_loaders/mongodb.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load data from MSSQL.',
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='MSSQL',
        path='data_loaders/mssql.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Export data to a Delta Table in Amazon S3.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Amazon S3',
        path='data_exporters/deltalake/s3.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Export data to a Delta Table in Azure Blob Storage.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Azure Blob Storage',
        path='data_exporters/deltalake/azure_blob_storage.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Export data to a Delta Table in Google Cloud Storage.',
        groups=[GROUP_DELTA_LAKE],
        language=BlockLanguage.PYTHON,
        name='Google Cloud Storage',
        path='data_exporters/deltalake/gcs.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Export data to MongoDB.',
        language=BlockLanguage.PYTHON,
        name='MongoDB',
        path='data_exporters/mongodb.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Export data to MSSQL.',
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='MSSQL',
        path='data_exporters/mssql.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Trigger another pipeline to run.',
        groups=[GROUP_ORCHESTRATION],
        language=BlockLanguage.PYTHON,
        name='Trigger pipeline',
        path='data_loaders/orchestration/triggers/default.jinja',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        description='Trigger another pipeline to run.',
        groups=[GROUP_ORCHESTRATION],
        language=BlockLanguage.PYTHON,
        name='Trigger pipeline',
        path='data_exporters/orchestration/triggers/default.jinja',
    ),
    dict(
        block_type=BlockType.CALLBACK,
        description='Base template with empty functions.',
        language=BlockLanguage.PYTHON,
        name='Base template',
        path='callbacks/base.jinja',
    ),
    dict(
        block_type=BlockType.CALLBACK,
        description='Trigger another pipeline to run.',
        groups=[GROUP_ORCHESTRATION],
        language=BlockLanguage.PYTHON,
        name='Trigger pipeline',
        path='callbacks/orchestration/triggers/default.jinja',
    ),
    dict(
        block_type=BlockType.CONDITIONAL,
        description='Base template with empty functions.',
        language=BlockLanguage.PYTHON,
        name='Base template',
        path='conditionals/base.jinja',
    ),
]

TEMPLATES_ONLY_FOR_V2 = [
    # Data loaders
    dict(
        block_type=BlockType.DATA_LOADER,
        language=BlockLanguage.PYTHON,
        name='Base template (generic)',
        path='data_loaders/default.jinja',
    ),
    #   Data lakes
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Amazon S3',
        path='data_loaders/s3.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Azure Blob Storage',
        path='data_loaders/azure_blob_storage.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Google Cloud Storage',
        path='data_loaders/google_cloud_storage.py',
    ),
    #   Data warehouses
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Amazon Redshift',
        path='data_loaders/redshift.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load data from Google BigQuery.',
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Google BigQuery',
        path='data_loaders/bigquery.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Snowflake',
        path='data_loaders/snowflake.py',
    ),
    #   Databases
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Algolia',
        path='data_loaders/algolia.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Chroma',
        path='data_loaders/chroma.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='DuckDB',
        path='data_loaders/duckdb.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='MySQL',
        path='data_loaders/mysql.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Oracle DB',
        path='data_loaders/oracledb.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='PostgreSQL',
        path='data_loaders/postgres.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Qdrant',
        path='data_loaders/qdrant.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Weaviate',
        path='data_loaders/weaviate.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Fetch data from an API request.',
        language=BlockLanguage.PYTHON,
        name='API',
        path='data_loaders/api.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load data from a file on your machine.',
        language=BlockLanguage.PYTHON,
        name='Local file',
        path='data_loaders/file.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load data from a worksheet in Google Sheets.',
        language=BlockLanguage.PYTHON,
        name='Google Sheets',
        path='data_loaders/google_sheets.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        language=BlockLanguage.PYTHON,
        name='Druid',
        path='data_loaders/druid.py',
    ),
    # Transformers
    dict(
        block_type=BlockType.TRANSFORMER,
        language=BlockLanguage.PYTHON,
        name='Base template (generic)',
        path='transformers/default.jinja',
    ),
    #   Data warehouses
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Amazon Redshift',
        path='transformers/data_warehouse_transformer.jinja',
        template_variables=dict(
            additional_args='\n        loader.commit() # Permanently apply database changes',
            data_source=DataSource.REDSHIFT.value,
            data_source_handler='Redshift',
        ),
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Google BigQuery',
        path='transformers/data_warehouse_transformer.jinja',
        template_variables=dict(
            additional_args='',
            data_source=DataSource.BIGQUERY.value,
            data_source_handler='BigQuery',
        ),
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Snowflake',
        path='transformers/data_warehouse_transformer.jinja',
        template_variables=dict(
            additional_args='\n        loader.commit() # Permanently apply database changes',
            data_source=DataSource.SNOWFLAKE.value,
            data_source_handler='Snowflake',
        ),
    ),
    #   Databases
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='PostgreSQL',
        path='transformers/data_warehouse_transformer.jinja',
        template_variables=dict(
            additional_args='\n        loader.commit() # Permanently apply database changes',
            data_source=DataSource.POSTGRES.value,
            data_source_handler='Postgres',
        ),
    ),
    #   Row actions
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_ROW_ACTIONS],
        language=BlockLanguage.PYTHON,
        name='Drop duplicate rows',
        path='transformers/transformer_actions/row/drop_duplicate.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_ROW_ACTIONS],
        language=BlockLanguage.PYTHON,
        name='Filter rows',
        path='transformers/transformer_actions/row/filter.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_ROW_ACTIONS],
        language=BlockLanguage.PYTHON,
        name='Remove rows',
        path='transformers/transformer_actions/row/remove.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_ROW_ACTIONS],
        language=BlockLanguage.PYTHON,
        name='Sort rows',
        path='transformers/transformer_actions/row/sort.py',
    ),
    #   Column actions
    #       Aggregate
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Average value of column',
        path='transformers/transformer_actions/column/average.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Count unique values in column',
        path='transformers/transformer_actions/column/count_distinct.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='First value in column',
        path='transformers/transformer_actions/column/first.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Last value in column',
        path='transformers/transformer_actions/column/last.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Maximum value in column',
        path='transformers/transformer_actions/column/max.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Median value in column',
        path='transformers/transformer_actions/column/median.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Min value in column',
        path='transformers/transformer_actions/column/min.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Sum of all values in column',
        path='transformers/transformer_actions/column/sum.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_AGGREGATE],
        language=BlockLanguage.PYTHON,
        name='Total count of values in column',
        path='transformers/transformer_actions/column/count.py',
    ),
    #       Formatting
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FORMATTING],
        language=BlockLanguage.PYTHON,
        name='Clean column name',
        path='transformers/transformer_actions/column/clean_column_name.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FORMATTING],
        language=BlockLanguage.PYTHON,
        name='Fix syntax errors',
        path='transformers/transformer_actions/column/fix_syntax_errors.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FORMATTING],
        language=BlockLanguage.PYTHON,
        name='Reformat values in column',
        path='transformers/transformer_actions/column/reformat.py',
    ),
    #       Column removal
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_COLUMN_REMOVAL],
        language=BlockLanguage.PYTHON,
        name='Keep column(s)',
        path='transformers/transformer_actions/column/select.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_COLUMN_REMOVAL],
        language=BlockLanguage.PYTHON,
        name='Remove column(s)',
        path='transformers/transformer_actions/column/remove.py',
    ),
    #       Shift
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_SHIFT],
        language=BlockLanguage.PYTHON,
        name='Shift row values down',
        path='transformers/transformer_actions/column/shift_down.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_SHIFT],
        language=BlockLanguage.PYTHON,
        name='Shift row values up',
        path='transformers/transformer_actions/column/shift_up.py',
    ),
    #       Feature scaling
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FEATURE_SCALING],
        language=BlockLanguage.PYTHON,
        name='Normalize data',
        path='transformers/transformer_actions/column/normalize.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FEATURE_SCALING],
        language=BlockLanguage.PYTHON,
        name='Standardize data',
        path='transformers/transformer_actions/column/standardize.py',
    ),
    #       Data cleaning
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_DATA_CLEANING],
        language=BlockLanguage.PYTHON,
        name='Fill in missing values',
        path='transformers/transformer_actions/column/impute.py',
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_DATA_CLEANING],
        language=BlockLanguage.PYTHON,
        name='Remove outliers',
        path='transformers/transformer_actions/column/remove_outliers.py',
    ),
    #       Feature extraction
    dict(
        block_type=BlockType.TRANSFORMER,
        groups=[GROUP_COLUMN_ACTIONS, GROUP_FEATURE_EXTRACTION],
        language=BlockLanguage.PYTHON,
        name='Calculate difference between values',
        path='transformers/transformer_actions/column/diff.py',
    ),
    # Data exporters
    dict(
        block_type=BlockType.DATA_EXPORTER,
        language=BlockLanguage.PYTHON,
        name='Base template (generic)',
        path='data_exporters/default.jinja',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        language=BlockLanguage.PYTHON,
        name='Local file',
        path='data_exporters/file.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        language=BlockLanguage.PYTHON,
        name='Google Sheets',
        path='data_exporters/google_sheets.py',
    ),
    #   Data lakes
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Amazon S3',
        path='data_exporters/s3.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Azure Blob Storage',
        path='data_exporters/azure_blob_storage.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Google Cloud Storage',
        path='data_exporters/google_cloud_storage.py',
    ),
    #   Data warehouses
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Amazon Redshift',
        path='data_exporters/redshift.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Google BigQuery',
        path='data_exporters/bigquery.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Snowflake',
        path='data_exporters/snowflake.py',
    ),
    #   Databases
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Algolia',
        path='data_exporters/algolia.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Chroma',
        path='data_exporters/chroma.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='DuckDB',
        path='data_exporters/duckdb.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='MySQL',
        path='data_exporters/mysql.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='OracleDB',
        path='data_exporters/oracledb.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='PostgreSQL',
        path='data_exporters/postgres.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Qdrant',
        path='data_exporters/qdrant.py',
    ),
    dict(
        block_type=BlockType.DATA_EXPORTER,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='Weaviate',
        path='data_exporters/weaviate.py',
    ),
    # Sensors
    dict(
        block_type=BlockType.SENSOR,
        language=BlockLanguage.PYTHON,
        name='Base template (generic)',
        path='sensors/default.py',
    ),
    #   Data lakes
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Amazon S3',
        path='sensors/s3.py',
    ),
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATA_LAKES],
        language=BlockLanguage.PYTHON,
        name='Google Cloud Storage',
        path='sensors/google_cloud_storage.py',
    ),
    #   Data warehouses
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Amazon Redshift',
        path='sensors/redshift.py',
    ),
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Google BigQuery',
        path='sensors/bigquery.py',
    ),
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATA_WAREHOUSES],
        language=BlockLanguage.PYTHON,
        name='Snowflake',
        path='sensors/snowflake.py',
    ),
    #   Databases
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='MySQL',
        path='sensors/mysql.py',
    ),
    dict(
        block_type=BlockType.SENSOR,
        groups=[GROUP_DATABASES],
        language=BlockLanguage.PYTHON,
        name='PostgreSQL',
        path='sensors/postgres.py',
    ),
]


def _r_template(block_type, name, file_name, description, groups=None):
    folder = {
        BlockType.DATA_LOADER: 'data_loaders',
        BlockType.DATA_EXPORTER: 'data_exporters',
        BlockType.TRANSFORMER: 'transformers',
    }[block_type]
    return dict(
        block_type=block_type,
        description=description,
        groups=groups or [],
        language=BlockLanguage.R,
        name=name,
        path=f'{folder}/r/{file_name}',
    )


# The templates of R blocks, which the block menus list under R.
R_TEMPLATES = [
    _r_template(
        BlockType.DATA_LOADER, 'Local file', 'file.r',
        'Read a CSV, TSV, Parquet, Feather, JSON, NDJSON or RDS file.', [GROUP_FILES],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'Amazon S3', 's3.r',
        'Read a data file from S3 or S3-compatible storage.', [GROUP_CLOUD_STORAGE],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'API', 'api.r', 'Request JSON from an API with httr2.',
        [GROUP_APIS],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'PostgreSQL', 'postgres.r',
        'Query PostgreSQL with DBI and RPostgres.', [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'MySQL', 'mysql.r', 'Query MySQL with DBI and RMariaDB.',
        [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'DuckDB', 'duckdb.r', 'Query DuckDB with DBI and duckdb.',
        [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_LOADER, 'SQLite', 'sqlite.r',
        'Query a SQLite database file with DBI and RSQLite.', [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Clean data', 'clean.r',
        'snake_case column names, trimmed text, no empty or duplicate rows.',
        [GROUP_DATA_CLEANING],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Aggregate', 'aggregate.r',
        'Group rows and sum the numeric columns of each group.', [GROUP_AGGREGATE],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Join', 'join.r', 'Join the outputs of two upstream blocks.',
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Reshape', 'reshape.r',
        'Turn columns into rows with pivot_longer.',
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'Local file', 'file.r',
        'Write a CSV, TSV, Parquet, Feather, JSON, NDJSON or RDS file.', [GROUP_FILES],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'Amazon S3', 's3.r',
        'Write a data file to S3 or S3-compatible storage.', [GROUP_CLOUD_STORAGE],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'API', 'api.r',
        'Send rows to an API as JSON with httr2.', [GROUP_APIS],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'PostgreSQL', 'postgres.r',
        'Write a table to PostgreSQL with DBI and RPostgres.', [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'MySQL', 'mysql.r',
        'Write a table to MySQL with DBI and RMariaDB.', [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'DuckDB', 'duckdb.r',
        'Write a table to DuckDB with DBI and duckdb.', [GROUP_DATABASES],
    ),
    _r_template(
        BlockType.DATA_EXPORTER, 'SQLite', 'sqlite.r',
        'Write a table to a SQLite database file with DBI and RSQLite.', [GROUP_DATABASES],
    ),
]
TEMPLATES += R_TEMPLATES

# By name, which R templates share with Python ones; R templates are left out.
TEMPLATES_BY_UUID = index_by(
    lambda x: x['name'],
    [t for t in TEMPLATES + TEMPLATES_ONLY_FOR_V2 if t['language'] != BlockLanguage.R],
)
