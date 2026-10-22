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
GROUP_COMBINE = 'Combine'
GROUP_COMPUTE = 'Compute'
GROUP_MACHINE_LEARNING = 'Machine learning'
GROUP_RESHAPE = 'Reshape'
GROUP_SEARCH = 'Search'
GROUP_SPREADSHEETS = 'Spreadsheets'
GROUP_VECTOR_DATABASES = 'Vector databases'

TEMPLATES = [
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load Amplitude events, metrics, cohorts and the tracking plan.',
        groups=[GROUP_APIS],
        language=BlockLanguage.PYTHON,
        name='Amplitude',
        path='data_loaders/amplitude.py',
    ),
    dict(
        block_type=BlockType.DATA_LOADER,
        description='Load a Table from Airtable App.',
        groups=[GROUP_APIS],
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
        groups=[GROUP_DATABASES_NO_SQL],
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
        name='IBM Cloud Object Storage',
        path='data_loaders/ibm_cos.py',
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
        name='IBM Db2',
        path='data_loaders/db2.py',
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
        name='IBM Cloud Object Storage',
        path='data_exporters/ibm_cos.py',
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
        description='Send rows to Amplitude as user properties or events.',
        groups=[GROUP_APIS],
        language=BlockLanguage.PYTHON,
        name='Amplitude',
        path='data_exporters/amplitude.py',
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
        name='IBM Db2',
        path='data_exporters/db2.py',
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


def _python_transformer(name, file_name, description, groups):
    return dict(
        block_type=BlockType.TRANSFORMER,
        description=description,
        groups=groups,
        language=BlockLanguage.PYTHON,
        name=name,
        path=f'transformers/python/{file_name}',
    )


# Python transformers in Polars; each takes a Polars or pandas frame.
PYTHON_TRANSFORMER_TEMPLATES = [
    _python_transformer(
        'Clean data', 'clean.py',
        'snake_case column names, trimmed text, no empty or duplicate rows.',
        [GROUP_DATA_CLEANING],
    ),
    _python_transformer(
        'Deduplicate', 'deduplicate.py', 'Keep the latest row of each key.',
        [GROUP_DATA_CLEANING],
    ),
    _python_transformer(
        'Data quality checks', 'quality.py',
        'Fail with every problem found: missing values, repeated keys, negative amounts.',
        [GROUP_DATA_CLEANING],
    ),
    _python_transformer(
        'Aggregate', 'aggregate.py', 'Group rows and sum the numeric columns of each group.',
        [GROUP_AGGREGATE],
    ),
    _python_transformer(
        'Join', 'join.py', 'Join the outputs of two upstream blocks.', [GROUP_COMBINE],
    ),
    _python_transformer(
        'Reshape', 'reshape.py', 'Turn columns into rows with unpivot.', [GROUP_RESHAPE],
    ),
    _python_transformer(
        'Date features', 'dates.py',
        'Day, ISO week day, month, hour and weekend of a date column.',
        [GROUP_FEATURE_EXTRACTION],
    ),
    _python_transformer(
        'One-hot encoding', 'encode.py', 'A 0 or 1 column per value of a column.',
        [GROUP_FEATURE_EXTRACTION],
    ),
    dict(
        block_type=BlockType.TRANSFORMER,
        description='Score rows with a model file: skops, CatBoost, XGBoost, ONNX, torch.export.',
        groups=[GROUP_MACHINE_LEARNING],
        language=BlockLanguage.PYTHON,
        name='Model inference',
        # In TEMPLATES_ONLY_FOR_V2 before, so the default block menu never listed it.
        path='transformers/model_inference.jinja',
    ),
    _python_transformer(
        'Train a model', 'train.py',
        'A scikit-learn random forest saved for Model inference, with its test metrics.',
        [GROUP_MACHINE_LEARNING],
    ),
]
TEMPLATES += PYTHON_TRANSFORMER_TEMPLATES


def _r_template(block_type, name, file_name, description, groups=None):
    folder = {
        BlockType.DATA_LOADER: 'data_loaders',
        BlockType.DATA_EXPORTER: 'data_exporters',
        BlockType.TRANSFORMER: 'transformers',
        BlockType.CUSTOM: 'custom',
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
        [GROUP_COMBINE],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Reshape', 'reshape.r',
        'Turn columns into rows with pivot_longer.', [GROUP_RESHAPE],
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
    _r_template(
        BlockType.TRANSFORMER, 'Deduplicate', 'deduplicate.r',
        'Keep the latest row of each key.', [GROUP_DATA_CLEANING],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Data quality checks', 'quality.r',
        'Stop with every problem found: missing values, repeated keys, negative amounts.',
        [GROUP_DATA_CLEANING],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Date features', 'dates.r',
        'Day, ISO week day, month, hour and weekend of a date column, with lubridate.',
        [GROUP_FEATURE_EXTRACTION],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'One-hot encoding', 'encode.r',
        'A 0 or 1 column per value of a column.', [GROUP_FEATURE_EXTRACTION],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Train a model', 'train.r',
        'A tidymodels random forest saved for Model inference, with its test metrics.',
        [GROUP_MACHINE_LEARNING],
    ),
    _r_template(
        BlockType.TRANSFORMER, 'Model inference', 'predict.r',
        'Score rows with a model saved with saveRDS (lm, glm, ranger, tidymodels).',
        [GROUP_MACHINE_LEARNING],
    ),
]
TEMPLATES += R_TEMPLATES


def _rust_template(block_type, name, file_name, description, groups=None):
    folder = {
        BlockType.DATA_LOADER: 'data_loaders',
        BlockType.DATA_EXPORTER: 'data_exporters',
        BlockType.TRANSFORMER: 'transformers',
        BlockType.CUSTOM: 'custom',
    }[block_type]
    return dict(
        block_type=block_type,
        description=description,
        groups=groups or [],
        language=BlockLanguage.RUST,
        name=name,
        path=f'{folder}/rust/{file_name}',
    )


RUST_TEMPLATES = [
    _rust_template(
        BlockType.DATA_LOADER, 'Local file', 'file.rs',
        'Read a CSV or Parquet file lazily with Polars.', [GROUP_FILES],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'API', 'api.rs',
        'Load JSON records from an HTTP API into a table.', [GROUP_APIS],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'PostgreSQL', 'postgres.rs',
        'Read a query from PostgreSQL with binary COPY, with exact types.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'Local file', 'file.rs',
        'Write a Parquet (zstd) or CSV file.', [GROUP_FILES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'PostgreSQL', 'postgres.rs',
        'Write a table to PostgreSQL with binary COPY, in one transaction.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'API', 'api.rs',
        'Send rows to an HTTP API as JSON, in batches.', [GROUP_APIS],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Model features', 'features.rs',
        'Lags, changes, running means, z-scores and ranks per entity, in time order.',
        [GROUP_FEATURE_EXTRACTION],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Aggregate', 'aggregate.rs',
        'Totals per group with a Polars lazy query.', [GROUP_AGGREGATE],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Join', 'join.rs',
        'Join the outputs of two upstream blocks.', [GROUP_COMBINE],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Clean data', 'clean.rs',
        'Remove duplicates and missing ids, trim text, fill missing numbers.',
        [GROUP_DATA_CLEANING],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Parallel computation', 'parallel.rs',
        'Compute a value per row on every core with Rayon.', [GROUP_COMPUTE],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'Cloud storage', 'cloud_storage.rs',
        'Read Parquet or CSV files lazily from S3, Google Cloud Storage or Azure.',
        [GROUP_CLOUD_STORAGE],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'Cloud storage', 'cloud_storage.rs',
        'Write a Parquet or CSV file to S3, Google Cloud Storage or Azure.',
        [GROUP_CLOUD_STORAGE],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'DuckDB', 'duckdb.rs',
        'Query DuckDB, built into the block, with the result\'s types.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'DuckDB', 'duckdb.rs',
        'Write a table to DuckDB: replace, append or fail.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'MySQL', 'mysql.rs',
        'Query MySQL or MariaDB into typed columns, exact decimals.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'MySQL', 'mysql.rs',
        'Write a table to MySQL or MariaDB in batched prepared inserts.', [GROUP_DATABASES],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'ClickHouse', 'clickhouse.rs',
        'Query ClickHouse, rows arriving as Arrow.', [GROUP_DATA_WAREHOUSES],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'ClickHouse', 'clickhouse.rs',
        'Insert a table into ClickHouse as one Arrow insert.', [GROUP_DATA_WAREHOUSES],
    ),
    _rust_template(
        BlockType.DATA_LOADER, 'MongoDB', 'mongodb.rs',
        'Read a collection\'s documents, one column per field.', [GROUP_DATABASES_NO_SQL],
    ),
    _rust_template(
        BlockType.DATA_EXPORTER, 'MongoDB', 'mongodb.rs',
        'Write rows as documents to a collection: replace or append.',
        [GROUP_DATABASES_NO_SQL],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Deduplicate', 'deduplicate.rs',
        'Keep the latest row of each key.', [GROUP_DATA_CLEANING],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Data quality checks', 'quality.rs',
        'Fail with every problem found: missing values, repeated keys, negative amounts.',
        [GROUP_DATA_CLEANING],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Unpivot', 'reshape.rs',
        'Turn columns into rows: a variable and value pair per column.', [GROUP_RESHAPE],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'Cast types', 'cast.rs',
        'Cast columns to the types a variable names; a value that does not convert fails.',
        [GROUP_FORMATTING],
    ),
    _rust_template(
        BlockType.TRANSFORMER, 'ONNX model inference', 'onnx.rs',
        'Score rows with an ONNX model (needs the onnx feature in rust/Cargo.toml).',
        [GROUP_MACHINE_LEARNING],
    ),
]
TEMPLATES += RUST_TEMPLATES

# Each Python source a data loader, data exporter or sensor template exists for, in
# <folder>/<source>.py: its name in the menus and its family. A template file without an
# entry fails the catalog test (templates/catalog.py), so every file has a menu item.
PYTHON_SOURCES = {
    'airtable': ('Airtable', GROUP_APIS),
    'algolia': ('Algolia', GROUP_SEARCH),
    'amplitude': ('Amplitude', GROUP_APIS),
    'api': ('API', GROUP_APIS),
    'azure_blob_storage': ('Azure Blob Storage', GROUP_CLOUD_STORAGE),
    'bigquery': ('Google BigQuery', GROUP_DATA_WAREHOUSES),
    'chroma': ('Chroma', GROUP_VECTOR_DATABASES),
    'db2': ('IBM Db2', GROUP_DATABASES),
    'druid': ('Druid', GROUP_DATABASES),
    'duckdb': ('DuckDB', GROUP_DATABASES),
    'file': ('Local file', GROUP_FILES),
    'google_cloud_storage': ('Google Cloud Storage', GROUP_CLOUD_STORAGE),
    'google_sheets': ('Google Sheets', GROUP_SPREADSHEETS),
    'ibm_cos': ('IBM Cloud Object Storage', GROUP_CLOUD_STORAGE),
    'mongodb': ('MongoDB', GROUP_DATABASES_NO_SQL),
    'mssql': ('Microsoft SQL Server', GROUP_DATABASES),
    'mysql': ('MySQL', GROUP_DATABASES),
    'oracledb': ('OracleDB', GROUP_DATABASES),
    'pinot': ('Pinot', GROUP_DATABASES),
    'postgres': ('PostgreSQL', GROUP_DATABASES),
    'qdrant': ('Qdrant', GROUP_VECTOR_DATABASES),
    'redshift': ('Amazon Redshift', GROUP_DATA_WAREHOUSES),
    's3': ('Amazon S3', GROUP_CLOUD_STORAGE),
    'snowflake': ('Snowflake', GROUP_DATA_WAREHOUSES),
    'weaviate': ('Weaviate', GROUP_VECTOR_DATABASES),
}
# Python transformers that run their SQL in a database or warehouse.
PYTHON_WAREHOUSE_TRANSFORMERS = ('bigquery', 'postgres', 'redshift', 'snowflake')
_SOURCE_FOLDERS = {
    BlockType.DATA_LOADER: ('data_loaders', 'Load data from {}.'),
    BlockType.DATA_EXPORTER: ('data_exporters', 'Export data to {}.'),
    BlockType.SENSOR: ('sensors', 'Wait until {} has what the pipeline needs.'),
}
BASE_TEMPLATE_NAME = 'Base template (generic)'


def _python_source_templates() -> list:
    """
    A record per Python source template file: a block created from it has the source as
    its data_source, as the menus always made them. Files with a record above are left out.
    """
    import os

    folder_of_templates = os.path.dirname(__file__)
    listed = {t['path'] for t in TEMPLATES}
    records = [dict(
        block_type=BlockType.TRANSFORMER,
        description='A block with an empty function to fill in.',
        language=BlockLanguage.PYTHON,
        name=BASE_TEMPLATE_NAME,
        path='transformers/default.jinja',
    )]
    for block_type, (folder, description) in _SOURCE_FOLDERS.items():
        directory = os.path.join(folder_of_templates, folder)
        records.append(dict(
            block_type=block_type,
            description='A block with an empty function to fill in.',
            language=BlockLanguage.PYTHON,
            name=BASE_TEMPLATE_NAME,
            path=f'{folder}/default.{"py" if block_type == BlockType.SENSOR else "jinja"}',
        ))
        for name in sorted(os.listdir(directory)):
            source, extension = os.path.splitext(name)
            path = f'{folder}/{name}'
            if extension != '.py' or source == 'default' or path in listed:
                continue
            title, family = PYTHON_SOURCES.get(source, (source, None))
            records.append(dict(
                block_type=block_type,
                data_source=source,
                description=description.format(title),
                groups=[family] if family else [],
                language=BlockLanguage.PYTHON,
                name=title,
                path=path,
            ))
    for source in PYTHON_WAREHOUSE_TRANSFORMERS:
        title, family = PYTHON_SOURCES[source]
        records.append(dict(
            block_type=BlockType.TRANSFORMER,
            data_source=source,
            description=f'Transform data with SQL that runs in {title}.',
            groups=[family],
            language=BlockLanguage.PYTHON,
            name=title,
            path='transformers/data_warehouse_transformer.jinja',
        ))
    return records


def _sql_template(block_type, name, file_name, description, groups):
    folder = {
        BlockType.DATA_LOADER: 'data_loaders',
        BlockType.TRANSFORMER: 'transformers',
    }[block_type]
    return dict(
        block_type=block_type,
        description=description,
        groups=groups,
        language=BlockLanguage.SQL,
        name=name,
        path=f'{folder}/sql/{file_name}',
    )


# The templates of SQL blocks, which run with the connection the block chooses.
SQL_TEMPLATES = [
    _sql_template(
        BlockType.DATA_LOADER, 'Incremental load', 'incremental.sql',
        'The rows changed since the run\'s execution date.', [GROUP_DATABASES],
    ),
    _sql_template(
        BlockType.DATA_LOADER, 'Files with DuckDB', 'files.sql',
        'Parquet, CSV or JSON files, local or on S3, read by DuckDB.', [GROUP_FILES],
    ),
    _sql_template(
        BlockType.TRANSFORMER, 'Deduplicate', 'deduplicate.sql',
        'Keep the latest row of each key.', [GROUP_DATA_CLEANING],
    ),
    _sql_template(
        BlockType.TRANSFORMER, 'Data quality report', 'quality.sql',
        'Counts of missing ids, repeated ids and negative amounts.', [GROUP_DATA_CLEANING],
    ),
    _sql_template(
        BlockType.TRANSFORMER, 'Date spine', 'date_spine.sql',
        'A row per day with zeros on days without rows.', [GROUP_AGGREGATE],
    ),
    _sql_template(
        BlockType.TRANSFORMER, 'Running total', 'running_total.sql',
        'A running sum per key in time order, with a window function.',
        [GROUP_FEATURE_EXTRACTION],
    ),
]
TEMPLATES += SQL_TEMPLATES

PYTHON_SOURCE_TEMPLATES = _python_source_templates()
TEMPLATES += PYTHON_SOURCE_TEMPLATES
# The records above hold the Python sources and base templates the newer menus listed
# here too; one record per template.
_GENERATED = {(t['block_type'], t['path'], t['name']) for t in PYTHON_SOURCE_TEMPLATES}
_GENERATED_PATHS = {(t['block_type'], t['path']) for t in PYTHON_SOURCE_TEMPLATES
                    if t['path'] != 'transformers/data_warehouse_transformer.jinja'}
TEMPLATES_ONLY_FOR_V2 = [
    t for t in TEMPLATES_ONLY_FOR_V2
    if (t['block_type'], t.get('path')) not in _GENERATED_PATHS
    and (t['block_type'], t.get('path'), t['name']) not in _GENERATED
    and not (t.get('path') == 'transformers/data_warehouse_transformer.jinja')
]

# By name, which R and Rust templates share with Python ones; they are left out.
TEMPLATES_BY_UUID = index_by(
    lambda x: x['name'],
    [
        t for t in TEMPLATES + TEMPLATES_ONLY_FOR_V2
        if t['language'] not in (BlockLanguage.R, BlockLanguage.RUST, BlockLanguage.SQL)
    ],
)
