"""
The R block templates of the block menus, run as a user starts from them: only their
placeholders, such as your_table, are replaced. Files, S3 (MinIO), the test API,
PostgreSQL, MySQL, DuckDB and SQLite.
"""
import datetime as dt
import sqlite3
import subprocess
from pathlib import Path

import pandas as pd
import pytest

from mage_ai.data_preparation.models.block.r import execute_r_code
from mage_ai.data_preparation.models.constants import BlockType

LOADER, TRANSFORMER, EXPORTER = BlockType.DATA_LOADER, BlockType.TRANSFORMER, \
    BlockType.DATA_EXPORTER
VARIABLES = {'execution_date': '2024-01-01'}


def template(block_type, name, **replacements):
    """A template of the catalog, rendered as for a new block, with replacements."""
    from mage_ai.data_preparation.templates.constants import R_TEMPLATES
    from mage_ai.data_preparation.templates.template import fetch_template_source

    entry = next(
        t for t in R_TEMPLATES if t['block_type'] == block_type and t['name'] == name
    )
    code = fetch_template_source(block_type, {'template_path': entry['path']}, language='r')
    for old, new in replacements.items():
        assert old in code, (name, old)
        code = code.replace(old, new)
    return code


def run(repo, block_type, code, *inputs):
    result = execute_r_code(
        block_type, code, input_vars=list(inputs), global_vars=dict(VARIABLES),
        repo_path=repo, block_uuid='template',
    )
    assert all(t['passed'] for t in result.tests), result.tests
    return result.outputs[0] if result.outputs else None


def frame():
    return pd.DataFrame({
        'id': pd.array([1, 2, 3], dtype='Int64'),
        'name': pd.Series(['ñ "quoted"', "it's", None], dtype='str'),
        'amount': [1234.56789012, -0.5, None],
        'day': [dt.date(2024, 1, 31), dt.date(1900, 1, 1), None],
    })


def test_every_template_parses_and_follows_the_style(r_library, tmp_path):
    from mage_ai.data_preparation.templates.constants import R_TEMPLATES

    for entry in R_TEMPLATES:
        path = tmp_path / entry['path'].replace('/', '_')
        path.write_text(template(entry['block_type'], entry['name']))
    # object_usage_linter would flag mageml's functions, which R blocks attach.
    script = f'''
        .libPaths("{r_library}")
        files <- list.files("{tmp_path}", full.names = TRUE)
        for (file in files) parse(file)
        linters <- lintr::linters_with_defaults(object_usage_linter = NULL)
        lints <- unlist(lapply(files, lintr::lint, linters = linters), recursive = FALSE)
        for (lint in lints) cat(format(lint), "\\n")
        quit(status = length(lints) > 0)
    '''
    result = subprocess.run(['Rscript', '--vanilla', '-e', script],
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('extension', ['csv', 'parquet', 'json'])
def test_local_file_templates(mage_project, extension):
    path = f'data/r_template.{extension}'
    run(mage_project, EXPORTER, template(
        EXPORTER, 'Local file', **{'output/result.parquet': path},
    ), frame())

    loaded = run(mage_project, LOADER, template(LOADER, 'Local file', **{
        'data/input.csv': path,
    }))

    assert (Path(mage_project) / path).exists()
    assert loaded['id'].tolist() == [1, 2, 3]
    assert loaded['name'].tolist()[:2] == ['ñ "quoted"', "it's"]
    assert loaded['amount'].tolist()[0] == pytest.approx(1234.56789012, abs=1e-9)


def test_s3_templates(mage_project, s3, bucket):
    uri = f's3://{bucket}/r/orders.parquet'
    run(mage_project, EXPORTER, template(EXPORTER, 'Amazon S3', **{
        's3://your-bucket/path/result.parquet': uri, 'profile = "default"': 'profile = "s3"',
    }), frame())

    loaded = run(mage_project, LOADER, template(LOADER, 'Amazon S3', **{
        's3://your-bucket/path/input.parquet': uri, 'profile = "default"': 'profile = "s3"',
    }))

    assert s3.head_object(Bucket=bucket, Key='r/orders.parquet')['ContentLength'] > 0
    assert loaded['id'].tolist() == [1, 2, 3]
    assert loaded['day'].tolist()[:2] == [dt.date(2024, 1, 31), dt.date(1900, 1, 1)]


def test_api_templates(mage_project, api_url, collection):
    loaded = run(mage_project, LOADER, template(LOADER, 'API', **{
        'https://api.example.com/v1/orders': f'{api_url}/datasets/orders?rows=5',
    }))
    assert len(loaded) == 5
    # Integers above 2^53 come as text instead of rounded numbers.
    assert loaded['big_id'].tolist()[0] == '4611686018427387905'

    rows = pd.DataFrame({'id': range(1200), 'name': [f'n{i}' for i in range(1200)]})
    run(mage_project, EXPORTER, template(EXPORTER, 'API', **{
        'https://api.example.com/v1/orders': f'{api_url}/collections/{collection}?mode=append',
    }), rows)

    import requests

    stored = requests.get(f'{api_url}/collections/{collection}', params={'format': 'json'})
    stored.raise_for_status()
    # Three batches of at most 500 rows.
    assert sorted(r['id'] for r in stored.json()) == list(range(1200))


def test_postgres_templates(mage_project, pg, schema, profile_schema):
    run(mage_project, EXPORTER, template(
        EXPORTER, 'PostgreSQL', **{'profile = "default"': 'profile = "test_schema"'},
    ), frame().assign(updated_at=pd.Timestamp('2024-06-01')))

    loaded = run(mage_project, LOADER, template(
        LOADER, 'PostgreSQL', **{'profile = "default"': 'profile = "test_schema"'},
    ))

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM {schema}.your_table')
        assert cursor.fetchone()[0] == 3
    pg.rollback()
    assert loaded['name'].tolist()[:2] == ['ñ "quoted"', "it's"]


def test_mysql_templates(mage_project, my, mysql_database, monkeypatch):
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    run(mage_project, EXPORTER, template(
        EXPORTER, 'MySQL', **{'profile = "default"': 'profile = "mysql"'},
    ), frame().assign(updated_at=pd.Timestamp('2024-06-01')))

    loaded = run(mage_project, LOADER, template(
        LOADER, 'MySQL', **{'profile = "default"': 'profile = "mysql"'},
    ))

    assert loaded['id'].tolist() == [1, 2, 3]
    assert loaded['name'].tolist()[:2] == ['ñ "quoted"', "it's"]


def test_duckdb_templates(mage_project, duckdb_path, monkeypatch):
    monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', duckdb_path)
    run(mage_project, EXPORTER, template(
        EXPORTER, 'DuckDB', **{'profile = "default"': 'profile = "duckdb"'},
    ), frame().assign(updated_at=pd.Timestamp('2024-06-01')))

    loaded = run(mage_project, LOADER, template(
        LOADER, 'DuckDB', **{'profile = "default"': 'profile = "duckdb"'},
    ))

    assert loaded['id'].tolist() == [1, 2, 3]


def test_sqlite_templates(mage_project):
    # The exporter creates the database's directory, which SQLite does not.
    path = 'sqlite_test/nested/database.sqlite'
    assert not (Path(mage_project) / 'sqlite_test').exists()
    run(mage_project, EXPORTER, template(
        EXPORTER, 'SQLite', **{'data/database.sqlite': path},
    ), frame())

    loaded = run(mage_project, LOADER, template(
        LOADER, 'SQLite', **{'data/database.sqlite': path},
    ))

    with sqlite3.connect(Path(mage_project) / path) as connection:
        assert connection.execute('SELECT count(*) FROM your_table').fetchone()[0] == 3
    assert loaded['name'].tolist()[:2] == ['ñ "quoted"', "it's"]


def test_transformer_templates(mage_project):
    messy = pd.DataFrame({
        'Order ID': [1, 1, None],
        'Customer Name': ['  ana   lópez ', '  ana   lópez ', None],
    })
    cleaned = run(mage_project, TRANSFORMER, template(TRANSFORMER, 'Clean data'), messy)
    assert list(cleaned.columns) == ['order_id', 'customer_name']
    assert cleaned.to_dict('records') == [{'order_id': 1.0, 'customer_name': 'ana lópez'}]

    sales = pd.DataFrame({'region': ['a', 'a', 'b'], 'amount': [1.5, 2.5, 4.0]})
    totals = run(mage_project, TRANSFORMER, template(
        TRANSFORMER, 'Aggregate', your_column='region',
    ), sales)
    assert totals.sort_values('region').to_dict('records') == [
        {'region': 'a', 'rows': 2, 'amount': 4.0}, {'region': 'b', 'rows': 1, 'amount': 4.0},
    ]

    joined = run(mage_project, TRANSFORMER, template(TRANSFORMER, 'Join'),
                 pd.DataFrame({'id': [1, 2], 'x': ['a', 'b']}),
                 pd.DataFrame({'id': [2], 'y': ['c']}))
    assert joined.to_dict('records')[1] == {'id': 2, 'x': 'b', 'y': 'c'}

    long = run(mage_project, TRANSFORMER, template(TRANSFORMER, 'Reshape'),
               pd.DataFrame({'id': [1], 'a': [1.5], 'b': ['x']}))
    assert long.to_dict('records') == [
        {'id': 1, 'variable': 'a', 'value': '1.5'}, {'id': 1, 'variable': 'b', 'value': 'x'},
    ]
