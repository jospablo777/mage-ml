"""
The SQL templates of the block menus, run on a database as a user starts from them: an
upstream Python block's rows go through each transformer template.
"""
import datetime as dt

from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

ORDERS = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'id': pd.array([1, 1, 2, None], dtype='Int64'),
        'customer_id': [7, 7, 8, 8],
        'created_at': pd.to_datetime(['2024-01-01', '2024-01-04', '2024-01-02', '2024-01-02']),
        'updated_at': pd.to_datetime(['2024-01-01', '2024-01-05', '2024-01-02', '2024-01-03']),
        'amount': [5.0, 7.0, -1.0, 2.0],
    })
'''


def sql_template(path: str) -> str:
    from mage_ai.data_preparation.templates.template import fetch_template_source

    block_type = 'data_loader' if path.startswith('data_loaders') else 'transformer'
    return fetch_template_source(block_type, {'template_path': path}, language='sql')


def run_template(mage_project, config, path):
    pipeline = build(mage_project, [
        dict(uuid='orders', type='data_loader', code=ORDERS),
        dict(uuid='template', type='transformer', language='sql', upstream=['orders'],
             config=dict(config, export_write_policy='replace'), code=sql_template(path)),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    return output(pipeline, run, 'template')


def rows(frame):
    return frame.to_pandas().to_dict('records') if hasattr(frame, 'to_pandas') else \
        frame.to_dict('records')


def check_sql_templates(mage_project, config):
    """Each SQL transformer template on the database the config names."""
    latest = rows(run_template(mage_project, config, 'transformers/sql/deduplicate.sql'))
    assert sorted((int(r['id']), r['amount']) for r in latest) == [(1, 7.0), (2, -1.0)]

    report = rows(run_template(mage_project, config, 'transformers/sql/quality.sql'))
    assert {k: int(v) for k, v in report[0].items()} == dict(
        row_count=4, missing_ids=1, repeated_ids=1, negative_amounts=1,
    )

    days = rows(run_template(mage_project, config, 'transformers/sql/date_spine.sql'))
    assert [(r['day'] if isinstance(r['day'], dt.date) else r['day'].date(),
             float(r['amount']), int(r['row_count'])) for r in days] == [
        (dt.date(2024, 1, 1), 5.0, 1), (dt.date(2024, 1, 2), 1.0, 2),
        (dt.date(2024, 1, 3), 0.0, 0), (dt.date(2024, 1, 4), 7.0, 1),
    ]

    running = rows(run_template(mage_project, config, 'transformers/sql/running_total.sql'))
    sevens = sorted((r for r in running if r['customer_id'] == 7), key=lambda r: r['created_at'])
    assert [r['running_amount'] for r in sevens] == [5.0, 12.0]
