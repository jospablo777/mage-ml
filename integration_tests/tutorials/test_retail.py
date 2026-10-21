"""
The retail tutorials (docs/getting-started/retail-<language>.mdx) run as written: the code
blocks of each page, taken from the page itself, make a pipeline that runs in a MageML
project with the tutorial's data, and every language gives the same tables as a reference
written here, apart from float rounding. The numbers the pages quote are checked too.
"""
import os
import re
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

DOCS = Path(__file__).resolve().parents[2] / 'docs' / 'getting-started'
SUFFIXES = {'python': 'py', 'r': 'r', 'rust': 'rs', 'sql': 'sql'}
EXTENSIONS = {'python': 'py', 'r': 'r', 'rust': 'rs', 'sql': 'sql'}
# (block, type, upstream), in the order the pages add them.
GRAPH = [
    ('load_orders', 'data_loader', []),
    ('load_customers', 'data_loader', []),
    ('clean_orders', 'transformer', ['load_orders']),
    ('monthly_revenue', 'transformer', ['clean_orders', 'load_customers']),
    ('customer_features', 'transformer', ['clean_orders']),
    ('churn_score', 'transformer', ['customer_features']),
    ('export_revenue', 'data_exporter', ['monthly_revenue']),
    ('export_scores', 'data_exporter', ['churn_score']),
]
# The numbers the pages quote.
CLEAN_ORDERS, REVENUE_ROWS, SCORED, HIGH_RISK = 27_349, 48, 1_693, 390

RATES_TO_USD = {'USD': 1.0, 'EUR': 1.08, 'CRC': 0.0019}
# The SQL page's exporters write DuckDB tables instead of Parquet files.
EXPORT_TABLES = {'export_revenue': 'monthly_revenue', 'export_scores': 'customer_scores'}


def reference(folder: Path):
    orders = pd.read_parquet(folder / 'orders.parquet').drop_duplicates()
    customers = pd.read_parquet(folder / 'customers.parquet')
    orders = orders[(orders['status'] == 'completed') & orders['amount'].notna()].copy()
    orders['amount_usd'] = orders['amount'] * orders['currency'].map(RATES_TO_USD)

    joined = orders.merge(customers[['customer_id', 'region']], on='customer_id')
    joined['month'] = joined['ordered_at'].dt.strftime('%Y-%m')
    revenue = joined.groupby(['region', 'month']).agg(
        revenue_usd=('amount_usd', 'sum'),
        orders=('order_id', 'count'),
        customers=('customer_id', 'nunique'),
    ).reset_index()

    days = orders['ordered_at'].dt.tz_convert('UTC').dt.tz_localize(None).dt.normalize()
    scores = orders.assign(day=days).groupby('customer_id').agg(
        last_day=('day', 'max'), frequency=('order_id', 'count'),
        monetary_usd=('amount_usd', 'sum'),
    ).reset_index()
    scores['recency_days'] = (pd.Timestamp('2025-07-01') - scores['last_day']).dt.days
    z = (-1.0 + 0.04 * scores['recency_days'] - 0.35 * scores['frequency']
         - 0.15 * np.log1p(scores['monetary_usd']))
    scores['churn_score'] = 1 / (1 + np.exp(-z))
    scores['risk'] = np.where(scores['churn_score'] >= 0.5, 'high', 'low')
    scores = scores[['customer_id', 'recency_days', 'frequency', 'monetary_usd',
                     'churn_score', 'risk']]
    return len(orders), revenue, scores


def page_code(language: str) -> dict:
    """The code blocks of a page whose fence names a file: {block name: code}."""
    text = (DOCS / f'retail-{language}.mdx').read_text()
    extension = EXTENSIONS[language]
    found = re.findall(
        rf'```{language} (\w+)\.{extension}\n(.*?)\n```', text, flags=re.DOTALL,
    )
    return {name: code + '\n' for name, code in found}


def comparable(frame: pd.DataFrame, keys) -> pd.DataFrame:
    frame = frame.sort_values(keys, ignore_index=True)
    for column in frame.columns:
        if pd.api.types.is_integer_dtype(frame[column]):
            frame[column] = frame[column].astype('int64')
        elif pd.api.types.is_float_dtype(frame[column]):
            frame[column] = frame[column].astype('float64')
        else:
            frame[column] = frame[column].astype(str)
    return frame


@pytest.fixture
def shop(mage_project):
    from mage_ai.examples import retail

    return retail.write(mage_project).folder


def build(project: str, language: str, replace_tables: bool = False):
    """The page's pipeline; with replace_tables, every block replaces its table."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = SUFFIXES[language]
    code = page_code(language)
    assert set(code) == {f'{name}_{suffix}' for name, _, _ in GRAPH}, sorted(code)
    name = f'retail_{language}_compiled' if replace_tables else f'retail_{language}'
    pipeline = Pipeline.create(name, repo_path=project)
    for name, block_type, upstream in GRAPH:
        uuid = f'{name}_{suffix}'
        configuration = None
        if language == 'sql':
            # The page uses the default profile; the test project keeps DuckDB in its
            # duckdb profile. The exporters' table names are the page's.
            configuration = dict(
                data_provider='duckdb', data_provider_profile='duckdb',
                data_provider_schema='main',
            )
            if block_type == 'data_exporter':
                configuration.update(
                    data_provider_table=EXPORT_TABLES[name], export_write_policy='replace',
                )
            elif replace_tables:
                configuration['export_write_policy'] = 'replace'
        block = Block.create(
            uuid, block_type, project, language=language, configuration=configuration,
        )
        Path(block.file_path).write_text(code[uuid])
        pipeline.add_block(block, upstream_block_uuids=[f'{u}_{suffix}' for u in upstream])
    return Pipeline.get(pipeline.uuid, repo_path=project)


def runtime(language: str, monkeypatch, tmp_path) -> None:
    """The R environment and the Rust toolchain the pages need."""
    if language == 'r':
        path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
        if not path or shutil.which('Rscript') is None:
            pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
        monkeypatch.setenv('MAGE_R_PROJECT_DIR', path)
        monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))
    if language == 'rust' and shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
    if language == 'sql':
        monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', str(tmp_path / 'retail.duckdb'))


def outputs(project: str, language: str, tmp_path):
    """The two tables the pipeline wrote: Parquet files, or DuckDB tables for SQL."""
    if language == 'sql':
        import duckdb

        with duckdb.connect(str(tmp_path / 'retail.duckdb'), read_only=True) as con:
            return tuple(
                con.sql(f'SELECT * FROM main.{EXPORT_TABLES[name]}').df()
                for name in ('export_revenue', 'export_scores')
            )
    output = Path(project) / 'output' / language
    return (
        pd.read_parquet(output / 'monthly_revenue.parquet'),
        pd.read_parquet(output / 'customer_scores.parquet'),
    )


@pytest.mark.parametrize('language', [
    p.stem.removeprefix('retail-') for p in sorted(DOCS.glob('retail-*.mdx'))
])
def test_the_tutorial_runs_as_written_and_gives_the_reference_tables(
    mage_project, shop, language, monkeypatch, tmp_path,
):
    from mage_ai.orchestration import fusion_verify
    from mage_ai.shared.dates import utc_now

    runtime(language, monkeypatch, tmp_path)

    clean, revenue, scores = reference(shop)
    assert (clean, len(revenue), len(scores), int((scores['risk'] == 'high').sum())) == (
        CLEAN_ORDERS, REVENUE_ROWS, SCORED, HIGH_RISK
    )
    text = (DOCS / f'retail-{language}.mdx').read_text()
    for number in (CLEAN_ORDERS, SCORED, HIGH_RISK):
        assert f'{number:,}' in text, f'retail-{language}.mdx does not quote {number:,}'

    pipeline = build(mage_project, language)
    run = fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, 'chains', {}, utc_now()), log=lambda text: None,
    )
    statuses = {b.block_uuid: getattr(b.status, 'value', b.status) for b in run.block_runs}
    assert set(statuses.values()) == {'completed'}, statuses

    got_revenue, got_scores = outputs(mage_project, language, tmp_path)
    assert list(got_revenue.columns) == list(revenue.columns)
    assert list(got_scores.columns) == list(scores.columns)
    pd.testing.assert_frame_equal(
        comparable(got_revenue, ['region', 'month']), comparable(revenue, ['region', 'month']),
        rtol=1e-9,
    )
    pd.testing.assert_frame_equal(
        comparable(got_scores, ['customer_id']), comparable(scores, ['customer_id']),
        rtol=1e-9, atol=1e-15,
    )


def test_the_sql_tutorial_compiled_into_duckdb_views_gives_the_same_tables(
    mage_project, shop, monkeypatch, tmp_path,
):
    """
    With every block replacing its table, the SQL chains compile: the intermediate blocks
    become views and each exporter writes its table from one query across them.
    """
    import duckdb

    from mage_ai.orchestration import fusion_verify
    from mage_ai.shared.dates import utc_now

    runtime('sql', monkeypatch, tmp_path)
    _, revenue, scores = reference(shop)
    pipeline = build(mage_project, 'sql', replace_tables=True)
    run = fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, 'chains', {}, utc_now()), log=lambda text: None,
    )
    statuses = {b.block_uuid: getattr(b.status, 'value', b.status) for b in run.block_runs}
    assert set(statuses.values()) == {'completed'}, statuses

    with duckdb.connect(str(tmp_path / 'retail.duckdb'), read_only=True) as con:
        kinds = dict(con.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall())
    assert 'VIEW' in kinds.values(), kinds
    got_revenue, got_scores = outputs(mage_project, 'sql', tmp_path)
    pd.testing.assert_frame_equal(
        comparable(got_revenue, ['region', 'month']), comparable(revenue, ['region', 'month']),
        rtol=1e-9,
    )
    pd.testing.assert_frame_equal(
        comparable(got_scores, ['customer_id']), comparable(scores, ['customer_id']),
        rtol=1e-9, atol=1e-15,
    )
