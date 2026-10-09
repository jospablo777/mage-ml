"""
Mage pipelines that pull data from a REST API, transform it, and push it back.

The pandas pipeline reads Parquet with pyarrow-backed dtypes and sends Parquet; the
Polars pipeline reads an Arrow stream, returns a LazyFrame from its transformer, and
sends an Arrow stream. What the API stored is compared with the same transformation
computed here. Other pipelines check how runs end when a request fails.
"""
import io
import uuid

import polars as pl
import pytest
import requests

from integration_tests.api.canonical import mismatched_columns
from integration_tests.mage_runner import block_statuses, execute_pipeline, run_pipeline
from integration_tests.services.api.app import datasets

ROWS = 300


def expected_frame() -> pl.DataFrame:
    return datasets.orders(ROWS).with_columns(
        name_length=pl.col('name').str.len_chars(),
        doubled=pl.col('amount') * 2,
        tag_count=pl.col('tags').list.len(),
        year=pl.col('day').dt.year(),
        is_even=pl.col('id') % 2 == 0,
        upper_name=pl.col('name').str.to_uppercase(),
    )


def stored(api_url, collection) -> pl.DataFrame:
    response = requests.get(f'{api_url}/collections/{collection}', timeout=60)
    response.raise_for_status()
    return pl.read_parquet(io.BytesIO(response.content))


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_pull_transform_push(mage_project, api_url, collection, engine):
    run_pipeline(f'api_{engine}', api_url=api_url, collection=collection, rows=ROWS)

    assert mismatched_columns(stored(api_url, collection), expected_frame()) == {}


def test_block_retries_recover_from_transient_failures(mage_project, api_url):
    key = uuid.uuid4().hex

    run_pipeline('api_retry', api_url=api_url, key=key, failures=2)

    assert requests.get(f'{api_url}/flaky/{key}/calls', timeout=10).json()['calls'] == 3


def test_block_retries_give_up(mage_project, api_url):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    key = uuid.uuid4().hex

    run = execute_pipeline('api_retry', api_url=api_url, key=key, failures=10)

    assert run.status == PipelineRun.PipelineRunStatus.FAILED
    assert block_statuses(run) == {'api_flaky_load': 'failed'}
    # The first call and three retries.
    assert requests.get(f'{api_url}/flaky/{key}/calls', timeout=10).json()['calls'] == 4


@pytest.mark.parametrize('code', [404, 422, 500])
def test_an_http_error_fails_the_run_before_the_export(mage_project, api_url, collection, code):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = execute_pipeline('api_error', api_url=api_url, collection=collection, code=code)

    assert run.status == PipelineRun.PipelineRunStatus.FAILED
    statuses = block_statuses(run)
    assert statuses['api_error_load'] == 'failed'
    assert statuses['api_after_error_export'] != 'completed'
    assert requests.get(f'{api_url}/collections/{collection}', timeout=10).status_code == 404


def test_a_polars_panic_fails_the_run(mage_project):
    """A Rust panic raises PanicException, a BaseException; the run must still end."""
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = execute_pipeline('polars_panic')

    assert run.status == PipelineRun.PipelineRunStatus.FAILED
    assert block_statuses(run) == {'polars_panic_load': 'failed'}
