"""
Rust blocks on object storage (mage::storage) against MinIO, with the s3 profile of the
test project: a Rust loader scans Parquet files that pandas wrote, through a glob and a
filter, and a Rust exporter writes Parquet and CSV that pandas reads back with the same
values and types.
"""
import io

import pandas as pd
import pyarrow.parquet as pq
import pytest

from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of

LOAD = '''
use mage::prelude::*;
use mage::storage::Storage;

fn load_data(vars: Vars) -> Result<LazyFrame> {
    let bucket: String = vars.get("bucket")?;
    let minimum: i64 = vars.get("minimum")?;
    Ok(Storage::connect()?
        .scan_parquet(&format!("s3://{bucket}/events/*.parquet"))?
        .filter(col("id").gt_eq(lit(minimum))))
}
'''

TRANSFORM = '''
use mage::prelude::*;

fn transform(events: LazyFrame) -> Result<LazyFrame> {
    Ok(events
        .with_column((col("amount") * lit(2.0)).alias("doubled"))
        .sort(["id"], Default::default()))
}
'''

EXPORT = '''
use mage::prelude::*;
use mage::storage::Storage;

fn export_data(events: DataFrame, vars: Vars) -> Result<()> {
    let bucket: String = vars.get("bucket")?;
    let storage = Storage::connect()?;
    storage.write_parquet(&events, &format!("s3://{bucket}/out/events.parquet"))?;
    storage.write_csv(&events, &format!("s3://{bucket}/out/events.csv"))?;
    Ok(())
}
'''


@pytest.fixture
def storage_workspace(mage_project):
    """The project's Rust environment with mage::storage."""
    from mage_ai.data_preparation.models.block.rust import workspace

    rust = workspace.init(mage_project)
    text = rust.cargo_toml.read_text()
    if '"storage"' not in text:
        rust.cargo_toml.write_text(
            text.replace('features = ["postgres"]', 'features = ["postgres", "storage"]'),
        )
    return rust


def part(first: int, count: int) -> pd.DataFrame:
    ids = range(first, first + count)
    return pd.DataFrame({
        'id': pd.array(list(ids), dtype='int64'),
        'amount': [i / 4 for i in ids],
        'label': [f'é {i}' if i % 3 else None for i in ids],
        'at': pd.to_datetime([f'2024-01-01 00:00:{i % 60:02}.123456' for i in ids], utc=True),
    })


def put_parquet(s3, bucket, key, frame):
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def get(s3, bucket, key) -> bytes:
    return s3.get_object(Bucket=bucket, Key=key)['Body'].read()


@pytest.mark.timeout(1800)
def test_rust_blocks_scan_and_write_object_storage(
    mage_project, storage_workspace, s3, bucket,
):
    put_parquet(s3, bucket, 'events/part-0.parquet', part(0, 500))
    put_parquet(s3, bucket, 'events/part-1.parquet', part(500, 500))
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', language='rust', code=LOAD,
             config=dict(data_provider_profile='s3')),
        dict(uuid='double', type='transformer', language='rust', code=TRANSFORM,
             upstream=['load']),
        dict(uuid='export', type='data_exporter', language='rust', code=EXPORT,
             upstream=['double'], config=dict(data_provider_profile='s3')),
    ])

    run = execute_pipeline(pipeline, bucket=bucket, minimum=250)
    assert run.status.value == 'completed', error_of(run)

    expected = pd.concat([part(0, 500), part(500, 500)])
    expected = expected[expected['id'] >= 250].reset_index(drop=True)
    expected['doubled'] = expected['amount'] * 2
    written = pq.read_table(io.BytesIO(get(s3, bucket, 'out/events.parquet')))
    assert str(written.schema.field('at').type) == 'timestamp[us, tz=UTC]'
    actual = written.to_pandas()
    actual['at'] = actual['at'].astype('datetime64[ns, UTC]')
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)

    csv = pd.read_csv(io.BytesIO(get(s3, bucket, 'out/events.csv')))
    assert csv['id'].tolist() == expected['id'].tolist()
    assert csv['label'].isna().sum() == expected['label'].isna().sum()


def test_a_missing_object_names_the_path(mage_project, storage_workspace, bucket):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', language='rust', code=LOAD,
             config=dict(data_provider_profile='s3')),
    ])
    run = execute_pipeline(pipeline, bucket=bucket, minimum=0)
    assert run.status.value == 'failed'
    assert f's3://{bucket}/events/*.parquet' in error_of(run, 'load')
