"""
A Mage pipeline whose block outputs are stored in S3 (remote_variables_dir).

s3_frames loads a Parquet object with Polars, transforms it with a LazyFrame, converts it
to pyarrow-backed pandas and exports it to S3. Each block reads its upstream output from
S3. The result is compared with the same transformation computed here.
"""
import os
from pathlib import Path

import polars as pl
import pytest
import yaml

from integration_tests.data import s3_dataset
from integration_tests.mage_runner import run_pipeline

BLOCKS = ['s3_polars_load', 's3_polars_transform', 's3_pandas_transform', 's3_pandas_export']


@pytest.fixture
def remote_variables(mage_project, s3_env, bucket):
    """Store the project's block outputs under s3://bucket/variables."""
    metadata_path = Path(mage_project) / 'metadata.yaml'
    original = metadata_path.read_text() if metadata_path.exists() else None
    metadata = yaml.safe_load(original or '') or {}
    metadata['remote_variables_dir'] = f's3://{bucket}/variables'
    metadata_path.write_text(yaml.safe_dump(metadata))
    yield f's3://{bucket}/variables'
    if original is None:
        metadata_path.unlink()
    else:
        metadata_path.write_text(original)


def test_block_outputs_in_s3(mage_project, remote_variables, mage_s3, s3, bucket):
    dataset = s3_dataset.frame()
    mage_s3.export(dataset, bucket, 'source/data.parquet')

    run_pipeline('s3_frames', bucket=bucket, expected_rows=dataset.height)

    expected = dataset.with_columns(
        text_length=pl.col('text').str.len_chars(),
        ints_length=pl.col('ints').list.len(),
        day_year=pl.col('day').dt.year(),
        # pandas 3 turns NaN into NA in arithmetic on pyarrow and nullable floats,
        # unless the option future.distinguish_nan_and_na is set.
        f64_doubled=(pl.col('f64') * 2).fill_nan(None),
    )
    result = pl.from_pandas(mage_s3.load(bucket, 'result/data.parquet', exact_types=True))
    # pandas keeps an Enum as a dictionary, which converts back to a Categorical.
    assert s3_dataset.mismatches(expected, result) == {
        'level': ("Enum(categories=['low', 'mid', 'high'])", 'Categorical'),
    }

    stored = [
        item['Key']
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,
                                                                  Prefix='variables/')
        for item in page.get('Contents', [])
    ]
    for block in BLOCKS[:-1]:
        assert any(f'/{block}/output_0/' in key for key in stored), (block, stored)
    # The LazyFrame output was streamed to S3 by Polars.
    assert any(
        key.endswith('/s3_polars_transform/output_0/data_polars_lazy.json') for key in stored
    ), stored
    local_outputs = [
        str(path) for path in Path(mage_project).rglob('output_0')
        if any(block in str(path) for block in BLOCKS)
    ]
    assert local_outputs == []
    assert os.environ['AWS_ENDPOINT_URL'] == mage_s3.client.meta.endpoint_url
