"""
S3Storage, the storage of block outputs when remote_variables_dir is an s3:// path.

S3 has no directories, so paths are key prefixes. A prefix without a trailing slash
also matches siblings: 'output_1' is a prefix of 'output_10'.
"""
import asyncio

import botocore.exceptions
import pandas as pd
import polars as pl
import pytest


@pytest.fixture
def storage(s3_env, bucket):
    from mage_ai.data_preparation.storage.s3_storage import S3Storage

    return S3Storage(dirpath=f's3://{bucket}/variables')


def keys(s3, bucket, prefix=''):
    found = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        found += [item['Key'] for item in page.get('Contents', [])]
    return sorted(found)


@pytest.fixture
def siblings(s3, bucket):
    names = [
        'variables/b/output_1/data.parquet',
        'variables/b/output_10/data.parquet',
        'variables/b/output_11/sample.json',
        'variables/b/data.json',
        'variables/b/data.json.bak',
    ]
    for name in names:
        s3.put_object(Bucket=bucket, Key=name, Body=b'{}')
    return names


def test_removing_a_directory_leaves_its_siblings(storage, s3, bucket, siblings):
    storage.remove_dir(f's3://{bucket}/variables/b/output_1')

    assert keys(s3, bucket) == sorted(set(siblings) - {'variables/b/output_1/data.parquet'})


def test_removing_a_file_leaves_its_siblings(storage, s3, bucket, siblings):
    storage.remove(f's3://{bucket}/variables/b/data.json')

    assert keys(s3, bucket) == sorted(set(siblings) - {'variables/b/data.json'})


def test_path_exists_matches_whole_names(storage, bucket, siblings):
    root = f's3://{bucket}/variables/b'

    assert storage.path_exists(f'{root}/output_1')
    assert storage.path_exists(f'{root}/data.json')
    assert not storage.path_exists(f'{root}/output_')
    assert not storage.path_exists(f'{root}/data.js')
    assert storage.isdir(f'{root}/output_1')
    assert not storage.isdir(f'{root}/data.json')


def test_removing_a_missing_path_does_nothing(storage, s3, bucket, siblings):
    storage.remove(f's3://{bucket}/variables/missing.json')
    storage.remove_dir(f's3://{bucket}/variables/missing')

    assert keys(s3, bucket) == sorted(siblings)


def test_listing_and_removing_more_than_a_thousand_keys(storage, s3, bucket):
    """S3 returns at most 1,000 keys per listing and deletes at most 1,000 per request."""
    from concurrent.futures import ThreadPoolExecutor

    names = [f'variables/many/f{i:05d}' for i in range(2500)]
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda n: s3.put_object(Bucket=bucket, Key=n, Body=b'x'), names))

    listed = storage.listdir(f's3://{bucket}/variables/many')
    storage.remove_dir(f's3://{bucket}/variables/many')

    assert sorted(listed) == [n.rsplit('/', 1)[1] for n in names]
    assert keys(s3, bucket) == []


def test_listdir_lists_files_and_directories(storage, bucket, siblings):
    assert sorted(storage.listdir(f's3://{bucket}/variables/b')) == [
        'data.json', 'data.json.bak', 'output_1', 'output_10', 'output_11',
    ]
    assert storage.listdir(f's3://{bucket}/variables/b', suffix='.bak') == [
        'data.json.bak', 'output_1', 'output_10', 'output_11',
    ]


def test_json_files(storage, bucket):
    path = f's3://{bucket}/variables/b/data.json'
    value = {'text': 'ñandú 🐍', 'number': 2**53 + 1, 'nan': float('nan'), 'list': [1, None]}

    storage.write_json_file(path, value)

    assert storage.read_json_file(path) == dict(value, nan=None)
    assert asyncio.run(storage.read_json_file_async(path)) == dict(value, nan=None)
    assert storage.read_json_file(f's3://{bucket}/missing.json', default_value=[]) == []
    with pytest.raises(botocore.exceptions.ClientError, match='NoSuchKey'):
        storage.read_json_file(f's3://{bucket}/missing.json', raise_exception=True)


def test_read_async_returns_the_text(storage, s3, bucket):
    """read_async returned None for S3 and GCS."""
    s3.put_object(Bucket=bucket, Key='variables/cache.txt', Body='a\nñ'.encode())

    assert asyncio.run(storage.read_async(f's3://{bucket}/variables/cache.txt')) == 'a\nñ'


def test_open_to_write(storage, s3, bucket):
    with storage.open_to_write(f's3://{bucket}/variables/log.txt') as stream:
        stream.write('first\n')
        stream.write('ñ')

    body = s3.get_object(Bucket=bucket, Key='variables/log.txt')['Body'].read()
    assert body.decode() == 'first\nñ'


def test_parquet_files(storage, bucket):
    path = f's3://{bucket}/variables/b/output_0/data.parquet'
    frame = pd.DataFrame({
        'n': pd.array([2**53 + 1, None], dtype='Int64'),
        'text': pd.Series(['ñ', None], dtype='str'),
    })
    polars_path = f's3://{bucket}/variables/b/output_1/data.parquet'
    polars_frame = pl.DataFrame({'n': [2**53 + 1, None], 'raw': [b'\x00', None]})

    storage.write_parquet(frame, path)
    storage.write_polars_dataframe(polars_frame, polars_path)

    pd.testing.assert_frame_equal(storage.read_parquet(path), frame)
    assert storage.read_polars_parquet(polars_path).equals(polars_frame)


def test_download_file_uses_the_client_settings(s3_settings, s3, bucket, tmp_path,
                                                monkeypatch):
    """
    download_file used a boto3 resource built from the default settings, so a client
    given an endpoint and credentials downloaded from AWS.
    """
    from mage_ai.services.aws.s3.s3 import Client

    s3.put_object(Bucket=bucket, Key='variables/file.bin', Body=b'\x00\x01')
    for variable in ('AWS_ENDPOINT_URL', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        monkeypatch.delenv(variable, raising=False)
    client = Client(bucket, **s3_settings)
    target = tmp_path / 'file.bin'

    client.download_file('variables/file.bin', str(target))

    assert target.read_bytes() == b'\x00\x01'
    assert client.read('variables/file.bin') == b'\x00\x01'
