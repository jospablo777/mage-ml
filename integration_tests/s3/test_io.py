"""
Mage's S3 client (mage_ai.io.s3) against MinIO: frames exported in each format and
loaded back with pandas, with pyarrow-backed pandas (exact_types) and with Polars.
"""
import datetime as dt
import io

import botocore.exceptions
import numpy as np
import pandas as pd
import polars as pl
import pytest

from integration_tests.data import s3_dataset

FLAT_COLUMNS = [
    'id', 'i8', 'i16', 'i32', 'i64', 'u8', 'u16', 'u32', 'u64', 'f64', 'flag', 'text',
    'day', 'ts_us', 'ts_utc',
]


@pytest.fixture(scope='module')
def dataset():
    return s3_dataset.frame()


def test_polars_parquet_round_trip(mage_s3, bucket, dataset):
    mage_s3.export(dataset, bucket, 'frames/data.parquet')

    back = mage_s3.load(bucket, 'frames/data.parquet', polars=True)

    assert s3_dataset.mismatches(dataset, back) == {}


def test_exact_types_load_of_a_polars_file(mage_s3, bucket, dataset):
    mage_s3.export(dataset, bucket, 'frames/data.parquet')

    back = mage_s3.load(bucket, 'frames/data.parquet', exact_types=True)

    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in back.dtypes)
    # A pandas dictionary column converts to a Polars Categorical, not to an Enum.
    assert s3_dataset.mismatches(dataset, pl.from_pandas(back)) == {
        'level': ("Enum(categories=['low', 'mid', 'high'])", 'Categorical'),
    }


def test_default_load_turns_integers_with_nulls_into_floats(mage_s3, bucket, dataset):
    """
    The default load is pd.read_parquet's: an integer column with a null becomes float64
    and loses integers above 2**53. exact_types and polars keep them.
    """
    mage_s3.export(dataset, bucket, 'frames/data.parquet')

    back = mage_s3.load(bucket, 'frames/data.parquet')

    assert back['i64'].dtype == 'float64'
    assert back.loc[back['id'] == 3, 'i64'].item() == 2**53
    assert dataset.filter(pl.col('id') == 3)['i64'].item() == 2**53 + 1


def test_arrow_backed_pandas_round_trip(mage_s3, bucket, dataset):
    frame = dataset.to_pandas(use_pyarrow_extension_array=True)
    # Nanoseconds are written as nanoseconds only on request; see the next test.
    mage_s3.export(frame, bucket, 'frames/arrow.parquet', coerce_timestamps=None)

    back = mage_s3.load(bucket, 'frames/arrow.parquet', exact_types=True)

    # Parquet does not keep the width of dictionary values: large_string comes back as
    # string. The values are equal.
    dictionaries = ['level', 'label']
    for column in dictionaries:
        assert back[column].dtype.pyarrow_dtype.value_type == 'string'
        assert back[column].tolist() == frame[column].tolist()
    pd.testing.assert_frame_equal(back.drop(columns=dictionaries),
                                  frame.drop(columns=dictionaries))


def test_nanoseconds_raise_unless_written_as_nanoseconds(mage_s3, bucket):
    """
    pandas nanosecond columns are written in microseconds, which Spark, Athena and Hive
    read. A value with nanoseconds raises; it used to be truncated without an error.
    """
    whole = pd.DataFrame({'at': pd.to_datetime(['2024-01-01 00:00:00.000001'])
                          .astype('datetime64[ns]')})
    precise = pd.DataFrame({'at': pd.to_datetime(['2024-01-01 00:00:00.000000001'])
                            .astype('datetime64[ns]')})

    with pytest.raises(Exception, match='would lose data'):
        mage_s3.export(precise, bucket, 'frames/ns.parquet')
    mage_s3.export(whole, bucket, 'frames/us.parquet')
    mage_s3.export(precise, bucket, 'frames/ns.parquet', coerce_timestamps=None)

    assert str(mage_s3.load(bucket, 'frames/us.parquet')['at'].dtype) == 'datetime64[us]'
    pd.testing.assert_frame_equal(mage_s3.load(bucket, 'frames/ns.parquet'), precise)


def test_numpy_backed_pandas_round_trip(mage_s3, bucket, dataset):
    frame = dataset.select(FLAT_COLUMNS).to_pandas()
    mage_s3.export(frame, bucket, 'frames/numpy.parquet')

    back = mage_s3.load(bucket, 'frames/numpy.parquet')

    pd.testing.assert_frame_equal(back, frame)


def test_csv_round_trip(mage_s3, bucket, dataset):
    flat = dataset.select(FLAT_COLUMNS)
    mage_s3.export(flat, bucket, 'frames/data.csv')

    back = mage_s3.load(bucket, 'frames/data.csv', polars=True, try_parse_dates=True)

    assert s3_dataset.mismatches(flat, back.cast(flat.schema)) == {}


def test_csv_export_leaves_out_the_default_index(mage_s3, bucket):
    """
    pandas writes the index to CSV unless told not to, and the default index came back
    as a column named 'Unnamed: 0'. A named or non-default index is still written.
    """
    frame = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    indexed = frame.set_index(pd.Index([10, 20], name='key'))
    mage_s3.export(frame, bucket, 'frames/plain.csv')
    mage_s3.export(indexed, bucket, 'frames/indexed.csv')

    assert list(mage_s3.load(bucket, 'frames/plain.csv').columns) == ['a', 'b']
    pd.testing.assert_frame_equal(
        mage_s3.load(bucket, 'frames/indexed.csv', index_col='key'), indexed,
    )


def test_pandas_csv_parser_loses_the_int64_minimum(mage_s3, bucket):
    """
    pandas 3.0's C parser reads -2**63 as missing when the column has another missing
    value. Polars and pyarrow read it.
    """
    frame = pl.DataFrame({'n': [1, None, -(2**63)], 'text': ['a', 'b', 'c']})
    mage_s3.export(frame, bucket, 'frames/minimum.csv')

    default = mage_s3.load(bucket, 'frames/minimum.csv')
    exact = mage_s3.load(bucket, 'frames/minimum.csv', exact_types=True)
    polars = mage_s3.load(bucket, 'frames/minimum.csv', polars=True)

    assert default['n'].isna().tolist() == [False, True, True]
    assert exact['n'].tolist() == [1, pd.NA, -(2**63)]
    assert polars['n'].to_list() == [1, None, -(2**63)]


def test_pandas_csv_parser_drops_null_rows_of_one_column_files(mage_s3, bucket):
    """
    A null in a one-column CSV is an empty line, which pandas skips by default
    (skip_blank_lines=True), so the row is lost. Polars and pyarrow keep it.
    """
    frame = pl.DataFrame({'n': [1, None, 3]})
    mage_s3.export(frame, bucket, 'frames/one_column.csv')

    assert mage_s3.load(bucket, 'frames/one_column.csv')['n'].tolist() == [1, 3]
    assert mage_s3.load(bucket, 'frames/one_column.csv', polars=True).equals(frame)
    assert mage_s3.load(
        bucket, 'frames/one_column.csv', exact_types=True,
    )['n'].tolist() == [1, pd.NA, 3]


def test_json_from_polars_and_from_pandas(mage_s3, bucket):
    """
    Polars writes JSON as an array of rows and pandas as {column: {index: value}}. Both
    load with exact integers through exact_types and polars.
    """
    frame = pl.DataFrame({
        'n': [2**53 + 1, None, -(2**63)],
        'text': ['ñ "q"\n', '', None],
        'flag': [True, None, False],
        'ints': [[1, None], None, []],
        'record': [{'a': 1, 'b': 'x'}, None, {'a': None, 'b': None}],
    })
    mage_s3.export(frame, bucket, 'frames/polars.json')
    mage_s3.export(frame.to_pandas(use_pyarrow_extension_array=True), bucket,
                   'frames/pandas.json')

    for key in ('frames/polars.json', 'frames/pandas.json'):
        back = mage_s3.load(bucket, key, polars=True)
        exact = mage_s3.load(bucket, key, exact_types=True)
        assert back['n'].to_list() == frame['n'].to_list(), key
        assert back['text'].to_list() == frame['text'].to_list(), key
        assert back['flag'].to_list() == frame['flag'].to_list(), key
        assert back['record'].to_list() == frame['record'].to_list(), key
        assert exact['n'].tolist() == [2**53 + 1, pd.NA, -(2**63)], key
    # pandas writes the integers of nested values as floats.
    assert mage_s3.load(bucket, 'frames/polars.json', polars=True)['ints'].to_list() == [
        [1, None], None, [],
    ]
    assert mage_s3.load(bucket, 'frames/pandas.json', polars=True)['ints'].to_list() == [
        [1.0, None], None, [],
    ]


def test_ndjson_loads_with_polars(mage_s3, bucket, s3):
    s3.put_object(Bucket=bucket, Key='frames/rows.json',
                  Body=b'{"n": 9007199254740993, "s": "a"}\n{"n": null, "s": null}\n')

    back = mage_s3.load(bucket, 'frames/rows.json', polars=True)

    assert back.to_dicts() == [{'n': 2**53 + 1, 's': 'a'}, {'n': None, 's': None}]


def test_lazy_frames_are_collected_before_export(mage_s3, bucket, dataset):
    lazy = dataset.lazy().filter(pl.col('id') > 5).select('id', 'text')

    mage_s3.export(lazy, bucket, 'frames/lazy.parquet')

    back = mage_s3.load(bucket, 'frames/lazy.parquet', polars=True)
    assert back.equals(lazy.collect())


def test_export_of_a_file_path(mage_s3, bucket, tmp_path, dataset):
    path = tmp_path / 'local.parquet'
    dataset.write_parquet(path)

    mage_s3.export(str(path), bucket, 'frames/uploaded.parquet')

    assert mage_s3.load(bucket, 'frames/uploaded.parquet', polars=True).equals(dataset)


def test_export_overwrites_the_object(mage_s3, bucket):
    mage_s3.export(pl.DataFrame({'a': [1, 2, 3]}), bucket, 'frames/data.parquet')
    mage_s3.export(pl.DataFrame({'b': ['x']}), bucket, 'frames/data.parquet')

    assert mage_s3.load(bucket, 'frames/data.parquet', polars=True).to_dicts() == [
        {'b': 'x'},
    ]


def test_keys_with_spaces_and_unicode(mage_s3, bucket, s3):
    key = 'dir with spaces/ñandú 🐍/data (1).parquet'
    frame = pl.DataFrame({'a': [1]})

    mage_s3.export(frame, bucket, key)

    assert [o['Key'] for o in s3.list_objects_v2(Bucket=bucket)['Contents']] == [key]
    assert mage_s3.load(bucket, key, polars=True).equals(frame)
    assert mage_s3.exists(bucket, 'dir with spaces/')


def test_a_missing_object_raises(mage_s3, bucket):
    with pytest.raises(botocore.exceptions.ClientError, match='NoSuchKey'):
        mage_s3.load(bucket, 'frames/missing.parquet')


def test_a_missing_bucket_raises(mage_s3):
    with pytest.raises(botocore.exceptions.ClientError, match='NoSuchBucket'):
        mage_s3.export(pl.DataFrame({'a': [1]}), 'it-missing-bucket', 'data.parquet')


def test_fixed_size_lists_with_nulls_load_into_pandas(mage_s3, bucket):
    """
    pyarrow before 26 cannot read a fixed-size list column holding a null from Parquet,
    which Polars and pyarrow 25 write (apache/arrow#35692). Mage reads those files
    with Polars and casts them back to the schema stored in the file.
    """
    frame = pl.DataFrame(
        {'vector': [[0.5, 1.5], None, [2.5, None]], 'id': [1, 2, 3]},
        schema={'vector': pl.Array(pl.Float32, 2), 'id': pl.Int64},
    )
    mage_s3.export(frame, bucket, 'frames/vectors.parquet')

    exact = mage_s3.load(bucket, 'frames/vectors.parquet', exact_types=True)
    default = mage_s3.load(bucket, 'frames/vectors.parquet')

    assert str(exact['vector'].dtype) == 'fixed_size_list<element: float>[2][pyarrow]'
    assert exact['vector'].tolist()[0] == [0.5, 1.5]
    assert exact['vector'].isna().tolist() == [False, True, False]
    # The default load holds numpy arrays, with NaN for a null element.
    assert default['vector'][0].tolist() == [0.5, 1.5]
    assert default['vector'][1] is None
    assert default['vector'][2][0] == 2.5 and np.isnan(default['vector'][2][1])


def test_dates_and_timestamps_in_csv(mage_s3, bucket):
    frame = pl.DataFrame({
        'day': [dt.date(1, 1, 1), None],
        'at': [dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc), None],
    })
    mage_s3.export(frame, bucket, 'frames/dates.csv')

    back = mage_s3.load(bucket, 'frames/dates.csv', polars=True, try_parse_dates=True)

    assert back.equals(frame)


def test_object_bytes_match_the_written_parquet(mage_s3, bucket, s3, dataset):
    mage_s3.export(dataset, bucket, 'frames/data.parquet')
    body = s3.get_object(Bucket=bucket, Key='frames/data.parquet')['Body'].read()

    assert pl.read_parquet(io.BytesIO(body)).equals(dataset)
