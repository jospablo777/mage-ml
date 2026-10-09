"""
DuckDB database files and the data files DuckDB reads.
"""
import datetime as dt
import decimal
import subprocess
import sys

import pandas as pd
import polars as pl

from mage_ai.io.duckdb import DuckDB

FRAME = pl.DataFrame({
    'id': [1, 2, 3],
    'big': [2**53 + 1, None, -(2**63)],
    'price': pl.Series([decimal.Decimal('1.10'), None, decimal.Decimal('-99999.99')],
                       dtype=pl.Decimal(10, 2)),
    'name': ['ñandú, "quoted"\nline', '', None],
    'day': [dt.date(2024, 2, 29), None, dt.date(1, 1, 1)],
    'at': pl.Series([dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc), None,
                     dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)],
                    dtype=pl.Datetime('us', 'UTC')),
    'flag': [True, None, False],
})


def test_data_persists_in_the_file(duckdb_path):
    writer = DuckDB(database=duckdb_path, verbose=False)
    writer.export(FRAME, table_name='kept', verbose=False, allow_reserved_words=True)
    writer.close()

    reader = DuckDB(database=duckdb_path, verbose=False)
    back = reader.load('SELECT * FROM kept ORDER BY id', verbose=False, polars=True)
    reader.close()

    assert back.equals(FRAME)


def test_another_process_cannot_open_a_file_in_use(duckdb_path):
    """
    Mage opens DuckDB files for writing, so blocks running in other processes on the same
    file fail until the connection closes.
    """
    holder = DuckDB(database=duckdb_path, verbose=False)
    holder.execute('CREATE TABLE t (id BIGINT)')

    other = subprocess.run(
        [sys.executable, '-c', f'import duckdb; duckdb.connect({duckdb_path!r})'],
        capture_output=True, text=True,
    )
    holder.close()
    after = subprocess.run(
        [sys.executable, '-c', f'import duckdb; duckdb.connect({duckdb_path!r}).close()'],
        capture_output=True, text=True,
    )

    assert other.returncode != 0
    assert 'lock' in other.stderr.lower()
    assert after.returncode == 0, after.stderr


def test_clients_in_one_process_share_the_database(duckdb_path):
    first = DuckDB(database=duckdb_path, verbose=False)
    second = DuckDB(database=duckdb_path, verbose=False)

    first.export(pd.DataFrame({'id': [1]}), table_name='shared', verbose=False)
    seen = second.load('SELECT id FROM shared', verbose=False, polars=True)
    first.close()
    second.close()

    assert seen['id'].to_list() == [1]


def test_reading_parquet_csv_and_json_files(duckdb_client, tmp_path):
    parquet = tmp_path / 'frame.parquet'
    csv = tmp_path / 'frame.csv'
    ndjson = tmp_path / 'frame.ndjson'
    FRAME.write_parquet(parquet)
    FRAME.drop('price').write_csv(csv)
    FRAME.drop('price').write_ndjson(ndjson)

    from_parquet = duckdb_client.load(
        f"SELECT * FROM read_parquet('{parquet}') ORDER BY id", verbose=False, polars=True,
    )
    from_csv = duckdb_client.load(
        f"SELECT * FROM read_csv('{csv}') ORDER BY id", verbose=False, polars=True,
    )
    from_csv_keeping_empty = duckdb_client.load(
        f"SELECT * FROM read_csv('{csv}', allow_quoted_nulls = false) ORDER BY id",
        verbose=False, polars=True,
    )
    from_json = duckdb_client.load(
        f"SELECT * FROM read_json('{ndjson}') ORDER BY id", verbose=False, polars=True,
    )

    assert from_parquet.equals(FRAME)
    # read_csv reads a quoted empty string as NULL unless allow_quoted_nulls is false.
    assert from_csv['name'].to_list() == ['ñandú, "quoted"\nline', None, None]
    assert from_csv_keeping_empty['name'].to_list() == FRAME['name'].to_list()
    assert from_csv['big'].to_list() == FRAME['big'].to_list()
    assert from_csv['day'].to_list() == FRAME['day'].to_list()
    assert from_json['big'].to_list() == FRAME['big'].to_list()
    assert from_json['name'].to_list() == FRAME['name'].to_list()


def test_pandas_parquet_with_extension_types(duckdb_client, tmp_path):
    path = tmp_path / 'pandas.parquet'
    frame = pd.DataFrame({
        'n': pd.array([1, None], dtype='Int64'),
        's': pd.Series(['a', None], dtype='str'),
        'tz': pd.to_datetime(['2024-01-01', None]).tz_localize('Europe/Madrid'),
    })
    frame.to_parquet(path)

    back = duckdb_client.load(
        f"SELECT * FROM read_parquet('{path}')", verbose=False, exact_types=True,
    )

    assert back['n'].tolist() == [1, pd.NA]
    assert back['s'].tolist() == ['a', pd.NA]
    assert back['tz'].tolist()[0] == pd.Timestamp('2023-12-31 23:00', tz='UTC')
