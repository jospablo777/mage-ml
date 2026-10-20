"""
Exports through binary COPY encoded in Rust (mage_ai/io/postgres_native.py) write what COPY
text writes: the same column types and the same values, for pandas and Polars frames, into
new tables and into tables that already exist.
"""
import datetime
import decimal
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import pytest
from psycopg2.extras import RealDictCursor

from mage_ai.io import postgres_native

pytestmark = pytest.mark.skipif(not postgres_native.enabled(),
                                reason='column_atlas_native has no binary COPY')


def pandas_frame():
    return pd.DataFrame({
        'id': pd.array([1, 2, 3, 4], dtype='Int64'),
        'small': pd.array([1, -2, None, 4], dtype='Int16'),
        'big': pd.array([2**62, None, -5, 0], dtype='Int64'),
        'unsigned': pd.array([2**64 - 1, 0, None, 7], dtype='UInt64'),
        'ratio': [0.5, float('nan'), float('inf'), -1e-300],
        'single': np.array([1.5, 2.25, -3.0, 0.0], dtype='float32'),
        'flag': pd.array([True, False, None, True], dtype='boolean'),
        'name': ['plain', '', None, 'naïve ✓ \t tab'],
        'kind': pd.Categorical(['a', 'b', 'a', None]),
        # Digits past the microsecond: PostgreSQL rounds them, half to even.
        'stamp': pd.to_datetime(['2026-01-01 00:00:00.1234567', '2026-01-01 00:00:00.9999995',
                              None, '1970-01-01 00:00:00.0'], format='ISO8601'),
        'stamp_utc': pd.to_datetime(['2026-03-01 12:00:00+02:00', None,
                                  '2026-03-01 00:00:00.000001+00:00',
                                  '1999-12-31 23:59:59+00:00'], utc=True, format='ISO8601'),
        'took': pd.to_timedelta(['1 day 00:00:01', None, '-3 seconds', '0s']),
        'price': [decimal.Decimal('12.3400'), None, decimal.Decimal('-0.0001'),
                  decimal.Decimal('99999999999999.9999')],
    })


def polars_frame():
    return pl.DataFrame({
        'id': [1, 2, 3],
        'score': [1.0, None, float('nan')],
        'label': ['x', None, 'z'],
        'day': [datetime.date(2026, 1, 1), None, datetime.date(1900, 2, 28)],
        'clock': [datetime.time(23, 59, 59, 999999), datetime.time(0, 0), None],
        'blob': [b'\x00\x01', None, b''],
        'tags': [[1, 2], None, []],
        'amount': pl.Series([decimal.Decimal('1.25'), None, decimal.Decimal('-3.50')],
                            dtype=pl.Decimal(10, 2)),
    }, schema_overrides={'id': pl.Int64})


def table(pg, schema, name):
    with pg.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(f'SELECT * FROM {schema}.{name} ORDER BY id')
        rows = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            'SELECT column_name, data_type, numeric_precision, numeric_scale '
            'FROM information_schema.columns WHERE table_schema = %s AND table_name = %s '
            'ORDER BY ordinal_position', (schema, name),
        )
        types = [tuple(row.values()) for row in cursor.fetchall()]
    pg.rollback()
    return types, rows


def same(rows_a, rows_b):
    """Rows compared value by value; NaN equals NaN."""
    assert len(rows_a) == len(rows_b)
    for a, b in zip(rows_a, rows_b):
        assert a.keys() == b.keys()
        for key in a:
            x, y = a[key], b[key]
            if isinstance(x, float) and isinstance(y, float) and np.isnan(x) and np.isnan(y):
                continue
            assert x == y, (key, x, y)


def spied_export(client, frame, **kwargs):
    """Exports and returns what postgres_native.copy answered: None when it sent the rows."""
    reasons = []
    real = postgres_native.copy

    def spy(*args, **kw):
        reasons.append(real(*args, **kw))
        return reasons[-1]

    with patch.object(postgres_native, 'copy', spy):
        client.export(frame, verbose=False, **kwargs)
    return reasons


def export_both(client, pg, schema, frame, **kwargs):
    """The frame through COPY text and through binary COPY; returns both tables."""
    with patch.dict('os.environ', MAGE_POSTGRES_BINARY_COPY='off'):
        client.export(frame, schema_name=schema, table_name='by_text', verbose=False, **kwargs)
    reasons = spied_export(client, frame, schema_name=schema, table_name='by_binary', **kwargs)
    assert reasons == [None], f'binary COPY was not used: {reasons}'
    return table(pg, schema, 'by_text'), table(pg, schema, 'by_binary')


@pytest.mark.parametrize('make', [pandas_frame, polars_frame], ids=['pandas', 'polars'])
def test_a_new_table_gets_the_same_types_and_values(mage_postgres, pg, schema, make):
    (text_types, text_rows), (binary_types, binary_rows) = export_both(
        mage_postgres, pg, schema, make(), if_exists='replace',
    )
    assert binary_types == text_types
    same(binary_rows, text_rows)


def test_sub_microsecond_digits_round_as_postgres_rounds_them(mage_postgres, pg, schema):
    _, (_, rows) = export_both(mage_postgres, pg, schema, pandas_frame(), if_exists='replace')
    assert rows[0]['stamp'] == datetime.datetime(2026, 1, 1, 0, 0, 0, 123457)
    assert rows[1]['stamp'] == datetime.datetime(2026, 1, 1, 0, 0, 1)


def test_appending_to_an_existing_table_with_its_own_types(mage_postgres, pg, schema):
    with pg.cursor() as cursor:
        for name in ('by_text', 'by_binary'):
            cursor.execute(f'CREATE TABLE {schema}.{name} (id bigint, label varchar(10), '
                           f'amount numeric(12, 3))')
    pg.commit()
    frame = pd.DataFrame({'id': [1, 2], 'label': ['a', 'b'],
                          'amount': [decimal.Decimal('1.2345'), decimal.Decimal('2')]})
    (text_types, text_rows), (binary_types, binary_rows) = export_both(
        mage_postgres, pg, schema, frame, if_exists='append',
    )
    assert binary_types == text_types
    same(binary_rows, text_rows)
    # numeric(12, 3) rounds as it does for COPY text.
    assert binary_rows[0]['amount'] == decimal.Decimal('1.235')


def test_a_column_of_another_type_goes_through_copy_text(mage_postgres, pg, schema):
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.t (id bigint, stamp timestamptz)')
    pg.commit()
    # A naive timestamp into timestamptz: binary would not convert it as text does.
    frame = pd.DataFrame({'id': [1], 'stamp': pd.to_datetime(['2026-01-01 08:00:00'])})
    reasons = spied_export(mage_postgres, frame, schema_name=schema, table_name='t',
                           if_exists='append')
    assert reasons and 'encodes as timestamp' in reasons[0]
    _, rows = table(pg, schema, 't')
    assert len(rows) == 1


def test_a_json_column_goes_through_copy_text(mage_postgres, pg, schema):
    frame = pd.DataFrame({'id': [1, 2], 'payload': [{'a': 1}, {'b': [1, 2]}]})
    reasons = spied_export(mage_postgres, frame, schema_name=schema, table_name='t',
                           if_exists='replace')
    assert reasons and reasons[0] is not None
    _, rows = table(pg, schema, 't')
    assert [row['payload'] for row in rows] == [{'a': 1}, {'b': [1, 2]}]
