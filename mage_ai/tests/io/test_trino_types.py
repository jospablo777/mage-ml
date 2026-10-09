import datetime as dt
import decimal
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa

from mage_ai.io import trino_types
from mage_ai.tests.base_test import TestCase


class ColumnTypeTest(TestCase):
    def test_types_hold_the_values(self):
        cases = [
            (pd.Series([1], dtype='int8'), 'TINYINT'),
            (pd.Series([1], dtype='uint8'), 'SMALLINT'),
            (pd.Series([1], dtype='uint32'), 'BIGINT'),
            (pd.Series([2**64 - 1], dtype='uint64'), 'DECIMAL(20, 0)'),
            (pd.array([1, None], dtype='Int64'), 'BIGINT'),
            (pd.Series([1.5], dtype='float32'), 'REAL'),
            (pd.Series([True, None], dtype='boolean'), 'BOOLEAN'),
            (pd.Series(['a'], dtype='str'), 'VARCHAR'),
            (pd.Series(pd.to_datetime(['2024-01-01'])), 'TIMESTAMP(6)'),
            (pd.Series(pd.to_datetime(['2024-01-01']).tz_localize('UTC')),
             'TIMESTAMP(6) WITH TIME ZONE'),
            (pd.Series(pd.to_timedelta(['1s'])), 'BIGINT'),
            (pd.Series([dt.date(2024, 1, 1), None]), 'DATE'),
            (pd.Series([decimal.Decimal('1.25'), None]), 'DECIMAL(3, 2)'),
            (pd.Series([decimal.Decimal('NaN')]), 'VARCHAR'),
            (pd.Series([2**100], dtype=object), 'DECIMAL(31, 0)'),
            (pd.Series([2**200], dtype=object), 'VARCHAR'),
            (pd.Series([uuid.uuid4()]), 'UUID'),
            (pd.Series([b'x']), 'VARBINARY'),
            (pd.Series([dt.time(1)]), 'TIME(6)'),
            (pd.Series([{'a': 1}]), 'VARCHAR'),
            (pd.Series([None, None], dtype=object), 'VARCHAR'),
            (pd.Series(pd.Categorical(['a'])), 'VARCHAR'),
            (pd.Series([1, None], dtype=pd.ArrowDtype(pa.int16())), 'SMALLINT'),
            (pd.Series([dt.date(2024, 1, 1)], dtype=pd.ArrowDtype(pa.date32())), 'DATE'),
        ]
        for series, expected in cases:
            self.assertEqual(trino_types.column_type(pd.Series(series)), expected, series)

    def test_delta_lake_types(self):
        """The Delta Lake connector has no UUID or TIME type and stores zoned timestamps
        in milliseconds."""
        zoned = pd.Series(pd.to_datetime(['2024-01-01']).tz_localize('UTC'))
        self.assertEqual(
            trino_types.column_type(zoned, connector='delta_lake'),
            'TIMESTAMP(3) WITH TIME ZONE',
        )
        self.assertEqual(
            trino_types.column_type(pd.Series([uuid.uuid4()]), connector='delta_lake'),
            'VARCHAR',
        )
        self.assertEqual(
            trino_types.column_type(pd.Series([dt.time(1)]), connector='delta_lake'),
            'VARCHAR',
        )

    def test_timestamp_precision(self):
        series = pd.Series(pd.to_datetime(['2024-01-01']))
        self.assertEqual(
            trino_types.column_type(series, timestamp_precision=3), 'TIMESTAMP(3)',
        )


class LiteralTest(TestCase):
    def test_literals(self):
        zone = dt.timezone(dt.timedelta(hours=5, minutes=30))
        cases = [
            (None, 'BIGINT', 'NULL'),
            (np.nan, 'DOUBLE', 'NULL'),
            (pd.NA, 'VARCHAR', 'NULL'),
            (pd.NaT, 'TIMESTAMP(6)', 'NULL'),
            (np.datetime64('NaT', 'ns'), 'TIMESTAMP(6)', 'NULL'),
            (True, 'BOOLEAN', 'TRUE'),
            (np.int64(-5), 'BIGINT', '-5'),
            (pd.Timedelta('1.5s'), 'BIGINT', '1500000'),
            (float('inf'), 'DOUBLE', "DOUBLE 'Infinity'"),
            (np.float32(1.5), 'REAL', "REAL '1.5'"),
            (2**64 - 1, 'DECIMAL(20, 0)', "DECIMAL '18446744073709551615'"),
            (decimal.Decimal('1E+2'), 'DECIMAL(3,0)', "DECIMAL '100'"),
            ("it's", 'VARCHAR', "'it''s'"),
            ({'a': [1, None]}, 'VARCHAR', '\'{"a": [1, null]}\''),
            (['x'], 'array(varchar)', 'CAST(JSON \'["x"]\' AS array(varchar))'),
            (b'\x00\xff', 'VARBINARY', "X'00ff'"),
            (dt.date(1, 2, 3), 'DATE', "DATE '0001-02-03'"),
            (pd.Timestamp('2024-01-02 03:04:05.123456789'), 'TIMESTAMP(6)',
             "TIMESTAMP '2024-01-02 03:04:05.123456789'"),
            (dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=zone), 'TIMESTAMP(6) WITH TIME ZONE',
             "TIMESTAMP '2024-01-02 03:04:05.000000 +05:30'"),
            # A naive column holds the instant in UTC.
            (dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=zone), 'TIMESTAMP(6)',
             "TIMESTAMP '2024-01-01 21:34:05.000000'"),
            (dt.datetime(2024, 1, 2), 'timestamp(6) with time zone',
             "TIMESTAMP '2024-01-02 00:00:00.000000 UTC'"),
            (dt.time(1, 2, 3), 'TIME(6)', "TIME '01:02:03'"),
            (uuid.UUID(int=1), 'UUID', "UUID '00000000-0000-0000-0000-000000000001'"),
            ('2024-01-01', 'DATE', "CAST('2024-01-01' AS DATE)"),
            ('{"a": 1}', 'json', 'JSON \'{"a": 1}\''),
        ]
        for value, trino_type, expected in cases:
            self.assertEqual(trino_types.literal(value, trino_type), expected, value)


class InsertStatementsTest(TestCase):
    def test_rows_are_split_by_length(self):
        rows = [(i, f'text {i}') for i in range(100)]

        statements = list(trino_types.insert_statements(
            '"c"."s"."t"', ['id', 'text'], ['BIGINT', 'VARCHAR'], iter(rows), 500,
        ))

        self.assertTrue(all(len(s) <= 500 for s in statements))
        self.assertTrue(statements[0].startswith('INSERT INTO "c"."s"."t" ("id", "text") VALUES'))
        self.assertEqual(sum(s.count("'text ") for s in statements), 100)

    def test_no_rows(self):
        self.assertEqual(list(trino_types.insert_statements('t', ['a'], ['BIGINT'], iter([]),
                                                            100)), [])


class LoadTest(TestCase):
    DESCRIPTION = [('big', 'bigint'), ('amount', 'decimal(10, 2)'),
                   ('at', 'timestamp(6) with time zone'), ('key', 'uuid')]
    ROWS = [
        [2**63 - 1, decimal.Decimal('1.25'),
         dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=-4))),
         uuid.UUID(int=1)],
        [None, None, None, None],
    ]

    def test_nullable_integers(self):
        """read_sql turned integer columns with a NULL into float64."""
        frame = trino_types.frame_with_nullable_integers(self.DESCRIPTION, self.ROWS)

        self.assertEqual(str(frame['big'].dtype), 'Int64')
        self.assertEqual(frame['big'][0], 2**63 - 1)

    def test_arrow_table(self):
        table = trino_types.arrow_table(self.DESCRIPTION, self.ROWS)

        self.assertEqual(table.schema.types, [
            pa.int64(), pa.decimal128(10, 2), pa.timestamp('us', tz='UTC'), pa.string(),
        ])
        self.assertEqual(table['at'][0].as_py(), dt.datetime(2024, 1, 1, 16,
                                                             tzinfo=dt.timezone.utc))
