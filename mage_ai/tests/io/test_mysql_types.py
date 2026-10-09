import datetime as dt
import decimal

from mysql.connector.constants import FieldFlag, FieldType

from mage_ai.io import mysql_types
from mage_ai.tests.base_test import TestCase


class FakeCursor:
    def __init__(self, description, rows):
        self.description = description
        self._rows = rows

    def fetchall(self):
        return self._rows


def column(name, type_code, flags=0):
    return (name, type_code, None, None, None, None, True, flags)


class FrameWithNullableIntegersTest(TestCase):
    def test_integers_stay_integers_and_other_columns_match_read_sql(self):
        cursor = FakeCursor(
            [
                column('big', FieldType.LONGLONG),
                column('ubig', FieldType.LONGLONG, FieldFlag.UNSIGNED),
                column('flag', FieldType.TINY),
                column('amount', FieldType.NEWDECIMAL),
                column('name', FieldType.VAR_STRING),
                column('at', FieldType.DATETIME),
                column('bits', FieldType.BIT, FieldFlag.UNSIGNED),
            ],
            [
                (2**63 - 1, 2**64 - 1, 1, decimal.Decimal('1.25'), 'a',
                 dt.datetime(2024, 1, 1, 12), 5),
                (None, None, None, None, None, None, None),
            ],
        )

        frame = mysql_types.frame_with_nullable_integers(cursor)

        # read_sql made these float64, rounding 2**63 - 1 to 9.223372036854776e18.
        self.assertEqual(frame['big'].tolist()[0], 2**63 - 1)
        self.assertEqual(str(frame['big'].dtype), 'Int64')
        self.assertEqual(frame['ubig'].tolist()[0], 2**64 - 1)
        self.assertEqual(str(frame['ubig'].dtype), 'UInt64')
        self.assertEqual(str(frame['flag'].dtype), 'Int64')
        self.assertEqual(str(frame['bits'].dtype), 'UInt64')
        # As read_sql builds them: DECIMAL as float, text as str, DATETIME as datetime64.
        self.assertEqual(str(frame['amount'].dtype), 'float64')
        self.assertEqual(str(frame['name'].dtype), 'str')
        self.assertEqual(str(frame['at'].dtype), 'datetime64[us]')

    def test_duplicate_column_names(self):
        cursor = FakeCursor(
            [column('a', FieldType.LONG), column('a', FieldType.VAR_STRING)],
            [(1, 'x'), (None, 'y')],
        )

        frame = mysql_types.frame_with_nullable_integers(cursor)

        self.assertEqual(frame.columns.tolist(), ['a', 'a'])
        self.assertEqual(frame.iloc[:, 0].tolist()[0], 1)
        self.assertEqual(frame.iloc[:, 1].tolist(), ['x', 'y'])
