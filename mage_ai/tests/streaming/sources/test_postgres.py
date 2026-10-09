import struct
from types import SimpleNamespace

from mage_ai.streaming.sources.postgres import (
    PostgresSource,
    Reader,
    Relation,
    int_to_lsn,
    lsn_to_int,
)
from mage_ai.tests.base_test import TestCase

INT4, TEXT = 23, 25


def tuple_data(*values):
    data = struct.pack('>h', len(values))
    for value in values:
        if value is None:
            data += b'n'
        elif value == 'unchanged':
            data += b'u'
        else:
            encoded = value.encode()
            data += b't' + struct.pack('>i', len(encoded)) + encoded
    return data


RELATION = Relation('public', 'users', 'd', [(True, 'id', INT4), (False, 'name', TEXT)])


def source():
    """
    A source without connections. psycopg2's own casters need a cursor, so the
    connection's casters, which the source looks up first, stand in for them.
    """
    instance = PostgresSource.__new__(PostgresSource)
    instance.connection = SimpleNamespace(string_types={
        INT4: lambda value, cursor: int(value),
        TEXT: lambda value, cursor: value,
    })
    instance.cursor = None
    return instance


class PostgresSourceTest(TestCase):
    def test_lsn_conversions(self):
        self.assertEqual(lsn_to_int('0/16B3748'), 0x16B3748)
        self.assertEqual(lsn_to_int('1/0'), 1 << 32)
        self.assertEqual(int_to_lsn(lsn_to_int('A/16B3748')), 'A/16B3748')

    def test_tuples(self):
        reader = Reader(tuple_data('1', None, 'unchanged', 'ñ'))

        self.assertEqual(reader.tuple(), [('t', '1'), ('n', None), ('u', None), ('t', 'ñ')])

    def test_rows_of_each_operation(self):
        rows = source()._PostgresSource__rows
        insert = rows(b'I', Reader(b'N' + tuple_data('1', "it's")), RELATION)
        self.assertEqual(insert, [{'id': 1, 'name': "it's", '_mage_operation': 'insert'}])

        update = rows(b'U', Reader(b'N' + tuple_data('1', 'new')), RELATION)
        self.assertEqual(update[0]['_mage_operation'], 'update')
        self.assertEqual(update[0]['name'], 'new')

        # With the default replica identity, a delete keeps the key alone.
        delete = rows(b'D', Reader(b'K' + tuple_data('1', None)), RELATION)
        self.assertEqual(delete[0]['id'], 1)
        self.assertNotIn('name', delete[0])
        self.assertEqual(delete[0]['_mage_operation'], 'delete')

    def test_a_changed_key_deletes_the_old_row(self):
        rows = source()._PostgresSource__rows(
            b'U', Reader(b'K' + tuple_data('1', None) + b'N' + tuple_data('2', 'b')), RELATION,
        )

        self.assertEqual(
            [(r['_mage_operation'], r['id']) for r in rows], [('delete', 1), ('update', 2)],
        )
