import unittest

from mage_integrations.destinations.mysql import MySQL
from mage_integrations.destinations.mysql.utils import convert_column_to_type
from mage_integrations.tests.destinations.sql.mixins import SQLDestinationMixin

SCHEMA = {
    'properties': {
        'ID': {
            'type': ['null', 'string'],
        },
    },
    'type': 'object',
}
SCHEMA_NAME = 'test'
STREAM = 'mysqltest'
TABLE_NAME = 'test_table'
DATABASE_NAME = 'test_db'


class MySQLDestinationTests(unittest.TestCase, SQLDestinationMixin):
    config = {
        'database': 'database',
        'host': 'host',
        'password': 'password',
        'username': 'username',
        'lower_case': False,
    }
    conn_class_path = 'mage_integrations.destinations.mysql.MySQLConnection'
    destination_class = MySQL
    expected_conn_class_kwargs = dict(
        database='database',
        host='host',
        password='password',
        port=None,
        username='username',
        connection_method='direct',
        conn_kwargs=None,
        ssh_host=None,
        ssh_port=22,
        ssh_username=None,
        ssh_password=None,
        ssh_pkey=None,
    )
    expected_template_config = {
        'config': {
          'database': '',
          'host': '',
          'password': '',
          'port': 3306,
          'table': '',
          'username': '',
          'use_lowercase': True
        },
    }

    def test_create_table_commands(self):
        destination = MySQL(config=self.config)
        destination.key_properties = {}
        table_commands = destination.build_create_table_commands(SCHEMA,
                                                                 SCHEMA_NAME,
                                                                 STREAM,
                                                                 TABLE_NAME,
                                                                 DATABASE_NAME)
        self.assertEqual(
            table_commands,
            # CHAR(255) cut text at 255 characters and dropped trailing spaces.
            ['CREATE TABLE test_db.test_table (ID LONGTEXT)']
        )

    def test_create_table_column_types(self):
        destination = MySQL(config=self.config)
        destination.key_properties = {STREAM: ['id']}
        schema = {
            'properties': {
                'id': {'type': ['string']},
                'flag': {'type': ['null', 'boolean']},
                'count': {'type': ['null', 'integer']},
                'unsigned_count': {'type': ['null', 'integer'], 'minimum': 0},
                'price': {'type': ['null', 'number']},
                'payload': {'type': ['null', 'object']},
                'tags': {'type': ['null', 'array'], 'items': {'type': ['null', 'string']}},
                'created': {'type': ['null', 'string'], 'format': 'date-time'},
            },
            'type': 'object',
        }

        table_commands = destination.build_create_table_commands(
            schema, SCHEMA_NAME, STREAM, TABLE_NAME, DATABASE_NAME,
        )

        self.assertEqual(table_commands, [
            'CREATE TABLE test_db.test_table (id VARCHAR(255) NOT NULL, flag BOOLEAN, '
            '_count BIGINT, unsigned_count BIGINT UNSIGNED, price DOUBLE, payload JSON, '
            'tags LONGTEXT, created DATETIME(6), PRIMARY KEY (id))'
        ])

    def test_integers_are_written_as_numbers(self):
        # CAST to UNSIGNED wrapped negative values around; values above 2**63 must stay.
        self.assertEqual(convert_column_to_type('-5', 'SIGNED'), '-5')
        self.assertEqual(convert_column_to_type(str(2**64 - 1), 'SIGNED'), str(2**64 - 1))
        self.assertEqual(convert_column_to_type('True', 'SIGNED'), '1')
        self.assertEqual(convert_column_to_type('False', 'SIGNED'), '0')
