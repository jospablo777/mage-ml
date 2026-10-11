from unittest.mock import patch

from mage_ai.io.mssql import MSSQL
from mage_ai.tests.base_test import TestCase


def client(host='db.example.com', port=1433):
    return MSSQL(
        database='sales', host=host, password='secret', user='mage', port=port,
        driver='ODBC Driver 18 for SQL Server',
    )


class MSSQLPortTests(TestCase):
    def test_the_port_reaches_the_connection_string(self):
        self.assertIn('SERVER=db.example.com,14330;', client(port=14330).connection_string)
        self.assertIn('SERVER=db.example.com,1433;', client().connection_string)

    def test_a_port_or_instance_in_the_server_wins(self):
        self.assertIn('SERVER=db.example.com,1444;',
                      client(host='db.example.com,1444', port=14330).connection_string)
        self.assertIn('SERVER=db\\SQLEXPRESS;',
                      client(host='db\\SQLEXPRESS').connection_string)

    def test_the_port_reaches_the_export_engine(self):
        with patch('mage_ai.io.mssql.create_engine') as create_engine:
            create_engine.side_effect = RuntimeError('stop')
            with self.assertRaises(RuntimeError):
                client(port=14330).upload_dataframe_fast(
                    None, schema_name='dbo', table_name='t',
                )
        url = create_engine.call_args[0][0]
        self.assertEqual((url.host, url.port), ('db.example.com', 14330))
