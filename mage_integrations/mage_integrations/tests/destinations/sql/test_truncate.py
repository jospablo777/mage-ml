import unittest
from unittest.mock import MagicMock, patch

from mage_integrations.destinations.bigquery import BigQuery
from mage_integrations.destinations.doris import Doris
from mage_integrations.destinations.mssql import MSSQL
from mage_integrations.destinations.mysql import MySQL
from mage_integrations.destinations.oracledb import OracleDB
from mage_integrations.destinations.postgresql import PostgreSQL
from mage_integrations.destinations.redshift import Redshift
from mage_integrations.destinations.snowflake import Snowflake
from mage_integrations.destinations.sql.base import Destination
from mage_integrations.destinations.teradata import Teradata
from mage_integrations.destinations.trino.connectors.base import TrinoConnector

NAMES = dict(database_name='db', schema_name='sch', table_name='users')


class NoTruncate(Destination):
    pass


class TruncateCommandTests(unittest.TestCase):
    def test_each_destination_names_the_table_it_inserts_into(self):
        expected = {
            BigQuery: 'TRUNCATE TABLE `db.sch.users`',
            Doris: 'TRUNCATE TABLE db.users',
            MSSQL: 'TRUNCATE TABLE sch.users',
            MySQL: 'TRUNCATE TABLE db.users',
            OracleDB: 'TRUNCATE TABLE users',
            PostgreSQL: 'TRUNCATE TABLE "sch"."users"',
            Redshift: 'TRUNCATE TABLE sch.users',
            Snowflake: 'TRUNCATE TABLE "db"."sch"."users"',
            Teradata: 'DELETE FROM db.users ALL',
            TrinoConnector: 'DELETE FROM sch.users',
        }
        for destination_class, command in expected.items():
            with self.subTest(destination_class.__name__):
                destination = destination_class(config=dict(database='db'))
                self.assertEqual(
                    destination.build_truncate_table_commands(**NAMES),
                    [command],
                )

    def test_snowflake_without_double_quotes(self):
        destination = Snowflake(config=dict(disable_double_quotes=True))
        self.assertEqual(
            destination.build_truncate_table_commands(**NAMES),
            ['TRUNCATE TABLE db.sch.users'],
        )


class TruncateTableTests(unittest.TestCase):
    def destination(self, exists: bool):
        destination = PostgreSQL(config=dict(schema='sch', table='users'))
        connection = MagicMock()
        destination.build_connection = MagicMock(return_value=connection)
        destination.does_table_exist = MagicMock(return_value=exists)
        return destination, connection

    def test_truncates_an_existing_table(self):
        destination, connection = self.destination(exists=True)

        destination.truncate_table()

        connection.execute.assert_called_once_with(
            ['TRUNCATE TABLE "sch"."users"'],
            commit=True,
        )

    def test_a_missing_table_is_left_alone(self):
        destination, connection = self.destination(exists=False)

        destination.truncate_table()

        connection.execute.assert_not_called()

    def test_a_destination_without_truncate_fails_before_connecting(self):
        destination = NoTruncate(config=dict(table='users'))
        destination.build_connection = MagicMock()

        with self.assertRaisesRegex(Exception, 'NoTruncate cannot truncate its table'):
            destination.truncate_table()
        destination.build_connection.assert_not_called()

    def test_the_truncate_mode_writes_nothing(self):
        destination, connection = self.destination(exists=True)
        destination.should_truncate_table = True

        with patch.object(PostgreSQL, '_process') as process:
            destination.process(None)

        process.assert_not_called()
        connection.execute.assert_called_once()
