from mage_integrations.sources.postgresql import PostgreSQL
from mage_integrations.tests.sources.test_base import build_sample_streams_catalog
from unittest.mock import MagicMock, patch
import unittest


class PostgreSQLSourceTests(unittest.TestCase):
    def test_load_data_from_logs(self):
        """A message past the LSN the run started at ends the run without records."""
        source = PostgreSQL(config=dict(replication_slot='mage_test_slot', schema='public'))
        replication = MagicMock()
        replication_connection = MagicMock()
        replication.build_connection.return_value = replication_connection
        values = MagicMock()
        values_connection = MagicMock()
        # PostgreSQL 13 has no logical decoding messages, so the run ends at the end LSN.
        values_connection.server_version = 130000
        values.build_connection.return_value = values_connection

        replication_cursor = MagicMock()
        replication_cursor.__enter__ = MagicMock(return_value=replication_cursor)
        replication_cursor.__exit__ = MagicMock(return_value=None)
        # The version, then the current LSN, 0/1631B60, which is 23272288.
        replication_cursor.fetchone.side_effect = [['PostgreSQL 13.9'], ['0/1631B60']]
        replication_connection.cursor.return_value = replication_cursor

        replication_message = MagicMock()
        replication_message.data_start = 23272289
        replication_cursor.read_message.return_value = replication_message

        stream = build_sample_streams_catalog().streams[1]

        with patch.object(source, 'build_connection', side_effect=[replication, values]):
            self.assertIsNone(next(source.load_data_from_logs(stream), None))

        self.assertEqual(replication_cursor.fetchone.call_count, 2)
        replication_cursor.start_replication.assert_called_once()
        self.assertEqual(
            replication_cursor.start_replication.call_args.kwargs['slot_name'], 'mage_test_slot',
        )
        replication_cursor.read_message.assert_called_once()
        # Nothing was read, and no bookmark was passed, so nothing is confirmed.
        replication_cursor.send_feedback.assert_not_called()
        replication.close_connection.assert_called_once_with(replication_connection)
        values_connection.close.assert_called_once()
