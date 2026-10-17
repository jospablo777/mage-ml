from unittest.mock import MagicMock

from mage_ai.io.snowflake import Snowflake
from mage_ai.tests.base_test import TestCase


class SnowflakeRawQueryTest(TestCase):
    def test_every_statement_runs_and_the_last_rows_return(self):
        """One cursor.execute takes one statement; several failed (upstream #5695)."""
        cursors = [MagicMock(), MagicMock()]
        cursors[0].fetchall.return_value = [('Table T successfully created.',)]
        cursors[1].fetchall.return_value = [(3,)]
        client = Snowflake(account='a', user='u', password='p', verbose=False)
        client._ctx = MagicMock()
        client._ctx.execute_string.return_value = iter(cursors)

        rows = client.execute_query_raw('CREATE TABLE t (id INT); SELECT COUNT(*) FROM t;')

        client._ctx.execute_string.assert_called_once()
        self.assertEqual(
            client._ctx.execute_string.call_args.args[0],
            'CREATE TABLE t (id INT); SELECT COUNT(*) FROM t;',
        )
        self.assertEqual(rows, [(3,)])
        self.assertTrue(all(c.close.called for c in cursors))
