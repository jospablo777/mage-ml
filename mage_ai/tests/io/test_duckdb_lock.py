"""
DuckDB allows one writing process per database file. Blocks of a pipeline that run in
parallel run in separate processes, and the second one failed at once with "Could not set
lock on file"; a connection now waits for the lock.
"""
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import duckdb

from mage_ai.io.duckdb import DuckDB, connect_waiting_for_lock

HOLD = '''
import sys, time, duckdb
connection = duckdb.connect(sys.argv[1])
print("locked", flush=True)
time.sleep(float(sys.argv[2]))
connection.close()
'''


class DuckDBLockTest(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.database = str(Path(folder.name) / 'shop.duckdb')

    def hold_lock(self, seconds: float) -> subprocess.Popen:
        """Another process with the database open for writing, for seconds."""
        holder = subprocess.Popen(
            [sys.executable, '-c', HOLD, self.database, str(seconds)],
            stdout=subprocess.PIPE, text=True,
        )
        self.addCleanup(holder.wait, 30)
        self.addCleanup(holder.kill)
        self.assertEqual(holder.stdout.readline().strip(), 'locked')
        return holder

    def test_a_connection_waits_until_the_other_process_releases_the_file(self):
        self.hold_lock(1.0)
        started = time.monotonic()

        connection = connect_waiting_for_lock(self.database, timeout=30)

        self.assertGreater(time.monotonic() - started, 0.3)
        self.assertEqual(connection.sql('SELECT 42').fetchone(), (42,))
        connection.close()

    def test_a_lock_held_past_the_timeout_says_what_to_do(self):
        self.hold_lock(10.0)

        with self.assertRaises(duckdb.IOException) as raised:
            connect_waiting_for_lock(self.database, timeout=0.3)

        message = str(raised.exception)
        self.assertIn('is open for writing in another process', message)
        self.assertIn('one writing process per database file', message)
        self.assertIn('MAGE_DUCKDB_LOCK_TIMEOUT', message)

    def test_other_errors_are_not_retried(self):
        with self.assertRaises(duckdb.Error):
            missing_folder = Path(self.database).parent / 'no' / 'x.duckdb'
            connect_waiting_for_lock(str(missing_folder), timeout=30)

    def test_open_and_close_can_be_called_again(self):
        client = DuckDB(database=self.database, verbose=False)
        first = client._ctx
        with client:
            self.assertIs(client._ctx, first, 'with opened a second connection')
        self.assertIsNone(client._ctx)
        client.close()
