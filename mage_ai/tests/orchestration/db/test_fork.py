import os
import unittest

from sqlalchemy import text

from mage_ai.orchestration.db import engine
from mage_ai.tests.base_test import DBTestCase


@unittest.skipUnless(hasattr(os, 'fork'), 'needs fork')
class ForkTest(DBTestCase):
    def test_a_forked_process_opens_its_own_connections(self):
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        self.assertGreater(engine.pool.checkedin(), 0)
        pid = os.fork()
        if pid == 0:
            code = 1
            try:
                # The parent's pooled connection is not reused here.
                if engine.pool.checkedin() == 0:
                    with engine.connect() as connection:
                        connection.execute(text('SELECT 1'))
                    code = 0
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        with engine.connect() as connection:
            self.assertEqual(connection.execute(text('SELECT 1')).scalar(), 1)
