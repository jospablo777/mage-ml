import shutil
import tempfile
from pathlib import Path

from mage_ai.orchestration.db.process import create_process
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.orchestration.queue.test_process_queue import use_polars, warm_polars


class CreateProcessTest(DBTestCase):
    def test_the_scheduler_process_runs_polars_after_the_server_used_polars(self):
        """
        The server starts the scheduler with create_process. On Linux it was a fork, and a
        fork of a server that had run Polars waited forever at its first Polars call.
        """
        warm_polars()
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        result = Path(directory, 'result')

        process = create_process(use_polars, (str(result),))
        process.start()
        process.join(120)
        alive = process.is_alive()
        if alive:
            process.kill()
            process.join()

        self.assertFalse(alive, 'The process did not finish its Polars call.')
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(result.read_text(), '7')
