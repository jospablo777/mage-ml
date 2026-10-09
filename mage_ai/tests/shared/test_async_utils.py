import asyncio

from mage_ai.shared.async_utils import run_sync
from mage_ai.tests.base_test import TestCase


async def double(value: int) -> int:
    await asyncio.sleep(0)
    return value * 2


class RunSyncTest(TestCase):
    def test_without_a_running_loop(self):
        self.assertEqual(run_sync(double(2)), 4)

    def test_inside_a_running_loop(self):
        """
        asyncio.run raised RuntimeError here, so cloning a Git repository from the server
        fell back to git init.
        """
        async def caller():
            return run_sync(double(3))

        self.assertEqual(asyncio.run(caller()), 6)

    def test_errors_reach_the_caller(self):
        async def fail():
            raise ChildProcessError('clone failed')

        async def caller():
            return run_sync(fail())

        with self.assertRaises(ChildProcessError):
            asyncio.run(caller())
