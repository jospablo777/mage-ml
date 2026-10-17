"""The event loop watch names the code that blocked the server's event loop."""
import asyncio
import time
import unittest

from mage_ai.server.loop_watch import LoopWatch


def blocking_handler():
    time.sleep(0.4)


class LoopWatchTest(unittest.TestCase):
    def test_a_stall_is_reported_with_the_code_that_held_the_loop(self):
        reports = []

        async def scenario():
            watch = LoopWatch(asyncio.get_running_loop(), limit=0.15,
                              report=lambda seconds, stack: reports.append((seconds, stack)))
            watch.start()
            await asyncio.sleep(0.2)
            blocking_handler()
            await asyncio.sleep(0.3)
            watch.stop()

        asyncio.run(scenario())
        self.assertEqual(len(reports), 1)
        seconds, stack = reports[0]
        self.assertGreater(seconds, 0.3)
        self.assertIn('blocking_handler', stack)
        self.assertIn('test_loop_watch.py', stack)

    def test_a_responsive_loop_reports_nothing(self):
        reports = []

        async def scenario():
            watch = LoopWatch(asyncio.get_running_loop(), limit=0.15,
                              report=lambda *args: reports.append(args))
            watch.start()
            for _ in range(10):
                await asyncio.sleep(0.03)
            watch.stop()

        asyncio.run(scenario())
        self.assertEqual(reports, [])
