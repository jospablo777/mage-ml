"""A terminal's shell starts in a thread: forking the server held its event loop ~1 s."""
import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mage_ai.server.terminal_server import MageTermManager


class TerminalSpawnTest(unittest.TestCase):
    def test_starting_a_terminal_leaves_the_event_loop_free(self):
        manager = MageTermManager(shell_command=['bash'], max_terminals=2)

        def slow_new_terminal(**kwargs):
            time.sleep(0.3)
            return SimpleNamespace(kill=lambda: None)

        async def scenario():
            ticks = []

            async def ticker():
                for _ in range(25):
                    ticks.append(time.monotonic())
                    await asyncio.sleep(0.02)

            with patch.object(manager, 'new_terminal', side_effect=slow_new_terminal), \
                    patch.object(manager, 'start_reading') as start_reading:
                ticking = asyncio.ensure_future(ticker())
                await asyncio.sleep(0.05)
                first, second = await asyncio.gather(
                    manager.get_terminal_async('tty'), manager.get_terminal_async('tty'),
                )
                await ticking
            return first, second, start_reading.call_count, max(
                b - a for a, b in zip(ticks, ticks[1:])
            )

        first, second, reads, longest_gap = asyncio.run(scenario())
        self.assertLess(longest_gap, 0.2)
        # Two connections asking at once share one terminal.
        self.assertIs(first, second)
        self.assertEqual(reads, 1)
        self.assertEqual(list(manager.terminals), ['tty'])

    def test_a_terminal_whose_shell_exited_is_started_again(self):
        manager = MageTermManager(shell_command=['bash'], max_terminals=2)
        dead = SimpleNamespace(ptyproc=SimpleNamespace(closed=True, isalive=lambda: False),
                               kill=lambda: None)
        fresh = SimpleNamespace(ptyproc=SimpleNamespace(closed=False, isalive=lambda: True),
                                kill=lambda: None)
        manager.terminals['tty'] = dead

        async def scenario():
            with patch.object(manager, 'new_terminal', return_value=fresh), \
                    patch.object(manager, 'start_reading'):
                return await manager.get_terminal_async('tty')

        self.assertIs(asyncio.run(scenario()), fresh)
        self.assertIs(manager.terminals['tty'], fresh)
        # A live one is kept.
        with patch.object(manager, 'new_terminal') as new_terminal:
            self.assertIs(manager.get_terminal('tty'), fresh)
        new_terminal.assert_not_called()
