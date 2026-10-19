"""
Reading NATS dead letters ends on the consumer's pending count, and either timeout nats-py
raises on an empty wait (its own or asyncio's) is not an error (I-215).
"""
import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

from nats.errors import TimeoutError as NatsTimeout

from mage_ai.streaming import dead_letters


def letter(sequence, pending):
    return SimpleNamespace(
        headers={'mage-error': f'error {sequence}', 'mage-stream': 'orders'},
        data=b'{}',
        metadata=SimpleNamespace(
            sequence=SimpleNamespace(stream=sequence), num_pending=pending,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
    )


class Subscription:
    def __init__(self, fetches, pending):
        self.fetches = list(fetches)
        self.pending = list(pending)
        self.fetch_calls = 0

    async def fetch(self, batch, timeout):
        self.fetch_calls += 1
        result = self.fetches.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    async def consumer_info(self):
        return SimpleNamespace(name='reader', num_pending=self.pending.pop(0)
                               if self.pending else 0)

    async def unsubscribe(self):
        pass


def read(subscription, total):
    info = SimpleNamespace(state=SimpleNamespace(
        subjects={'dead': total}, first_seq=1, last_seq=total))
    js = mock.MagicMock()
    js.find_stream_name_by_subject = mock.AsyncMock(return_value='letters')
    js.stream_info = mock.AsyncMock(return_value=info)
    js.pull_subscribe = mock.AsyncMock(return_value=subscription)
    js.delete_consumer = mock.AsyncMock()
    connection = mock.MagicMock()
    connection.jetstream.return_value = js
    connection.close = mock.AsyncMock()
    with mock.patch('nats.connect', mock.AsyncMock(return_value=connection)), \
            mock.patch.object(dead_letters, '_nats_settings', return_value={}):
        return asyncio.run(dead_letters._read_nats({'subject': 'dead'}, 10))


class ReadNatsTest(unittest.TestCase):
    def test_an_empty_wait_with_letters_pending_keeps_reading(self):
        # asyncio's TimeoutError, then nats-py's, while the server still holds letters.
        subscription = Subscription(
            [asyncio.TimeoutError(), NatsTimeout(), [letter(1, 1), letter(2, 0)]],
            pending=[2, 2],
        )

        letters, total = read(subscription, 2)

        self.assertEqual([d['offset'] for d in letters], [2, 1])
        self.assertEqual(total, 2)

    def test_the_last_pending_letter_ends_the_read_without_another_wait(self):
        subscription = Subscription([[letter(1, 0)]], pending=[])

        letters, _ = read(subscription, 1)

        self.assertEqual(len(letters), 1)
        self.assertEqual(subscription.fetch_calls, 1)

    def test_an_empty_wait_with_nothing_pending_ends_the_read(self):
        subscription = Subscription([[letter(1, 3)], asyncio.TimeoutError()], pending=[0])

        letters, _ = read(subscription, 4)

        self.assertEqual(len(letters), 1)
        self.assertEqual(subscription.fetch_calls, 2)


if __name__ == '__main__':
    unittest.main()
