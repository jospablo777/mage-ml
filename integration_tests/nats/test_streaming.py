"""
Mage's NATS JetStream source against NATS 2.15, with pull and push consumers.
"""
import asyncio
import json
import threading
import time
import uuid

import pytest


class Stop(Exception):
    pass


async def _js(url, action):
    import nats

    connection = await nats.connect(url)
    try:
        return await action(connection.jetstream())
    finally:
        await connection.close()


def jetstream(url, action):
    return asyncio.run(_js(url, action))


@pytest.fixture
def stream(nats_url):
    """A stream with one subject, deleted after the test."""
    name = f'it_{uuid.uuid4().hex[:12]}'
    subject = f'{name}.events'
    jetstream(nats_url, lambda js: js.add_stream(name=name, subjects=[subject]))
    yield name, subject
    jetstream(nats_url, lambda js: js.delete_stream(name))


def publish(url, subject, *payloads):
    async def action(js):
        for payload in payloads:
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            await js.publish(subject, data)

    jetstream(url, action)


def consumer_state(url, name, consumer):
    async def action(js):
        info = await js.consumer_info(name, consumer)
        return info.num_pending, info.num_ack_pending

    return jetstream(url, action)


def source(url, name, subject, **config):
    from mage_ai.streaming.sources.nats_js import NATSSource

    return NATSSource(dict(
        server_url=url, stream_name=name, subject=subject, consumer_name='mage', timeout=0.5,
        **config,
    ))


def read_until_stop(reader, handle):
    with pytest.raises(Stop):
        if reader.consume_method == 'READ':
            reader.read(handle)
        else:
            reader.batch_read(handle)
    reader.destroy()


def test_pulled_messages_are_acked_after_the_handler(nats_url, stream):
    """Messages were acked before the handler ran, so a failed handler lost them."""
    name, subject = stream
    publish(nats_url, subject, *range(5))
    received = []

    def handle(messages):
        if 'stop' in messages:
            raise Stop
        received.extend(messages)
        if len(received) == 5:
            publish(nats_url, subject, 'stop')

    read_until_stop(source(nats_url, name, subject, batch_size=10), handle)
    again = []

    def handle_again(messages):
        again.extend(messages)
        raise Stop

    # The handled messages were acked; the one that stopped the handler comes again.
    read_until_stop(source(nats_url, name, subject, batch_size=10), handle_again)

    assert received == list(range(5))
    assert again == ['stop']


def test_a_failed_handler_gets_its_messages_again(nats_url, stream):
    name, subject = stream
    publish(nats_url, subject, 1, 2)

    def fail(messages):
        raise Stop

    read_until_stop(source(nats_url, name, subject, batch_size=10), fail)
    received = []

    def handle(messages):
        received.extend(messages)
        if len(received) >= 2:
            raise Stop

    # The messages come again once their ack wait passes or the consumer is reconnected.
    read_until_stop(source(nats_url, name, subject, batch_size=10, ack_wait=1), handle)

    assert sorted(received) == [1, 2]


def test_messages_that_are_not_json_reach_the_handler_as_text(nats_url, stream):
    """One message that was not JSON failed every fetch, and it came back each time."""
    name, subject = stream
    publish(nats_url, subject, b'plain text', {'n': 1})
    received = []

    def handle(messages):
        received.extend(messages)
        if len(received) >= 2:
            raise Stop

    read_until_stop(source(nats_url, name, subject, batch_size=10), handle)

    assert received == ['plain text', {'n': 1}]


def test_a_push_consumer_keeps_reading_when_idle(nats_url, stream):
    """The push consumer stopped the first time no message came within the timeout."""
    name, subject = stream
    reader = source(nats_url, name, subject, consumer_type='PUSH')
    received = []

    def publish_later():
        time.sleep(2)
        publish(nats_url, subject, {'n': 1})

    def handle(message):
        received.append(message)
        raise Stop

    thread = threading.Thread(target=publish_later)
    thread.start()
    read_until_stop(reader, handle)
    thread.join()

    assert received == [{'n': 1}]


def test_connection_errors_raise(stream):
    """The error was printed, and the source failed later with an AttributeError."""
    name, subject = stream

    with pytest.raises(Exception, match='nodename|connect|Connect|refused|NoServers'):
        source('nats://127.0.0.1:1', name, subject)


def read_stream(url, name, subject, count):
    async def action(js):
        subscription = await js.pull_subscribe(subject, durable='reader', stream=name)
        messages = await subscription.fetch(count, timeout=5)
        for message in messages:
            await message.ack()
        return [(json.loads(m.data), m.headers) for m in messages]

    return jetstream(url, action)


def sink(url, subject, **config):
    from mage_ai.streaming.sinks.nats_js import NATSSink

    return NATSSink(dict(server_url=url, subject=subject, **config))


def test_sink_publishes_every_value(nats_url, stream):
    import datetime as dt
    import decimal

    name, subject = stream
    writer = sink(nats_url, subject, stream_name=name)
    writer.batch_write([
        dict(
            data=dict(at=dt.datetime(2024, 1, 1, 12, 0, 0, 123456), key=uuid.UUID(int=3),
                      amount=decimal.Decimal('1.10'), text='ñ'),
            metadata=dict(source='mage'),
        ),
        dict(n=2),
    ])
    writer.destroy()

    messages = read_stream(nats_url, name, subject, 2)
    assert messages[0][0] == dict(
        at='2024-01-01T12:00:00.123456', key=str(uuid.UUID(int=3)), amount=1.1, text='ñ',
    )
    # nats-py adds Nats-Expected-Stream when the stream is named.
    assert messages[0][1]['source'] == 'mage'
    assert messages[1][0] == dict(n=2)


def test_sink_raises_when_no_stream_takes_the_subject(nats_url):
    from nats.js.errors import NoStreamResponseError

    writer = sink(nats_url, f'missing_{uuid.uuid4().hex}')

    with pytest.raises(NoStreamResponseError):
        writer.batch_write([dict(n=1)])
    writer.destroy()
