"""
Mage's RabbitMQ source and sink against RabbitMQ 4.3. The broker's password has
characters that a URL must quote.
"""
import datetime as dt
import decimal
import json
import threading
import time
import uuid

import pika
import pytest


class Stop(Exception):
    pass


def source(settings, queue, **config):
    from mage_ai.streaming.sources.rabbitmq import RabbitMQSource

    return RabbitMQSource(dict(settings, queue_name=queue, **config))


def sink(settings, queue):
    from mage_ai.streaming.sinks.rabbitmq import RabbitMQSink

    return RabbitMQSink(dict(settings, queue_name=queue))


def publish(channel, queue, *bodies):
    for body in bodies:
        channel.basic_publish('', queue, json.dumps(body).encode())


def queue_state(channel, queue):
    """Ready messages and consumers. Unacked messages count once their consumer is gone."""
    method = channel.queue_declare(queue, passive=True).method
    return method.message_count, method.consumer_count


def consume(reader, handler=None):
    """Messages the source hands over until it reads the message "stop"."""
    received = []

    def handle(message, **kwargs):
        if message.body == b'"stop"':
            raise Stop
        if handler:
            handler(message, **kwargs)
        received.append(message)

    with pytest.raises(Stop):
        reader.batch_read(handle)
    reader.destroy()
    return received


def wait_for_state(channel, queue, expected):
    deadline = time.time() + 10
    while queue_state(channel, queue) != expected and time.time() < deadline:
        time.sleep(0.1)
    return queue_state(channel, queue)


def test_messages_are_acked_once_handled(rabbitmq_settings, rabbitmq_channel, rabbitmq_queue):
    """
    Messages were never acked, so a restarted pipeline read every message again; the
    source never closed its connection.
    """
    publish(rabbitmq_channel, rabbitmq_queue, *range(5), 'stop')

    received = consume(source(rabbitmq_settings, rabbitmq_queue))

    assert [json.loads(m.body) for m in received] == list(range(5))
    # The stop message failed its handler and is back in the queue.
    assert wait_for_state(rabbitmq_channel, rabbitmq_queue, (1, 0)) == (1, 0)


def test_messages_the_transformer_acks_are_not_acked_twice(
    rabbitmq_settings, rabbitmq_channel, rabbitmq_queue,
):
    """The documented transformer acks with kwargs['channel']."""
    publish(rabbitmq_channel, rabbitmq_queue, 1, 2, 3, 'stop')

    def ack(message, channel, **kwargs):
        channel.basic_ack(message.method.delivery_tag)

    received = consume(source(rabbitmq_settings, rabbitmq_queue), ack)

    assert len(received) == 3
    assert wait_for_state(rabbitmq_channel, rabbitmq_queue, (1, 0)) == (1, 0)


def test_a_failed_handler_leaves_its_message_in_the_queue(
    rabbitmq_settings, rabbitmq_channel, rabbitmq_queue,
):
    publish(rabbitmq_channel, rabbitmq_queue, 1, 2)
    reader = source(rabbitmq_settings, rabbitmq_queue)

    def fail(message, **kwargs):
        raise ValueError('transformer failed')

    with pytest.raises(ValueError):
        reader.batch_read(fail)
    reader.destroy()

    assert wait_for_state(rabbitmq_channel, rabbitmq_queue, (2, 0)) == (2, 0)


def test_inactivity_timeouts_do_not_reach_the_handler(
    rabbitmq_settings, rabbitmq_channel, rabbitmq_queue,
):
    """Each inactivity timeout called the handler with a message of None values."""
    publish(rabbitmq_channel, rabbitmq_queue, 1)
    reader = source(
        rabbitmq_settings, rabbitmq_queue, configure_consume=True,
        consume_config=dict(auto_ack=False, exclusive=False, inactivity_timeout=0.1),
    )

    def publish_stop_later():
        time.sleep(1)
        connection = pika.BlockingConnection(rabbitmq_channel.connection._impl.params)
        publish(connection.channel(), rabbitmq_queue, 'stop')
        connection.close()

    thread = threading.Thread(target=publish_stop_later)
    thread.start()
    received = consume(reader)
    thread.join()

    assert [m.body for m in received] == [b'1']


def test_deliveries_are_bounded_by_the_prefetch_count(
    rabbitmq_settings, rabbitmq_channel, rabbitmq_queue,
):
    """The broker sent the whole queue to the source, which held it in memory."""
    publish(rabbitmq_channel, rabbitmq_queue, *range(300), 'stop')
    ready = []

    def check(message, **kwargs):
        if not ready:
            ready.append(queue_state(rabbitmq_channel, rabbitmq_queue)[0])
            raise Stop

    reader = source(rabbitmq_settings, rabbitmq_queue, prefetch_count=50)
    with pytest.raises(Stop):
        reader.batch_read(check)
    reader.destroy()

    assert ready == [301 - 50]


def test_credentials_are_not_printed(rabbitmq_settings, rabbitmq_queue, capsys):
    """The source and the sink printed the connection URL with the password."""
    source(rabbitmq_settings, rabbitmq_queue).destroy()
    sink(rabbitmq_settings, rabbitmq_queue).destroy()

    output = capsys.readouterr().out
    assert rabbitmq_settings['password'] not in output
    assert 'Connected' in output


def test_sink_writes_every_value(rabbitmq_settings, rabbitmq_channel, rabbitmq_queue):
    """Dates, decimals and UUIDs failed json.dumps."""
    key = uuid.UUID(int=7)
    writer = sink(rabbitmq_settings, rabbitmq_queue)
    writer.batch_write([
        dict(
            data=dict(
                at=dt.datetime(2024, 1, 1, 12, 0, 0, 123456), day=dt.date(2024, 1, 2),
                amount=decimal.Decimal('1.10'), key=key, missing=float('nan'),
                nested={'a': [1, None]}, text='ñ "q"',
            ),
            metadata=dict(source='mage'),
        ),
        dict(plain=1),
        'text',
    ])
    writer.destroy()

    messages = []
    for _ in range(3):
        method, properties, body = rabbitmq_channel.basic_get(rabbitmq_queue, auto_ack=True)
        messages.append((properties, json.loads(body, parse_float=decimal.Decimal)))
    assert messages[0][1] == dict(
        at='2024-01-01T12:00:00.123456', day='2024-01-02', amount=decimal.Decimal('1.10'),
        key=str(key),
        missing=None, nested={'a': [1, None]}, text='ñ "q"',
    )
    assert messages[0][0].headers == {'source': 'mage'}
    assert [m[1] for m in messages[1:]] == [dict(plain=1), 'text']
    # Messages survive a broker restart in a durable queue.
    assert {m[0].delivery_mode for m in messages} == {2}


def test_unroutable_messages_raise(rabbitmq_settings):
    """A message to a missing queue was dropped without an error."""
    writer = sink(rabbitmq_settings, f'missing_{uuid.uuid4().hex}')

    with pytest.raises(Exception, match='Unroutable|NO_ROUTE|returned'):
        writer.batch_write([dict(a=1)])
    writer.destroy()


def test_batches(rabbitmq_settings, rabbitmq_channel, rabbitmq_queue):
    """With batch_size, the handler gets lists, the last one after batch_timeout."""
    publish(rabbitmq_channel, rabbitmq_queue, *range(25))
    reader = source(rabbitmq_settings, rabbitmq_queue, batch_size=10, batch_timeout=0.3)
    sizes = []

    def handle(messages, **kwargs):
        if any(m.body == b'"stop"' for m in messages):
            raise Stop
        sizes.append(len(messages))

    def publish_stop_later():
        time.sleep(1.5)
        connection = pika.BlockingConnection(rabbitmq_channel.connection._impl.params)
        publish(connection.channel(), rabbitmq_queue, 'stop')
        connection.close()

    thread = threading.Thread(target=publish_stop_later)
    thread.start()
    with pytest.raises(Stop):
        reader.batch_read(handle)
    reader.destroy()
    thread.join()

    assert sizes == [10, 10, 5]
    assert wait_for_state(rabbitmq_channel, rabbitmq_queue, (1, 0)) == (1, 0)
