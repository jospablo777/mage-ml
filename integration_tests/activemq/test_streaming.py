"""
Mage's ActiveMQ source and sink, which use STOMP, against ActiveMQ Classic 6.2.
"""
import datetime as dt
import decimal
import json
import threading
import time
import uuid

import pytest
import stomp


class Stop(Exception):
    pass


@pytest.fixture
def queue_name():
    return f'it_{uuid.uuid4().hex[:12]}'


def connect(port):
    connection = stomp.Connection11([('127.0.0.1', port)])
    connection.connect('admin', 'admin', wait=True)
    return connection


def publish(port, queue, *bodies):
    connection = connect(port)
    for body in bodies:
        connection.send(destination=f'/queue/{queue}', body=json.dumps(body))
    connection.disconnect()


class Collector(stomp.ConnectionListener):
    def __init__(self):
        self.frames = []

    def on_message(self, frame):
        self.frames.append(frame)


def drain(port, queue, wait=2.0):
    """The messages left in the queue, each acked."""
    connection = connect(port)
    collector = Collector()
    connection.set_listener('collector', collector)
    connection.subscribe(destination=f'/queue/{queue}', id='drain', ack='client-individual')
    time.sleep(wait)
    for frame in collector.frames:
        connection.ack(frame.headers['message-id'], 'drain')
    connection.disconnect()
    return collector.frames


def config(port, queue, **extra):
    return dict(
        connection_host='127.0.0.1', connection_port=port, queue_name=queue, **extra,
    )


def run_source(reader, handler, timeout=20):
    """batch_read in a thread; the old source never returned, whatever the handler did."""
    errors = []

    def target():
        try:
            reader.batch_read(handler)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        reader.destroy()
        raise AssertionError('batch_read did not return')
    reader.destroy()
    return errors[0] if errors else None


def source(port, queue, **extra):
    from mage_ai.streaming.sources.activemq import ActiveMQSource

    return ActiveMQSource(config(port, queue, **extra))


def test_messages_are_acked_after_the_handler(activemq_port, queue_name):
    """Subscriptions acked on delivery, so a failed handler lost its message."""
    publish(activemq_port, queue_name, 1, 2, 3, 'stop')
    received = []

    def handle(messages):
        if '"stop"' in messages:
            raise Stop
        received.extend(messages)

    error = run_source(source(activemq_port, queue_name), handle)

    assert isinstance(error, Stop)
    assert [json.loads(m) for m in received] == [1, 2, 3]
    assert [json.loads(f.body) for f in drain(activemq_port, queue_name)] == ['stop']


def test_a_failed_handler_stops_the_source(activemq_port, queue_name):
    """
    The handler ran on the STOMP receiver thread, which swallowed its errors, while the
    source slept forever.
    """
    publish(activemq_port, queue_name, 1)

    def fail(messages):
        raise ValueError('transformer failed')

    error = run_source(source(activemq_port, queue_name), fail)

    assert isinstance(error, ValueError)
    assert [json.loads(f.body) for f in drain(activemq_port, queue_name)] == [1]


def test_batches(activemq_port, queue_name):
    publish(activemq_port, queue_name, *range(25))
    sizes = []

    def handle(messages):
        sizes.append(len(messages))
        if sum(sizes) >= 25:
            raise Stop

    error = run_source(
        source(activemq_port, queue_name, batch_size=10, batch_timeout=0.5), handle,
    )

    assert isinstance(error, Stop)
    assert sizes == [10, 10, 5]


def test_sink_writes_persistent_json(activemq_port, queue_name):
    """Dates and decimals failed json.dumps, and messages were not persistent."""
    from mage_ai.streaming.sinks.activemq import ActiveMQSink

    sink = ActiveMQSink(config(activemq_port, queue_name))
    sink.batch_write([
        dict(
            data=dict(at=dt.datetime(2024, 1, 1, 12, 0, 0, 123456),
                      amount=decimal.Decimal('1.10'), text='ñ'),
            metadata=dict(source='mage'),
        ),
        dict(n=2),
    ])
    sink.destroy()

    frames = drain(activemq_port, queue_name)
    assert json.loads(frames[0].body) == dict(
        at='2024-01-01T12:00:00.123456', amount=1.1, text='ñ',
    )
    assert frames[0].headers['source'] == 'mage'
    assert json.loads(frames[1].body) == dict(n=2)
    assert {f.headers['persistent'] for f in frames} == {'true'}
