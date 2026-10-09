"""
Mage's Kafka sink and source (mage_ai.streaming) against Kafka 4.1.
"""
import datetime as dt
import decimal
import json
import uuid

import pytest
from kafka import KafkaConsumer
from kafka.errors import MessageSizeTooLargeError

from mage_ai.streaming.sinks.kafka import KafkaSink
from mage_ai.streaming.sources.kafka import KafkaSource


class Stop(Exception):
    """Raised by a handler to end a source's endless read."""


def sink(bootstrap, topic, **config):
    return KafkaSink(dict(bootstrap_server=bootstrap, topic=topic, **config))


def source(bootstrap, topic, group=None, **config):
    return KafkaSource(dict(
        bootstrap_server=bootstrap,
        topic=topic,
        consumer_group=group or f'group_{uuid.uuid4().hex[:8]}',
        auto_offset_reset='earliest',
        **config,
    ))


def collect(read, count):
    """Read until count messages arrive, and return them."""
    messages = []

    def handler(received):
        messages.extend(received if isinstance(received, list) else [received])
        if len(messages) >= count:
            raise Stop()

    with pytest.raises(Stop):
        read(handler)
    return messages


def test_the_default_settings_work_with_kafka_4(kafka_bootstrap, kafka_topic):
    """
    api_version defaulted to 0.10.2, so every request timed out against Kafka 4, which
    removed the protocol versions older than 2.1.
    """
    sink(kafka_bootstrap, kafka_topic).write({'id': 1})

    messages = collect(source(kafka_bootstrap, kafka_topic).batch_read, 1)

    assert messages == [{'id': 1}]


def test_batches_round_trip(kafka_bootstrap, kafka_topic):
    sent = [
        {
            'id': i,
            'text': f'ñ "{i}"',
            'at': dt.datetime(2024, 1, 1, 0, 0, i % 60),
            'amount': decimal.Decimal('1.10'),
            'tags': ['a', None],
        }
        for i in range(500)
    ]

    sink(kafka_bootstrap, kafka_topic).batch_write(sent)
    received = collect(source(kafka_bootstrap, kafka_topic, batch_size=100).batch_read, 500)

    # Dates and decimals are written as JSON; they failed to serialize.
    assert sorted(received, key=lambda m: m['id']) == [
        dict(m, at=m['at'].isoformat(), amount=1.1) for m in sent
    ]


def test_sent_messages_are_on_the_broker_when_batch_write_returns(kafka_bootstrap, kafka_topic):
    """
    Messages stayed in the producer's buffer after batch_write returned, while the
    source committed the offsets of the messages it had read; a crash lost them.
    """
    kafka_sink = sink(kafka_bootstrap, kafka_topic, timeout_ms=60_000)

    kafka_sink.batch_write([{'id': i} for i in range(10)])

    consumer = KafkaConsumer(
        kafka_topic, bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
    )
    assert sorted(json.loads(m.value)['id'] for m in consumer) == list(range(10))
    consumer.close()


def test_failed_sends_raise(kafka_bootstrap, kafka_topic):
    with pytest.raises(MessageSizeTooLargeError):
        sink(kafka_bootstrap, kafka_topic).batch_write([{'blob': 'x' * 2_000_000}])


def test_single_reads_commit_their_offsets(kafka_bootstrap, kafka_topic):
    """
    read() never committed, so a consumer group that restarted read every message again
    or skipped the ones that came while it was down.
    """
    sink(kafka_bootstrap, kafka_topic).batch_write([{'id': i} for i in range(10)])
    group = f'group_{uuid.uuid4().hex[:8]}'

    first = source(kafka_bootstrap, kafka_topic, group=group)
    seen = collect(first.read, 4)
    first.consumer.close()

    second = source(kafka_bootstrap, kafka_topic, group=group)
    rest = collect(second.read, 7)

    first_ids = [m['id'] for m in seen]
    rest_ids = [m['id'] for m in rest]
    # The handler of the fourth message raised before its commit, so it comes again.
    assert sorted(set(first_ids) | set(rest_ids)) == list(range(10))
    assert len(set(first_ids[:3]) & set(rest_ids)) == 0


def test_metadata(kafka_bootstrap, kafka_topic):
    sink(kafka_bootstrap, kafka_topic).write({'data': {'id': 1}, 'metadata': {'key': 'k1'}})

    message = collect(source(kafka_bootstrap, kafka_topic, include_metadata=True).read, 1)[0]

    assert message['data'] == {'id': 1}
    assert message['metadata']['key'] == 'k1'
    assert message['metadata']['topic'] == kafka_topic
    assert message['metadata']['offset'] == 0
