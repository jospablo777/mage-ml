"""
The streaming source and sink templates a new block starts from, with only the
connection settings a user edits changed. The Kafka templates set api_version 0.10.2,
which failed against Kafka 4.
"""
import json
from pathlib import Path

import yaml

TEMPLATES = Path(__file__).resolve().parents[2] / 'mage_ai' / 'data_preparation' / 'templates'


def template(kind, name, **settings):
    config = yaml.safe_load((TEMPLATES / kind / 'streaming' / f'{name}.yaml').read_text())
    config.update(settings)
    return config


def test_kafka_templates(kafka_bootstrap, kafka_topic):
    from mage_ai.streaming.sinks.sink_factory import SinkFactory
    from mage_ai.streaming.sources.source_factory import SourceFactory

    sink = SinkFactory.get_sink(template(
        'data_exporters', 'kafka', bootstrap_server=kafka_bootstrap, topic=kafka_topic,
    ))
    sink.batch_write([{'n': 1}])
    sink.destroy()

    source = SourceFactory.get_source(template(
        'data_loaders', 'kafka', bootstrap_server=kafka_bootstrap, topic=kafka_topic,
        consumer_group='templates', auto_offset_reset='earliest',
    ))
    source.test_connection()
    received = []

    def handle(messages):
        received.extend(messages)
        raise StopIteration

    try:
        source.batch_read(handle)
    except StopIteration:
        pass
    source.destroy()
    assert received == [{'n': 1}]


def test_rabbitmq_templates(rabbitmq_settings, rabbitmq_channel, rabbitmq_queue):
    from mage_ai.streaming.sinks.sink_factory import SinkFactory
    from mage_ai.streaming.sources.source_factory import SourceFactory

    sink = SinkFactory.get_sink(
        template('data_exporters', 'rabbitmq', queue_name=rabbitmq_queue, **rabbitmq_settings),
    )
    sink.batch_write([{'n': 1}])
    sink.destroy()

    source = SourceFactory.get_source(
        template('data_loaders', 'rabbitmq', queue_name=rabbitmq_queue, **rabbitmq_settings),
    )
    received = []

    def handle(message, **kwargs):
        received.append(json.loads(message.body))
        raise StopIteration

    try:
        source.batch_read(handle)
    except StopIteration:
        pass
    source.destroy()
    assert received == [{'n': 1}]
