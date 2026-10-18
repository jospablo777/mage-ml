"""
The Kafka source's connection report (the block's Check connection): partitions, retained
messages and retention, and where the consumer group stands, read without joining it.
"""
import io
import json
import uuid
from contextlib import redirect_stdout

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata


def produce(bootstrap, topic, count):
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    for i in range(count):
        producer.send(topic, json.dumps(dict(n=i)).encode(), partition=i % 2)
    producer.flush()
    producer.close()


def check(bootstrap, topic, group, reset='latest'):
    from mage_ai.data_preparation.models.block.streaming import check_connection

    out = io.StringIO()
    with redirect_stdout(out):
        check_connection('data_loader', (
            f'connector_type: kafka\nbootstrap_server: {bootstrap}\ntopic: {topic}\n'
            f'consumer_group: {group}\nauto_offset_reset: {reset}\n'
        ))
    return out.getvalue()


def test_the_report_says_what_a_new_group_would_read(kafka_bootstrap, kafka_topic):
    produce(kafka_bootstrap, kafka_topic, 10)
    group = f'it_{uuid.uuid4().hex[:8]}'

    earliest = check(kafka_bootstrap, kafka_topic, group, 'earliest')
    latest = check(kafka_bootstrap, kafka_topic, group, 'latest')

    assert f'Topic {kafka_topic}: 2 partitions, 10 messages retained, kept ' in earliest
    assert 'its first run reads all 10 retained messages' in earliest
    assert 'its first run starts at the end and skips the 10 retained messages' in latest


def test_the_report_counts_how_far_a_group_is_behind(kafka_bootstrap, kafka_topic):
    produce(kafka_bootstrap, kafka_topic, 10)
    group = f'it_{uuid.uuid4().hex[:8]}'
    consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=group,
                             enable_auto_commit=False)
    # 3 of the 5 messages on partition 0 are processed; partition 1 has nothing committed.
    consumer.commit({TopicPartition(kafka_topic, 0): OffsetAndMetadata(3, '', -1)})
    consumer.close()

    report = check(kafka_bootstrap, kafka_topic, group)

    assert f'Consumer group {group}: committed on 1 of 2 partitions, 2 messages behind.' \
        in report
    # The check joined nothing: the group's committed offsets are where they were.
    probe = KafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=group)
    assert probe.committed(TopicPartition(kafka_topic, 0)) == 3
    probe.close()
