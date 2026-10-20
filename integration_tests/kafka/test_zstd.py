"""
Kafka topics compressed with Zstandard, as Amplitude runs its own Kafka and as its
Confluent destination's topics can be: an exported stream reads events a producer wrote
with zstd, and its sink writes them with zstd when its librdkafka settings say so. The
engine's librdkafka had no zstd, so the read failed to decompress, and the sink set lz4
over the compression the settings asked for.
"""
import json
import uuid

from kafka import KafkaProducer

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_exported_stream import committed, consume
from integration_tests.kafka.test_stream_conformance import (  # noqa: F401
    make_pipeline,
    out_consumer,
    topics,
    wait_for,
)

COUNT = 300


def amplitude_events(count):
    """Events as Amplitude's Confluent destination produces them: keyed by user_id."""
    return [
        (f'user-{i % 13}', dict(id=i, uuid=f'00000000-0000-0000-0000-{i:012d}',
                                event_type='Purchase', event_properties=dict(price=i / 4)))
        for i in range(count)
    ]


def test_a_stream_reads_and_writes_zstd_topics(
    mage_project, kafka_bootstrap, topics, tmp_path,  # noqa: F811
):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap, compression_type='zstd',
        key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode(),
    )
    for key, value in amplitude_events(COUNT):
        producer.send(topics[0], key=key, value=value)
    producer.flush()
    producer.close()

    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group,
                             sink_librdkafka={'compression.type': 'zstd'})
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        wait_for(lambda: committed(kafka_bootstrap, group, topics[0]) == COUNT)
        out = consume(out_consumer(kafka_bootstrap, topics[1]), COUNT)
        assert sorted(m['id'] for m in out) == list(range(COUNT))
        assert out[0]['event_properties']['price'] == out[0]['id'] / 4
    finally:
        if service:
            service.stop()
        pipeline.delete()
