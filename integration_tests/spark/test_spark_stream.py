"""
Streaming pipelines on Spark Structured Streaming (ADR-0047), on a Spark Connect server
(services/spark) and Kafka: the pipeline test_exported_stream runs on the engine, with the
same messages and the same output, its Python block run by the engine's worker code in
mapInArrow. A restart continues from the checkpoint, and a failing block stops the query
with the block's name and reason.
"""
import time
import uuid

from kafka import KafkaConsumer

from integration_tests.kafka.test_exported_stream import (  # noqa: F401 - fixtures
    consume,
    produce,
    stream_pipeline,
    topics,
)

# Spark reaches Kafka on the compose network.
SPARK_ENVIRON = dict(KAFKA_BOOTSTRAP='kafka:9092')


def expected(ids):
    # TRANSFORM: every message doubled, but those whose id ends in 0.
    return sorted((i, i * 2) for i in ids if i % 10 != 0)


def started(mage_project, pipeline, spark_remote, checkpoint):
    from mage_ai.pipeline_services import spark_stream

    stream = spark_stream.plan(mage_project, pipeline.uuid, environ=SPARK_ENVIRON)
    spark, query = spark_stream.start(stream, spark_remote, checkpoint)
    assert spark_stream.python_version_check(spark) is None
    return query


def wait_until_idle(query, timeout=60):
    """Until the query has read everything and no batch runs, so its last one is committed."""
    deadline = time.time() + timeout
    while time.time() < deadline and query.isActive:
        status = query.status
        if not status['isDataAvailable'] and not status['isTriggerActive']:
            return
        time.sleep(0.5)
    raise AssertionError(f'The query is still busy: {query.status}')


def test_the_engines_pipeline_runs_on_spark_and_a_restart_continues(
    kafka_bootstrap, spark_remote, mage_project, stream_pipeline, topics,  # noqa: F811
):
    pipeline, _ = stream_pipeline
    checkpoint = f'/checkpoints/{uuid.uuid4().hex}'
    out = KafkaConsumer(topics[1], bootstrap_servers=kafka_bootstrap,
                        auto_offset_reset='earliest', enable_auto_commit=False, group_id=None)
    produce(kafka_bootstrap, topics[0], range(200))

    query = started(mage_project, pipeline, spark_remote, checkpoint)
    try:
        first = consume(out, len(expected(range(200))))
        assert sorted((m['id'], m['doubled']) for m in first) == expected(range(200))
        wait_until_idle(query)
    finally:
        query.stop()

    # Stopped: the next run starts where the checkpoint says, so only new messages go out.
    produce(kafka_bootstrap, topics[0], range(200, 260))
    query = started(mage_project, pipeline, spark_remote, checkpoint)
    try:
        second = consume(out, len(expected(range(200, 260))))
        assert sorted((m['id'], m['doubled']) for m in second) == expected(range(200, 260))
        wait_until_idle(query)
        assert consume(out, 1, timeout=3) == []
    finally:
        query.stop()
        out.close()


def test_a_failing_block_stops_the_query_with_its_name_and_reason(
    kafka_bootstrap, spark_remote, mage_project, stream_pipeline, topics,  # noqa: F811
):
    pipeline, _ = stream_pipeline
    pipeline.variables = dict(reject=[7])
    pipeline.save()
    produce(kafka_bootstrap, topics[0], range(20))

    query = started(mage_project, pipeline, spark_remote, f'/checkpoints/{uuid.uuid4().hex}')
    deadline = time.time() + 180
    while query.isActive and time.time() < deadline:
        time.sleep(1)
    error = query.exception()
    assert error is not None
    assert f'Block {pipeline.uuid}_double failed (run)' in str(error)
    assert 'message 7 is not accepted' in str(error)
