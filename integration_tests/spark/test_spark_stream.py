"""
Streaming pipelines on Spark Structured Streaming (ADR-0047), on a Spark Connect server
(services/spark) and Kafka: the pipeline test_exported_stream runs on the engine, with the
same messages and the same output, its Python block run by the engine's worker code in
mapInArrow. A restart continues from the checkpoint, a failing block stops the query with
the block's name and reason, several sinks and a dead-letter sink each get their messages,
and the export runs the stream from a container of its own.
"""
import subprocess
import textwrap
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.kafka.test_exported_stream import (  # noqa: F401 - fixtures
    SINK,
    TRANSFORM,
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
    from mage_ai.pipeline_services.runtime import spark_runner

    stream = spark_stream.plan(mage_project, pipeline.uuid)
    spark, query = spark_runner.start(stream, spark_remote, checkpoint, environ=SPARK_ENVIRON)
    assert spark_runner.python_version_check(spark) is None
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


def compose_network(spark_remote) -> str:
    """The network of the container that publishes the Spark Connect port."""
    port = urlparse(spark_remote.replace('sc://', 'http://')).port
    found = subprocess.run(
        ['docker', 'ps', '--filter', f'publish={port}', '--format', '{{.Networks}}'],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    assert found, f'No container publishes {port}'
    return found[0].split(',')[0]


def test_the_export_runs_the_stream_from_its_own_container_and_stops_on_sigterm(
    kafka_bootstrap, spark_remote, mage_project, stream_pipeline, topics, tmp_path,  # noqa: F811
):
    from mage_ai.pipeline_services import spark_stream

    pipeline, _ = stream_pipeline
    folder = spark_stream.export(mage_project, pipeline.uuid, str(tmp_path / 'spark'))
    image = f'mageml-spark-stream-it:{uuid.uuid4().hex[:8]}'
    subprocess.run(['docker', 'build', '-q', '-t', image, str(folder)], check=True,
                   capture_output=True, timeout=900)
    out = KafkaConsumer(topics[1], bootstrap_servers=kafka_bootstrap,
                        auto_offset_reset='earliest', enable_auto_commit=False, group_id=None)
    produce(kafka_bootstrap, topics[0], range(50))
    container = subprocess.run([
        'docker', 'run', '-d', '--network', compose_network(spark_remote),
        '-e', 'MAGE_SPARK_REMOTE=sc://spark:15002',
        '-e', f'MAGE_SPARK_CHECKPOINT=/checkpoints/{uuid.uuid4().hex}',
        '-e', 'MAGE_SPARK_PROGRESS_SECONDS=1',
        '-e', 'KAFKA_BOOTSTRAP=kafka:9092',
        image,
    ], capture_output=True, text=True, check=True).stdout.strip()
    try:
        messages = consume(out, len(expected(range(50))))
        assert sorted((m['id'], m['doubled']) for m in messages) == expected(range(50))
        subprocess.run(['docker', 'stop', '--time', '60', container], check=True,
                       capture_output=True, timeout=90)
        logs = subprocess.run(['docker', 'logs', container], capture_output=True,
                              text=True).stdout
        assert f'Started {pipeline.uuid}' in logs, logs
        assert 'Stopped; the next start continues from the checkpoint.' in logs, logs
        status = subprocess.run(['docker', 'inspect', '-f', '{{.State.ExitCode}}', container],
                                capture_output=True, text=True).stdout.strip()
        assert status == '0', logs
    finally:
        out.close()
        subprocess.run(['docker', 'rm', '-f', container], capture_output=True)
        subprocess.run(['docker', 'rmi', '-f', image], capture_output=True)


def test_several_sinks_and_dead_letters_each_get_their_messages(
    kafka_bootstrap, spark_remote, mage_project, stream_pipeline, topics,  # noqa: F811
):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline, _ = stream_pipeline
    extra = [f'it_copy_{uuid.uuid4().hex[:10]}', f'it_dead_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=1) for n in extra])
    double = pipeline.get_block(f'{pipeline.uuid}_double')
    for suffix, topic, more in [('copy', extra[0], ''), ('dead', extra[1], 'dead_letter: true\n')]:
        block = Block.create(f'{pipeline.uuid}_{suffix}', 'data_exporter', mage_project,
                             language='yaml', pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(SINK % dict(topic=topic)) + more)
        pipeline.add_block(block, upstream_block_uuids=[double.uuid])
    pipeline = Pipeline.get(pipeline.uuid, repo_path=mage_project)
    pipeline.variables = dict(reject=[7])
    pipeline.save()
    consumers = [KafkaConsumer(t, bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
                               enable_auto_commit=False, group_id=None)
                 for t in (topics[1], *extra)]
    produce(kafka_bootstrap, topics[0], range(20))

    query = started(mage_project, pipeline, spark_remote, f'/checkpoints/{uuid.uuid4().hex}')
    try:
        kept = expected(i for i in range(20) if i != 7)
        for consumer in consumers[:2]:
            got = consume(consumer, len(kept))
            assert sorted((m['id'], m['doubled']) for m in got) == kept
        dead = []
        deadline = time.time() + 60
        while not dead and time.time() < deadline:
            for records in consumers[2].poll(timeout_ms=500).values():
                dead.extend(records)
        assert [r.value for r in dead] == [b'{"id": 7, "amount": 7}']
        headers = dict(dead[0].headers)
        assert headers['mage-stream'] == pipeline.uuid.encode()
        assert b'message 7 is not accepted' in headers['mage-error']
        assert dead[0].key == b'7'
    finally:
        query.stop()
        for consumer in consumers:
            consumer.close()
        admin.delete_topics(extra)
        admin.close()
