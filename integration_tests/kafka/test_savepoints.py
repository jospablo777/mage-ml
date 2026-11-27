"""
Savepoints (SAVEPOINTS.md): a stream that counts per key with keyed state reads 1,000
messages; a savepoint keeps its state and offsets; it reads 1,000 more; started from the
savepoint it reads those 1,000 again from the counts it had then, so no key is counted past
its true total. The savepoint is taken of a stopped service (`mage-service savepoint`, runs
bounded with stop_at: end) and of a running one (POST /v1/streams/{pipeline}/savepoints).
"""
import json
import subprocess
import textwrap
import uuid
from collections import defaultdict
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_bounded_stream import finished_stream
from integration_tests.kafka.test_exactly_once import COUNT, read_committed
from integration_tests.kafka.test_stream_conformance import wait_for

KEYS = 10


def make_pipeline(project, topics, group, bounded=True):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'savepoint_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=(), configuration=None):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             configuration=configuration)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 100
        timeout_ms: 200
        {'stop_at: end' if bounded else ''}
    ''', 'yaml')
    counted = add('count', 'transformer', COUNT, 'python', [source],
                  configuration=dict(state_key='key'))
    add('out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', 'yaml', [counted])
    return pipeline


def produce(bootstrap, topic, ids):
    from kafka import KafkaProducer

    # Keyed by the key: keyed state is kept per partition, and a key stays in one.
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode())
    for i in ids:
        producer.send(topic, key=f'k{i % KEYS}', value=dict(id=i, key=f'k{i % KEYS}'))
    producer.flush()
    producer.close()


def test_a_stream_started_from_its_savepoint_counts_from_the_state_it_had(
    kafka_bootstrap, mage_project, tmp_path,
):
    topics = [f'it_sp_in_{uuid.uuid4().hex[:8]}', f'it_sp_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    pipeline = make_pipeline(mage_project, topics, f'g_{uuid.uuid4().hex[:8]}')
    binary = stream_service_binary()
    service = None

    def run_to_end(**env):
        nonlocal service
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap, **env)
        service.start()
        wait_for(lambda: finished_stream(service), timeout=180)
        service.stop()

    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        produce(kafka_bootstrap, topics[0], range(1000))
        run_to_end()
        taken = subprocess.run([str(binary), 'savepoint', 'before-v2'], env=service.env,
                               capture_output=True, text=True, timeout=120)
        assert taken.returncode == 0, taken.stdout + taken.stderr
        assert 'savepoint before-v2 with 2 state files and the offsets of 2 partitions' \
            in taken.stdout, taken.stdout

        produce(kafka_bootstrap, topics[0], range(1000, 2000))
        run_to_end()
        run_to_end(MAGE_SERVICE_START_FROM_SAVEPOINT='before-v2')
        # A restart with the setting still there does not read them a third time.
        run_to_end(MAGE_SERVICE_START_FROM_SAVEPOINT='before-v2')

        seen = defaultdict(list)
        for message in read_committed(kafka_bootstrap, topics[1]):
            seen[message['id']].append(message['seen'])
        # 1,000 once, 1,000 twice (after the savepoint, then again from it), each time with
        # the same count: the second pass started from the counts the savepoint kept.
        assert sorted(len(v) for v in seen.values()) == [1] * 1000 + [2] * 1000
        assert all(len(set(v)) == 1 for v in seen.values()), 'a message counted differently'
        assert max(max(v) for v in seen.values()) == 2000 // KEYS
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()


def counts_of(messages):
    seen = defaultdict(list)
    for message in messages:
        seen[message['id']].append(message['seen'])
    return seen


def test_a_savepoint_of_a_running_stream_restores_the_counts_it_had(
    kafka_bootstrap, mage_project, tmp_path,
):
    topics = [f'it_spl_in_{uuid.uuid4().hex[:8]}', f'it_spl_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    pipeline = make_pipeline(mage_project, topics, f'g_{uuid.uuid4().hex[:8]}', bounded=False)
    binary = stream_service_binary()
    service = None

    def outputs(at_least):
        wait_for(lambda: len(read_committed(kafka_bootstrap, topics[1])) >= at_least,
                 timeout=180)

    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        service.wait_running()
        produce(kafka_bootstrap, topics[0], range(1000))
        outputs(1000)

        # Taken while it runs: paused until settled, then resumed.
        taken = service.post(f'/v1/streams/{pipeline.uuid}/savepoints', dict(name='live-1'))
        assert (taken['files'], taken['partitions']) == (2, 2), taken
        assert service.streams()[0]['state'] == 'running'

        produce(kafka_bootstrap, topics[0], range(1000, 2000))
        outputs(2000)
        service.stop()

        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap,
                                 MAGE_SERVICE_START_FROM_SAVEPOINT='live-1')
        service.start()
        service.wait_running()
        outputs(3000)
        service.stop()

        seen = counts_of(read_committed(kafka_bootstrap, topics[1]))
        # The 1,000 after the savepoint were counted again from the counts it kept.
        assert sorted(len(v) for v in seen.values()) == [1] * 1000 + [2] * 1000
        assert all(len(set(v)) == 1 for v in seen.values()), 'a message counted differently'
        assert max(max(v) for v in seen.values()) == 2000 // KEYS
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
