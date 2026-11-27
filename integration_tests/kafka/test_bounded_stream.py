"""
A bounded Kafka stream (stop_at) in an exported service: it reads every partition up to
where it was when the stream started, writes each message once, commits the group's offsets
at that end and finishes; messages produced after the start are left for the next stream.
"""
import textwrap
import time
import uuid
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_broker_restart import committed_now
from integration_tests.kafka.test_exactly_once import read_committed
from integration_tests.kafka.test_exported_stream import produce
from integration_tests.kafka.test_stream_conformance import wait_for

TOTAL = 3000


def make_pipeline(project, topics, group):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'bounded_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml')
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
        stop_at: end
    ''')
    add('out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', [source])
    return pipeline


def finished_stream(service):
    """The finished stream, once the service answers and says so."""
    assert service.process.poll() is None, service.log.read_text()[-4000:]
    try:
        return next((s for s in service.streams() if s['state'] == 'finished'), None)
    except OSError:
        return None


def test_a_bounded_stream_reads_to_its_end_and_finishes(
    kafka_bootstrap, mage_project, tmp_path,
):
    topics = [f'it_bounded_in_{uuid.uuid4().hex[:8]}', f'it_bounded_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1) for t in topics])
    admin.close()
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        produce(kafka_bootstrap, topics[0], range(TOTAL))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()

        finished = wait_for(lambda: finished_stream(service), timeout=180)

        assert finished['pipeline'] == pipeline.uuid
        ids = sorted(m['id'] for m in read_committed(kafka_bootstrap, topics[1]))
        assert ids == list(range(TOTAL)), 'each message once'
        assert committed_now(kafka_bootstrap, group, topics[0]) == TOTAL
        # Finished, the stream does not read on: newer messages wait for the next stream.
        produce(kafka_bootstrap, topics[0], range(TOTAL, TOTAL + 100))
        time.sleep(3)
        assert committed_now(kafka_bootstrap, group, topics[0]) == TOTAL
        assert [s['state'] for s in service.streams()] == ['finished']
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
