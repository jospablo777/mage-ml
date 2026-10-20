"""
Stream conformance on the engine (NEXT_RELEASE S3): two sinks with one failing, a slow sink
with bounded memory, and a shutdown that drains its batches. The HTTP sink posts to a server in
the test, which fails, delays and counts as each test needs.
"""
import json
import signal
import textwrap
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_exported_stream import committed, consume, produce


class Receiver:
    """An HTTP sink's receiver: fails the first `fail` requests, delays each by `delay`."""

    def __init__(self, fail: int = 0, delay: float = 0):
        self.fail, self.delay = fail, delay
        self.requests = 0
        self.by_key = {}
        self.lock = threading.Lock()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                with receiver.lock:
                    receiver.requests += 1
                    failing = receiver.requests <= receiver.fail
                time.sleep(receiver.delay)
                if failing:
                    self.send_response(503)
                    self.end_headers()
                    return
                with receiver.lock:
                    # A retried batch carries the same key: kept once.
                    receiver.by_key[self.headers['Idempotency-Key']] = json.loads(body)
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}/events'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def ids(self):
        with self.lock:
            return sorted(m['id'] for batch in self.by_key.values() for m in batch)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def topics(kafka_bootstrap):
    names = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=2, replication_factor=1) for n in names])
    yield names
    admin.delete_topics(names)
    admin.close()


def make_pipeline(project, topics, group, http_url=None, batch_size=100, librdkafka=None,
                  sink_librdkafka=None):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'conformance_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml')
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    # Client properties for librdkafka, as the source's `librdkafka:` setting.
    settings = ''.join(f'\n          {k}: {v}' for k, v in (librdkafka or {}).items())
    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: {batch_size}
        timeout_ms: 200
        {'librdkafka:' + settings if librdkafka else ''}
    ''')
    sink_settings = ''.join(f'\n          {k}: {v}' for k, v in (sink_librdkafka or {}).items())
    add('kafka_out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
        {'librdkafka:' + sink_settings if sink_librdkafka else ''}
    ''', [source])
    if http_url:
        add('http_out', 'data_exporter', f'''
            connector_type: http
            url: {http_url}
            allow_private: true
            timeout_seconds: 30
        ''', [source])
    return pipeline


def out_consumer(bootstrap, topic):
    return KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest',
                         enable_auto_commit=False, group_id=None)


def wait_for(condition, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = condition()
        if found:
            return found
        time.sleep(0.25)
    raise AssertionError('timed out')


def test_a_failing_sink_holds_the_position_until_it_recovers(
    mage_project, kafka_bootstrap, topics, tmp_path,
):
    """The healthy sink writes, the failing one is retried, and nothing is committed early."""
    receiver = Receiver(fail=4)
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group, receiver.url)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        produce(kafka_bootstrap, topics[0], range(500))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        wait_for(lambda: len(receiver.ids()) == 500)
        wait_for(lambda: committed(kafka_bootstrap, group, topics[0]) == 500)

        assert receiver.ids() == list(range(500))
        assert receiver.requests > len(receiver.by_key)
        out = consume(out_consumer(kafka_bootstrap, topics[1]), 500)
        # The healthy sink may write a batch again while the other is retried, never less.
        assert sorted(set(m['id'] for m in out)) == list(range(500))
    finally:
        if service:
            service.stop()
        receiver.close()
        pipeline.delete()


def test_a_slow_sink_keeps_the_engine_memory_bounded(
    mage_project, kafka_bootstrap, topics, tmp_path,
):
    """A sink that answers slowly holds the source back; memory does not grow with the lag."""
    import psutil

    receiver = Receiver(delay=0.3)
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group, receiver.url, batch_size=200)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        produce(kafka_bootstrap, topics[0], range(20000))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        process = psutil.Process(service.process.pid)
        wait_for(lambda: len(receiver.ids()) >= 200)
        samples = []
        for _ in range(20):
            samples.append(process.memory_info().rss)
            time.sleep(0.5)
        stream = service.streams()[0]

        # The engine read ahead only by its in-flight window, not the whole topic.
        assert stream['messages_in'] < 20000, stream
        assert max(samples) - min(samples) < 64 * 1024 * 1024, samples
        assert max(samples) < 512 * 1024 * 1024, samples
    finally:
        if service:
            service.stop()
        receiver.close()
        pipeline.delete()


def test_sigterm_drains_written_batches_and_a_restart_repeats_none(
    mage_project, kafka_bootstrap, topics, tmp_path,
):
    """A stop commits what the sinks wrote; the next run starts right after it."""
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group, batch_size=100)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        produce(kafka_bootstrap, topics[0], range(3000))
        consumer = out_consumer(kafka_bootstrap, topics[1])
        first = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                               KAFKA_BOOTSTRAP=kafka_bootstrap)
        first.start()
        written = consume(consumer, 500)
        first.process.send_signal(signal.SIGTERM)
        assert first.process.wait(timeout=60) == 0
        after_stop = committed(kafka_bootstrap, group, topics[0])
        written += consume(consumer, after_stop - len(written), timeout=10)

        assert len(written) == after_stop, (len(written), after_stop)
        second = RunningService(stream_service_binary(), directory, tmp_path / 'data2',
                                KAFKA_BOOTSTRAP=kafka_bootstrap)
        second.start()
        written += consume(consumer, 3000 - len(written))
        second.stop()

        ids = [m['id'] for m in written]
        assert sorted(ids) == list(range(3000)), 'a message was lost or written twice'
    finally:
        pipeline.delete()
