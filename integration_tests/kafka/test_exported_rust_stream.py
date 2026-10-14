"""
A stream whose transformers are Rust blocks (Kafka in, Kafka out), exported and run by
mage-service: the blocks run in one resident program on each batch, take messages as JSON
values or as a Polars DataFrame, read pipeline variables, send their failing messages to the
dead-letter topic, and a program that dies is started again without losing a message.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    build_rust_blocks,
    export_service,
    stream_service_binary,
)

VALUES = '''
use mage::prelude::*;

// Kept for the program's life: code at the top runs once, not for every batch.
static STARTED: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

fn transform(messages: Vec<Value>, vars: Vars) -> Result<Vec<Value>> {
    let _ = *STARTED;
    let rate: f64 = vars.get_or("rate", 1.0)?;
    let marker: String = vars.get_or("marker", String::new())?;
    let mut out = Vec::with_capacity(messages.len());
    for mut message in messages {
        if message["die"].as_bool() == Some(true) && !std::path::Path::new(&marker).exists() {
            // The first time only: the program is gone, and the batch is read again.
            std::fs::write(&marker, b"died")?;
            std::process::exit(3);
        }
        let amount = message["amount"]
            .as_f64()
            .ok_or_else(|| anyhow!("message {} has no amount", message["id"]))?;
        message["total"] = json!(amount * rate);
        out.push(message);
    }
    println!("priced {} messages", out.len());
    Ok(out)
}
'''
FRAME = '''
use mage::prelude::*;

fn transform(orders: DataFrame) -> Result<DataFrame> {
    Ok(orders
        .lazy()
        .filter(col("total").gt(lit(10.0)))
        .with_column((col("total") * lit(2.0)).alias("double"))
        .collect()?)
}
'''


def make_pipeline(project, topics, variables, dead_letter=None):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'rust_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)
    pipeline.variables = variables
    pipeline.save()

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: {name}
        auto_offset_reset: earliest
        batch_size: 100
        timeout_ms: 200
    ''', 'yaml')
    price = add('price', 'transformer', VALUES, 'rust', [source])
    large = add('large', 'transformer', FRAME, 'rust', [price])
    add('sink', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', 'yaml', [large])
    if dead_letter:
        add('dead', 'data_exporter', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {dead_letter}
            dead_letter: true
        ''', 'yaml', [source])
    return pipeline


@pytest.fixture
def topics(kafka_bootstrap):
    names = [f'it_rust_{uuid.uuid4().hex[:10]}' for _ in range(3)]
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=1) for n in names])
    admin.close()
    yield names
    # A new client: the first one's connection can drop while the blocks build.
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.delete_topics(names)
    admin.close()


def produce(bootstrap, topic, messages):
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    for message in messages:
        producer.send(topic, json.dumps(message).encode())
    producer.flush()
    producer.close()


def records(bootstrap, topic, count, timeout=120):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest',
                             group_id=f'reader_{uuid.uuid4().hex[:8]}')
    found = []
    deadline = time.time() + timeout
    while len(found) < count and time.time() < deadline:
        for batch in consumer.poll(timeout_ms=500).values():
            found.extend(batch)
    consumer.close()
    return found


def consume(bootstrap, topic, count, timeout=120):
    return [json.loads(r.value) for r in records(bootstrap, topic, count, timeout)]


def run(project, pipeline, tmp_path, bootstrap):
    directory = export_service(project, pipeline.uuid, tmp_path)
    build_rust_blocks(directory)
    return RunningService(stream_service_binary(), directory, tmp_path / 'data',
                          KAFKA_BOOTSTRAP=bootstrap)


def test_rust_blocks_transform_a_stream_and_survive_their_program_dying(
    mage_project, topics, kafka_bootstrap, tmp_path,
):
    marker = tmp_path / 'died'
    pipeline = make_pipeline(mage_project, topics, dict(rate=3.0, marker=str(marker)))
    try:
        service = run(mage_project, pipeline, tmp_path, kafka_bootstrap)
        produce(kafka_bootstrap, topics[0], [dict(id=i, amount=i) for i in range(10)]
                + [dict(id=10, amount=10, die=True)])
        service.start()
        try:
            service.wait_running()
            out = consume(kafka_bootstrap, topics[1], 7)
            status = service.streams()[0]
            log = service.log.read_text()
        finally:
            service.stop()
    finally:
        pipeline.delete()

    # total = amount * 3, kept when over 10, doubled by the DataFrame block.
    assert sorted((m['id'], m['total'], m['double']) for m in out) == [
        (i, i * 3.0, i * 6.0) for i in range(4, 11)
    ]
    assert marker.exists(), 'the program never died'
    assert status['retries'] >= 1, status
    assert 'priced' in log


def test_failing_messages_go_to_the_dead_letter_topic_with_the_blocks_error(
    mage_project, topics, kafka_bootstrap, tmp_path,
):
    pipeline = make_pipeline(mage_project, topics[:2], dict(rate=1.0, marker=''),
                             dead_letter=topics[2])
    try:
        service = run(mage_project, pipeline, tmp_path, kafka_bootstrap)
        produce(kafka_bootstrap, topics[0], [dict(id=1, amount=20), dict(id=2),
                                             dict(id=3, amount=30)])
        service.start()
        try:
            service.wait_running()
            out = consume(kafka_bootstrap, topics[1], 2)
            dead = records(kafka_bootstrap, topics[2], 1)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert sorted(m['id'] for m in out) == [1, 3]
    assert [json.loads(r.value)['id'] for r in dead] == [2]
    assert 'message 2 has no amount' in dict(dead[0].headers)['mage-error'].decode()
