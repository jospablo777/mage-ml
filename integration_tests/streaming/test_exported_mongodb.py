"""
An exported stream from a MongoDB change stream through a Python block to a MongoDB
collection, run by mage-service: changes reach the block as JSON (ObjectIds as text, dates
ISO, decimals exact text), integers past 64 bits are stored as Decimal128 with every digit,
upserts keep the last message of a key, and a restart resumes after the last committed
change, from the resume token saved in the database.
"""
import textwrap
import time
import uuid
from decimal import Decimal
from pathlib import Path

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

TRANSFORM = '''
@transformer
def keep(messages, *args, **kwargs):
    out = []
    for change in messages:
        if change['operationType'] != 'insert':
            continue
        document = change['fullDocument']
        out.append(dict(
            # Decimal128 arrives as its exact text; as an int it goes back as Decimal128.
            order=document['order'], state=document['state'],
            big=int(document['big']) if document.get('big') else None,
            source_id=document['_id'], at=document['at'],
        ))
    return out
'''


def make_pipeline(project, mongodb_url, database):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'mongo_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: mongodb
        connection_str: "{mongodb_url}"
        database: {database}
        collection: orders
        batch_size: 20
    ''', 'yaml')
    step = add('keep', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: mongodb
        connection_string: "{mongodb_url}"
        database_name: {database}
        collection_name: states
        unique_constraints: [order]
    ''', 'yaml', [step])
    return pipeline


def wait_for(predicate, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    return predicate()


def test_changes_flow_to_a_collection_and_a_restart_resumes(
    mage_project, mongodb_url, mongo, tmp_path,
):
    from datetime import datetime, timezone

    from bson import Decimal128

    pipeline = make_pipeline(mage_project, mongodb_url, mongo.name)
    at = datetime(2026, 10, 14, 17, 46, 40, 123000, tzinfo=timezone.utc)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        service.start()
        try:
            service.wait_running()
            time.sleep(2)  # the change stream opens at the current time
            mongo.orders.insert_many([
                dict(order=1, state='new', at=at), dict(order=2, state='new', at=at),
                dict(order=1, state='paid', at=at,
                     big=Decimal128('1180591620717411303425')),
            ])
            first = wait_for(lambda: mongo.states.count_documents({}) == 2)
        finally:
            service.stop()
        # Changes while the service is stopped are read after the restart, and only those.
        mongo.orders.insert_one(dict(order=3, state='new', at=at))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2')
        service.start()
        try:
            service.wait_running()
            second = wait_for(lambda: mongo.states.count_documents({}) == 3)
            time.sleep(2)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert first and second
    states = {d['order']: d for d in mongo.states.find({}, {'_id': 0})}
    assert states[1]['state'] == 'paid'
    assert states[1]['big'].to_decimal() == Decimal('1180591620717411303425')
    assert states[2]['state'] == 'new' and states[3]['state'] == 'new'
    ids = {str(d['_id']) for d in mongo.orders.find({}, {'_id': 1})}
    assert {d['source_id'] for d in states.values()} <= ids
    assert states[2]['at'] == '2026-10-14T17:46:40.123Z'
    checkpoint = mongo['_mage_stream_checkpoints'].find_one({'_id': pipeline.uuid})
    assert checkpoint and checkpoint['namespace'] == f'{mongo.name}.orders'
