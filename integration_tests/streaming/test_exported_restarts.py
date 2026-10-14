"""
An exported stream whose source numbers its messages from the start again after a restart
(webhooks; also SQS, Pub/Sub, ActiveMQ, NATS, MongoDB and PostgreSQL changes): the objects a
sink names by position in one run are not overwritten in the next (I-101).
"""
import json
import textwrap
import urllib.request
import uuid
from pathlib import Path

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)


def make_pipeline(project, folder):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'restarts_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\n', 'yaml')
    step = add('step', 'transformer', '''
        @transformer
        def keep(messages, *args, **kwargs):
            return messages
    ''', 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: object_storage
        url: "file://{folder}"
        format: jsonl
    ''', 'yaml', [step])
    return pipeline


def post(service, pipeline, messages):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.status


def test_a_restart_does_not_overwrite_the_last_runs_objects(mage_project, tmp_path):
    folder = tmp_path / 'lake'
    pipeline = make_pipeline(mage_project, folder)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        for run, values in enumerate(([1, 2], [3, 4])):
            service = RunningService(stream_service_binary(), directory,
                                     tmp_path / f'data{run}')
            service.start()
            try:
                service.wait_running()
                # One request is one batch: positions 0 and 1 in both runs.
                assert post(service, pipeline.uuid, [dict(n=v) for v in values]) == 200
            finally:
                service.stop()
    finally:
        pipeline.delete()

    objects = sorted(folder.rglob('*.jsonl'))
    rows = [json.loads(line) for path in objects for line in path.read_text().splitlines()]
    assert sorted(r['n'] for r in rows) == [1, 2, 3, 4], [p.name for p in objects]
    assert len(objects) == 2
