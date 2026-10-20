"""
A stream from the Amplitude template, exported and run on the engine, receiving events as
Amplitude's Webhook destination sends them: one JSON event per request, with the bearer
token in a header it is configured with. An event Amplitude sends again (it retries for
hours) is written once, since its uuid identifies it; user payloads are skipped by the
template's transformer.
"""
import json
import urllib.error
import urllib.request
import uuid

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.postgres.test_exported_stream_sink import (  # noqa: F401
    rows,
    schema,
    sink_yaml,
)


def amplitude_pipeline(project, sink: str):
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import templates

    pipeline = Pipeline.create(
        f'amplitude_stream_{uuid.uuid4().hex[:8]}', pipeline_type=PipelineType.STREAMING,
        repo_path=project,
    )
    templates.apply(pipeline, 'amplitude')
    pipeline = Pipeline.get(pipeline.uuid, repo_path=project)
    printer = next(b for b in pipeline.blocks_by_uuid.values() if b.uuid.endswith('_print'))
    with open(printer.file_path, 'w') as file:
        file.write(sink)
    return pipeline


def send_like_amplitude(service, pipeline, payload) -> tuple:
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(payload).encode(), method='POST',
        headers={
            'Authorization': f'Bearer {TOKEN}',
            'Content-Type': 'application/json',
            'User-Agent': 'Amplitude/Webhook/1.0',
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def test_amplitude_events_arrive_once_as_flat_rows(
    mage_project, tmp_path, postgres_settings, schema,  # noqa: F811
):
    from mage_ai.streaming.templates import AMPLITUDE_EVENTS

    pipeline = amplitude_pipeline(mage_project, sink_yaml(postgres_settings, schema))
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    manifest = json.loads((directory / 'service.json').read_text())
    source = manifest['pipelines'][0]['stream']['source']
    assert (source['kind'], source['config']['idempotency_key_field']) == ('http', 'uuid')
    service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                             PG_PASSWORD=postgres_settings['password'])
    try:
        service.start()
        service.wait_running()
        answers = [send_like_amplitude(service, pipeline.uuid, e) for e in AMPLITUDE_EVENTS]
        # Amplitude did not get the answer to the third event and sends it again.
        retry = send_like_amplitude(service, pipeline.uuid, AMPLITUDE_EVENTS[2])
        # A user payload (user forwarding): no event_type, so no row.
        user = send_like_amplitude(service, pipeline.uuid, {
            'user_id': 'u-17', 'user_properties': {'plan': 'pro'},
        })
    finally:
        service.stop()
        pipeline.delete()

    assert [status for status, _ in answers] == [200] * len(AMPLITUDE_EVENTS), answers
    assert retry == (200, {'accepted': 1, 'duplicate': True}), retry
    assert user[0] == 200, user
    found = rows(postgres_settings, (
        f'SELECT event_type, user_id, property_price, property_quantity, user_plan '
        f'FROM {schema}.orders ORDER BY event_time'
    ))
    assert len(found) == len(AMPLITUDE_EVENTS)
    assert found[2] == ('Purchase', 'u-17', 12.5, 2, 'free')
    assert found[3][1] is None
