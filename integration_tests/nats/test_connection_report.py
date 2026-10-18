"""The NATS source's connection report: the stream's messages and the consumer's position."""
import io
import uuid
from contextlib import redirect_stdout

import pytest

from integration_tests.nats.test_streaming import jetstream, publish


@pytest.fixture
def report_stream(nats_url):
    name = f'it_{uuid.uuid4().hex[:12]}'
    subject = f'{name}.events'
    jetstream(nats_url, lambda js: js.add_stream(name=name, subjects=[subject]))
    yield name, subject
    jetstream(nats_url, lambda js: js.delete_stream(name))


def test_the_report_counts_the_stream_and_the_consumer(nats_url, report_stream):
    from mage_ai.data_preparation.models.block.streaming import check_connection

    name, subject = report_stream
    publish(nats_url, subject, *[{'n': i} for i in range(4)])
    out = io.StringIO()
    with redirect_stdout(out):
        check_connection('data_loader', (
            f'connector_type: nats\nserver_url: {nats_url}\nstream_name: {name}\n'
            f'subject: {subject}\nconsumer_name: report\n'
        ))

    report = out.getvalue()
    assert f'Stream {name}: 4 messages kept, sequences 1 to 4' in report
    assert 'Consumer report: 4 messages to read, 0 delivered and not acknowledged.' in report
