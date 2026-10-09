from unittest.mock import patch

from mage_ai.streaming.sinks.nats_js import NATSSink
from mage_ai.streaming.sinks.sink_factory import SinkFactory
from mage_ai.tests.base_test import TestCase


class NATSSinkTests(TestCase):
    def test_factory_builds_the_sink(self):
        with patch.object(NATSSink, 'init_client') as mock_init_client:
            sink = SinkFactory.get_sink(dict(
                connector_type='nats',
                server_url='nats://localhost:4222',
                subject='events',
            ))
            self.assertIsInstance(sink, NATSSink)
            self.assertEqual(sink.config.subject, 'events')
            self.assertIsNone(sink.config.stream_name)
            mock_init_client.assert_called_once()

    def test_subject_is_required(self):
        with patch.object(NATSSink, 'init_client'):
            with self.assertRaises(TypeError):
                NATSSink(dict(server_url='nats://localhost:4222'))
