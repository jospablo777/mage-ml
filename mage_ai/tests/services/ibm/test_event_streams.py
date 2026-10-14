import json
from unittest import TestCase

from mage_ai.services.ibm.event_streams import (
    EventStreamsCredentialsError,
    expand,
    kafka_settings,
)

CREDENTIALS = dict(
    api_key='k3y',
    kafka_brokers_sasl=['broker-0.example.eventstreams.cloud.ibm.com:9093',
                        'broker-1.example.eventstreams.cloud.ibm.com:9093'],
    user='token',
)


class EventStreamsTest(TestCase):
    def test_credentials_become_the_brokers_and_sign_in(self):
        settings = kafka_settings(json.dumps(CREDENTIALS))
        self.assertEqual(settings['bootstrap_server'],
                         'broker-0.example.eventstreams.cloud.ibm.com:9093,'
                         'broker-1.example.eventstreams.cloud.ibm.com:9093')
        self.assertEqual(settings['security_protocol'], 'SASL_SSL')
        self.assertEqual(settings['sasl_config'],
                         dict(mechanism='PLAIN', username='token', password='k3y'))

    def test_a_kafka_config_with_credentials_loads_in_mageml(self):
        from mage_ai.streaming.sinks.kafka import KafkaConfig as SinkConfig
        from mage_ai.streaming.sources.kafka import KafkaConfig as SourceConfig

        source = SourceConfig.load(config=dict(
            event_streams_credentials=json.dumps(CREDENTIALS), consumer_group='g', topic='t',
        ))
        self.assertTrue(source.bootstrap_server.startswith('broker-0'))
        self.assertEqual(source.sasl_config.password, 'k3y')
        sink = SinkConfig.load(config=dict(event_streams_credentials=CREDENTIALS, topic='t'))
        self.assertEqual(sink.sasl_config.username, 'token')

    def test_incomplete_credentials_say_what_is_missing(self):
        with self.assertRaisesRegex(EventStreamsCredentialsError, 'kafka_brokers_sasl'):
            kafka_settings(dict(api_key='k'))
        with self.assertRaisesRegex(EventStreamsCredentialsError, 'not the service'):
            kafka_settings('not json')
        self.assertEqual(expand(dict(topic='t')), dict(topic='t'))
