"""Development runs read with positions of their own (SAFETY.md section 3)."""
import unittest

from mage_ai.streaming.development import for_development, for_development_manifest


class ForDevelopmentTest(unittest.TestCase):
    def test_each_source_gets_a_position_of_its_own_or_a_warning(self):
        kafka, notes = for_development(
            dict(connector_type='kafka', consumer_group='scoring', topic='orders'), 'ada')
        self.assertEqual(kafka['consumer_group'], 'scoring-dev-ada')
        self.assertIn('scoring keeps its position', notes[0])
        # Applying it twice changes nothing more.
        self.assertEqual(for_development(kafka, 'ada')[0]['consumer_group'], 'scoring-dev-ada')

        slot, notes = for_development(
            dict(connector_type='postgres', replication_slot='orders_slot'), 'Ada L.')
        self.assertEqual(slot['replication_slot'], 'orders_slot_dev_ada_l')
        self.assertIn('Drop it when you are done', notes[0])

        nats, _ = for_development(dict(connector_type='nats', stream_name='orders'), 'ada')
        self.assertEqual(nats['consumer_name'], 'orders-dev-ada')

        queue, notes = for_development(
            dict(connector_type='rabbitmq', queue_name='orders'), 'ada')
        self.assertEqual(queue['queue_name'], 'orders')
        self.assertIn('reads orders; messages the run acknowledges leave it', notes[0])

        hub, notes = for_development(dict(connector_type='azure_event_hub'), 'ada')
        self.assertIn('$Default', notes[0])

    def test_an_exported_manifest_is_rewritten_in_place(self):
        manifest = dict(pipelines=[
            dict(uuid='score', stream=dict(source=dict(
                name='src', kind='kafka', config=dict(consumer_group='scoring')))),
            dict(uuid='batch'),
            dict(uuid='drain', stream=dict(source=dict(
                name='q', kind='sqs', config=dict(queue_name='jobs')))),
        ])

        notes = for_development_manifest(manifest, 'ada')

        config = manifest['pipelines'][0]['stream']['source']['config']
        self.assertEqual(config, dict(consumer_group='scoring-dev-ada'))
        self.assertTrue(notes[0].startswith('score: Development run'))
        self.assertTrue(notes[1].startswith('drain: Development run: this source reads jobs'))
