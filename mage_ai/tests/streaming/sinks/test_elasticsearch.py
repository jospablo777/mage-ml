from unittest.mock import patch

from mage_ai.streaming.sinks.elasticsearch import ElasticSearchSink
from mage_ai.tests.base_test import TestCase


class ElasticSearchTests(TestCase):
    def test_init(self):
        with patch.object(ElasticSearchSink, 'init_client') as mock_init_client:
            ElasticSearchSink(dict(
                host='test_host',
                index_name='test_index_name',
            ))
            mock_init_client.assert_called_once()

    def test_init_invalid_config(self):
        with patch.object(ElasticSearchSink, 'init_client') as mock_init_client:
            with self.assertRaises(Exception) as context:
                ElasticSearchSink(dict(
                    host='test_host',
                ))
            self.assertTrue(
                '__init__() missing 1 required positional argument: \'index_name\''
                in str(context.exception),
            )
            self.assertEqual(mock_init_client.call_count, 0)


class ElasticSearchBatchTests(TestCase):
    def actions(self, messages, **config):
        from elasticsearch.helpers.actions import expand_action

        with patch.object(ElasticSearchSink, 'init_client'):
            sink = ElasticSearchSink(dict(host='h', index_name='orders', **config))
        sink.client = object()
        with patch('mage_ai.streaming.sinks.elasticsearch.helpers.bulk') as bulk:
            sink.batch_write(messages)
        return [expand_action(doc) for doc in bulk.call_args.args[1]]

    def test_the_message_is_the_document_and_id_sets_the_documents_id(self):
        # Documents were stored as {"_doc": message, "id": ...} and _id was never set.
        action, document = self.actions([{'order': 7, 'total': 3}], _id='order')[0]
        self.assertEqual(action, {'index': {'_index': 'orders', '_id': 7}})
        self.assertEqual(document, {'order': 7, 'total': 3})

    def test_an_update_sends_the_fields_under_doc(self):
        action, document = self.actions(
            [{'order': 7, 'total': 4}], _id='order', _op_type='update',
        )[0]
        self.assertEqual(action, {'update': {'_index': 'orders', '_id': 7}})
        self.assertEqual(document, {'doc': {'order': 7, 'total': 4}})

    def test_a_named_source_key_still_wraps_the_message(self):
        _, document = self.actions([{'n': 1}], _source='payload')[0]
        self.assertEqual(document, {'payload': {'n': 1}})
