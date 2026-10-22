import importlib
import logging
import unittest


class SearchSinksTests(unittest.TestCase):
    def test_the_sinks_import_with_the_installed_singer_sdk(self):
        # singer_sdk 0.54 has no io_base.logger; the build_fields annotation named it, so
        # the modules failed on import and the destinations list left both out.
        for engine in ('elasticsearch', 'opensearch'):
            with self.subTest(engine=engine):
                try:
                    importlib.import_module(engine if engine == 'elasticsearch' else
                                            'opensearchpy')
                except ImportError:
                    self.skipTest(f'{engine} client not installed')
                sinks = importlib.import_module(
                    f'mage_integrations.destinations.{engine}.target_{engine}.sinks',
                )
                annotation = sinks.build_fields.__annotations__['logger']
                self.assertIs(annotation, logging.Logger)


class BulkIndexErrorTests(unittest.TestCase):
    """A sync whose documents failed to index completed: the sink logged the error and
    dropped it."""

    def sink(self, module_name, class_name):
        from unittest.mock import MagicMock

        try:
            module = importlib.import_module(module_name)
        except ImportError as error:
            self.skipTest(str(error))
        sink = object.__new__(getattr(module, class_name))
        sink._config = {}
        sink.client = MagicMock()
        sink.logger = MagicMock()
        sink.build_body = lambda records: records
        return module, sink

    def test_elasticsearch_fails_the_sync(self):
        from unittest.mock import patch

        module, sink = self.sink(
            'mage_integrations.destinations.elasticsearch.target_elasticsearch.sinks',
            'ElasticSink',
        )
        failed = module.elasticsearch.helpers.BulkIndexError(
            '1 document(s) failed to index.', [{'index': {'error': 'mapper_parsing_exception'}}],
        )
        with patch.object(module, 'bulk', side_effect=failed), \
                self.assertRaises(module.elasticsearch.helpers.BulkIndexError):
            sink.write_output([{'_index': 'i', '_source': {}}])
        message, errors = sink.logger.error.call_args.args
        self.assertIn('%s', message)
        self.assertEqual(errors[0]['index']['error'], 'mapper_parsing_exception')

    def test_opensearch_fails_the_sync(self):
        from unittest.mock import patch

        module, sink = self.sink(
            'mage_integrations.destinations.opensearch.target_opensearch.sinks',
            'OpenSink',
        )
        failed = module.opensearchpy.helpers.BulkIndexError(
            '1 document(s) failed to index.', [{'index': {'error': 'mapper_parsing_exception'}}],
        )
        with patch.object(module, 'bulk', side_effect=failed), \
                self.assertRaises(module.opensearchpy.helpers.BulkIndexError):
            sink.write_output([{'_index': 'i', '_source': {}}])
