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
