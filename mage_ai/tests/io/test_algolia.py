import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


class AlgoliaExportTest(unittest.TestCase):
    def test_a_failed_export_fails_the_block(self):
        """The error was logged and dropped, so the block succeeded with nothing exported."""
        # The client library is optional; a stand-in lets the module import.
        stub = types.ModuleType('algoliasearch')
        stub.search_client = types.ModuleType('algoliasearch.search_client')
        stub.search_client.SearchClient = MagicMock()
        with patch.dict(sys.modules, {
            'algoliasearch': stub, 'algoliasearch.search_client': stub.search_client,
        }):
            sys.modules.pop('mage_ai.io.algolia', None)
            from mage_ai.io import algolia
        loader = object.__new__(algolia.Algolia)
        loader.index_name = 'people'
        loader.client = MagicMock()
        loader.client.init_index.return_value.save_objects.side_effect = RuntimeError('quota')

        with patch.object(algolia, 'LOGGER'), self.assertRaisesRegex(RuntimeError, 'quota'):
            loader.export(pd.DataFrame({'id': [1]}))
