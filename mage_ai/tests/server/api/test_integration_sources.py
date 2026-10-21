import builtins
import importlib
from unittest import TestCase
from unittest.mock import patch

from mage_ai.server.api.integration_sources import build_integration_module_info


class IntegrationSourcesTest(TestCase):
    def test_a_source_whose_client_is_missing_is_listed_from_its_templates(self):
        real_import = importlib.import_module

        def without_client(name, *args, **kwargs):
            if name == 'mage_integrations.sources.dremio':
                raise ModuleNotFoundError("No module named 'dremio'")
            return real_import(name, *args, **kwargs)

        with patch.object(importlib, 'import_module', without_client):
            info = build_integration_module_info('sources', dict(name='Dremio'))

        self.assertEqual(info['uuid'], 'dremio')
        self.assertEqual(info['templates']['config']['port'], 32010)
        self.assertIn('Dremio', info['docs'])

    def test_a_source_without_templates_is_still_left_out(self):
        real_import = importlib.import_module

        def failing(name, *args, **kwargs):
            if name == 'mage_integrations.sources.not_a_source':
                raise ModuleNotFoundError('No module named not_a_source')
            return real_import(name, *args, **kwargs)

        with patch.object(importlib, 'import_module', failing):
            self.assertIsNone(build_integration_module_info('sources', dict(
                name='Not a source', uuid='not_a_source',
            )))

    def test_postmark_imports_without_pkg_resources(self):
        real_import = builtins.__import__

        def no_pkg_resources(name, *args, **kwargs):
            if name == 'pkg_resources':
                raise ModuleNotFoundError("No module named 'pkg_resources'")
            return real_import(name, *args, **kwargs)

        with patch.object(builtins, '__import__', no_pkg_resources):
            module = importlib.reload(
                importlib.import_module('mage_integrations.sources.postmark.tap_postmark.tap'),
            )
        self.assertTrue(hasattr(module, 'REQUIRED_CONFIG_KEYS'))
