import io
import unittest
from datetime import date

from yaml.constructor import ConstructorError

from mage_ai.shared.yaml import load_yaml


class YamlTest(unittest.TestCase):
    def test_loads_configuration_from_stream(self):
        data = io.StringIO('name: pipeline\nenabled: true\nstarted: 2026-09-10\n')

        self.assertEqual(load_yaml(data), {
            'name': 'pipeline',
            'enabled': True,
            'started': date(2026, 9, 10),
        })

    def test_rejects_python_objects(self):
        for data in [
            '!!python/object/apply:builtins.str [42]',
            '!!python/name:builtins.eval',
            '!!python/object:builtins.object {}',
        ]:
            with self.subTest(data=data), self.assertRaises(ConstructorError):
                load_yaml(data)

    def test_preserves_yaml_aliases(self):
        self.assertEqual(load_yaml('default: &value [1, 2]\ncopy: *value'), {
            'default': [1, 2],
            'copy': [1, 2],
        })
