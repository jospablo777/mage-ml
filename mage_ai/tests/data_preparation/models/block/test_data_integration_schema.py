import pandas as pd

from mage_ai.data_preparation.models.block.data_integration.schema import build_schema
from mage_ai.tests.base_test import TestCase


class BuildSchemaTest(TestCase):
    def types(self, frame):
        properties = build_schema(frame, 'stream')['schema']['properties']
        return {column: properties[column]['type'] for column in frame.columns}

    def test_numeric_only_frames(self):
        """to_numpy on the whole frame made ints and floats NumPy floats, typed as string."""
        frame = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})

        self.assertEqual(self.types(frame), dict(a=['null', 'integer'], b=['null', 'number']))

    def test_mixed_frames(self):
        frame = pd.DataFrame({
            'id': [1, 2], 'name': ['a', 'b'], 'flag': [True, False],
            'doc': [{'x': 1}, {'x': 2}],
        })

        types = self.types(frame)

        self.assertEqual(types['id'], ['null', 'integer'])
        self.assertEqual(types['name'], ['null', 'string'])
        self.assertEqual(types['doc'], ['null', 'object'])
