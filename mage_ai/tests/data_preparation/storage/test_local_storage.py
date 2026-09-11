"""
JSON variable writes.

Block outputs that are not dataframes are stored as JSON. Encoding used to happen
while writing into an already-truncated file, so an unserializable value left a
zero-byte data.json behind and the next block failed with a JSON decode error
pointing at the file instead of at the value.
"""
import json
import os
import tempfile

import numpy as np
import pandas as pd

from mage_ai.data_preparation.storage.local_storage import LocalStorage
from mage_ai.tests.base_test import TestCase


class Unserializable:
    pass


class WriteJsonFileTests(TestCase):
    def setUp(self):
        super().setUp()
        self.storage = LocalStorage()
        self.directory = tempfile.mkdtemp(prefix='mage-storage-tests-')
        self.file_path = os.path.join(self.directory, 'data.json')

    def read(self):
        with open(self.file_path) as file:
            return json.load(file)

    def test_writes_a_cursor_dictionary(self):
        """The shape a high-water-mark loader returns."""
        frame = pd.DataFrame({
            'last_case': pd.Series([345038], dtype='int64'),
            'last_opened_at': pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'], utc=True),
        })

        self.storage.write_json_file(self.file_path, frame.iloc[0].to_dict())

        self.assertEqual(self.read(), {
            'last_case': 345038,
            'last_opened_at': '2026-07-20T18:15:34.258586+00:00',
        })

    def test_writes_numpy_scalars(self):
        data = dict(count=np.int64(7), ratio=np.float64(0.5), flag=np.bool_(True))

        self.storage.write_json_file(self.file_path, data)

        self.assertEqual(self.read(), dict(count=7, ratio=0.5, flag=True))

    def test_overwrites_previous_contents(self):
        self.storage.write_json_file(self.file_path, dict(a=1))
        self.storage.write_json_file(self.file_path, dict(b=2))

        self.assertEqual(self.read(), dict(b=2))

    def test_unserializable_value_raises_and_writes_nothing(self):
        with self.assertRaisesRegex(ValueError, 'Unserializable'):
            self.storage.write_json_file(self.file_path, Unserializable())

        self.assertFalse(os.path.exists(self.file_path))

    def test_a_failed_write_leaves_the_previous_file_intact(self):
        self.storage.write_json_file(self.file_path, dict(a=1))

        with self.assertRaises(ValueError):
            self.storage.write_json_file(self.file_path, Unserializable())

        self.assertEqual(self.read(), dict(a=1))

    def test_missing_directories_are_created(self):
        nested = os.path.join(self.directory, 'a', 'b', 'data.json')

        self.storage.write_json_file(nested, dict(a=1))

        self.assertTrue(os.path.exists(nested))
