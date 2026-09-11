from datetime import datetime

import polars as pl
import simplejson

from mage_ai.shared.parsers import encode_complex, polars_to_dict_split, sample_output
from mage_ai.tests.base_test import TestCase


class ParsersTests(TestCase):
    def test_sample_output_empty_input(self):
        self.assertEqual(sample_output([]), ([], False))
        self.assertEqual(sample_output({}), ({}, False))

    def test_sample_output_no_sampling(self):
        input_data = [1, 2, 3]
        self.assertEqual(sample_output(input_data), (input_data, False))

        input_data = {'key': 'value', 'nested': {'inner': 'data'}}
        self.assertEqual(sample_output(input_data), (input_data, False))

    def test_sample_output_sampling_list(self):
        # A nested list with more than 20 items, should be sampled
        input_data = [i for i in range(30)]
        expected_output = ([i for i in range(20)], True)
        self.assertEqual(sample_output(input_data), expected_output)

    def test_sample_output_sampling_dict(self):
        input_data = {'key{}'.format(i): i for i in range(30)}
        expected_output = ({'key{}'.format(i): i for i in range(30)}, False)
        self.assertEqual(sample_output(input_data), expected_output)

    def test_sample_output_sampling_mixed(self):
        # A mixed structure with a nested list and dictionary, both exceeding the threshold
        input_data = {'key1': [i for i in range(30)], 'key2': {'inner_key': [i for i in range(30)]}}
        expected_output = (
            {
                'key1': [i for i in range(20)],
                'key2': {'inner_key': [i for i in range(20)]}
            },
            True,
        )
        self.assertEqual(sample_output(input_data), expected_output)

        input_data2 = {
            'key1': [
                {
                    'subkey_1': [i for i in range(30)],
                    'subkey_2': {
                        'subkey_21': {
                            'subkey_211': [i for i in range(30)]
                        }
                    },
                },
                {
                    'subkey_3': [1, 2, 3],
                }
            ]
        }
        expected_output2 = (
            {
                'key1': [
                    {
                        'subkey_1': [i for i in range(20)],
                        'subkey_2': {
                            'subkey_21': {
                                'subkey_211': [i for i in range(20)]
                            },
                        },
                    },
                    {
                        'subkey_3': [1, 2, 3],
                    }
                ]
            },
            True,
        )
        self.assertEqual(sample_output(input_data2), expected_output2)


class PolarsToDictSplitTests(TestCase):
    """The conversion behind the output table shown for a polars block."""

    def test_mixed_column_types(self):
        frame = pl.DataFrame({
            'record_id': [1, 2],
            'label': ['alpha', 'beta'],
            'score': [0.5, 1.5],
            'observed_at': [
                datetime(2026, 1, 1),
                datetime(2026, 1, 2),
            ],
        })

        result = polars_to_dict_split(frame)

        self.assertEqual(
            result['columns'], ['record_id', 'label', 'score', 'observed_at'],
        )
        self.assertEqual(result['data'][0], [1, 'alpha', 0.5, datetime(2026, 1, 1)])

    def test_each_column_keeps_its_own_type(self):
        """
        Going through numpy first collapsed every column into one array dtype, and
        numpy 2 refuses to promote datetimes together with numbers at all.
        """
        frame = pl.DataFrame({
            'n': [1, 2],
            'when': [datetime(2026, 1, 1), datetime(2026, 1, 2)],
        })

        row = polars_to_dict_split(frame)['data'][0]

        self.assertIsInstance(row[0], int)
        self.assertIsInstance(row[1], datetime)

    def test_missing_values_become_none(self):
        frame = pl.DataFrame({'label': ['alpha', None], 'score': [0.5, None]})

        self.assertEqual(polars_to_dict_split(frame)['data'][1], [None, None])

    def test_empty_frame_keeps_its_columns(self):
        frame = pl.DataFrame({'record_id': [], 'label': []})

        result = polars_to_dict_split(frame)

        self.assertEqual(result['columns'], ['record_id', 'label'])
        self.assertEqual(result['data'], [])

    def test_result_is_json_serializable(self):
        frame = pl.DataFrame({
            'n': [1],
            'when': [datetime(2026, 1, 1)],
            'label': ['alpha'],
        })

        encoded = simplejson.dumps(
            polars_to_dict_split(frame), default=encode_complex, ignore_nan=True,
        )

        self.assertIn('2026-01-01T00:00:00', encoded)
