import os
import tempfile

import numpy as np
import pandas as pd

from mage_ai.data_cleaner.column_types.constants import ColumnType
from mage_ai.data_cleaner.shared.utils import clean_series, parse_list
from mage_ai.tests.base_test import TestCase


class ParseListTest(TestCase):
    def test_literals_are_parsed(self):
        self.assertEqual(list(parse_list('[1, 2.5, "a", True, None]')),
                         [1, 2.5, 'a', True, None])
        self.assertTrue(np.isnan(parse_list('[nan]')[0]))
        self.assertEqual(list(parse_list('[name]')), [None])

    def test_text_in_the_data_never_runs_as_code(self):
        folder = tempfile.mkdtemp()
        marker = os.path.join(folder, 'ran')
        value = f"[__import__('pathlib').Path('{marker}').touch()]"

        cleaned = clean_series(pd.Series([value, '[1]']), ColumnType.LIST, dropna=False)

        self.assertFalse(os.path.exists(marker))
        self.assertEqual(list(cleaned.iloc[0]), [None])
        self.assertEqual(list(cleaned.iloc[1]), [1])
