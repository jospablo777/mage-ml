import sqlite3
import warnings
from unittest import TestCase

import pandas as pd

from mage_ai.io.sql import ignore_dbapi_connection_warning
from mage_ai.shared.column_type_detector import infer_column_types


class WarningFiltersTest(TestCase):
    """
    Loads and type detection added process-wide filters that silenced every UserWarning,
    or regex warnings, for the rest of the process.
    """

    def test_the_dbapi_warning_filter_is_scoped(self):
        before = list(warnings.filters)
        with ignore_dbapi_connection_warning():
            pass

        self.assertEqual(warnings.filters, before)

    def test_other_user_warnings_still_show(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            with ignore_dbapi_connection_warning():
                warnings.warn(
                    'pandas only supports SQLAlchemy connectable', UserWarning, stacklevel=1,
                )
                warnings.warn('something else', UserWarning, stacklevel=1)

        self.assertEqual([str(w.message) for w in caught], ['something else'])

    def test_type_detection_leaves_the_filters_unchanged(self):
        before = list(warnings.filters)
        frame = pd.DataFrame({
            'phone': ['555-123-4567', '555-765-4321', 'x', 'y'],
            'when': ['2024-01-01', '2024-01-02', '2024-01-03', 'x'],
        })

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            infer_column_types(frame)

        self.assertEqual(warnings.filters, before)
        # pandas 3 words the warning differently, so the old filter never matched.
        self.assertEqual([str(w.message) for w in caught if 'match groups' in str(w.message)], [])

    def test_read_sql_on_sqlite_runs_inside_the_filter(self):
        connection = sqlite3.connect(':memory:')
        with ignore_dbapi_connection_warning():
            frame = pd.read_sql('SELECT 1 AS a', connection)

        self.assertEqual(frame['a'].tolist(), [1])
