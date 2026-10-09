"""
Data cleaner actions under pandas 3: removed Series methods, deprecated unit aliases,
datetime resolutions other than nanoseconds, and regular expressions in str.replace.
"""
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from mage_ai.data_cleaner.column_types.column_type_detector import infer_column_types
from mage_ai.data_cleaner.shared.utils import clean_series
from mage_ai.data_cleaner.transformer_actions.base import BaseAction, join_compatible
from mage_ai.data_cleaner.transformer_actions.column import first, last
from mage_ai.data_cleaner.transformer_actions.constants import ColumnType
from mage_ai.data_cleaner.transformer_actions.udf.addition import Addition
from mage_ai.data_preparation.models.block.dbt.dbt_adapter import DBTAdapter
from mage_ai.shared.pandas_utils import timedelta_unit
from mage_ai.tests.base_test import TestCase


class FirstLastWithoutGroupsTest(TestCase):
    def action(self):
        return dict(
            action_arguments=['amount'],
            action_options={},
            outputs=[dict(uuid='result')],
        )

    def test_first_and_last_present_values(self):
        """Series.first and Series.last were removed, so these raised AttributeError."""
        df = pd.DataFrame({'amount': [np.nan, 2.0, 3.0, np.nan]})

        self.assertEqual(first(df.copy(), self.action())['result'].tolist(), [2.0] * 4)
        self.assertEqual(last(df.copy(), self.action())['result'].tolist(), [3.0] * 4)

    def test_all_missing(self):
        df = pd.DataFrame({'amount': [np.nan, np.nan]})

        self.assertTrue(first(df, self.action())['result'].isna().all())


class TimedeltaUnitTest(TestCase):
    def test_deprecated_aliases_map_to_current_units(self):
        for alias, unit in [('d', 'D'), ('H', 'h'), ('T', 'min'), ('S', 's'), ('w', 'W')]:
            with self.subTest(alias=alias):
                self.assertEqual(timedelta_unit(alias), unit)
                pd.to_timedelta(1, unit=timedelta_unit(alias))

    def test_adding_days_with_the_default_unit(self):
        """The default unit 'd' is deprecated in pandas 3."""
        df = pd.DataFrame({'when': ['2024-01-01 00:00:00']})
        options = dict(value=1, column_type=ColumnType.DATETIME)

        result = Addition(df, ['when'], options=options).execute()

        self.assertEqual(result.tolist(), ['2024-01-02 00:00:00'])


class JoinKeysTest(TestCase):
    def test_compatible_key_dtypes(self):
        self.assertTrue(join_compatible(
            pd.Series(pd.to_datetime(['2024-01-01'])).dt.as_unit('us').dtype,
            pd.Series(pd.to_datetime(['2024-01-01'])).dt.as_unit('ns').dtype,
        ))
        self.assertTrue(join_compatible(pd.Series(['a']).dtype, np.dtype(object)))
        self.assertFalse(join_compatible(np.dtype('int64'), pd.Series(['a']).dtype))

    def test_datetime_keys_of_different_resolutions_stay_datetimes(self):
        left = pd.DataFrame({'day': pd.to_datetime(['2024-01-01']).as_unit('us'), 'x': [1]})
        right = pd.DataFrame({'day': pd.to_datetime(['2024-01-01']).as_unit('ns'), 'y': [2]})
        action = BaseAction(dict(
            action_type='join',
            action_arguments=[],
            action_code='',
            action_options=dict(left_on=['day'], right_on=['day']),
            action_variables={},
        ))

        joined = action.execute(left, df_to_join=right)

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(joined['day']))
        self.assertEqual(joined['y'].tolist(), [2])


class CurrencyCleaningTest(TestCase):
    def test_currency_symbols_are_removed(self):
        """A compiled pattern with regex=False raised ValueError."""
        series = pd.Series(['$1,000', '$20', None], dtype=object)

        cleaned = clean_series(series, ColumnType.NUMBER)

        self.assertEqual(cleaned.tolist(), [1000, 20])

    def test_currency_column_type_detection(self):
        df = pd.DataFrame({'price': ['$1,000', '$2,500', '$3,000'] * 10})

        infer_column_types(df)


class DBTAdapterTest(TestCase):
    def test_execute_keeps_column_names(self):
        """The column names were passed as the index."""
        adapter = DBTAdapter.__new__(DBTAdapter)
        table = MagicMock(rows=[(1, 'a'), (2, 'b'), (3, 'c')], column_names=['id', 'name'])
        inner = MagicMock()
        inner.execute.return_value = ('ok', table)
        adapter._DBTAdapter__adapter = inner

        _, df = adapter.execute('select 1')

        self.assertEqual(df.columns.tolist(), ['id', 'name'])
        self.assertEqual(df['id'].tolist(), [1, 2, 3])
