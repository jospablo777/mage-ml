"""
Loads with a row limit say when the limit cut their result (I-108): the query asks for one
row more than the limit, so exactly `limit` rows is told apart from a cut result.
"""
import io
import os
from contextlib import redirect_stdout
from unittest.mock import patch

from mage_ai.io.base import ROW_LIMIT_POLICY_ENV, RowLimitReached, checks_row_limit
from mage_ai.io.duckdb import DuckDB
from mage_ai.tests.base_test import TestCase


class RowLimitTest(TestCase):
    def setUp(self):
        super().setUp()
        self.client = DuckDB(database=':memory:', verbose=False)
        self.client.execute('CREATE TABLE t AS SELECT range AS n FROM range(5)')

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def load(self, limit, **kwargs):
        out = io.StringIO()
        with redirect_stdout(out):
            frame = self.client.load('SELECT n FROM t ORDER BY n', limit=limit, verbose=False,
                                     **kwargs)
        return frame, out.getvalue()

    def test_a_complete_result_of_exactly_the_limit_says_nothing(self):
        frame, printed = self.load(5)
        self.assertEqual(frame['n'].tolist(), [0, 1, 2, 3, 4])
        self.assertNotIn('Warning', printed)

    def test_a_cut_result_keeps_the_limit_and_warns(self):
        frame, printed = self.load(4)
        self.assertEqual(len(frame), 4)
        self.assertIn('more than its limit of 4 rows', printed)
        frame, printed = self.load(2, polars=True)
        self.assertEqual(frame.height, 2)
        self.assertIn('more than its limit of 2 rows', printed)

    def test_no_rows_and_fewer_rows_than_the_limit_are_complete(self):
        frame, printed = self.load(10)
        self.assertEqual(len(frame), 5)
        self.assertNotIn('Warning', printed)
        out = io.StringIO()
        with redirect_stdout(out):
            frame = self.client.load('SELECT n FROM t WHERE n > 9', limit=3, verbose=False)
        self.assertEqual(len(frame), 0)
        self.assertNotIn('Warning', out.getvalue())

    def test_the_error_policy_fails_a_cut_result(self):
        with patch.dict(os.environ, {ROW_LIMIT_POLICY_ENV: 'error'}):
            with self.assertRaises(RowLimitReached):
                self.load(4)
            frame, _ = self.load(5)
            self.assertEqual(len(frame), 5)

    def test_the_allow_policy_cuts_silently(self):
        with patch.dict(os.environ, {ROW_LIMIT_POLICY_ENV: 'allow'}):
            frame, printed = self.load(3)
        self.assertEqual(len(frame), 3)
        self.assertNotIn('Warning', printed)

    def test_a_load_calling_another_checked_load_is_checked_once(self):
        calls = []

        class Inner:
            @checks_row_limit
            def load(self, query, limit=10):
                calls.append(limit)
                import pandas as pd
                return pd.DataFrame({'n': range(min(limit, 7))})

        class Outer(Inner):
            @checks_row_limit
            def load(self, query, limit=10):
                return super().load(query, limit)

        out = io.StringIO()
        with redirect_stdout(out):
            frame = Outer().load('q', limit=5)
        self.assertEqual(calls, [6])
        self.assertEqual(len(frame), 5)
        self.assertEqual(out.getvalue().count('Warning'), 1)
