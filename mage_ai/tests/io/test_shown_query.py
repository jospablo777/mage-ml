"""Queries in run logs: string literals masked, the rest as written (I-116)."""
import os
from unittest import TestCase, mock

from mage_ai.io.base import shown_query


class ShownQueryTest(TestCase):
    def test_string_literals_are_masked(self):
        self.assertEqual(
            shown_query("SELECT * FROM users WHERE email = 'ada@example.com' AND id > 10"),
            "SELECT * FROM users WHERE email = '…' AND id > 10",
        )

    def test_escaped_quotes_stay_inside_their_literal(self):
        self.assertEqual(shown_query("SELECT 'it''s', 'b'"), "SELECT '…', '…'")
        self.assertEqual(shown_query(r"SELECT E'a\'b', 1"), "SELECT '…', 1")

    def test_dollar_quoted_strings_are_masked_and_parameters_kept(self):
        self.assertEqual(shown_query('SELECT $$secret$$, $tag$x$tag$ WHERE a = $1'),
                         "SELECT '…', '…' WHERE a = $1")

    def test_identifiers_comments_and_casts_stay(self):
        query = ('SELECT "Name", `id`, total::numeric -- the \'total\'\n'
                 "/* 'kept' */ FROM shop.orders WHERE status = 'paid'")
        self.assertEqual(shown_query(query), (
            'SELECT "Name", `id`, total::numeric -- the \'total\'\n'
            "/* 'kept' */ FROM shop.orders WHERE status = '…'"
        ))

    def test_values_show_when_asked_and_empty_queries_pass(self):
        with mock.patch.dict(os.environ, {'MAGE_LOG_QUERY_VALUES': 'on'}):
            self.assertEqual(shown_query("WHERE a = 'x'"), "WHERE a = 'x'")
        self.assertEqual(shown_query(''), '')
        self.assertIsNone(shown_query(None))

    def test_an_unterminated_literal_is_masked_to_the_end(self):
        self.assertEqual(shown_query("SELECT 'open"), "SELECT '…'")
