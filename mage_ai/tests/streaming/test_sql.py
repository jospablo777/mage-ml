import json
import unittest

from mage_ai.streaming import sql


class StreamSqlTest(unittest.TestCase):
    """The editor runs a stream's SQL block as the engine does (stream_sql.rs's tests)."""

    def test_a_query_filters_and_computes_over_the_batch(self):
        messages = [
            dict(card='a', amount=50, country='cr'),
            dict(card='b', amount=150, country='us'),
            dict(card='c', amount=250.5),
        ]
        self.assertEqual(
            sql.run(
                'SELECT card, amount * 2 AS doubled, upper(country) AS country FROM messages '
                'WHERE amount > 100 ORDER BY card',
                messages,
            ),
            [
                dict(card='b', doubled=300.0, country='US'),
                dict(card='c', doubled=501.0, country=None),
            ],
        )

    def test_broadcast_values_are_a_table_and_aggregates_work(self):
        messages = [dict(card='a', amount=50), dict(card='a', amount=150),
                    dict(card='b', amount=120)]
        self.assertEqual(
            sql.run(
                'SELECT card, count(*) AS high FROM {{ df_1 }} CROSS JOIN broadcast '
                'WHERE amount > threshold GROUP BY card ORDER BY card;',
                messages,
                dict(threshold=100),
            ),
            [dict(card='a', high=1), dict(card='b', high=1)],
        )

    def test_references_and_messages_that_are_not_objects_are_explained(self):
        self.assertEqual(sql.query_of('SELECT * FROM {{ df_1 }};\n'), 'SELECT * FROM messages')
        with self.assertRaisesRegex(sql.StreamSqlError, 'df_2'):
            sql.query_of('SELECT * FROM {{ df_2 }}')
        with self.assertRaisesRegex(sql.StreamSqlError, 'no query'):
            sql.query_of('  ;  ')
        with self.assertRaisesRegex(sql.StreamSqlError, 'JSON objects'):
            sql.run('SELECT * FROM messages', [dict(a=1), [1, 2]])
        self.assertEqual(sql.run('SELECT * FROM messages', []), [])

    def test_a_comparison_with_a_subquery_is_refused_as_services_refuse_it(self):
        # Python's Polars runs it; the engine's (the Rust crate) does not.
        with self.assertRaisesRegex(sql.StreamSqlError, 'CROSS JOIN broadcast'):
            sql.run('SELECT * FROM messages WHERE amount > (SELECT threshold FROM broadcast)',
                    [dict(amount=1)], dict(threshold=0))
        # A subquery as a table is fine.
        self.assertEqual(
            sql.run('SELECT * FROM (SELECT amount FROM messages) WHERE amount > 0',
                    [dict(amount=1)]),
            [dict(amount=1)],
        )

    def test_columns_keep_field_order_and_types_follow_the_values(self):
        # The same case and text as stream_sql.rs's test of the same name.
        messages = [
            json.loads('{"id": 1, "ok": true, "score": 1.5, "tags": ["a"], "mixed": 1}'),
            json.loads('{"id": 2, "ok": null, "score": 2, "user": {"b": 1, "a": "x"}, '
                       '"mixed": "two", "big": 18446744073709551615}'),
        ]
        rows = sql.run('SELECT * FROM messages', messages)
        self.assertEqual(
            [json.dumps(row, separators=(',', ':'), ensure_ascii=False) for row in rows],
            [
                '{"id":1,"ok":true,"score":1.5,"tags":"[\\"a\\"]","mixed":"1","user":null,'
                '"big":null}',
                '{"id":2,"ok":null,"score":2.0,"tags":null,"mixed":"two",'
                '"user":"{\\"a\\":\\"x\\",\\"b\\":1}","big":1.8446744073709552e+19}',
            ],
        )
