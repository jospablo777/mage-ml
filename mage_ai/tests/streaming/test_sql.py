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


class DeclaredColumnsTest(unittest.TestCase):
    def test_declared_columns_are_there_of_their_type_whatever_the_batch_holds(self):
        # The same case and text as stream_sql.rs's test of the same name.
        rows = sql.run(
            'SELECT card, amount, flag, n FROM messages',
            [dict(card=7, amount=3, n=2.5), dict(card='b', amount='x')],
            columns=dict(amount='float', card='text', flag='boolean', n='integer'),
        )
        self.assertEqual(
            [json.dumps(row, separators=(',', ':')) for row in rows],
            ['{"card":"7","amount":3.0,"flag":null,"n":null}',
             '{"card":"b","amount":null,"flag":null,"n":null}'],
        )
        with self.assertRaisesRegex(sql.StreamSqlError, 'integer, float'):
            sql.columns_of(dict(columns=dict(a='number')))


class WindowedSqlTest(unittest.TestCase):
    """A query grouped by a window of event time is a window block (export._window_spec)."""

    def test_tumbling_hopping_and_session_windows_with_their_aggregates(self):
        self.assertEqual(
            sql.window_of(
                'SELECT card_id, window_start, count(*) AS payments, sum(amount) AS total, '
                'min(amount) AS smallest, max(amount) AS largest, avg(amount) AS mean, '
                'count(DISTINCT shop_id) AS shops FROM {{ df_1 }} '
                "GROUP BY card_id, HOP(INTERVAL '1 minute', INTERVAL '10 seconds')"
            ),
            dict(key='card_id', key_as='card_id', type='hopping', size_seconds=60.0,
                 slide_seconds=10.0, aggregate=dict(
                     payments='count', total=dict(sum='amount'), smallest=dict(min='amount'),
                     largest=dict(max='amount'), mean=dict(avg='amount'),
                     shops=dict(distinct='shop_id'))),
        )
        self.assertEqual(
            sql.window_of("SELECT user.id AS uid, max(x) AS m FROM messages "
                          "GROUP BY user.id, session(INTERVAL '5 minutes')"),
            dict(key='user.id', key_as='uid', type='session', gap_seconds=300.0,
                 aggregate=dict(m=dict(max='x'))),
        )
        self.assertEqual(
            sql.window_of("SELECT count(*) AS n FROM messages GROUP BY TUMBLE(INTERVAL '2 hours')"),
            dict(key=None, type='tumbling', size_seconds=7200.0, aggregate=dict(n='count')),
        )
        # Without a window, a query is not one.
        self.assertIsNone(sql.window_of('SELECT a, count(*) AS n FROM messages GROUP BY a'))

    def test_what_a_window_cannot_do_is_explained(self):
        for query, message in (
            ("SELECT count(*) AS n FROM messages WHERE x > 1 GROUP BY TUMBLE(INTERVAL '1 minute')",
             'no WHERE'),
            ("SELECT count(*) FROM messages GROUP BY TUMBLE(INTERVAL '1 minute')", 'with AS'),
            ('SELECT count(*) AS n FROM messages GROUP BY TUMBLE(5)', 'not a window length'),
            ("SELECT a, count(*) AS n FROM messages GROUP BY TUMBLE(INTERVAL '1 minute')",
             'neither the window'),
            ("SELECT a, b, count(*) AS n FROM messages GROUP BY a, b, "
             "TUMBLE(INTERVAL '1 minute')", 'at most one field'),
            ("SELECT median(a) AS m FROM messages GROUP BY TUMBLE(INTERVAL '1 minute')",
             'not a window aggregate'),
            ("SELECT count(*) AS n FROM messages GROUP BY TUMBLE(INTERVAL '1 minute'",
             'does not parse'),
        ):
            with self.subTest(query=query):
                with self.assertRaisesRegex(sql.StreamSqlError, message):
                    sql.window_of(query)
        # The editor's own streaming does not run windows; it says where they run.
        with self.assertRaisesRegex(sql.StreamSqlError, 'Run tab'):
            sql.run("SELECT count(*) AS n FROM messages GROUP BY TUMBLE(INTERVAL '1 minute')",
                    [dict(a=1)])
