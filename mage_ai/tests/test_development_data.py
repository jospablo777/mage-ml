import os
import unittest
from unittest.mock import patch

from mage_ai import data_samples


class DataSampleDefinitionTest(unittest.TestCase):
    def test_a_definition_says_what_to_fix(self):
        problems = data_samples.check(dict(source={}, tables=[
            dict(table='public.c', key='id', sample=dict(fraction=2),
                 columns=dict(a='scramble', b=dict(fake='ssn'))),
            dict(table='public.o', follow=dict(table='public.x', column='c_id')),
            dict(table='public.s', key='id', sample=dict(stratify='country', fraction=0.1)),
        ]))
        text = '\n'.join(problems)
        for expected in ('source needs the io_config.yaml profile', 'fraction is above 0',
                         "'scramble' is not one of", 'fake is one of',
                         'follow names a table sampled before it',
                         'a stratified sample takes rows per group'):
            self.assertIn(expected, text)

    def test_the_sql_draws_by_a_hash_of_the_key(self):
        sql, params = data_samples.query(dict(
            table='public.c', key='id', sample=dict(fraction=0.05), where='active'))
        self.assertIn('hashtextextended(CAST(id AS text), 0)) % 1000000 < 50000', sql)
        self.assertIn('(active)', sql)
        sql, _ = data_samples.query(dict(table='public.c', key='id',
                                         sample=dict(rows=100, stratify='country')))
        self.assertIn('PARTITION BY country', sql)
        sql, params = data_samples.query(
            dict(table='public.o', follow=dict(table='public.c', column='c_id')), [1, 2])
        self.assertEqual((sql, params), ('SELECT * FROM public.o WHERE c_id = ANY(%s)', [[1, 2]]))

    def test_personal_columns_need_a_rule(self):
        table = dict(columns=dict(email='hash'), keep=dict(first_name='a pseudonym'))
        self.assertEqual(
            data_samples.personal_without_rule(
                table, ['id', 'email', 'first_name', 'phone_number', 'product_name', 'zip',
                        'zipper_count', 'name', 'customer_name']),
            ['phone_number', 'zip', 'name', 'customer_name'],
        )

    def test_a_short_key_is_refused(self):
        with patch.object(data_samples, 'load', return_value=dict(
                source=dict(profile='p'), tables=[])), \
                patch.dict(os.environ, {'MAGE_SAMPLE_KEY': 'short'}):
            with self.assertRaisesRegex(data_samples.SampleError, 'at least 16 characters'):
                data_samples.run('.', 'dev')


class OtherSourcesTest(unittest.TestCase):
    """Samples were drawn from PostgreSQL only; MySQL, DuckDB and ClickHouse sources have
    their SQL."""

    def test_each_source_has_its_hash_and_parameters(self):
        table = dict(table='c', key='id', sample=dict(fraction=0.05))
        self.assertIn('SHA2(CAST(id AS CHAR), 256)', data_samples.query(table, None, 'mysql')[0])
        self.assertIn('hash(CAST(id AS VARCHAR))', data_samples.query(table, None, 'duckdb')[0])
        self.assertIn('cityHash64(toString(id)) % 1000000 < 50000',
                      data_samples.query(table, None, 'clickhouse')[0])
        follow = dict(table='o', follow=dict(table='c', column='c_id'))
        self.assertEqual(data_samples.query(follow, [1, 2], 'mysql'),
                         ('SELECT * FROM o WHERE c_id IN (%s, %s)', [1, 2]))
        self.assertEqual(data_samples.query(follow, [1], 'duckdb'),
                         ('SELECT * FROM o WHERE c_id IN (?)', [1]))
        self.assertEqual(data_samples.query(follow, [3], 'clickhouse'),
                         ('SELECT * FROM o WHERE c_id IN (%s)', [3]))
        self.assertIn('ABS(HASH(CAST(id AS VARCHAR))) % 1000000 < 50000',
                      data_samples.query(table, None, 'snowflake')[0])
        # BigQuery: no % operator, named parameters.
        self.assertIn('ABS(MOD(FARM_FINGERPRINT(CAST(id AS STRING)), 1000000)) < 50000',
                      data_samples.query(table, None, 'bigquery')[0])
        self.assertEqual(data_samples.query(follow, [1, 'x'], 'bigquery'),
                         ('SELECT * FROM o WHERE c_id IN (@k0, @k1)', [1, 'x']))
        rows = dict(table='c', key='id', sample=dict(rows=10))
        self.assertTrue(data_samples.query(rows, None, 'bigquery')[0].endswith(
            'ORDER BY FARM_FINGERPRINT(CAST(id AS STRING)) LIMIT 10'))
        # No key sampled, no row follows.
        self.assertEqual(data_samples.query(follow, [], 'duckdb')[0],
                         'SELECT * FROM o WHERE 1 = 0')
        self.assertIn('source kind is one of', '\n'.join(data_samples.check(dict(
            source=dict(profile='p', kind='oracle'), tables=[table]))))

    def test_a_duckdb_sample_is_drawn_followed_and_masked(self):
        import tempfile

        import duckdb
        import pandas as pd

        repo = tempfile.mkdtemp()
        database = os.path.join(repo, 'prod.duckdb')
        with duckdb.connect(database) as connection:
            connection.execute(
                'CREATE TABLE customers AS SELECT i AS id, '
                "'person' || i || '@example.com' AS email FROM range(200) t(i)")
            connection.execute(
                'CREATE TABLE orders AS SELECT i AS id, i % 200 AS customer_id '
                'FROM range(1000) t(i)')
        with open(os.path.join(repo, 'io_config.yaml'), 'w') as file:
            file.write(f'version: 0.1.1\nprod:\n  DUCKDB_DATABASE: {database}\n')
        os.makedirs(os.path.join(repo, data_samples.SAMPLES_DIR))
        with open(os.path.join(repo, data_samples.SAMPLES_DIR, 'dev.yaml'), 'w') as file:
            file.write(
                'source: {profile: prod, kind: duckdb}\n'
                'tables:\n'
                '  - table: customers\n'
                '    key: id\n'
                '    sample: {rows: 20}\n'
                '    columns: {id: hash, email: {fake: email}}\n'
                '  - table: orders\n'
                '    follow: {table: customers, column: customer_id}\n'
                '    columns: {id: hash, customer_id: hash}\n'
            )
        out = os.path.join(repo, 'out')
        with patch.dict(os.environ, {'MAGE_SAMPLE_KEY': 'a key of sixteen chars'}), \
                patch('mage_ai.pipeline_services.definitions.ensure_gitignore'):
            record = data_samples.run(repo, 'dev', out_dir=out)

        self.assertEqual(record['source'], dict(profile='prod', kind='duckdb'))
        self.assertEqual([t['rows'] for t in record['tables']], [20, 100])
        customers = pd.read_parquet(os.path.join(out, 'customers.parquet'))
        orders = pd.read_parquet(os.path.join(out, 'orders.parquet'))
        self.assertFalse(customers['email'].str.contains('person').any())
        # Hashed keys still join: every order belongs to a sampled customer.
        self.assertTrue(set(orders['customer_id']) <= set(customers['id']))


class BigQueryReaderTest(unittest.TestCase):
    def test_keys_go_as_typed_named_parameters(self):
        from unittest.mock import MagicMock, patch

        client = MagicMock()
        with patch('mage_ai.io.bigquery.BigQuery.with_config',
                   return_value=MagicMock(client=client)), \
                patch('mage_ai.io.config.ConfigFileLoader'):
            read = data_samples._reader('/repo', 'warehouse', 'bigquery')
            read('SELECT * FROM o WHERE c_id IN (@k0, @k1, @k2)', [7, 'x', 1.5])

        job_config = client.query.call_args.kwargs['job_config']
        self.assertEqual(
            [(p.name, p.type_, p.value) for p in job_config.query_parameters],
            [('k0', 'INT64', 7), ('k1', 'STRING', 'x'), ('k2', 'FLOAT64', 1.5)],
        )
