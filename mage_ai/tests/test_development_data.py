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
