"""
The Polars transformer templates of the Python block menus, rendered as for a new block
and run on Polars and pandas frames, with the tests each template carries.
"""
import datetime as dt
import inspect
import os
import tempfile
import unittest

import pandas as pd
import polars as pl

from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.data_preparation.templates.constants import PYTHON_TRANSFORMER_TEMPLATES
from mage_ai.data_preparation.templates.template import fetch_template_source


def load(name):
    """The template's transformer and tests, as the block runs them."""
    entry = next(t for t in PYTHON_TRANSFORMER_TEMPLATES if t['name'] == name)
    code = fetch_template_source(
        BlockType.TRANSFORMER, {'template_path': entry['path']}, language='python',
    )
    found = dict(transformer=[], test=[])
    namespace = dict(
        transformer=lambda f: found['transformer'].append(f) or f,
        test=lambda f: found['test'].append(f) or f,
    )
    exec(compile(code, entry['path'], 'exec'), namespace)
    [transform] = found['transformer']
    return transform, found['test']


def run(name, *inputs, **kwargs):
    transform, tests = load(name)
    output = transform(*inputs, **kwargs)
    for check in tests:
        # As Mage calls a block's tests: with the variables when they take **kwargs.
        takes_kwargs = any(
            p.kind == p.VAR_KEYWORD for p in inspect.signature(check).parameters.values()
        )
        check(output, **kwargs) if takes_kwargs else check(output)
    return output


ORDERS = pl.DataFrame({
    'id': [1, 1, 2, 3, None],
    'updated_at': [dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 2), dt.datetime(2026, 1, 1),
                   None, dt.datetime(2026, 1, 1)],
    'category': ['a', 'b', 'a', 'c', 'a'],
    'amount': [10.0, 12.5, 3.0, 4.0, 1.0],
})


class PythonTransformerTemplatesTest(unittest.TestCase):
    def test_every_template_takes_polars_and_pandas_frames(self):
        for frame in (ORDERS, ORDERS.to_pandas(), ORDERS.lazy()):
            with self.subTest(kind=type(frame).__name__):
                output = run('Aggregate', frame, group_by='category')
                self.assertEqual(
                    output.sort('category').rows(),
                    [('a', 3, 3.0, 14.0), ('b', 1, 1.0, 12.5), ('c', 1, 3.0, 4.0)],
                )

    def test_clean_data(self):
        messy = pd.DataFrame({
            'Customer Name ': ['  Ana   María ', '  Ana   María ', None],
            'Total($)': [1.0, 1.0, None],
        })
        output = run('Clean data', messy)
        self.assertEqual(output.columns, ['customer_name', 'total'])
        self.assertEqual(output.rows(), [('Ana María', 1.0)])

    def test_deduplicate_keeps_the_latest_row_of_each_key(self):
        output = run('Deduplicate', ORDERS).sort('id')
        self.assertEqual(output.select('id', 'amount').rows(),
                         [(1, 12.5), (2, 3.0), (3, 4.0)])

    def test_data_quality_checks_name_every_problem(self):
        with self.assertRaises(ValueError) as raised:
            run('Data quality checks', ORDERS.with_columns(amount=pl.col('amount') - 5))
        message = str(raised.exception)
        self.assertIn('id is missing in 1 rows', message)
        self.assertIn('id repeats in 1 rows', message)
        self.assertIn('amount is negative in 3 rows', message)
        clean = ORDERS.filter(pl.col('id').is_not_null()).unique('id')
        self.assertEqual(run('Data quality checks', clean).height, 3)

    def test_join(self):
        names = pd.DataFrame({'id': [1, 2], 'name': ['one', 'two']})
        output = run('Join', ORDERS, names, how='inner').sort('id', 'updated_at')
        self.assertEqual(output.select('id', 'name').rows(),
                         [(1, 'one'), (1, 'one'), (2, 'two')])

    def test_reshape_and_one_hot_encoding(self):
        small = pl.DataFrame({'id': [1, 2], 'x': [1.5, 2.0], 'y': ['a', 'b']})
        self.assertEqual(run('Reshape', small).sort('id', 'variable').rows(), [
            (1, 'x', '1.5'), (1, 'y', 'a'), (2, 'x', '2.0'), (2, 'y', 'b'),
        ])
        encoded = run('One-hot encoding', ORDERS)
        self.assertEqual(
            [c for c in encoded.columns if c.startswith('category_')],
            ['category_a', 'category_b', 'category_c'],
        )
        self.assertEqual(encoded['category_a'].to_list(), [1, 0, 1, 0, 1])

    def test_date_features_from_dates_datetimes_and_text(self):
        for values in ([dt.date(2026, 10, 24)], [dt.datetime(2026, 10, 24, 15)],
                       ['2026-10-24T15:00:00']):
            with self.subTest(values=values):
                output = run('Date features', pl.DataFrame({'created_at': values}))
                row = output.row(0, named=True)
                self.assertEqual(row['day'], dt.date(2026, 10, 24))
                self.assertEqual((row['weekday'], row['month'], row['weekend']), (6, 10, True))

    def test_a_trained_model_is_read_by_model_inference(self):
        from mage_ai.pipeline_services.models import load_artifact

        rows = pl.DataFrame({
            'x': [float(i) for i in range(100)],
            'flag': [i % 2 == 0 for i in range(100)],
            'target': [2.0 * i for i in range(100)],
        })
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, 'models', 'model.joblib')
            metrics = run('Train a model', rows, model=path)
            self.assertEqual(metrics['metric'].to_list(), ['mae', 'r2'])
            self.assertGreater(metrics.filter(pl.col('metric') == 'r2')['value'][0], 0.9)
            model = load_artifact(path)
            self.assertEqual(len(model.predict(rows.select('x', 'flag').to_pandas())), 100)

            labels = rows.with_columns(target=pl.when(pl.col('x') > 50).then(pl.lit('high'))
                                       .otherwise(pl.lit('low')))
            metrics = run('Train a model', labels, model=path)
            self.assertEqual(metrics['metric'].to_list(), ['accuracy'])

    def test_the_default_block_menu_lists_every_python_transformer_template(self):
        from mage_ai.data_preparation.templates.constants import TEMPLATES

        listed = {t['name'] for t in TEMPLATES if t['block_type'] == BlockType.TRANSFORMER
                  and str(getattr(t['language'], 'value', t['language'])) == 'python'}
        self.assertLessEqual({t['name'] for t in PYTHON_TRANSFORMER_TEMPLATES}, listed)
        self.assertIn('Model inference', listed)
