"""
Covers the great_expectations extension after the port from 0.18 to 1.x.

0.18 returned a Validator whose expect_* methods both registered and ran an
expectation. 1.x builds a suite and validates a batch. Block code still calls
expect_* then validate(), so these tests pin that surface.

Requires the great-expectations extra.
"""
import unittest

import pandas as pd

gx = pytest_skip = None
try:
    import great_expectations as gx
except ImportError:
    pytest_skip = 'great-expectations is not installed'


class FakeBlock:
    def __init__(self, upstream_block_uuids, language):
        self.upstream_block_uuids = upstream_block_uuids
        self.language = language


@unittest.skipIf(pytest_skip, pytest_skip or '')
class GreatExpectationsExtensionTest(unittest.TestCase):
    def setUp(self):
        from mage_ai.data_preparation.models.constants import BlockLanguage

        self.block = FakeBlock(['upstream_1'], BlockLanguage.PYTHON)
        self.df = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})

    def _build(self, expectations=None):
        from mage_ai.data_preparation.models.block.extension.great_expectations import (
            GreatExpectations,
        )

        return GreatExpectations(self.block, expectations=expectations).build_validators(self.df)

    def test_expectation_class_lookup(self):
        from mage_ai.data_preparation.models.block.extension.great_expectations import (
            expectation_class,
        )

        self.assertIs(
            expectation_class('expect_column_values_to_not_be_null'),
            gx.expectations.ExpectColumnValuesToNotBeNull,
        )
        # Acronyms keep their casing in the class name.
        self.assertIs(
            expectation_class('expect_column_kl_divergence_to_be_less_than'),
            gx.expectations.ExpectColumnKLDivergenceToBeLessThan,
        )
        self.assertIsNone(expectation_class('expect_nothing_at_all'))

    def test_build_validators_returns_one_per_upstream_block(self):
        validators = self._build()

        self.assertEqual(len(validators), 1)
        validator, uuid = validators[0]
        self.assertEqual(uuid, 'upstream_1')
        self.assertTrue(hasattr(validator, 'validate'))

    def test_extra_arguments_beyond_upstream_blocks_are_ignored(self):
        from mage_ai.data_preparation.models.block.extension.great_expectations import (
            GreatExpectations,
        )

        validators = GreatExpectations(self.block).build_validators(self.df, self.df)

        self.assertEqual(len(validators), 1)

    def test_expect_then_validate_passes(self):
        validator, _uuid = self._build()[0]
        validator.expect_column_values_to_not_be_null(column='id')

        result = validator.validate()

        self.assertTrue(result.success)
        self.assertEqual(len(result.results), 1)
        # Block code reads each result with .get('success').
        self.assertTrue(result.results[0].get('success', False))

    def test_expect_then_validate_fails_on_bad_data(self):
        self.df.loc[1, 'name'] = None
        validator, _uuid = self._build()[0]
        validator.expect_column_values_to_not_be_null(column='name')

        result = validator.validate()

        self.assertFalse(result.success)
        self.assertFalse(result.results[0].get('success', True))

    def test_unknown_expectation_raises(self):
        validator, _uuid = self._build()[0]

        with self.assertRaises(AttributeError):
            validator.expect_the_impossible(column='id')

    def test_non_expect_attribute_raises(self):
        validator, _uuid = self._build()[0]

        with self.assertRaises(AttributeError):
            validator.__getattr__('some_other_method')

    def test_configured_expectations_are_loaded(self):
        validators = self._build(
            expectations=[
                dict(
                    expectation_type='expect_column_values_to_not_be_null',
                    kwargs=dict(column='id'),
                ),
            ],
        )
        validator, _uuid = validators[0]

        result = validator.validate()

        self.assertTrue(result.success)
        self.assertEqual(len(result.results), 1)

    def test_configured_expectation_with_unknown_type_raises(self):
        with self.assertRaises(ValueError):
            self._build(expectations=[dict(expectation_type='expect_nothing_at_all')])

    def test_list_input_is_converted_to_dataframe(self):
        from mage_ai.data_preparation.models.block.extension.great_expectations import (
            GreatExpectations,
        )

        validators = GreatExpectations(self.block).build_validators([{'id': 1}, {'id': 2}])
        validator, _uuid = validators[0]
        validator.expect_column_values_to_not_be_null(column='id')

        self.assertTrue(validator.validate().success)

    def test_sql_blocks_produce_no_validators(self):
        from mage_ai.data_preparation.models.block.extension.great_expectations import (
            GreatExpectations,
        )
        from mage_ai.data_preparation.models.constants import BlockLanguage

        block = FakeBlock(['upstream_1'], BlockLanguage.SQL)

        self.assertEqual(GreatExpectations(block).build_validators(self.df), [])


if __name__ == '__main__':
    unittest.main()
