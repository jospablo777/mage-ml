from typing import Any, Dict, List, Tuple

import great_expectations as gx
import pandas as pd

from mage_ai.data_preparation.models.constants import BlockLanguage

# Built once. gx.expectations exposes CamelCase classes, block code calls the
# snake_case names the 0.18 Validator had.
EXPECTATION_CLASSES = {
    name.lower(): getattr(gx.expectations, name)
    for name in dir(gx.expectations)
    if name.startswith('Expect')
}


def expectation_class(method_name: str):
    """expect_column_values_to_not_be_null -> ExpectColumnValuesToNotBeNull."""
    return EXPECTATION_CLASSES.get(method_name.replace('_', '').lower())


class Validator:
    """
    Stands in for the Validator that great_expectations 0.18 returned.

    Block code calls validator.expect_*(...) and then validate(). 1.x builds a
    suite and validates a batch instead, so the expect_* calls collect
    expectations and validate() runs them in one pass.
    """

    def __init__(self, batch, suite):
        self.batch = batch
        self.suite = suite

    def __getattr__(self, name: str):
        if not name.startswith('expect_'):
            raise AttributeError(name)

        klass = expectation_class(name)
        if klass is None:
            raise AttributeError(f'{name} is not a great_expectations expectation')

        def add_expectation(*args, **kwargs):
            expectation = klass(*args, **kwargs)
            self.suite.add_expectation(expectation)
            return expectation

        return add_expectation

    def validate(self):
        return self.batch.validate(self.suite)


class GreatExpectations():
    def __init__(self, block, expectations: List[Dict] = None):
        self.block = block
        self.expectations = expectations

    def build_validators(
        self,
        *args,
        **kwargs,
    ) -> List[Tuple[Any, str]]:
        validators = []

        context = gx.get_context(mode='ephemeral')

        for idx, df in enumerate(args):
            if idx >= len(self.block.upstream_block_uuids):
                continue

            uuid = self.block.upstream_block_uuids[idx]
            suite_name = f'expectation_suite_for_block_{uuid}'

            if type(df) is list:
                df = pd.DataFrame(df)
            elif type(df) is dict:
                df = pd.DataFrame([df])

            if BlockLanguage.PYTHON != self.block.language:
                continue

            batch_definition = (
                context.data_sources.add_pandas(name=f'datasource_name_{uuid}')
                .add_dataframe_asset(name=f'data_asset_{uuid}')
                .add_batch_definition_whole_dataframe(f'batch_definition_{uuid}')
            )
            batch = batch_definition.get_batch(batch_parameters={'dataframe': df})

            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
            for expectation in self.expectations or []:
                suite.add_expectation(self.build_expectation(expectation))

            validators.append((Validator(batch, suite), uuid))

        return validators

    def build_expectation(self, expectation):
        """Config saved by the extension is a dict, block code passes objects."""
        if not isinstance(expectation, dict):
            return expectation

        method_name = expectation.get('expectation_type')
        klass = expectation_class(method_name or '')
        if klass is None:
            raise ValueError(f'{method_name} is not a great_expectations expectation')

        return klass(**(expectation.get('kwargs') or {}))
