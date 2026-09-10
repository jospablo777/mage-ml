from typing import Any, Dict, List, Tuple

import great_expectations as gx
import pandas as pd

from mage_ai.data_preparation.models.constants import BlockLanguage

# Map legacy Validator method names to expectation classes.
EXPECTATION_CLASSES = {
    name.lower(): getattr(gx.expectations, name)
    for name in dir(gx.expectations)
    if name.startswith('Expect')
}


def expectation_class(method_name: str):
    """expect_column_values_to_not_be_null -> ExpectColumnValuesToNotBeNull."""
    return EXPECTATION_CLASSES.get(method_name.replace('_', '').lower())


class Validator:
    """Register and evaluate expectations through the legacy Validator interface."""

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
            names = klass.args_keys
            if len(args) > len(names):
                raise TypeError(f'{name} accepts at most {len(names)} positional arguments')
            for key, value in zip(names, args):
                if key in kwargs:
                    raise TypeError(f'{name} got multiple values for {key}')
                kwargs[key] = value
            expectation = klass(**kwargs)
            self.suite.add_expectation(expectation)
            return self.batch.validate(expectation)

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
        """Accept saved expectation configurations and expectation objects."""
        if not isinstance(expectation, dict):
            return expectation

        method_name = expectation.get('expectation_type') or expectation.get('type')
        klass = expectation_class(method_name or '')
        if klass is None:
            raise ValueError(f'{method_name} is not a great_expectations expectation')

        kwargs = dict(expectation.get('kwargs') or {})
        if 'meta' in expectation:
            kwargs['meta'] = expectation['meta']
        return klass(**kwargs)
