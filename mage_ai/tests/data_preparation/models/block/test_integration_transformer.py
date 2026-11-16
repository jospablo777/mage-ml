import json
import os
import textwrap
from unittest.mock import MagicMock, PropertyMock, patch

from mage_ai.data_preparation.models.block.integration import TransformerBlock
from mage_ai.data_preparation.models.block.integration.records import (
    records_frame,
    records_per_call,
)
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.tests.base_test import TestCase

SCHEMA = dict(
    type='SCHEMA',
    stream='users',
    schema=dict(properties=dict(
        id=dict(type=['integer']),
        name=dict(type=['null', 'string']),
        score=dict(type=['null', 'integer']),
    )),
    key_properties=['id'],
)


def record(id: int, name: str = None, score: int = None) -> dict:
    return dict(type='RECORD', stream='users', record=dict(id=id, name=name, score=score))


def state(value: int) -> dict:
    return dict(type='STATE', value=dict(bookmark=value))


class TransformerTest(TestCase):
    def transform(self, code: str, messages: list, configuration: dict = None):
        """The messages after the transformer, and the frames it received."""
        path = os.path.join(self.repo_path, 'users_0.json')
        with open(path, 'w') as file:
            file.write('\n'.join(json.dumps(m) for m in messages))
        pipeline = MagicMock()
        pipeline.source_output_file_path.return_value = path
        block = TransformerBlock(
            'shout', 'shout', BlockType.TRANSFORMER, configuration=configuration,
            pipeline=pipeline,
        )
        block.template_runtime_configuration = dict(index=0, selected_streams=['users'])
        frames = []
        module = 'mage_ai.data_preparation.models.block.integration'
        with patch(f'{module}.get_catalog_by_stream', return_value=dict(
            replication_method='FULL_TABLE', schema=SCHEMA['schema'],
        )), patch.object(
            TransformerBlock, 'content', new_callable=PropertyMock,
            return_value=textwrap.dedent(code),
        ), patch.object(
            TransformerBlock, 'execute_block_function',
            side_effect=lambda function, input_vars, **kwargs: (
                frames.append(input_vars[0].copy()) or function(*input_vars)
            ),
        ):
            block._execute_block({})
        with open(path) as file:
            return [json.loads(line) for line in file], frames

    def test_one_call_per_batch_of_records(self):
        messages = [SCHEMA] + [record(i, f'n{i}') for i in range(1, 6)]
        output, frames = self.transform('''
            @transformer
            def shout(df, *args, **kwargs):
                df['name'] = df['name'].str.upper()
                return df
        ''', messages)

        self.assertEqual([len(f) for f in frames], [5])
        self.assertEqual(output[0]['type'], 'SCHEMA')
        self.assertEqual(
            [m['record']['name'] for m in output[1:]], ['N1', 'N2', 'N3', 'N4', 'N5'],
        )
        self.assertEqual({m['stream'] for m in output[1:]}, {'users'})

    def test_records_per_call_splits_the_records(self):
        messages = [SCHEMA] + [record(i) for i in range(1, 8)]
        _, frames = self.transform('''
            @transformer
            def same(df, *args, **kwargs):
                return df
        ''', messages, configuration=dict(records_per_call=3))

        self.assertEqual([list(f['id']) for f in frames], [[1, 2, 3], [4, 5, 6], [7]])

    def test_records_before_a_state_message_are_written_before_it(self):
        """A destination saves the state after writing the records before it, so a record
        moved past it could be lost on a retry."""
        messages = [SCHEMA, record(1), record(2), state(2), record(3), state(3)]
        output, frames = self.transform('''
            @transformer
            def same(df, *args, **kwargs):
                return df
        ''', messages)

        self.assertEqual(
            [m['type'] if m['type'] != 'RECORD' else m['record']['id'] for m in output],
            ['SCHEMA', 1, 2, 'STATE', 3, 'STATE'],
        )
        self.assertEqual(len(frames), 2)

    def test_rows_added_or_removed_by_the_transformer(self):
        """One call per record wrote only the first row the transformer returned."""
        messages = [SCHEMA, record(1, 'a'), record(2, 'b'), record(3, 'c')]
        output, _ = self.transform('''
            import pandas as pd

            @transformer
            def double(df, *args, **kwargs):
                df = df[df['id'] != 2]
                return pd.concat([df, df.assign(id=df['id'] * 10)])
        ''', messages)

        self.assertEqual(sorted(m['record']['id'] for m in output[1:]), [1, 3, 10, 30])

    def test_the_schema_follows_the_transformer_output(self):
        messages = [SCHEMA, record(1, 'a'), record(2, 'b')]
        output, _ = self.transform('''
            @transformer
            def rename(df, *args, **kwargs):
                return df.rename(columns=dict(name='label')).assign(ratio=0.5)
        ''', messages)

        properties = output[0]['schema']['properties']
        self.assertEqual(list(properties), ['id', 'label', 'score', 'ratio'])
        self.assertEqual(properties['id'], dict(type=['integer']))
        self.assertEqual(properties['ratio']['type'][1], 'number')
        self.assertEqual(output[0]['key_properties'], ['id'])
        self.assertEqual(SCHEMA['schema']['properties']['name'], dict(type=['null', 'string']))

    def test_whole_numbers_with_missing_values_stay_whole_numbers(self):
        messages = [SCHEMA, record(1, score=7), record(2), record(3, score=2**53 + 1)]
        output, frames = self.transform('''
            @transformer
            def same(df, *args, **kwargs):
                return df
        ''', messages)

        self.assertEqual(str(frames[0]['score'].dtype), 'Int64')
        self.assertEqual([m['record']['score'] for m in output[1:]], [7, None, 2**53 + 1])
        self.assertIn('"score": 7}', json.dumps(output[1]))

    def test_a_stream_without_records_or_output_keeps_its_schema(self):
        output, frames = self.transform('''
            @transformer
            def same(df, *args, **kwargs):
                return df
        ''', [SCHEMA, state(1)])
        self.assertEqual(output, [SCHEMA, state(1)])
        self.assertEqual(frames, [])

        output, _ = self.transform('''
            @transformer
            def none(df, *args, **kwargs):
                return df.head(0)
        ''', [SCHEMA, record(1), state(1)])
        self.assertEqual(output, [SCHEMA, state(1)])

    def test_an_error_names_the_setting_for_record_at_a_time_code(self):
        code = '''
            @transformer
            def one(df, *args, **kwargs):
                if len(df) != 1:
                    raise ValueError('expected one record')
                return df
        '''
        messages = [SCHEMA, record(1), record(2)]
        with self.assertRaises(ValueError) as context:
            self.transform(code, messages)
        self.assertIn('records_per_call', ' '.join(context.exception.__notes__))

        output, frames = self.transform(code, messages, configuration=dict(records_per_call=1))
        self.assertEqual([len(f) for f in frames], [1, 1])
        self.assertEqual(len(output), 3)


class RecordsPerCallTest(TestCase):
    def test_default_and_values(self):
        self.assertEqual(records_per_call(None), 10_000)
        self.assertEqual(records_per_call(dict(records_per_call='')), 10_000)
        self.assertEqual(records_per_call(dict(records_per_call=1)), 1)
        self.assertEqual(records_per_call(dict(records_per_call='500')), 500)

    def test_values_that_are_not_positive_whole_numbers(self):
        for value in (0, -1, 2.5, 'many', True):
            with self.assertRaisesRegex(ValueError, 'positive whole number'):
                records_per_call(dict(records_per_call=value))


class RecordsFrameTest(TestCase):
    def test_columns_keep_their_values(self):
        frame = records_frame([
            dict(i=1, f=1.5, b=True, s='x'),
            dict(i=None, f=None, b=None, s=None),
            dict(i=3, f=2.0, b=False, s='z'),
        ])
        self.assertEqual(str(frame['i'].dtype), 'Int64')
        self.assertEqual(str(frame['f'].dtype), 'float64')
        self.assertEqual(frame['b'].tolist(), [True, None, False])
