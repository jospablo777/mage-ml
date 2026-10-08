import io
import json
import unittest
from abc import ABC, abstractmethod
from typing import Dict, List
from unittest.mock import MagicMock, patch

import fastjsonschema
from jsonschema.validators import Draft4Validator

from mage_integrations.destinations.base import Destination, build_record_validator

SAMPLE_RECORD = {
    'id': 2,
    'first_name': 'jason',
    'last_name': 'scott',
    'age': 18,
    'color': 'red',
    'morphed': 1,
    'date_joined': '1993-08-28T00:00:00',
    'power_level': 99.99,
}
SAMPLE_RECORD_ROW = {
    'type': 'RECORD',
    'stream': 'demo_users',
    'record': SAMPLE_RECORD,
}
SAMPLE_SCHEMA = {
    'properties': {
        'id': {'type': ['null', 'string']},
        'first_name': {'type': ['null', 'string']},
        'last_name': {'type': ['null', 'string']},
        'age': {'type': ['null', 'integer']},
        'color': {'type': ['null', 'string']},
        'morphed': {'type': ['integer']},
        'date_joined': {'format': 'date-time', 'type': ['string']},
        'power_level': {'type': ['null', 'number']},
    },
    'type': 'object',
}
SAMPLE_SCHEMA_ROW = {
    'type': 'SCHEMA',
    'stream': 'demo_users',
    'schema': SAMPLE_SCHEMA,
    'key_properties': ['id'],
    'replication_method': 'INCREMENTAL',
    'unique_conflict_method': 'UPDATE',
    'unique_constraints': ['id'],
}
SAMPLE_STREAM_NAME = 'demo_users'


class MockDestination(Destination):
    def export_batch_data(self, record_data: List[Dict], stream: str, tags: Dict = None) -> None:
        pass

    def test_connection(self) -> None:
        pass


def build_test_destination():
    destination = MockDestination(
        config=dict(database='demo_db'),
    )
    destination.disable_column_type_check = dict(demo_users=True)
    destination.bookmark_properties = {}
    destination.key_properties = {}
    destination.partition_keys = {}
    destination.replication_methods = {}
    destination.schemas = {}
    destination.unique_conflict_methods = {}
    destination.unique_constraints = {}
    destination.validators = {}
    destination.versions = {}

    return destination


class BaseDestinationTests(ABC):
    """
    Base unit tests that will be applied to all subclasses.
    """
    @abstractmethod
    def test_templates(self):
        pass

    @abstractmethod
    def test_test_connection(self):
        pass


class DestinationTests(unittest.TestCase):
    """
    Unit tests for the common methods in the base destination.
    """
    def test_process_record(self):
        destination = build_test_destination()
        with patch.object(destination, 'export_data') as mock_export_data:
            destination.process_record(
                row=SAMPLE_RECORD_ROW,
                schema=SAMPLE_SCHEMA,
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_data.assert_called_once_with(
                record=SAMPLE_RECORD,
                schema=SAMPLE_SCHEMA,
                stream=SAMPLE_STREAM_NAME,
                tags={},
            )

    def test_process_record_data(self):
        destination = build_test_destination()
        with patch.object(destination, 'export_batch_data') as mock_export_batch_data:
            record_data = [
                dict(
                    row=SAMPLE_RECORD_ROW,
                    schema=SAMPLE_SCHEMA,
                    stream=SAMPLE_STREAM_NAME,
                )
            ]
            destination.process_record_data(
                record_data=record_data,
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_batch_data.assert_called_once_with(
                [dict(record=SAMPLE_RECORD, stream=SAMPLE_STREAM_NAME)],
                SAMPLE_STREAM_NAME,
                tags={'records': 1, 'stream': 'demo_users'},
            )

    def test_process_empty_record_data(self):
        destination = build_test_destination()
        with patch.object(destination, 'export_batch_data') as mock_export_batch_data:
            destination.process_record_data(
                record_data=[],
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_batch_data.assert_not_called()

    def test_process_schema(self):
        destination = build_test_destination()
        destination.process_schema(
            stream=SAMPLE_STREAM_NAME,
            schema=SAMPLE_SCHEMA,
            row=SAMPLE_SCHEMA_ROW,
        )
        self.assertEqual(
            destination.schemas,
            {
                'demo_users': {
                    'properties': {
                        'id': {'type': ['null', 'string']},
                        'first_name': {'type': ['null', 'string']},
                        'last_name': {'type': ['null', 'string']},
                        'age': {'type': ['null', 'integer']},
                        'color': {'type': ['null', 'string']},
                        'morphed': {'type': ['integer']},
                        'date_joined': {'format': 'date-time', 'type': ['string']},
                        'power_level': {'type': ['null', 'number']},
                        '_mage_created_at': {'format': 'date-time', 'type': ['null', 'string']},
                        '_mage_updated_at': {'format': 'date-time', 'type': ['null', 'string']},
                    },
                    'type': 'object',
                }
            },
        )
        self.assertEqual(
            destination.key_properties,
            {'demo_users': ['id']},
        )
        self.assertEqual(
            destination.unique_constraints,
            {'demo_users': ['id']},
        )

    def test_process_state(self):
        destination = build_test_destination()
        with patch.object(destination, '_emit_state') as mock_emit_state:
            destination.process_state(
                row=dict(value='test')
            )
            mock_emit_state.assert_called_once_with('test')

    def test_process_no_state(self):
        destination = build_test_destination()
        with self.assertRaises(Exception) as context:
            destination.process_state(
                row=SAMPLE_RECORD_ROW
            )
            self.assertEqual(
                str(context.exception), 'A state message is missing a state value.')

    def test_process_test_connection(self):
        destination = build_test_destination()
        destination.should_test_connection = True
        destination.before_process = MagicMock()
        destination.after_process = MagicMock()
        with patch.object(destination, 'test_connection') as mock_test_connection:
            with patch.object(destination, '_process') as mock_process:
                destination.process(None)
                mock_test_connection.assert_called_once()
                mock_process.assert_not_called()
                destination.before_process.assert_called_once()
                destination.after_process.assert_called_once()

    def test_process(self):
        destination = build_test_destination()
        destination.before_process = MagicMock()
        destination.after_process = MagicMock()
        with patch.object(destination, '_process') as mock_process:
            input_buffer = io.StringIO('mock_input_buffer')
            destination.process(input_buffer)
            mock_process.assert_called_once_with(input_buffer)
            destination.before_process.assert_called_once()
            destination.after_process.assert_called_once()


VALIDATION_SCHEMA = {
    'properties': {
        'id': {'type': ['integer']},
        'name': {'type': ['null', 'string'], 'maxLength': 5},
        'score': {'type': ['null', 'number'], 'maximum': 10, 'exclusiveMaximum': True},
        'joined': {'type': ['null', 'string'], 'format': 'date-time'},
        'tags': {'type': ['null', 'array'], 'items': {'type': 'integer'}},
        'payload': {'type': ['null', 'object'], 'properties': {'a': {'type': 'integer'}}},
        'raw': {'type': ['object', 'string']},
        'flag': {'anyOf': [{'type': 'null'}, {'type': 'boolean'}]},
    },
    'type': 'object',
}

VALIDATION_CASES = [
    dict(id=1),
    dict(id=1, name='abc', score=9.5, joined='2024-01-01T00:00:00', flag=True),
    dict(id=1.0),
    dict(id=True),
    dict(id='1'),
    dict(id=1, name='abcdef'),
    dict(id=1, name=None),
    dict(id=1, score=10),
    dict(id=1, score=True),
    dict(id=1, joined='not a date'),
    dict(id=1, tags=[1, 2]),
    dict(id=1, tags=['a']),
    dict(id=1, tags=None),
    dict(id=1, tags={'a': 1}),
    dict(id=1, tags='[1, 2]'),
    dict(id=1, payload={'a': 'not an integer'}),
    dict(id=1, payload=[1]),
    dict(id=1, payload=None),
    dict(id=1, payload='{}'),
    dict(id=1, payload=1),
    dict(id=1, raw='text'),
    dict(id=1, raw=None),
    dict(id=1, raw=1),
    dict(id=1, flag='yes'),
]


def validate_per_column(schema: Dict, record: Dict) -> bool:
    # Reference rules: one Draft4Validator call per column, object and array columns
    # validated only when the value fails the dict or list type check.
    validator = Draft4Validator(schema)
    for col, value in record.items():
        column_types = schema['properties'][col].get('type', [])
        if 'object' in column_types:
            valid = type(value) is dict or type(value) is list
        elif 'array' in column_types:
            valid = type(value) is list
        else:
            valid = False
        if not valid and not validator.is_valid({col: value}):
            return False
    return True


def is_valid(validate, record: Dict) -> bool:
    try:
        validate(record)
    except fastjsonschema.JsonSchemaValueException:
        return False
    return True


class RecordValidatorTests(unittest.TestCase):
    def test_matches_per_column_draft4_validation(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        for record in VALIDATION_CASES:
            with self.subTest(record=record):
                self.assertEqual(
                    is_valid(validate, record),
                    validate_per_column(VALIDATION_SCHEMA, record),
                )

    def test_rejects_wrong_scalar_type(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(id='1'))

    def test_skips_nested_schema_for_container_values(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        validate(dict(id=1, payload={'a': 'not an integer'}, tags=[1]))

    def test_validates_container_column_value_of_other_type(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        validate(dict(id=1, payload=None, raw='text'))
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(id=1, payload='{}'))
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(id=1, tags={'a': 1}))

    def test_ignores_format(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        validate(dict(id=1, joined='not a date'))

    def test_does_not_write_defaults(self):
        schema = {
            'properties': {
                'id': {'type': ['integer']},
                'status': {'type': ['string'], 'default': 'new'},
            },
            'type': 'object',
        }
        record = dict(id=1)
        build_record_validator(schema)(record)
        self.assertEqual(record, dict(id=1))

    def test_uses_draft_4_when_schema_declares_another_draft(self):
        schema = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'properties': {
                'score': {'type': ['number'], 'maximum': 10, 'exclusiveMaximum': True},
            },
            'type': 'object',
        }
        validate = build_record_validator(schema)
        validate(dict(score=9))
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(score=10))

    def test_rejects_columns_missing_from_schema(self):
        validate = build_record_validator(VALIDATION_SCHEMA)
        with self.assertRaises(fastjsonschema.JsonSchemaValueException) as context:
            validate(dict(id=1, extra=2))
        self.assertIn('extra', str(context.exception))

    def test_checks_required_against_the_whole_record(self):
        schema = {
            'properties': {
                'id': {'type': ['integer']},
                'name': {'type': ['string']},
            },
            'required': ['id', 'name'],
            'type': 'object',
        }
        validate = build_record_validator(schema)
        validate(dict(id=1, name='a'))
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(id=1))

    def test_resolves_definitions_for_container_columns(self):
        schema = {
            'definitions': {'nullable_object': {'type': ['null', 'object']}},
            'properties': {
                'id': {'type': ['integer']},
                'payload': {'type': ['null', 'object'], '$ref': '#/definitions/nullable_object'},
            },
            'type': 'object',
        }
        validate = build_record_validator(schema)
        validate(dict(id=1, payload=None))
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            validate(dict(id=1, payload='{}'))

    def test_ignores_non_standard_keywords(self):
        schema = {
            'properties': {
                'id': {'type': ['integer'], 'inclusion': 'automatic', 'selected': True},
            },
            'type': 'object',
            'selected': True,
        }
        build_record_validator(schema)(dict(id=1))


def build_validating_destination(**kwargs):
    destination = MockDestination(config=dict(database='demo_db'), **kwargs)
    destination.disable_column_type_check = {}
    destination.bookmark_properties = {}
    destination.key_properties = {}
    destination.partition_keys = {}
    destination.replication_methods = {}
    destination.schemas = {}
    destination.unique_conflict_methods = {}
    destination.unique_constraints = {}
    destination.validators = {}
    destination.versions = {}

    return destination


def schema_row(schema: Dict, disable_column_type_check: bool = False) -> Dict:
    return dict(
        SAMPLE_SCHEMA_ROW,
        disable_column_type_check=disable_column_type_check,
        schema=schema,
    )


def record_row(record: Dict) -> Dict:
    return dict(SAMPLE_RECORD_ROW, record=record)


def valid_sample_schema() -> Dict:
    schema = json.loads(json.dumps(SAMPLE_SCHEMA))
    schema['properties']['id'] = {'type': ['integer']}
    return schema


class DestinationValidationTests(unittest.TestCase):
    def setUp(self):
        self.schema = valid_sample_schema()

    def process_schema(self, destination, schema, disable_column_type_check=False):
        destination.process_schema(
            stream=SAMPLE_STREAM_NAME,
            schema=schema,
            row=schema_row(schema, disable_column_type_check),
        )

    def test_process_record_data_validates_when_check_enabled(self):
        destination = build_validating_destination()
        self.process_schema(destination, self.schema)

        with patch.object(destination, 'export_batch_data') as mock_export_batch_data:
            destination.process_record_data(
                record_data=[dict(
                    row=SAMPLE_RECORD_ROW,
                    schema=self.schema,
                    stream=SAMPLE_STREAM_NAME,
                )],
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_batch_data.assert_called_once()

        invalid = dict(SAMPLE_RECORD, morphed='one')
        with self.assertRaises(fastjsonschema.JsonSchemaValueException):
            destination.process_record_data(
                record_data=[dict(
                    row=record_row(invalid),
                    schema=self.schema,
                    stream=SAMPLE_STREAM_NAME,
                )],
                stream=SAMPLE_STREAM_NAME,
            )

    def test_process_record_skips_validation_when_check_disabled(self):
        destination = build_validating_destination()
        self.process_schema(destination, self.schema, disable_column_type_check=True)
        invalid = dict(SAMPLE_RECORD, morphed='one')

        with patch.object(destination, 'export_data') as mock_export_data:
            destination.process_record(
                row=record_row(invalid),
                schema=self.schema,
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_data.assert_called_once()
        self.assertEqual(destination.validators, {})

    def test_check_is_disabled_when_schema_row_omits_the_setting(self):
        destination = build_validating_destination()
        row = dict(SAMPLE_SCHEMA_ROW, schema=self.schema)
        destination.process_schema(stream=SAMPLE_STREAM_NAME, schema=self.schema, row=row)
        self.assertTrue(destination.disable_column_type_check[SAMPLE_STREAM_NAME])

    def test_validator_includes_internal_columns(self):
        destination = build_validating_destination()
        self.process_schema(destination, self.schema)
        record = dict(SAMPLE_RECORD, _mage_created_at='2024-01-01T00:00:00')

        with patch.object(destination, 'export_data') as mock_export_data:
            destination.process_record(
                row=record_row(record),
                schema=self.schema,
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_data.assert_called_once()

    def test_new_schema_message_replaces_compiled_validator(self):
        destination = build_validating_destination()
        self.process_schema(destination, self.schema)
        with patch.object(destination, 'export_data'):
            destination.process_record(
                row=SAMPLE_RECORD_ROW,
                schema=self.schema,
                stream=SAMPLE_STREAM_NAME,
            )
        self.assertIn(SAMPLE_STREAM_NAME, destination.validators)

        schema = valid_sample_schema()
        schema['properties']['morphed'] = {'type': ['string']}
        self.process_schema(destination, schema)
        self.assertNotIn(SAMPLE_STREAM_NAME, destination.validators)

        with patch.object(destination, 'export_data') as mock_export_data:
            destination.process_record(
                row=record_row(dict(SAMPLE_RECORD, morphed='one')),
                schema=schema,
                stream=SAMPLE_STREAM_NAME,
            )
            mock_export_data.assert_called_once()


class BatchSizeTests(unittest.TestCase):
    def lines(self, count: int) -> List[str]:
        record = dict(SAMPLE_RECORD, first_name='\u00e9' * 100)
        return [json.dumps(SAMPLE_SCHEMA_ROW)] + [
            json.dumps(record_row(dict(record, id=i)), ensure_ascii=False)
            for i in range(count)
        ]

    def run_process(self, lines: List[str], maximum_batch_size_bytes: int):
        destination = build_validating_destination(batch_processing=True)
        destination.config = dict(
            database='demo_db',
            maximum_batch_size_mb=maximum_batch_size_bytes / (1024 * 1024),
        )
        input_buffer = io.BytesIO(''.join(f'{line}\n' for line in lines).encode('utf-8'))
        with patch.object(destination, '_Destination__process_batch_set') as mock_batch_set:
            destination._process(input_buffer)
        return mock_batch_set

    def test_counts_utf_8_bytes_of_record_lines(self):
        lines = self.lines(4)
        record_bytes = len(f'{lines[1]}\n'.encode('utf-8'))
        self.assertGreater(record_bytes, len(f'{lines[1]}\n'))

        mock_batch_set = self.run_process(lines, maximum_batch_size_bytes=2 * record_bytes)

        self.assertEqual(mock_batch_set.call_count, 3)
        first, second, final = mock_batch_set.call_args_list
        for call in (first, second):
            batches = call.args[0]
            self.assertEqual(len(batches[SAMPLE_STREAM_NAME]['record_data']), 2)
            self.assertEqual(call.kwargs['tags']['batch_byte_size'], 2 * record_bytes)
        self.assertEqual(final.args[0], {})

    def test_schema_lines_do_not_count_toward_batch_size(self):
        lines = self.lines(1)
        record_bytes = len(f'{lines[1]}\n'.encode('utf-8'))

        mock_batch_set = self.run_process(lines, maximum_batch_size_bytes=record_bytes + 1)

        self.assertEqual(mock_batch_set.call_count, 1)
        batches = mock_batch_set.call_args.args[0]
        self.assertEqual(len(batches[SAMPLE_STREAM_NAME]['record_data']), 1)
