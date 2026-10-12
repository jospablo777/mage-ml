"""MongoDB discovery: the BSON types each top-level field holds, without pymongo-schema."""
import datetime
import unittest
import uuid

from bson import Binary, Decimal128, ObjectId, Timestamp
from bson.int64 import Int64

from mage_integrations.sources.mongodb import column_schema
from mage_integrations.sources.mongodb.schema import (
    extract_schema,
    infer_fields,
    type_name,
)


class TypeNameTest(unittest.TestCase):
    def test_bson_values_get_the_names_column_schema_reads(self):
        cases = [
            (True, 'boolean'),
            (3, 'integer'),
            (Int64(3), 'biginteger'),
            (1.5, 'float'),
            (Decimal128('1.5'), 'float'),
            ('a', 'string'),
            (ObjectId(), 'oid'),
            (datetime.datetime(2026, 1, 1), 'date'),
            (Timestamp(0, 1), 'timestamp'),
            ({'a': 1}, 'OBJECT'),
            ([1], 'ARRAY'),
            (Binary(b'x'), 'string'),
            (uuid.uuid4(), 'string'),
            (None, 'null'),
        ]
        for value, name in cases:
            with self.subTest(value=value):
                self.assertEqual(type_name(value), name)


class InferFieldsTest(unittest.TestCase):
    def test_every_type_a_field_held_is_counted_and_typed(self):
        fields = infer_fields([
            {'_id': ObjectId(), 'n': 1, 'price': Decimal128('2.5'), 'tags': ['a'], 'at': None},
            {'_id': ObjectId(), 'n': 2.5, 'meta': {'k': 1}, 'at': datetime.datetime(2026, 1, 1)},
        ])

        self.assertEqual(fields['n']['types_count'], {'integer': 1, 'float': 1})
        self.assertEqual(column_schema(fields['n']), {'type': ['null', 'number']})
        self.assertEqual(column_schema(fields['price']), {'type': ['null', 'number']})
        self.assertEqual(column_schema(fields['tags']), {'type': ['null', 'array']})
        self.assertEqual(column_schema(fields['meta']), {'type': ['null', 'object']})
        self.assertEqual(column_schema(fields['_id']), {'type': ['null', 'string']})
        self.assertEqual(column_schema(fields['at']),
                         {'type': ['null', 'string'], 'format': 'date-time'})


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents
        self.calls = []

    def find(self, query):
        self.calls.append(('find', query))
        return iter(self.documents)

    def aggregate(self, pipeline):
        self.calls.append(('aggregate', pipeline))
        return iter(self.documents[:pipeline[0]['$sample']['size']])


class FakeDatabase(dict):
    def list_collection_names(self):
        return list(self)


class ExtractSchemaTest(unittest.TestCase):
    def test_collections_are_read_whole_or_sampled_and_system_ones_skipped(self):
        db = FakeDatabase(
            orders=FakeCollection([{'a': 1}, {'a': 2, 'b': 'x'}]),
            **{'system.views': FakeCollection([{'x': 1}])},
        )
        client = {'shop': db}

        schema = extract_schema(client, 'shop')
        self.assertEqual(list(schema), ['orders'])
        self.assertEqual(schema['orders']['b']['types_count'], {'string': 1})
        self.assertEqual(db['orders'].calls, [('find', {})])

        sampled = extract_schema(client, 'shop', collection_names=['orders'], sample_size=1)
        self.assertEqual(list(sampled['orders']), ['a'])
        self.assertEqual(db['orders'].calls[-1], ('aggregate', [{'$sample': {'size': 1}}]))
