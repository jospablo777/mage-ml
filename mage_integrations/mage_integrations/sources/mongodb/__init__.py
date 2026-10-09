import uuid
from datetime import datetime
from typing import Dict, Generator, List

from bson import Binary, Code, Decimal128, Regex
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.mongo_sql_types import PYMONGO_TYPE_TO_TYPE_STRING
from singer import catalog

import mage_integrations.sources.mongodb.tap_mongodb.sync_strategies.common as common
from mage_integrations.sources.base import Source, main
from mage_integrations.sources.catalog import Catalog
from mage_integrations.sources.constants import (
    COLUMN_FORMAT_DATETIME,
    COLUMN_TYPE_ARRAY,
    COLUMN_TYPE_BOOLEAN,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_NULL,
    COLUMN_TYPE_NUMBER,
    COLUMN_TYPE_OBJECT,
    COLUMN_TYPE_STRING,
    REPLICATION_METHOD_LOG_BASED,
)
from mage_integrations.sources.mongodb.tap_mongodb import build_client, do_sync
from mage_integrations.sources.mongodb.tap_mongodb.sync_strategies.utils import (
    build_find_filter,
)
from mage_integrations.utils.array import find_index
from mage_integrations.utils.dictionary import index_by

# BSON types pymongo_schema does not know, typed as the sync emits them: Decimal128 as a
# number, and binary and UUID values as text.
PYMONGO_TYPE_TO_TYPE_STRING.setdefault(Decimal128, 'float')
for bson_text_type in (Binary, bytes, uuid.UUID, Regex, Code):
    PYMONGO_TYPE_TO_TYPE_STRING.setdefault(bson_text_type, 'string')

MONGO_TYPES = {
    'integer': COLUMN_TYPE_INTEGER,
    'biginteger': COLUMN_TYPE_INTEGER,
    'float': COLUMN_TYPE_NUMBER,
    'number': COLUMN_TYPE_NUMBER,
    'boolean': COLUMN_TYPE_BOOLEAN,
    'OBJECT': COLUMN_TYPE_OBJECT,
    'ARRAY': COLUMN_TYPE_ARRAY,
}


def column_schema(field: Dict) -> Dict:
    """
    The schema of a field from every type its values had. One type was taken from the
    field's common type, and embedded documents, arrays and Decimal128 values were typed
    as text, which the sync emits as objects, lists and numbers. Destinations then failed
    the records in schema validation.
    """
    type_names = [t for t in (field.get('types_count') or {field['type']: 1}) if t != 'null']
    types = []
    formats = set()
    for name in type_names:
        if name in ('date', 'timestamp'):
            formats.add(COLUMN_FORMAT_DATETIME)
        column_type = MONGO_TYPES.get(name, COLUMN_TYPE_STRING)
        if column_type not in types:
            types.append(column_type)
    if COLUMN_TYPE_INTEGER in types and COLUMN_TYPE_NUMBER in types:
        types.remove(COLUMN_TYPE_INTEGER)
    prop = dict(type=[COLUMN_TYPE_NULL] + (types or [COLUMN_TYPE_STRING]))
    if formats and types == [COLUMN_TYPE_STRING]:
        prop['format'] = COLUMN_FORMAT_DATETIME
    return prop

class MongoDB(Source):
    def discover(self, streams: List[str] = None) -> Catalog:
        client = build_client(self.config)

        database = self.config['database']
        schema = extract_pymongo_client_schema(
            client,
            database_names=[database],
            collection_names=streams,
        )

        catalog_entries = []

        for stream_id, data1 in schema[database].items():
            properties = {}

            for column, data2 in data1['object'].items():
                properties[column] = column_schema(data2)

            schema = catalog.Schema.from_dict(dict(
                properties=properties,
                type='object',
            ))
            catalog_entries.append(self.build_catalog_entry(
                stream_id,
                schema,
            ))

        return Catalog(catalog_entries)

    def sync(self, catalog: Catalog) -> None:
        client = build_client(self.config, logger=self.logger)

        database = self.config['database']
        catalog_dict = catalog.to_dict()
        streams_by_id = index_by(lambda x: x.tap_stream_id, catalog.streams)
        for stream in catalog_dict['streams']:
            tap_stream_id = stream['tap_stream_id']
            stream['table_name'] = tap_stream_id

            idx = find_index(lambda x: len(x['breadcrumb']) == 0, stream['metadata'])

            stream['metadata'][idx]['metadata']['database-name'] = database
            stream['metadata'][idx]['metadata']['replication-key'] = \
                self._get_bookmark_properties_for_stream(streams_by_id[tap_stream_id])
            stream['metadata'][idx]['metadata']['replication-method'] = stream['replication_method']

        do_sync(client, catalog_dict, self.state or {}, logger=self.logger)

    def count_records(
        self,
        stream,
        bookmarks: Dict = None,
        query: Dict = None,
        **kwargs,
    ) -> int:
        if query is None:
            query = {}
        if REPLICATION_METHOD_LOG_BASED == stream.replication_method:
            # Not support count records for LOG_BASED replication
            return 1

        client = build_client(self.config)
        db = client[self.config['database']]
        collection = db[stream.tap_stream_id]

        state = {}
        if query:
            state = query
        elif bookmarks:
            state = dict(bookmarks={
                stream.tap_stream_id: bookmarks,
            })

        find_filter = build_find_filter(
            stream.to_dict(),
            state,
        )

        return collection.count_documents(find_filter)

    def load_data(
        self,
        stream,
        bookmarks: Dict = None,
        query: Dict = None,
        sample_data: bool = False,
        start_date: datetime = None,
        **kwargs,
    ) -> Generator[List[Dict], None, None]:
        if query is None:
            query = {}
        client = build_client(self.config, logger=self.logger)
        db = client[self.config['database']]
        collection = db[stream.tap_stream_id]

        state = {}
        if query:
            state = query
        elif bookmarks:
            state = dict(bookmarks={
                stream.tap_stream_id: bookmarks,
            })

        find_filter = build_find_filter(
            stream.to_dict(),
            state,
        )

        arr = []

        if sample_data:
            with collection.find(find_filter).limit(100) as cursor:
                for row in cursor:
                    record_message = common.row_to_singer_record(
                        stream.to_dict(),
                        row,
                        None,
                        None,
                    )
                    arr.append(record_message.record)

        yield arr

    def test_connection(self):
        client = build_client(self.config, logger=self.logger)
        client.server_info()


if __name__ == '__main__':
    main(MongoDB)
