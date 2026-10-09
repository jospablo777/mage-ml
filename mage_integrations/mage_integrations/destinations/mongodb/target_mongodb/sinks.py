"""MongoDB target stream class, which handles writing streams."""

import ast
import json
import urllib.parse

import pymongo
from bson.objectid import ObjectId
from pymongo import UpdateOne

from mage_integrations.destinations.constants import (
    COLUMN_TYPE_ARRAY,
    COLUMN_TYPE_BOOLEAN,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_NULL,
    COLUMN_TYPE_NUMBER,
    COLUMN_TYPE_OBJECT,
    COLUMN_TYPE_STRING,
)
from mage_integrations.destinations.sink import BatchSink

# JSON schema types of the Python values Singer records hold.
JSON_TYPES = {
    bool: {COLUMN_TYPE_BOOLEAN},
    int: {COLUMN_TYPE_INTEGER, COLUMN_TYPE_NUMBER},
    float: {COLUMN_TYPE_NUMBER},
    str: {COLUMN_TYPE_STRING},
    dict: {COLUMN_TYPE_OBJECT},
    list: {COLUMN_TYPE_ARRAY},
}

TRUE_VALUES = {'1', 'on', 't', 'true', 'y', 'yes'}
FALSE_VALUES = {'0', 'off', 'f', 'false', 'n', 'no'}


def strtobool(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f'Invalid boolean value: {value}')


class MongoDbSink(BatchSink):
    """MongoDB target sink class."""

    max_size = 100000

    def preprocess_record(self, record: dict, context: dict) -> dict:
        for key, value in record.items():
            list_of_types = self.schema['properties'][key]['type']
            if isinstance(list_of_types, str):
                list_of_types = [list_of_types]

            if COLUMN_TYPE_NULL in list_of_types and value is None:
                continue

            types = [t for t in list_of_types if t != COLUMN_TYPE_NULL]
            if len(types) > 1:
                # MongoDB stores a field with values of several types. A value of one of
                # the declared types is kept; such schemas used to raise.
                if JSON_TYPES.get(type(value), set()) & set(types):
                    continue
                raise Exception(f"{value!r} in {key} is none of the types {types}")

            type_name = [type for type in list_of_types if type != 'null'][0]
            type_value = type(value)

            try:
                if type_name == COLUMN_TYPE_ARRAY and type_value is not list:
                    record[key] = ast.literal_eval(value)
                elif type_name == COLUMN_TYPE_BOOLEAN and type_value is not bool:
                    record[key] = strtobool(value)
                elif type_name == COLUMN_TYPE_INTEGER and type_value is not int:
                    record[key] = int(value)
                elif type_name == COLUMN_TYPE_NUMBER and type_value not in (float, int):
                    record[key] = float(value)
                elif type_name == COLUMN_TYPE_OBJECT and type_value is not dict:
                    record[key] = json.loads(value)
                elif type_name == COLUMN_TYPE_STRING and type_value is not str:
                    record[key] = str(value)
            except Exception:
                raise Exception(f"Error transforming {value} in {type_name}")

        return record

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # The SDK populates `context["records"]` automatically
        # since we do not override `process_record()`.

        # get connection configs
        connection_string = self.config.get("connection_string")
        db_name = self.config.get("db_name")
        collection = self.config.get("table_name")
        if collection is None:
            collection = urllib.parse.quote(self.stream_name)

        records = context["records"]
        # A client was created for every batch and never closed.
        with pymongo.MongoClient(connection_string,
                                 connectTimeoutMS=2000,
                                 retryWrites=True,
                                 uuidRepresentation='standard') as client:
            target = client[db_name][collection]
            if len(self.key_properties) > 0:
                # Every key property identifies a document; only the first one was used, so
                # records that differed in a later key replaced each other. One bulk write
                # replaces a round trip per record.
                operations = [
                    UpdateOne(
                        {key: self.__key_value(key, record.get(key))
                         for key in self.key_properties},
                        {"$set": {k: v for k, v in record.items() if k != "_id"}},
                        upsert=True,
                    )
                    for record in records
                ]
                if operations:
                    target.bulk_write(operations, ordered=True)
            elif records:
                target.insert_many(records)

        self.logger.info(f"Uploaded {len(records)} records into {collection}")

    @staticmethod
    def __key_value(key: str, value):
        """
        An _id that is an ObjectId's hex string becomes an ObjectId; any other _id is kept.
        Records whose _id was not an ObjectId were skipped without an error.
        """
        if key == "_id" and isinstance(value, str) and ObjectId.is_valid(value):
            return ObjectId(value)
        return value
