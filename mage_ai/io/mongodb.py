import uuid
from typing import Dict, List, Optional, Union
from urllib.parse import quote_plus

import pandas as pd
import polars as pl
from pandas import DataFrame
from pymongo import MongoClient, ReplaceOne, UpdateOne

from mage_ai.io.base import BaseIO
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.export_utils import to_pandas_frame
from mage_ai.io.mongodb_types import arrow_table_from_documents, records_for_export

# Documents per insert or bulk write request.
WRITE_BATCH_SIZE = 10_000


class MongoDB(BaseIO):
    def __init__(
        self,
        connection_string: str = None,
        host: str = None,
        port: int = 27017,
        user: str = None,
        password: str = None,
        database: str = None,
        collection: str = None,
        verbose: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(verbose=verbose)
        # UUIDs are stored as standard BSON binary; pymongo refuses them otherwise.
        options = dict(uuidRepresentation='standard')
        options.update(kwargs.get('client_options') or {})
        if connection_string:
            self.client = MongoClient(connection_string, **options)
        else:
            # The user and password were put in the URI as they were, so a password with
            # '@', ':' or '/' failed to connect.
            credentials = ''
            if user:
                credentials = quote_plus(str(user))
                if password is not None:
                    credentials += ':' + quote_plus(str(password))
                credentials += '@'
            self.client = MongoClient(f'mongodb://{credentials}{host}:{port}/', **options)
        self.database = self.client[database]
        self.collection = collection

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'MongoDB':
        return cls(
            connection_string=config[ConfigKey.MONGODB_CONNECTION_STRING],
            host=config[ConfigKey.MONGODB_HOST],
            port=config[ConfigKey.MONGODB_PORT],
            user=config[ConfigKey.MONGODB_USER],
            password=config[ConfigKey.MONGODB_PASSWORD],
            database=config[ConfigKey.MONGODB_DATABASE],
            collection=config[ConfigKey.MONGODB_COLLECTION],
        )

    def __collection_name(self, collection: Optional[str]) -> str:
        collection = collection or self.collection
        if collection is None:
            raise Exception(
                'Please provide the collection name either in the method args or in'
                ' io_config.yaml.')
        return collection

    def load(
        self,
        collection: str = None,
        query: Dict = None,
        exact_types: bool = False,
        polars: bool = False,
        projection: Dict = None,
        **kwargs,
    ) -> Union[DataFrame, pl.DataFrame]:
        """
        Loads the data frame from the MongoDB collection.

        Args:
            collection (str): MongoDB collection name.
            query (Dict): Filter the result by using a query object. Examples:
                { "address": "Park Lane 38" }, { "address": { "$gt": "S" } }
            exact_types (bool): Return pyarrow-backed columns: integers with missing values
                stay integers, Decimal128 is decimal, dates are UTC timestamps, ObjectId and
                UUID are text, and embedded documents are structs. A field whose values
                have different types is JSON text. Defaults to False, which builds a pandas
                frame from the documents: a field missing from some documents, or holding
                null, turns integers into float.
            polars (bool): Return a Polars frame with the same types.
            projection (Dict): The fields to return, as in find().

        Returns:
            DataFrame: Data frame object loaded from the MongoDB collection.
        """
        if query is None:
            query = dict()

        collection = self.__collection_name(collection)
        documents = list(self.database[collection].find(query, projection))
        if not (exact_types or polars):
            return DataFrame(documents)

        table = arrow_table_from_documents(documents)
        if polars:
            return pl.from_arrow(table)
        return table.to_pandas(types_mapper=pd.ArrowDtype)

    def export(
        self,
        data: Union[DataFrame, List[Dict]],
        collection: str = None,
        if_exists: str = 'append',
        unique_constraints: List[str] = None,
        unique_conflict_method: str = None,
        **kwargs,
    ) -> None:
        """
        Exports the input dataframe to the MongoDB collection.

        Args:
            data (Union[DataFrame, List[Dict]): Data frame or List of Dictionary to export.
            collection (str): MongoDB collection name.
            if_exists (str): 'append' (default) inserts the documents. 'replace' swaps the
                collection for the exported documents in one rename, keeping its indexes.
            unique_constraints (List[str]): Fields that identify a document. With
                unique_conflict_method 'UPDATE', a document with the same values replaces
                the stored one; with 'IGNORE', the stored one is kept. Rerunning an export
                used to insert every document again.
        """
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        if isinstance(data, pl.DataFrame):
            # Arrow-backed columns keep the integers inside lists; through NumPy they became
            # floats.
            data = data.to_pandas(use_pyarrow_extension_array=True)
        data = to_pandas_frame(data)
        if data is None:
            return
        collection = self.__collection_name(collection)

        if isinstance(data, list):
            if data and not all(isinstance(record, dict) for record in data):
                raise Exception('Please provide a pandas DataFrame or a list of dictionary as'
                                ' the input.')
            records = records_for_export(DataFrame(data)) if data else []
        elif isinstance(data, DataFrame):
            records = records_for_export(data)
        else:
            raise Exception('Please provide a pandas DataFrame or a list of dictionary as the'
                            ' input.')

        if if_exists not in ('append', 'replace'):
            raise ValueError(f"if_exists must be 'append' or 'replace', not {if_exists!r}.")
        method = (unique_conflict_method or '').upper() or None
        if unique_constraints and method not in ('UPDATE', 'IGNORE'):
            raise ValueError(
                "unique_conflict_method must be 'UPDATE' or 'IGNORE' with unique_constraints.",
            )
        if unique_constraints:
            missing = [
                index for index, record in enumerate(records)
                if any(record.get(key) is None for key in unique_constraints)
            ]
            if missing:
                raise ValueError(
                    f'Rows {missing[:5]} have no value for a unique constraint field '
                    f'{unique_constraints}; they would all match one document.',
                )

        if not records and if_exists != 'replace':
            # insert_many raised TypeError for an empty frame.
            return

        if if_exists == 'append':
            self.__write(self.database[collection], records, unique_constraints, method)
            return

        # Replace: write to a staging collection, give it the target's indexes, and rename
        # it over the target. Readers see the old documents or the new ones, and a failed
        # export leaves the target as it was. A transaction would end after 60 seconds by
        # default and needs a replica set.
        staging_name = f'{collection}__mage_staging_{uuid.uuid4().hex[:12]}'
        staging = self.database[staging_name]
        try:
            self.database.create_collection(staging_name)
            # Indexes first: building them on an empty collection is cheaper, and a unique
            # index rejects duplicate documents as they are written.
            if collection in self.database.list_collection_names():
                for name, index in self.database[collection].index_information().items():
                    if name == '_id_':
                        continue
                    options = {
                        k: v for k, v in index.items() if k not in ('key', 'v', 'ns')
                    }
                    staging.create_index(index['key'], name=name, **options)
            self.__write(staging, records, unique_constraints, method)
            staging.rename(collection, dropTarget=True)
        except BaseException:
            self.database.drop_collection(staging_name)
            raise

    def __write(
        self,
        target,
        records: List[Dict],
        unique_constraints: Optional[List[str]],
        method: Optional[str],
    ) -> None:
        for start in range(0, len(records), WRITE_BATCH_SIZE):
            batch = records[start:start + WRITE_BATCH_SIZE]
            if not unique_constraints:
                target.insert_many(batch)
                continue
            operations = []
            for record in batch:
                key = {field: record[field] for field in unique_constraints}
                if method == 'UPDATE':
                    # Within one export, the last document with a key wins.
                    operations.append(ReplaceOne(key, record, upsert=True))
                else:
                    operations.append(UpdateOne(key, {'$setOnInsert': record}, upsert=True))
            target.bulk_write(operations, ordered=True)
