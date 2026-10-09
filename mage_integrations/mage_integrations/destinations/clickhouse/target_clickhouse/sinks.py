"""clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, Iterable

import simplejson as json
import sqlalchemy.types
from clickhouse_sqlalchemy import Table, engines
from clickhouse_sqlalchemy import types as ch_types
from singer_sdk.connectors import SQLConnector
from sqlalchemy import Column, MetaData, create_engine

from mage_integrations.destinations.sqlsink import SQLSink

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class ClickhouseConnector(SQLConnector):
    """Clickhouse Meltano Connector.

    Inherits from `SQLConnector` class, overriding methods where needed
    for Clickhouse compatibility.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for clickhouse.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)

    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine for clickhouse."""
        return create_engine(self.get_sqlalchemy_url(self.config))

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """
        The ClickHouse type of a JSON schema type, Nullable unless the schema rules out
        null. singer-sdk's generic types made integers Int32, which wrapped 2**53 + 1
        around to 1, numbers Float32, and date-times DateTime, without microseconds; and
        no column was Nullable, so NULL became 0, '', false or 1970-01-01.
        """
        types = jsonschema_type.get('type') or ['string']
        if isinstance(types, str):
            types = [types]
        string_format = jsonschema_type.get('format')
        if 'integer' in types:
            sql_type = ch_types.Int64()
        elif 'number' in types:
            sql_type = ch_types.Float64()
        elif 'boolean' in types:
            sql_type = ch_types.Boolean()
        elif 'string' in types and string_format == 'date-time':
            sql_type = ch_types.DateTime64(6, 'UTC')
        elif 'string' in types and string_format == 'date':
            sql_type = ch_types.Date32()
        else:
            # Strings, and objects and arrays, which are written as JSON text.
            sql_type = ch_types.String()
        if 'null' in types or len(types) != 1:
            sql_type = ch_types.Nullable(sql_type)
        return sql_type

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """
        Keep a column whose type matches. Reflected time zones have doubled quotes, as
        DateTime64(6, ''UTC''), so a second sync into a table found every date-time
        column changed, and singer-sdk's type merge raised NotImplementedError for the
        ClickHouse types.
        """
        current_type = self._get_column_type(full_table_name, column_name)
        if str(current_type).replace("''", "'") == str(sql_type):
            return
        raise NotImplementedError(
            f"Altering columns is not supported. Column '{full_table_name}.{column_name}' "
            f"is {current_type}, and the stream's schema needs {sql_type}.",
        )

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """Create an empty target table, using Clickhouse Engine.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            msg = "Temporary tables are not supported."
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, _, table_name = self.parse_full_table_name(full_table_name)

        # If config table name is set, then use it instead of the table name.
        if self.config.get("table_name"):
            table_name = self.config.get("table_name")

        # Do not set schema, as it is not supported by Clickhouse. SQLAlchemy 2 removed
        # MetaData's bind, so creating the table failed on the first SCHEMA message;
        # create_all gets the engine.
        meta = MetaData(schema=None)
        columns: list[Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                ),
            )

        table_engine = engines.MergeTree(primary_key=primary_keys)
        _ = Table(table_name, meta, *columns, table_engine)
        meta.create_all(self._engine)

    def prepare_schema(self, _: str) -> None:
        """Create the target database schema.

        In Clickhouse, a schema is a database, so this method is a no-op.

        Args:
            schema_name: The target schema name.
        """
        return


class ClickhouseSink(SQLSink):
    """clickhouse target sink class."""

    connector_class = ClickhouseConnector

    @property
    def full_table_name(self) -> str:
        """Return the fully qualified table name.

        Returns
            The fully qualified table name.
        """
        # Use the config table name if set.
        if self.config.get("table_name"):
            return self.config.get("table_name")

        return self.connector.get_fully_qualified_name(
            table_name=self.table_name,
            schema_name=self.schema_name,
            db_name=self.database_name,
        )

    def bulk_insert_records(
            self,
            full_table_name: str,
            schema: dict,
            records: Iterable[dict[str, Any]],
         ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        # Need to convert any records with a dict type to a JSON string.
        records = list(records)
        for record in records:
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    record[key] = json.dumps(value)
                elif isinstance(value, datetime.datetime):
                    # The HTTP driver writes datetimes without fractions of a second.
                    if value.tzinfo is not None:
                        value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    record[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')

        return super().bulk_insert_records(full_table_name, schema, records)

    def _validate_and_parse(self, record: dict) -> dict:
        """Validate or repair the record, parsing to python-native types as needed.

        Args:
            record: Individual record in the stream.

        Returns:
            TODO
        """
        self._parse_timestamps_in_record(
            record=record,
            schema=self.schema,
            treatment=self.datetime_error_treatment,
        )
        return record
