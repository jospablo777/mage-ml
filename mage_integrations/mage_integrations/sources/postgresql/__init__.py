import datetime
import re
import uuid
from select import select
from typing import Dict, Generator, List

import psycopg2
import psycopg2.extras
import singer

from mage_integrations.connections.postgresql import PostgreSQL as PostgreSQLConnection
from mage_integrations.destinations.constants import (
    DATETIME_COLUMN_SCHEMA,
    INTERNAL_COLUMN_DELETED_AT,
)
from mage_integrations.sources.base import main
from mage_integrations.sources.constants import (
    COLUMN_FORMAT_DATETIME,
    COLUMN_FORMAT_UUID,
    REPLICATION_METHOD_FULL_TABLE,
    REPLICATION_METHOD_INCREMENTAL,
    REPLICATION_METHOD_LOG_BASED,
)
from mage_integrations.sources.messages import write_state
from mage_integrations.sources.postgresql.decoders import (
    Delete,
    Insert,
    Relation,
    Truncate,
    Update,
    decode_message,
)
from mage_integrations.sources.sql.base import Source
from mage_integrations.utils.dates import utc_now

INTERNAL_COLUMN_LSN = 'lsn'
# The prefix of the logical decoding messages that mark the end of a run.
MARKER_PREFIX = 'mage_cdc'


class PostgreSQL(Source):
    @property
    def table_prefix(self):
        schema = self.config['schema']
        return f'"{schema}".'

    def build_table_name(self, stream) -> str:
        table_name = stream.tap_stream_id

        return f'{self.table_prefix}"{table_name}"'

    def build_connection(self, connection_factory=None) -> PostgreSQLConnection:
        connect_kwargs = dict(
            database=self.config['database'],
            host=self.config['host'],
            password=self.config['password'],
            port=self.config.get('port'),
            username=self.config['username'],
        )
        if connection_factory is not None:
            connect_kwargs['connection_factory'] = connection_factory
        return PostgreSQLConnection(
            **connect_kwargs,
        )

    def build_discover_query(self, streams: List[str] = None):
        schema = self.config['schema']

        """
        pg_constraint: stores information about constraints defined on tables in the database.
        * conrelid: The OID of the table on which the constraint is defined.
        * conkey: For FOREIGN KEY constraints, this column contains an array of the local column
            numbers of the referencing columns.
        * contype: A single-character code representing the type of constraint. Possible values are:
            'p' for PRIMARY KEY constraints
            'u' for UNIQUE constraints
            'c' for CHECK constraints
            'f' for FOREIGN KEY constraints
            'x' for EXCLUSION constraints

        pg_class: stores metadata about database objects, primarily tables and indexes.
        * oid: used to uniquely identify database objects such as tables, indexes, sequences, and
            other database elements.
        * relnamespace: The OID of the namespace (schema) in which the table or index is defined.
        * relkind: A single-character code representing the type of relation.
            'r': Regular table
            'v': View
            'm': Materialized view
            'f': Foreign table

        pg_namespace: stores information about database namespaces, also known as schemas.
        * oid: The OID (object identifier) column stores a unique identifier for each namespace.
            This column serves as the primary key for the table.

        pg_attribute: contains metadata about the attributes (columns) of tables.
        * attrelid: stores the OID (object identifier) of the table.
        * attname: stores the name of the attribute (column).
        * atttypid: stores the OID of the data type of the attribute.
        * atttypmod: stores the type modifier of the attribute. It specifies additional information
            about the data type, such as length, precision, or scale.
        * attnum: stores the attribute number (column number) within the table or composite type.
            It serves as the ordinal position of the attribute within the table.
        * attnotnull: stores information about whether an attribute allows NULL values or not.
        * attisdropped: whether the attribute has been dropped (removed) from the table.

        pg_attrdef: stores default values for table columns.
        * adbin:  stores the binary representation of the default value expression. It contains the
            actual expression defining the default value.
        * adrelid: stores the OID (object identifier) of the table to which the default value
            belongs. It serves as a foreign key referencing the pg_class table.
        """
        query = f"""
WITH unnested_constraints AS (
    SELECT
        conrelid,
        UNNEST(conkey) AS conkey,
        contype
    FROM
        pg_constraint
)

SELECT DISTINCT
    pg_class.relname AS table_name,
    pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) AS column_default,
    CASE
        WHEN contype = 'p'
            THEN 'PRIMARY KEY'
        WHEN contype = 'f'
            THEN 'FOREIGN KEY'
        WHEN contype = 'u'
            THEN 'UNIQUE'
    END AS column_key,
    pg_attribute.attname AS column_name,
    format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type,
    CASE
        WHEN pg_attribute.attnotnull
            THEN 'NO'
        WHEN NOT pg_attribute.attnotnull
            THEN 'YES'
    END AS is_nullable
FROM pg_class
LEFT JOIN
    pg_namespace ON pg_class.relnamespace = pg_namespace.oid
LEFT JOIN
    pg_attribute ON pg_class.oid = pg_attribute.attrelid
LEFT JOIN
    unnested_constraints ON pg_class.oid = unnested_constraints.conrelid
        AND pg_attribute.attnum = unnested_constraints.conkey
LEFT JOIN
    pg_attrdef ON pg_class.oid = pg_attrdef.adrelid
        AND pg_attribute.attnum = pg_attrdef.adnum
WHERE
  pg_attribute.attnum > -1
    AND pg_attribute.attisdropped = 'f'
    AND pg_class.relkind IN ('r', 'v', 'm', 'f')
    AND nspname = '{schema}'
        """

        if streams:
            table_names = ', '.join([f"'{n}'" for n in streams])
            query = f'{query}\nAND pg_class.relname IN ({table_names})'
        return query

    def get_columns(self, table_name: str) -> List[str]:
        schema_name = self.config['schema']
        results = self.build_connection().load(f"""
SELECT
    column_name
    , data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
        """)
        return [r[0] for r in results]

    def internal_column_schema(self, stream, bookmarks: Dict = None) -> Dict[str, Dict]:
        if REPLICATION_METHOD_LOG_BASED == stream.replication_method:
            return {
                INTERNAL_COLUMN_DELETED_AT: DATETIME_COLUMN_SCHEMA,
                INTERNAL_COLUMN_LSN: {'type': ['null', 'integer']},
            }
        return dict()

    def update_column_names(self, columns: List[str]) -> List[str]:
        return list(map(lambda column: f'"{column}"', columns))

    def load_data_from_logs(
        self,
        stream,
        bookmarks: Dict = None,
        query: Dict = None,
        **kwargs,
    ) -> Generator[List[Dict], None, None]:
        """
        Changes of the stream's table from the write-ahead log, read through the logical
        replication slot with pgoutput, after the LSN of the bookmark.

        - Values are matched to columns by the names in the log's Relation messages and
          converted with PostgreSQL's type casters, so records have the types of a full
          sync. They were matched to the columns of information_schema, in no set order,
          and left as text, so arrays failed the destination.
        - Only the table of the configured schema is read; a table of the same name in
          another schema leaked in.
        - A large value an update did not change is not in the log. It is taken from the
          old row or read from the table by primary key; NULL was written over it.
        - An update that changes the primary key also deletes the row of the old key.
        - The run confirms to the server only the LSN of the bookmark, which earlier runs
          completed, so a failed destination does not lose changes. Each change was
          confirmed when it was read.
        - The change at the bookmark is not read again. From PostgreSQL 14, the run writes
          a logical decoding message and stops when it reads it, so it no longer waits
          logical_poll_total_seconds, 60 by default, after the last change.
        """
        if query is None:
            query = dict()
        tap_stream_id = stream.tap_stream_id
        schema_name = self.config['schema']
        bookmarks = bookmarks or dict()
        start_lsn = int(bookmarks.get(INTERNAL_COLUMN_LSN) or 0)
        slot = self.config.get('replication_slot', 'mage_slot')

        # Poll for this long without finding a record.
        poll_total_seconds = self.config.get('logical_poll_total_seconds') or 60 * 1
        keep_alive_time = 10.0
        begin_ts = datetime.datetime.now()

        postgres_connection = self.build_connection(
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
        )
        connection = postgres_connection.build_connection()
        values_connection = self.build_connection().build_connection()
        values_cursor = values_connection.cursor()

        try:
            # A message written to the log now marks where this run ends: the run stops
            # when it reads it. pgoutput sends such messages from PostgreSQL 14.
            marker = None
            if values_connection.server_version >= 140000:
                marker = f'mage-cdc-{uuid.uuid4().hex}'
                values_cursor.execute(
                    'SELECT pg_logical_emit_message(false, %s, %s)', (MARKER_PREFIX, marker),
                )
                values_connection.commit()

            with connection.cursor() as cur:
                end_lsn = self.__get_current_lsn(cur)

                self.logger.info(
                    f'Starting Logical Replication for {tap_stream_id}({slot}): {start_lsn} '
                    f'-> {end_lsn}. poll_total_seconds: {poll_total_seconds}',
                )

                try:
                    options = dict(
                        proto_version='1',
                        publication_names=self.config.get('publication_name', 'mage_pub'),
                    )
                    if marker:
                        options['messages'] = 'true'
                    cur.start_replication(
                        slot_name=slot,
                        decode=False,
                        start_lsn=start_lsn,
                        options=options,
                    )
                except psycopg2.ProgrammingError as error:
                    raise Exception(
                        f'Unable to start replication with logical replication slot {slot}: '
                        f'{error}',
                    ) from error
                if start_lsn:
                    cur.send_feedback(flush_lsn=start_lsn, force=True)

                relations = dict()
                skipped = 0
                while True:
                    poll_duration = (datetime.datetime.now() - begin_ts).total_seconds()
                    if poll_duration > poll_total_seconds:
                        self.logger.info(f'Breaking after {poll_duration} seconds of '
                                         'polling with no data')
                        break

                    msg = cur.read_message()
                    if not msg:
                        now = datetime.datetime.now()
                        timeout = keep_alive_time - (now - cur.io_timestamp).total_seconds()
                        try:
                            sel = select([cur], [], [], max(0, min(timeout, 1)))
                            if not any(sel):
                                cur.send_feedback()
                        except InterruptedError:
                            pass
                        continue

                    begin_ts = datetime.datetime.now()
                    if msg.payload[:1] == b'M':
                        if marker and marker.encode() in msg.payload:
                            self.logger.info(f'Read the log up to {end_lsn}.')
                            break
                        continue
                    if not marker and end_lsn and msg.data_start > end_lsn:
                        self.logger.info(
                            f'Gone past end_lsn {end_lsn} at {msg.data_start} '
                            f'({msg.payload[:1]!r}); {skipped} changes were at or before '
                            f'{start_lsn}.',
                        )
                        break
                    decoded = decode_message(msg.payload)

                    if type(decoded) is Relation:
                        relations[decoded.relation_id] = decoded
                        continue
                    if type(decoded) is Truncate:
                        self.logger.warning(
                            'TRUNCATE is not replicated; truncate the destination table too.',
                        )
                        continue
                    if type(decoded) not in [Delete, Insert, Update]:
                        continue
                    if msg.data_start <= start_lsn:
                        skipped += 1
                        continue

                    relation = relations.get(decoded.relation_id)
                    if relation is None or relation.namespace != schema_name or \
                            relation.relation_name != tap_stream_id:
                        continue

                    for payload in self.__payloads(decoded, relation, values_cursor):
                        payload[INTERNAL_COLUMN_LSN] = msg.data_start
                        yield [payload]
        finally:
            values_cursor.close()
            values_connection.close()
            postgres_connection.close_connection(connection)

    def __values(self, tuple_data, relation, values_cursor) -> Dict:
        """
        A row of the log, by column name, with values converted from their text by the
        type casters of psycopg2. Columns whose value the log left out are missing.
        """
        values = dict()
        for (_, name, type_oid, _), column in zip(relation.columns, tuple_data.column_data):
            if column.col_data_category == 'n':
                values[name] = None
            elif column.col_data_category == 't':
                caster = values_cursor.connection.string_types.get(type_oid) or \
                    psycopg2.extensions.string_types.get(type_oid)
                values[name] = caster(column.col_data, values_cursor) if caster \
                    else column.col_data
        return values

    def __payloads(self, decoded, relation, values_cursor) -> List[Dict]:
        key_columns = [c[1] for c in relation.columns if c[0]]
        old = None
        if getattr(decoded, 'old_tuple', None) is not None:
            old = self.__values(decoded.old_tuple, relation, values_cursor)

        if type(decoded) is Delete:
            return [dict(old or {}, **{INTERNAL_COLUMN_DELETED_AT: self.__now()})]

        new = self.__values(decoded.new_tuple, relation, values_cursor)
        missing = [c[1] for c in relation.columns if c[1] not in new]
        for name in missing:
            if old is not None and name in old:
                new[name] = old[name]
        missing = [name for name in missing if name not in new]
        if missing:
            new.update(self.__read_values(relation, key_columns, new, missing, values_cursor))

        payloads = []
        if type(decoded) is Update and old is not None and key_columns and \
                any(old.get(k) != new.get(k) for k in key_columns):
            payloads.append(dict(old, **{INTERNAL_COLUMN_DELETED_AT: self.__now()}))
        payloads.append(new)
        return payloads

    def __read_values(self, relation, key_columns, row, columns, values_cursor) -> Dict:
        """Values the log left out, which are unchanged, read from the table by key."""
        if not key_columns:
            self.logger.warning(
                f'Unchanged large values of {relation.relation_name} are not in the log and '
                'the table has no primary key to read them by. Set REPLICA IDENTITY FULL.',
            )
            return {}
        table = f'"{relation.namespace}"."{relation.relation_name}"'
        selected = ', '.join(f'"{c}"' for c in columns)
        condition = ' AND '.join(f'"{k}" = %s' for k in key_columns)
        values_cursor.execute(
            f'SELECT {selected} FROM {table} WHERE {condition}',
            [row.get(k) for k in key_columns],
        )
        found = values_cursor.fetchone()
        values_cursor.connection.rollback()
        return dict(zip(columns, found)) if found else {}

    def __now(self) -> str:
        return utc_now().strftime('%Y-%m-%d %H:%M:%S.%f')

    def column_type_mapping(self, column_type: str, column_format: str = None) -> str:
        if COLUMN_FORMAT_DATETIME == column_format:
            return 'TIMESTAMP'
        elif COLUMN_FORMAT_UUID == column_format:
            return 'UUID'

        return super().column_type_mapping(column_type, column_format)

    def _after_load_data(self, stream):
        if REPLICATION_METHOD_LOG_BASED == stream.replication_method:
            # The initial sync is bookmarked at the slot's confirmed LSN, which is before
            # every change the slot still holds. The LSN of the end of the sync was used,
            # so changes made while the table was read were never read from the log.
            slot = self.config.get('replication_slot', 'mage_slot')
            postgres_connection = self.build_connection()
            connection = postgres_connection.build_connection()
            with connection.cursor() as cur:
                cur.execute(
                    'SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = %s',
                    (slot,),
                )
                row = cur.fetchone()
                lsn = None
                if row and row[0]:
                    file, index = row[0].split('/')
                    lsn = (int(file, 16) << 32) + int(index, 16)
                if lsn is None:
                    lsn = self.__get_current_lsn(cur)
            postgres_connection.close_connection(connection)
            # A bookmark is the LSN of a change that was read, and the next run skips the
            # changes at or before it. The confirmed LSN, like the insert LSN, is where
            # the next change can start: on an idle server the first change after the
            # initial sync started there and was never read. WAL records are 8-byte
            # aligned, so none starts at the byte before.
            lsn -= 1

            state = singer.write_bookmark(
                {},
                stream.tap_stream_id,
                INTERNAL_COLUMN_LSN,
                lsn,
            )
            write_state(state)

    def _get_bookmark_properties_for_stream(self, stream, bookmarks: Dict = None) -> List[str]:
        if REPLICATION_METHOD_LOG_BASED == self._replication_method(stream, bookmarks=bookmarks):
            return [INTERNAL_COLUMN_LSN]
        elif REPLICATION_METHOD_LOG_BASED == stream.replication_method:
            # Initial sync for LOG_BASED replication
            return self._get_replication_key(stream)
        else:
            return super()._get_bookmark_properties_for_stream(stream)

    def _replication_method(self, stream, bookmarks: Dict = None):
        if REPLICATION_METHOD_LOG_BASED != stream.replication_method:
            return stream.replication_method
        # Ues full table sync for the initial sync of log based replcation
        if not bookmarks or not bookmarks.get(INTERNAL_COLUMN_LSN):
            if self._get_replication_key(stream):
                # If bookmark columns are selected, use incremental sync as the initial sync
                return REPLICATION_METHOD_INCREMENTAL
            else:
                return REPLICATION_METHOD_FULL_TABLE

        return stream.replication_method

    def __get_current_lsn(self, cur):
        # Fetch current lsn
        version = self.__get_pg_version(cur)
        if version == 9:
            cur.execute("SELECT pg_current_xlog_location()")
        elif version > 9:
            # The insert position. pg_current_wal_lsn is the write position, which can be
            # before transactions that committed with synchronous_commit off, so a run
            # stopped before changes that were already committed.
            cur.execute("SELECT pg_current_wal_insert_lsn()")
        else:
            raise Exception('unable to fetch current lsn for PostgresQL version {}'.format(version))

        current_lsn = cur.fetchone()[0]
        if not current_lsn:
            return None

        file, index = current_lsn.split('/')
        current_lsn = (int(file, 16) << 32) + int(index, 16)
        return current_lsn

    def __get_pg_version(self, cur):
        cur.execute("SELECT version()")
        res = cur.fetchone()[0]
        version_match = re.match(r'PostgreSQL (\d+)', res)
        if not version_match:
            raise Exception('unable to determine PostgreSQL version from {}'.format(res))

        version = int(version_match.group(1))
        self.logger.info(f'Detected PostgresSQL version: {version}')
        return version


if __name__ == '__main__':
    main(PostgreSQL)
