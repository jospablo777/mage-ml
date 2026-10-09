"""
A streaming source of PostgreSQL changes: logical replication with pgoutput.

The transformer gets lists of changed rows. Each row has its columns, with the types of
the columns, and:

- _mage_operation: insert, update or delete
- _mage_schema and _mage_table: the table of the change
- _mage_lsn: the log position of the change
- _mage_commit_time: when its transaction committed, in UTC
- _mage_deleted_at: for deletes, when the change was read

A delete has the key columns only, unless the table has REPLICA IDENTITY FULL. A large
value an update did not change is not in the log; it is read from the table by key.

The server learns that a transaction was handled once the transformer has returned for
every change of it, and that position is saved in the streaming checkpoint, so a
restarted pipeline continues after it. Changes of a transaction that was partly handled
come again.
"""
import io
import select
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sources.base import BaseSource

POSTGRES_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


@dataclass
class PostgresConfig(BaseConfig):
    host: str
    database: str
    user: str
    password: str = None
    port: int = 5432
    replication_slot: str = 'mage_slot'
    publication_name: str = 'mage_pub'
    # Tables to read, as schema.table. Without them, every table of the publication.
    tables: List[str] = field(default_factory=list)
    # Create the slot and the publication when they do not exist. A publication without
    # tables is FOR ALL TABLES, which needs a superuser.
    create_slot: bool = True
    batch_size: int = 100
    # Seconds to wait for a batch to fill before handing over what arrived.
    batch_timeout: float = 1.0
    # The log position to start at, such as 0/16B3748, when no position is saved.
    start_lsn: str = None
    connect_timeout: int = 10
    sslmode: str = None


@dataclass
class Relation:
    namespace: str
    name: str
    replica_identity: str
    # (is part of the key, name, type oid)
    columns: List[Tuple[bool, str, int]]


def lsn_to_int(lsn: str) -> int:
    high, low = lsn.split('/')
    return (int(high, 16) << 32) + int(low, 16)


def int_to_lsn(value: int) -> str:
    return f'{value >> 32:X}/{value & 0xFFFFFFFF:X}'


class Reader:
    """Reads the fields of a pgoutput message."""

    def __init__(self, payload: bytes):
        self.buffer = io.BytesIO(payload)

    def byte(self) -> bytes:
        return self.buffer.read(1)

    def int(self, size: int) -> int:
        return int.from_bytes(self.buffer.read(size), 'big', signed=True)

    def string(self) -> str:
        data = bytearray()
        while True:
            char = self.buffer.read(1)
            if char in (b'\x00', b''):
                return data.decode('utf-8')
            data += char

    def tuple(self) -> List[Tuple[str, Optional[str]]]:
        columns = []
        for _ in range(self.int(2)):
            kind = self.byte().decode()
            if kind in ('t', 'b'):
                data = self.buffer.read(self.int(4))
                columns.append((kind, data.decode('utf-8') if kind == 't' else data))
            else:
                # n: NULL; u: a large value the change did not modify, not in the log.
                columns.append((kind, None))
        return columns


class PostgresSource(BaseSource):
    config_class = PostgresConfig

    def init_client(self):
        import psycopg2
        import psycopg2.extras

        connect = dict(
            host=self.config.host,
            port=int(self.config.port),
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
            connect_timeout=self.config.connect_timeout,
        )
        if self.config.sslmode:
            connect['sslmode'] = self.config.sslmode
        self._print(
            f'Connecting to PostgreSQL at {self.config.host}:{self.config.port}/'
            f'{self.config.database}',
        )
        self.connection = psycopg2.connect(**connect)
        self.connection.autocommit = True
        psycopg2.extras.register_uuid(conn_or_curs=self.connection)
        self.cursor = self.connection.cursor()
        self.replication = psycopg2.connect(
            connection_factory=psycopg2.extras.LogicalReplicationConnection, **connect,
        )
        if self.config.create_slot:
            self.__create_slot_and_publication()
        self._print('Connected to PostgreSQL.')

    def __create_slot_and_publication(self):
        self.cursor.execute(
            'SELECT 1 FROM pg_publication WHERE pubname = %s', (self.config.publication_name,),
        )
        if self.cursor.fetchone() is None:
            tables = ', '.join(self.__quote_table(t) for t in self.config.tables)
            target = f'FOR TABLE {tables}' if tables else 'FOR ALL TABLES'
            self.cursor.execute(
                f'CREATE PUBLICATION "{self.config.publication_name}" {target}',
            )
            self._print(f'Created publication {self.config.publication_name} {target}.')
        self.cursor.execute(
            'SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
            (self.config.replication_slot,),
        )
        if self.cursor.fetchone() is None:
            self.cursor.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                (self.config.replication_slot,),
            )
            self._print(f'Created replication slot {self.config.replication_slot}.')

    def __quote_table(self, table: str) -> str:
        schema, _, name = table.rpartition('.')
        quoted = f'"{name}"'
        return f'"{schema}".{quoted}' if schema else quoted

    def test_connection(self):
        self.cursor.execute('SELECT 1')

    def describe_setup(self) -> List[str]:
        """Whether the server, the slot and the publication are ready."""
        self.cursor.execute('SHOW wal_level')
        lines = [f'wal_level: {self.cursor.fetchone()[0]} (logical replication needs logical)']
        self.cursor.execute(
            'SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
            (self.config.replication_slot,),
        )
        exists = self.cursor.fetchone() is not None
        lines.append(
            f'Replication slot {self.config.replication_slot}: '
            f'{"exists" if exists else "missing, created when the pipeline starts"}',
        )
        self.cursor.execute(
            'SELECT 1 FROM pg_publication WHERE pubname = %s', (self.config.publication_name,),
        )
        exists = self.cursor.fetchone() is not None
        lines.append(
            f'Publication {self.config.publication_name}: '
            f'{"exists" if exists else "missing, created when the pipeline starts"}',
        )
        return lines

    def read(self, handler: Callable):
        pass

    def batch_read(self, handler: Callable):
        start = (self.checkpoint or {}).get('lsn')
        if start is None and self.config.start_lsn:
            start = lsn_to_int(self.config.start_lsn)
        cursor = self.replication.cursor()
        cursor.start_replication(
            slot_name=self.config.replication_slot,
            decode=False,
            start_lsn=start or 0,
            options={
                'proto_version': '1',
                'publication_names': self.config.publication_name,
            },
        )
        self._print(
            f'Reading changes of slot {self.config.replication_slot} from '
            f'{int_to_lsn(start) if start else "its confirmed position"}.',
        )
        tables = {t if '.' in t else f'public.{t}' for t in self.config.tables}
        relations: Dict[int, Relation] = {}
        batch: List[Dict] = []
        batch_started = None
        commit_time = None
        # The end of the last transaction whose changes are all in the batch or handled.
        committed = None
        last_feedback = time.monotonic()

        while True:
            message = cursor.read_message()
            if message is None:
                if batch and time.monotonic() - batch_started >= self.config.batch_timeout:
                    self.__handle(handler, batch, cursor, committed)
                    batch, batch_started = [], None
                    last_feedback = time.monotonic()
                elif time.monotonic() - last_feedback >= 10:
                    cursor.send_feedback()
                    last_feedback = time.monotonic()
                select.select([cursor], [], [], 0.2)
                continue

            reader = Reader(message.payload)
            kind = reader.byte()
            if kind == b'B':
                reader.int(8)
                commit_time = POSTGRES_EPOCH + timedelta(microseconds=reader.int(8))
            elif kind == b'C':
                reader.int(1)
                reader.int(8)
                committed = reader.int(8)
                if not batch:
                    # A transaction without changes of the selected tables.
                    cursor.send_feedback(flush_lsn=committed)
                    self.__save(committed)
            elif kind == b'R':
                relation_id = reader.int(4)
                namespace, name = reader.string(), reader.string()
                replica_identity = reader.byte().decode()
                columns = []
                for _ in range(reader.int(2)):
                    flags = reader.int(1)
                    column_name = reader.string()
                    type_oid = reader.int(4)
                    reader.int(4)
                    columns.append((bool(flags & 1), column_name, type_oid))
                relations[relation_id] = Relation(namespace, name, replica_identity, columns)
            elif kind in (b'I', b'U', b'D'):
                relation = relations.get(reader.int(4))
                if relation is None or (
                    tables and f'{relation.namespace}.{relation.name}' not in tables
                ):
                    continue
                for row in self.__rows(kind, reader, relation):
                    row.update(
                        _mage_schema=relation.namespace,
                        _mage_table=relation.name,
                        _mage_lsn=message.data_start,
                        _mage_commit_time=commit_time.isoformat() if commit_time else None,
                    )
                    batch.append(row)
                    if batch_started is None:
                        batch_started = time.monotonic()
                if len(batch) >= self.config.batch_size:
                    self.__handle(handler, batch, cursor, committed)
                    batch, batch_started = [], None
                    last_feedback = time.monotonic()
            elif kind == b'T':
                self._print('TRUNCATE is not replicated; truncate the destination too.')

    def __rows(self, kind: bytes, reader: Reader, relation: Relation) -> List[Dict]:
        key_columns = [name for is_key, name, _ in relation.columns if is_key]
        old = None
        marker = reader.byte()
        if kind in (b'U', b'D') and marker in (b'K', b'O'):
            old = self.__values(reader.tuple(), relation)
            if kind == b'U':
                marker = reader.byte()
        if kind == b'D':
            if relation.replica_identity != 'f':
                old = {k: v for k, v in old.items() if k in key_columns}
            return [dict(old, _mage_operation='delete', _mage_deleted_at=self.__now())]

        new = self.__values(reader.tuple(), relation)
        missing = [name for _, name, _ in relation.columns if name not in new]
        if missing and old is not None:
            new.update({k: old[k] for k in missing if k in old})
            missing = [name for name in missing if name not in new]
        if missing:
            new.update(self.__read_values(relation, key_columns, new, missing))

        rows = []
        if kind == b'U' and old is not None and key_columns and \
                any(old.get(k) != new.get(k) for k in key_columns):
            # The primary key changed: the row of the old key is gone.
            rows.append(dict(
                {k: old.get(k) for k in key_columns},
                _mage_operation='delete',
                _mage_deleted_at=self.__now(),
            ))
        rows.append(dict(new, _mage_operation='insert' if kind == b'I' else 'update'))
        return rows

    def __values(self, columns: List[Tuple[str, Any]], relation: Relation) -> Dict:
        import psycopg2.extensions

        values = {}
        for (_, name, type_oid), (kind, data) in zip(relation.columns, columns):
            if kind == 'n':
                values[name] = None
            elif kind == 't':
                caster = self.connection.string_types.get(type_oid) or \
                    psycopg2.extensions.string_types.get(type_oid)
                values[name] = caster(data, self.cursor) if caster else data
            elif kind == 'b':
                values[name] = data
        return values

    def __read_values(self, relation, key_columns, row, columns) -> Dict:
        if not key_columns:
            self._print(
                f'Unchanged large values of {relation.namespace}.{relation.name} are not in '
                'the log, and the table has no key to read them by. Set REPLICA IDENTITY FULL.',
            )
            return {}
        selected = ', '.join(f'"{c}"' for c in columns)
        condition = ' AND '.join(f'"{k}" = %s' for k in key_columns)
        self.cursor.execute(
            f'SELECT {selected} FROM "{relation.namespace}"."{relation.name}" '
            f'WHERE {condition}',
            [row.get(k) for k in key_columns],
        )
        found = self.cursor.fetchone()
        return dict(zip(columns, found)) if found else {}

    def __handle(self, handler: Callable, batch: List[Dict], cursor, committed) -> None:
        self._print(f'Handing over {len(batch)} changes.')
        handler(batch)
        if committed:
            cursor.send_feedback(flush_lsn=committed)
            self.__save(committed)

    def __save(self, lsn: int) -> None:
        self.checkpoint = dict(lsn=lsn)
        self.update_checkpoint()

    def __now(self) -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')

    def destroy(self):
        for name in ('replication', 'connection'):
            connection = getattr(self, name, None)
            if connection is not None and not getattr(connection, 'closed', True):
                try:
                    connection.close()
                except Exception:
                    pass
