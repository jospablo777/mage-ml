import time
import traceback
from dataclasses import dataclass, field
from typing import Dict, List

from mage_ai.io.base import ExportWritePolicy
from mage_ai.io.constants import UNIQUE_CONFLICT_METHOD_IGNORE
from mage_ai.io.postgres import Postgres
from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sinks.base import BaseSink


@dataclass
class PostgresConfig(BaseConfig):
    database: str
    host: str
    password: str
    schema: str
    table: str
    username: str
    port: int = 5432
    unique_conflict_method: str = UNIQUE_CONFLICT_METHOD_IGNORE
    unique_constraints: List = field(default_factory=list)
    allow_reserved_words: bool = False
    auto_clean_name: bool = True
    case_sensitive: bool = False
    # TLS as libpq takes it; sslrootcert is a file, PEM, or base64 PEM. The engine's sink
    # in exported services takes the same.
    sslmode: str = None
    sslrootcert: str = None
    connect_timeout: int = None
    # History mode (ADR-0043): every version of each key, as the engine's sink keeps them.
    history: Dict = None
    # Mirror mode (ADR-0045) runs on the engine: the Run tab and exported services.
    mirror: Dict = None


class PostgresSink(BaseSink):
    config_class = PostgresConfig

    def init_client(self):
        if self.config.mirror:
            raise ValueError(
                'PostgreSQL mirror runs on the engine: run the stream from the Run tab, or '
                'export it as a service.'
            )
        # Initialize postgres connection
        tls = {}
        if self.config.sslmode:
            tls['sslmode'] = self.config.sslmode
        if self.config.sslrootcert:
            from mage_ai.io.tls import ca_certificate_file

            tls['sslrootcert'] = ca_certificate_file(self.config.sslrootcert)
        self.postgres_client = Postgres(
            dbname=self.config.database,
            user=self.config.username,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
            schema=self.config.schema,
            connect_timeout=self.config.connect_timeout,
            **tls,
        )
        self.postgres_client.open()

    def write(self, message: Dict):
        self.batch_write([message])

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        self._print(
            f'Batch ingest {len(messages)} records, time={time.time()}. Sample: {messages[0]}')

        df = self._frame(messages)
        if self.config.history:
            self._write_history(df)
            return
        self.postgres_client.export(
            df,
            self.config.schema,
            self.config.table,
            if_exists=ExportWritePolicy.APPEND,
            unique_conflict_method=self.config.unique_conflict_method,
            unique_constraints=self.config.unique_constraints,
            allow_reserved_words=self.config.allow_reserved_words,
            auto_clean_name=self.config.auto_clean_name,
            case_sensitive=self.config.case_sensitive,
        )

    def _write_history(self, df):
        """
        The batch as versions of its keys (a type 2 history): each row starts at its time
        (valid_from) and ends where its key's next version starts (valid_to); a change capture
        delete is a version marked _mage_deleted. The same SQL as the engine's sink
        (connector-postgres/src/history.rs), through a staging table.
        """
        from mage_ai.shared.utils import clean_name

        history = self.config.history
        keys = history.get('key') if isinstance(history, dict) else None
        keys = [keys] if isinstance(keys, str) else keys
        if not keys:
            raise ValueError('PostgreSQL history: {key: [id], time: _mage_commit_time}.')
        time_field = history.get('time') or '_mage_commit_time'

        def column(name: str) -> str:
            if not self.config.auto_clean_name:
                return name
            return clean_name(name, case_sensitive=self.config.case_sensitive)

        def quote(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        for needed in [*keys, time_field]:
            if needed not in df.columns:
                raise ValueError(f'PostgreSQL history: the messages have no {needed}.')
        schema, table = self.config.schema, self.config.table
        stage = f'{table}__mage_stage'
        self.postgres_client.export(
            df, schema, stage, if_exists=ExportWritePolicy.REPLACE,
            allow_reserved_words=self.config.allow_reserved_words,
            auto_clean_name=self.config.auto_clean_name,
            case_sensitive=self.config.case_sensitive,
        )
        full = f'{quote(schema)}.{quote(table)}'
        staged = f'{quote(schema)}.{quote(stage)}'
        key_list = ', '.join(quote(column(k)) for k in keys)
        time_column = quote(column(time_field))
        with self.postgres_client.conn.cursor() as cursor:
            cursor.execute(
                'SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a '
                'JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = '
                'c.relnamespace WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND '
                'NOT a.attisdropped', (schema, stage),
            )
            stage_columns = dict(cursor.fetchall())
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {full} (LIKE {staged}); '
                f'ALTER TABLE {full} ADD COLUMN IF NOT EXISTS valid_from timestamptz, '
                'ADD COLUMN IF NOT EXISTS valid_to timestamptz, '
                'ADD COLUMN IF NOT EXISTS _mage_deleted boolean NOT NULL DEFAULT false; '
                f'CREATE UNIQUE INDEX IF NOT EXISTS {quote(table[:47] + "_mageml_versions")} '
                f'ON {full} ({key_list}, valid_from); '
                f'CREATE TABLE IF NOT EXISTS {quote(schema)}.mageml_history_progress '
                '(history_table text PRIMARY KEY, through timestamptz NOT NULL, '
                'updated_at timestamptz NOT NULL)'
            )
            for name, kind in stage_columns.items():
                cursor.execute(
                    f'ALTER TABLE {full} ADD COLUMN IF NOT EXISTS {quote(name)} {kind}'
                )
            kind = stage_columns.get(column(time_field), 'text')
            if kind.startswith('timestamp') or kind == 'date' or kind == 'text':
                time = f'{time_column}::timestamptz'
            else:
                time = f'to_timestamp({time_column}::double precision / 1000)'
            deleted = (
                "coalesce(\"_mage_operation\" = 'delete', false)"
                if '_mage_operation' in stage_columns else 'false'
            )
            cols = ', '.join(quote(c) for c in stage_columns)
            cursor.execute(
                f'INSERT INTO {full} ({cols}, valid_from, _mage_deleted) '
                f'SELECT DISTINCT ON ({key_list}, {time}) {cols}, {time}, {deleted} '
                f'FROM {staged} ORDER BY {key_list}, {time} '
                f'ON CONFLICT ({key_list}, valid_from) DO NOTHING; '
                f'UPDATE {full} AS t SET valid_to = n.next FROM (SELECT ctid AS id, '
                f'lead(valid_from) OVER (PARTITION BY {key_list} ORDER BY valid_from) AS next '
                f'FROM {full} WHERE ({key_list}) IN (SELECT {key_list} FROM {staged})) AS n '
                'WHERE t.ctid = n.id AND t.valid_to IS DISTINCT FROM n.next; '
                f'INSERT INTO {quote(schema)}.mageml_history_progress '
                f'(history_table, through, updated_at) SELECT %s, max({time}), now() '
                f'FROM {staged} ON CONFLICT (history_table) DO UPDATE SET through = '
                'greatest(mageml_history_progress.through, EXCLUDED.through), '
                f'updated_at = now(); DROP TABLE {staged}',
                (f'{schema}.{table}',),
            )
        self.postgres_client.conn.commit()

    def destroy(self):
        try:
            self.postgres_client.close()
        except Exception:
            traceback.print_exc()
