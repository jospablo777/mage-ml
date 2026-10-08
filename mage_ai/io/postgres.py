import io
from typing import Dict, List, Union

import numpy as np
import polars as pl
from pandas import DataFrame
from psycopg2 import connect, errors
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, UNICODE
from psycopg2.extensions import adapt as psycopg2_adapt
from psycopg2.extensions import new_array_type, register_adapter, register_type
from psycopg2.extras import (
    execute_values,
    register_default_json,
    register_default_jsonb,
)

from mage_ai.io import postgres_types
from mage_ai.io.base import QUERY_ROW_LIMIT, ExportWritePolicy
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.constants import (
    UNIQUE_CONFLICT_METHOD_IGNORE,
    UNIQUE_CONFLICT_METHOD_UPDATE,
)
from mage_ai.io.sql import BaseSQL
from mage_ai.shared.ssh import SSHTunnelForwarder

# Rows per INSERT statement when a unique constraint rules out COPY.
INSERT_PAGE_SIZE = 1000
# Rows per COPY statement, which bounds the text held in memory at once.
COPY_CHUNK_ROWS = 50_000
UNIQUE_CONFLICT_METHODS = (UNIQUE_CONFLICT_METHOD_IGNORE, UNIQUE_CONFLICT_METHOD_UPDATE)
UUID_ARRAY = new_array_type((postgres_types.OID_UUID_ARRAY,), 'UUID[]', UNICODE)


def _adapt_numpy_scalar(value):
    """Hand psycopg2 the equivalent Python scalar."""
    return psycopg2_adapt(value.item())


def register_numpy_adapters() -> None:
    """
    Teach psycopg2 how to bind numpy scalars.

    psycopg2 rejects numpy integers and booleans outright. numpy floats reach the
    float adapter through inheritance, and numpy 2 changed their repr, so they used
    to be bound as the literal text "np.float64(0.5)".
    """
    types = [
        np.bool_,
        np.float16,
        np.float32,
        np.float64,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
    ]

    for numpy_type in types:
        register_adapter(numpy_type, _adapt_numpy_scalar)


register_numpy_adapters()


class Postgres(BaseSQL):
    """
    Handles data transfer between a PostgreSQL database and the Mage app.
    """
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: Union[str, None] = None,
        schema: str = None,
        connection_method: str = 'direct',
        ssh_host: Union[str, None] = None,
        ssh_port: Union[str, None] = None,
        ssh_username: Union[str, None] = None,
        ssh_password: Union[str, None] = None,
        ssh_pkey: Union[str, None] = None,
        verbose=True,
        connect_timeout: int = None,
        **kwargs,
    ) -> None:
        """
        Initializes the data loader.

        Args:
            dbname (str): The name of the database to connect to.
            user (str): The user with which to connect to the database with.
            password (str): The login password for the user.
            host (str): Path to host address for database.
            port (str): Port on which the database is running.
            **kwargs: Additional settings for creating SQLAlchemy engine and connection
        """
        self.ssh_tunnel = None
        super().__init__(
            verbose=verbose,
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            schema=schema,
            connection_method=connection_method,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            ssh_pkey=ssh_pkey,
            connect_timeout=connect_timeout,
            **kwargs,
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'Postgres':
        return cls(
            dbname=config[ConfigKey.POSTGRES_DBNAME],
            user=config[ConfigKey.POSTGRES_USER],
            password=config[ConfigKey.POSTGRES_PASSWORD],
            host=config[ConfigKey.POSTGRES_HOST],
            port=config[ConfigKey.POSTGRES_PORT],
            schema=config[ConfigKey.POSTGRES_SCHEMA],
            connection_method=config[ConfigKey.POSTGRES_CONNECTION_METHOD],
            ssh_host=config[ConfigKey.POSTGRES_SSH_HOST],
            ssh_port=config[ConfigKey.POSTGRES_SSH_PORT],
            ssh_username=config[ConfigKey.POSTGRES_SSH_USERNAME],
            ssh_password=config[ConfigKey.POSTGRES_SSH_PASSWORD],
            ssh_pkey=config[ConfigKey.POSTGRES_SSH_PKEY],
            connect_timeout=config[ConfigKey.POSTGRES_CONNECT_TIMEOUT],
        )

    def default_database(self) -> str:
        return self.settings['dbname']

    def default_schema(self) -> str:
        return self.settings.get('schema')

    def open(self) -> None:
        with self.printer.print_msg('Opening connection to PostgreSQL database'):
            database = self.settings['dbname']
            host = self.settings['host']
            password = self.settings['password']
            port = self.settings['port']
            user = self.settings['user']
            keepalives = self.settings.get('keepalives', 1)
            keepalives_idle = self.settings.get('keepalives_idle', 300)
            if self.settings['connection_method'] == 'ssh_tunnel':
                ssh_setting = dict(ssh_username=self.settings['ssh_username'])
                if self.settings['ssh_pkey'] is not None:
                    ssh_setting['ssh_pkey'] = self.settings['ssh_pkey']
                else:
                    ssh_setting['ssh_password'] = self.settings['ssh_password']

                self.ssh_tunnel = SSHTunnelForwarder(
                    (self.settings['ssh_host'], int(self.settings['ssh_port'] or 22)),
                    remote_bind_address=(host, int(port or 5432)),
                    local_bind_address=('127.0.0.1', 0),
                    **ssh_setting,
                )
                try:
                    self.ssh_tunnel.start()
                    self.ssh_tunnel._check_is_started()
                except Exception:
                    self.ssh_tunnel.stop()
                    self.ssh_tunnel = None
                    raise

                host = '127.0.0.1'
                port = self.ssh_tunnel.local_bind_port

            connect_opts = self.settings.copy()
            # See recognized keyword parameters for psycopg2.connect()
            # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
            unrecognized_keys = [
                'dbname',
                'schema',
                'connection_method',
                'ssh_host',
                'ssh_port',
                'ssh_username',
                'ssh_password',
                'ssh_pkey',
                "verbose"
            ]
            for key in unrecognized_keys:
                connect_opts.pop(key, None)

            connect_opts.update(
                {
                    'database': database,
                    'host': host,
                    'password': password,
                    'port': port,
                    'user': user,
                    'keepalives': keepalives,
                    'keepalives_idle': keepalives_idle,
                }
            )

            try:
                self._ctx = connect(**connect_opts)
            except Exception:
                if self.ssh_tunnel is not None:
                    self.ssh_tunnel.stop()
                    self.ssh_tunnel = None
                raise

    def close(self) -> None:
        """
        Close the underlying connection to the SQL data source if open. Else will do nothing.
        """
        if '_ctx' in self.__dict__:
            self._ctx.close()
            del self._ctx
        # __del__ calls close on an object whose __init__ raised before setting ssh_tunnel.
        if getattr(self, 'ssh_tunnel', None) is not None:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None
        if self.verbose and self.printer.exists_previous_message:
            print('')

    def _rollback(self) -> None:
        """
        End a failed transaction, so later statements on this connection run. psycopg2
        rejects every statement after an error until the transaction ends.
        """
        connection = self.__dict__.get('_ctx')
        if connection is not None and not connection.closed:
            try:
                connection.rollback()
            except Exception:
                pass

    def _transaction_idle(self) -> bool:
        connection = self.__dict__.get('_ctx')
        return (
            connection is not None
            and not connection.closed
            and connection.info.transaction_status == TRANSACTION_STATUS_IDLE
        )

    def load(
        self,
        query_string: str,
        limit: int = QUERY_ROW_LIMIT,
        display_query: Union[str, None] = None,
        verbose: bool = True,
        exact_types: bool = False,
        polars: bool = False,
        **kwargs,
    ) -> Union[DataFrame, pl.DataFrame]:
        """
        Load the result of a query.

        Args:
            exact_types (bool): Build each column from the PostgreSQL column type, so values
                keep their exact value: Int64 for integers, Decimal for numeric, bytes for
                bytea, date and time objects. pandas stores NULL and NaN in a float column
                as one value unless the column holds both. Defaults to False, which uses
                pandas read_sql: integer columns with NULLs become float64 and numeric
                becomes float.
            polars (bool): Return a Polars DataFrame with exact column types. JSON columns
                hold the JSON text.
            **kwargs: Passed to pandas read_sql. With exact_types or polars, only params is
                used.

        The query runs in a transaction that holds locks on the tables it reads. When the
        load starts that transaction, it commits it, so the client does not block other
        sessions' ALTER, DROP or TRUNCATE on those tables. A transaction already open on
        the connection, for example after execute, stays open.
        """
        started_transaction = self._transaction_idle()
        try:
            if exact_types or polars:
                frame = self.__load_typed(
                    query_string,
                    limit=limit,
                    display_query=display_query,
                    verbose=verbose,
                    polars=polars,
                    params=kwargs.get('params'),
                )
            else:
                frame = super().load(
                    query_string,
                    limit=limit,
                    display_query=display_query,
                    verbose=verbose,
                    **kwargs,
                )
        except Exception:
            self._rollback()
            raise
        if started_transaction:
            self.conn.commit()
        return frame

    def __load_typed(
        self,
        query_string: str,
        limit: int,
        display_query: Union[str, None],
        verbose: bool,
        polars: bool,
        params=None,
    ) -> Union[DataFrame, pl.DataFrame]:
        query = self._enforce_limit(self._clean_query(query_string), limit)

        def __load():
            with self.conn.cursor() as cursor:
                # psycopg2 has no reader for uuid[] and returns the array literal as text.
                register_type(UUID_ARRAY, cursor)
                if polars:
                    # Keep the JSON text. psycopg2 parses it by default.
                    register_default_json(conn_or_curs=cursor, loads=lambda text: text)
                    register_default_jsonb(conn_or_curs=cursor, loads=lambda text: text)
                cursor.execute(query, params)
                return postgres_types.frame_from_cursor(cursor, polars=polars)

        if verbose:
            message = f'Loading data with query\n\n{display_query or query_string}\n\n'
            with self.printer.print_msg(message):
                return __load()
        return __load()

    def build_create_schema_command(
        self,
        schema_name: str
    ) -> str:
        clean_schema_name = schema_name.strip('"')
        return f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = '{clean_schema_name}'
            ) THEN
                EXECUTE 'CREATE SCHEMA {schema_name}';
            END IF;
        END
        $$;
        """

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        with self.conn.cursor() as cur:
            table_name = table_name.replace('"', '')
            cur.execute(
                f'SELECT * FROM pg_tables WHERE schemaname = \'{schema_name}\' AND '
                f'tablename = \'{table_name}\''
            )
            return bool(cur.rowcount)

    def _table_column_types(self, full_table_name: str) -> Dict[str, str]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute '
                'WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped',
                (full_table_name,),
            )
            return dict(cursor.fetchall())

    @staticmethod
    def _conflict_method(unique_conflict_method: Union[str, None]) -> Union[str, None]:
        if unique_conflict_method is None:
            return None
        method = str(unique_conflict_method).strip().upper()
        if method not in UNIQUE_CONFLICT_METHODS:
            raise ValueError(
                f'unique_conflict_method must be one of {", ".join(UNIQUE_CONFLICT_METHODS)}, '
                f'got {unique_conflict_method!r}.'
            )
        return method

    @staticmethod
    def _raise_on_duplicate_keys(frame, key_columns: List[str]) -> None:
        """
        ON CONFLICT DO UPDATE fails when one statement touches a key twice, and rows go
        out in pages, so duplicates would fail or succeed depending on the page size.
        Rows with a NULL key never conflict in PostgreSQL and are left out.
        """
        if isinstance(frame, pl.DataFrame):
            keys = frame.select(key_columns).drop_nulls()
            duplicated = keys.filter(keys.is_duplicated()).unique(maintain_order=True)
            count = keys.is_duplicated().sum()
            examples = duplicated.head(5).rows()
        else:
            keys = frame[key_columns].dropna()
            mask = keys.duplicated(keep=False)
            count = int(mask.sum())
            examples = list(
                keys[mask].drop_duplicates().head(5).itertuples(index=False, name=None),
            )
        if count:
            raise ValueError(
                f'{count} rows share a value of {key_columns} with another row in this '
                f'export, for example {examples}. ON CONFLICT DO UPDATE cannot change the '
                'same row twice in one statement. Remove the duplicates before exporting.'
            )

    def export(
        self,
        df,
        schema_name: str = None,
        table_name: str = None,
        if_exists: ExportWritePolicy = ExportWritePolicy.REPLACE,
        index: bool = False,
        verbose: bool = True,
        allow_reserved_words: bool = False,
        auto_clean_name: bool = True,
        case_sensitive: bool = False,
        cascade_on_drop: bool = False,
        drop_table_on_replace: bool = False,
        overwrite_types: Dict = None,
        query_string: Union[str, None] = None,
        unique_conflict_method: str = None,
        unique_constraints: List[str] = None,
        skip_semicolon_at_end: bool = False,
        insert_page_size: int = INSERT_PAGE_SIZE,
        **kwargs,
    ) -> None:
        """
        Export a pandas or Polars DataFrame to a table, creating the schema and table when
        they do not exist.

        Each value is rendered in the text input format of its target column: the column
        type of an existing table, or for a new table a type chosen from the column dtype.
        Rows go through COPY, or through INSERT ... ON CONFLICT pages of insert_page_size
        rows when unique_conflict_method and unique_constraints are both set.

        Args:
            unique_conflict_method (str): UPDATE or IGNORE, in any case. UPDATE raises
                ValueError when two rows share a key, because PostgreSQL rejects updating a
                row twice in one statement. IGNORE keeps the first of them.
            unique_constraints (List[str]): Columns of the unique index ON CONFLICT uses.
        """
        if query_string:
            try:
                return super().export(
                    df,
                    schema_name=schema_name,
                    table_name=table_name,
                    if_exists=if_exists,
                    index=index,
                    verbose=verbose,
                    allow_reserved_words=allow_reserved_words,
                    auto_clean_name=auto_clean_name,
                    case_sensitive=case_sensitive,
                    cascade_on_drop=cascade_on_drop,
                    drop_table_on_replace=drop_table_on_replace,
                    overwrite_types=overwrite_types,
                    query_string=query_string,
                    unique_conflict_method=unique_conflict_method,
                    unique_constraints=unique_constraints,
                    skip_semicolon_at_end=skip_semicolon_at_end,
                    **kwargs,
                )
            except Exception:
                self._rollback()
                raise

        if table_name is None:
            raise Exception('Please provide a table_name argument in the export method.')
        if schema_name is None:
            schema_name = self.default_schema()
        full_table_name = f'{schema_name}.{table_name}' if schema_name else table_name

        method = self._conflict_method(unique_conflict_method)
        # Without unique_constraints there is no ON CONFLICT target, and rows go through COPY.
        if not unique_constraints:
            method = None

        frame = df
        if isinstance(frame, dict):
            frame = DataFrame([frame])
        elif isinstance(frame, list):
            frame = DataFrame(frame)
        elif isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        if isinstance(frame, DataFrame) and index:
            frame = frame.reset_index()

        def clean(column: str) -> str:
            return self._clean_column_name(
                column,
                allow_reserved_words=allow_reserved_words,
                auto_clean_name=auto_clean_name,
                case_sensitive=case_sensitive,
            )

        if auto_clean_name:
            mapping = {column: clean(column) for column in frame.columns}
            if isinstance(frame, pl.DataFrame):
                frame = frame.rename(mapping)
            else:
                frame = frame.rename(columns=mapping)
        columns = [str(column) for column in frame.columns]
        key_columns = [clean(column) for column in (unique_constraints or [])]

        if method == UNIQUE_CONFLICT_METHOD_UPDATE:
            self._raise_on_duplicate_keys(frame, key_columns)

        def __process():
            with self.conn.cursor() as cursor:
                if schema_name:
                    cursor.execute(self.build_create_schema_command(schema_name))

                table_exists = self.table_exists(schema_name, table_name)
                create_table = not table_exists
                if table_exists:
                    if ExportWritePolicy.FAIL == if_exists:
                        raise ValueError(f'Table \'{full_table_name}\' already exists in database.')
                    elif ExportWritePolicy.REPLACE == if_exists:
                        if drop_table_on_replace:
                            command = f'DROP TABLE {full_table_name}'
                            if cascade_on_drop:
                                command = f'{command} CASCADE'
                            cursor.execute(command)
                            create_table = True
                        else:
                            cursor.execute(f'DELETE FROM {full_table_name}')

                if create_table:
                    if isinstance(frame, pl.DataFrame):
                        target_types = {
                            column: postgres_types.polars_column_type(frame.schema[column])
                            for column in columns
                        }
                    else:
                        target_types = {
                            column: postgres_types.pandas_column_type(frame[column])
                            for column in columns
                        }
                    # gen_table_creation_query applies overwrite_types to this mapping.
                    cursor.execute(self.build_create_table_command(
                        target_types,
                        schema_name,
                        table_name,
                        auto_clean_name=auto_clean_name,
                        case_sensitive=case_sensitive,
                        unique_constraints=unique_constraints,
                        overwrite_types=overwrite_types,
                        skip_semicolon_at_end=skip_semicolon_at_end,
                    ))
                else:
                    target_types = self._table_column_types(full_table_name)
                    missing = [column for column in columns if column not in target_types]
                    if missing:
                        raise ValueError(
                            f'Columns {missing} are not in table {full_table_name}, which has '
                            f'{sorted(target_types)}.'
                        )

                rendered = postgres_types.render_columns(frame, columns, target_types)
                insert_columns = ', '.join(f'"{column}"' for column in columns)

                if method:
                    commands = [
                        f'INSERT INTO {full_table_name} ({insert_columns})',
                        'VALUES %s',
                        'ON CONFLICT ({})'.format(', '.join(f'"{c}"' for c in key_columns)),
                    ]
                    if method == UNIQUE_CONFLICT_METHOD_UPDATE:
                        updates = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in columns)
                        commands.append(f'DO UPDATE SET {updates}')
                    else:
                        commands.append('DO NOTHING')
                    # Each value is text cast to its column type, as COPY parses it.
                    template = '(' + ', '.join(
                        f'%s::{target_types[column]}' for column in columns
                    ) + ')'
                    execute_values(
                        cursor,
                        '\n'.join(commands),
                        list(zip(*rendered)) if rendered else [],
                        template=template,
                        page_size=insert_page_size,
                    )
                else:
                    row_count = len(rendered[0]) if rendered else 0
                    for start in range(0, row_count, COPY_CHUNK_ROWS):
                        chunk = [column[start:start + COPY_CHUNK_ROWS] for column in rendered]
                        cursor.copy_expert(
                            f'COPY {full_table_name} ({insert_columns}) FROM STDIN',
                            io.StringIO(postgres_types.copy_text(chunk)),
                        )
            self.conn.commit()

        try:
            if verbose:
                with self.printer.print_msg(f'Exporting data to \'{full_table_name}\''):
                    __process()
            else:
                __process()
        except errors.InvalidColumnReference as err:
            self._rollback()
            raise ValueError(
                f'ON CONFLICT ({", ".join(key_columns)}) needs a unique index or constraint on '
                f'exactly those columns of {full_table_name}. PostgreSQL reported: {err}'
            ) from err
        except Exception:
            self._rollback()
            raise

    def execute(self, query_string: str, **query_vars) -> None:
        """
        Sends query to the connected database.

        Args:
            query_string (str): SQL query string to apply on the connected database.
            query_vars: Variable values to fill in when using format strings in query.
        """
        with self.printer.print_msg(f'Executing query \'{query_string}\''):
            query_string = self._clean_query(query_string)
            try:
                with self.conn.cursor() as cur:
                    cur.execute(query_string, query_vars)
            except Exception:
                self._rollback()
                raise
