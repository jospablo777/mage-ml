from typing import Dict, List, Mapping, Union

import pandas as pd
import polars as pl
import urllib3
from pandas import DataFrame, Series, read_sql
from trino.auth import BasicAuthentication
from trino.dbapi import Connection
from trino.dbapi import Cursor as CursorParent
from trino.exceptions import TrinoUserError
from trino.transaction import IsolationLevel

from mage_ai.io import trino_types
from mage_ai.io.base import QUERY_ROW_LIMIT, ExportWritePolicy
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.export_utils import to_pandas_frame
from mage_ai.io.sql import BaseSQL, ignore_dbapi_connection_warning
from mage_ai.shared.utils import clean_name

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Cursor(CursorParent):
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass


class ConnectionWrapper(Connection):
    def cursor(self, legacy_primitive_types: bool = None):
        """Return a new :py:class:`Cursor` object using the connection."""
        if self.isolation_level != IsolationLevel.AUTOCOMMIT:
            if self.transaction is None:
                self.start_transaction()
        if self.transaction is not None:
            request = self.transaction.request
        else:
            request = self._create_request()
        return Cursor(
            self,
            request,
            legacy_primitive_types
            if legacy_primitive_types is not None
            else False
        )




class Trino(BaseSQL):
    # Rows are inserted in statements of at most this many characters. Trino rejects
    # statements longer than its query.max-length, 1,000,000 by default.
    QUERY_MAX_LENGTH = 500_000

    def __init__(
        self,
        catalog: str,
        host: str,
        user: str,
        password: str = None,
        port: int = 8080,
        schema: str = None,
        verbose: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(
            verbose=verbose,
            catalog=catalog,
            host=host,
            user=user,
            password=password,
            port=port,
            schema=schema,
            **kwargs
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'Trino':
        if config.get('trino'):
            settings = config['trino']

            return cls(
                catalog=settings.get('catalog'),
                host=settings.get('host'),
                http_headers=settings.get('http_headers'),
                http_scheme=settings.get('http_scheme'),
                password=settings.get('password'),
                port=settings.get('port'),
                schema=settings.get('schema'),
                session_properties=settings.get('session_properties'),
                source=settings.get('source'),
                user=settings.get('user'),
                verify=settings.get('verify'),
                data_type_properties=settings.get('data_type_properties'),
                overwrite_types=settings.get('overwrite_types'),
            )

        return cls(
            catalog=config[ConfigKey.TRINO_CATALOG],
            host=config[ConfigKey.TRINO_HOST],
            user=config[ConfigKey.TRINO_USER],
            password=config[ConfigKey.TRINO_PASSWORD],
            port=config[ConfigKey.TRINO_PORT],
            schema=config[ConfigKey.TRINO_SCHEMA],
        )

    def default_database(self) -> str:
        return self.settings.get('catalog')

    def default_schema(self) -> str:
        return self.settings.get('schema')

    def build_create_table_command(
        self,
        dtypes: Mapping[str, str],
        schema_name: str,
        table_name: str,
        auto_clean_name: bool = True,
        case_sensitive: bool = False,
        unique_constraints: List[str] = None,
        overwrite_types: Dict = None,
        **kwargs,
    ):
        if unique_constraints is None:
            unique_constraints = []
        query = []
        for cname in dtypes:
            if overwrite_types is not None and cname in overwrite_types.keys():
                dtypes[cname] = overwrite_types[cname]
            if auto_clean_name:
                cleaned_col_name = clean_name(cname, case_sensitive=case_sensitive)
            else:
                cleaned_col_name = cname
            query.append(f'"{cleaned_col_name}" {dtypes[cname]}')

        full_table_name = '.'.join(list(filter(lambda x: x, [
            schema_name,
            table_name,
        ])))

        return f'CREATE TABLE {full_table_name} (' + ','.join(query) + ')'

    def load(
        self,
        query_string: str,
        limit: int = QUERY_ROW_LIMIT,
        display_query: Union[str, None] = None,
        verbose: bool = True,
        exact_types: bool = False,
        polars: bool = False,
        nullable_integers: bool = False,
        **kwargs,
    ) -> Union[DataFrame, pl.DataFrame]:
        """
        Load the result of a query, at most limit rows. A failed query raises; it was
        printed, run twice more, and load returned None.

        The query runs as written and the rows after limit are not fetched. It was run as
        SELECT * FROM (query) LIMIT n, and Trino drops an ORDER BY in a subquery, so the
        rows came in any order.

        Args:
            exact_types (bool): Return pyarrow-backed columns built from the Trino column
                types: integers with NULLs stay integers, DECIMAL stays decimal, timestamps
                keep microseconds, and zoned timestamps are in UTC. Defaults to False,
                which uses pandas read_sql: integer columns with NULLs become float64.
            polars (bool): Return a Polars DataFrame with the same types.
            nullable_integers (bool): read_sql's types, except that integer columns stay
                integers, as nullable Int64. SQL blocks load with it.
            **kwargs: Passed to pandas read_sql, without exact_types, polars or
                nullable_integers.
        """
        query = self._clean_query(query_string).rstrip(';')

        def __load():
            if not (exact_types or polars or nullable_integers):
                with ignore_dbapi_connection_warning():
                    chunks = read_sql(query, self.conn, chunksize=limit, **kwargs)
                    try:
                        return next(chunks)
                    finally:
                        # Closes the cursor, which cancels the rest of the query.
                        chunks.close()

            cursor = self.conn.cursor()
            try:
                cursor.execute(query)
                rows = cursor.fetchmany(limit)
                description = cursor.description or []
            finally:
                cursor.close()
            if not (exact_types or polars):
                return trino_types.frame_with_nullable_integers(description, rows)
            table = trino_types.arrow_table(description, rows)
            if polars:
                return pl.from_arrow(table)
            return table.to_pandas(types_mapper=pd.ArrowDtype)

        if verbose:
            message = f'Loading data with query\n\n{display_query or query_string}\n\n'
            with self.printer.print_msg(message):
                return __load()
        return __load()

    def open(self) -> None:
        with self.printer.print_msg('Opening connection to Trino database'):
            connect_kwargs = dict(
                catalog=self.settings.get('catalog'),
                host=self.settings.get('host'),
                http_headers=self.settings.get('http_headers'),
                http_scheme=self.settings.get('http_scheme'),
                port=self.settings.get('port'),
                schema=self.settings.get('schema'),
                session_properties=self.settings.get('session_properties'),
                source=self.settings.get('source'),
                user=self.settings.get('user'),
                verify=self.settings.get('verify'),
            )

            if self.settings.get('password'):
                connect_kwargs['auth'] = \
                    BasicAuthentication(
                        self.settings['user'], self.settings['password'])
                if 'http_scheme' not in connect_kwargs:
                    connect_kwargs['http_scheme'] = 'https'
            self._ctx = ConnectionWrapper(**connect_kwargs)

    def _table_types(self, cursor: Cursor, catalog: str, schema_name: str, table_name: str):
        """
        The names and types of a table's columns, empty when it does not exist. SHOW
        SCHEMAS and SHOW TABLES with LIKE read _ as any character, so another table could
        match. Trino stores names in lower case.
        """
        cursor.execute(
            'SELECT column_name, data_type FROM '
            f'{trino_types.quote(catalog)}.information_schema.columns '
            f'WHERE table_schema = {trino_types.string_literal(str(schema_name).lower())} '
            f'AND table_name = {trino_types.string_literal(str(table_name).lower())} '
            'ORDER BY ordinal_position'
        )
        return trino_types.table_types(cursor.fetchall())

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        with self.conn.cursor() as cursor:
            return bool(self._table_types(
                cursor,
                self.default_database(),
                schema_name or self.default_schema(),
                table_name,
            ))

    def _connector(self, cursor: Cursor, catalog: str) -> Union[str, None]:
        """The connector of a catalog, such as iceberg or delta_lake."""
        try:
            cursor.execute(
                'SELECT connector_name FROM system.metadata.catalogs '
                f'WHERE catalog_name = {trino_types.string_literal(catalog)}'
            )
            rows = cursor.fetchall()
        except TrinoUserError:
            # Trino versions before 400 have no connector_name.
            return None
        return rows[0][0] if rows else None

    def get_type(self, column: Series, dtype: str, settings, connector: str = None) -> str:
        precision = (settings.get('data_type_properties') or {}).get('timestamp_precision')
        return trino_types.column_type(
            column,
            connector=connector,
            timestamp_precision=precision,
        )

    def export(
        self,
        df: DataFrame,
        schema_name: str = None,
        table_name: str = None,
        if_exists: ExportWritePolicy = ExportWritePolicy.REPLACE,
        index: bool = False,
        verbose: bool = True,
        query_string: Union[str, None] = None,
        drop_table_on_replace: bool = False,
        cascade_on_drop: bool = False,
    ) -> None:
        """
        Exports dataframe to the connected database from a Pandas data frame. If table doesn't
        exist, the table is automatically created. If the schema doesn't exist, the schema is
        also created.

        New tables get column types that hold the values: integers of their width, uint64
        as DECIMAL(20, 0), DECIMAL(P, S), DATE, TIMESTAMP(6), with time zone for zoned
        columns, UUID and VARBINARY. Durations are stored in microseconds, and lists and
        dicts as JSON text. The Delta Lake connector has no UUID or TIME type, so these
        are text, and stores zoned timestamps in milliseconds. Values are written as the
        types of an existing table's columns, which are matched by name.

        Args:
            schema_name (str): Name of the schema of the table to export data to.
            table_name (str): Name of the table to insert rows from this data frame into.
            if_exists (ExportWritePolicy): Specifies export policy if table exists. Either
                - `'fail'`: throw an error.
                - `'replace'`: deletes the rows of the existing table, or drops it with
                    drop_table_on_replace.
                - `'append'`: appends data frame to existing table.
            Defaults to `'replace'`.
            index (bool): If true, the data frame index is also exported alongside the table.
                            Defaults to False.
        """
        df = to_pandas_frame(df)
        if table_name is None:
            raise Exception('Please provide a table_name argument in the export method.')
        if schema_name is None:
            schema_name = self.default_schema()

        if type(df) is dict:
            df = DataFrame([df])
        elif type(df) is list:
            df = DataFrame(df)

        catalog = self.default_database()
        full_table_name = '.'.join(
            trino_types.quote(name) for name in (catalog, schema_name, table_name)
        )

        if not query_string and index:
            df = df.reset_index()

        def __process():
            with self.conn.cursor() as cursor:
                def run(statement: str) -> List:
                    cursor.execute(statement)
                    return cursor.fetchall()

                if schema_name:
                    run(
                        'CREATE SCHEMA IF NOT EXISTS '
                        f'{trino_types.quote(catalog)}.{trino_types.quote(schema_name)}'
                    )

                existing = self._table_types(cursor, catalog, schema_name, table_name)
                if existing:
                    if ExportWritePolicy.FAIL == if_exists:
                        raise ValueError(
                            f'Table \'{full_table_name}\' already exists in database.'
                        )
                    elif ExportWritePolicy.REPLACE == if_exists:
                        if drop_table_on_replace:
                            cmd = f'DROP TABLE {full_table_name}'
                            if cascade_on_drop:
                                cmd = f'{cmd} CASCADE'
                            run(cmd)
                            existing = {}
                        else:
                            self.__delete_rows(run, full_table_name)

                if query_string:
                    # CREATE TABLE AS failed for a table kept by replace.
                    if existing:
                        run(f'INSERT INTO {full_table_name}\n{query_string}')
                    else:
                        run(f'CREATE TABLE {full_table_name} AS\n{query_string}')
                    return

                self.__insert_frame(run, cursor, df, catalog, full_table_name, existing)

            self.conn.commit()

        if verbose:
            with self.printer.print_msg(
                f'Exporting data to \'{full_table_name}\''
            ):
                __process()
        else:
            __process()

    def __delete_rows(self, run, full_table_name: str) -> None:
        try:
            run(f'DELETE FROM {full_table_name}')
        except TrinoUserError as error:
            # The memory connector, among others, cannot delete rows.
            if error.error_name != 'NOT_SUPPORTED':
                raise
            run(f'TRUNCATE TABLE {full_table_name}')

    def __insert_frame(
        self,
        run,
        cursor: Cursor,
        df: DataFrame,
        catalog: str,
        full_table_name: str,
        existing: Dict[str, str],
    ) -> None:
        connector = self._connector(cursor, catalog)
        overwrite_types = self.settings.get('overwrite_types') or {}
        names = []
        types = []
        create_types = {}
        for position, column in enumerate(df.columns):
            name = clean_name(str(column), case_sensitive=False)
            if existing and name not in existing and str(column).lower() in existing:
                # A table created outside Mage, with a name clean_name changes.
                name = str(column).lower()
            column_type = overwrite_types.get(column) or self.get_type(
                df.iloc[:, position],
                None,
                self.settings,
                connector=connector,
            )
            names.append(name)
            types.append(existing.get(name, column_type))
            create_types[name] = column_type

        if not existing:
            run(self.build_create_table_command(
                create_types,
                None,
                full_table_name,
                auto_clean_name=False,
            ))

        rows = df.itertuples(index=False, name=None)
        for statement in trino_types.insert_statements(
            full_table_name,
            names,
            types,
            rows,
            self.QUERY_MAX_LENGTH,
        ):
            run(statement)
