import decimal
from typing import IO, Dict, List, Mapping, Union

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import simplejson
from mysql.connector import connect
from mysql.connector.cursor import MySQLCursor
from pandas import DataFrame, Series

from mage_ai.io import mysql_types
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.constants import UNIQUE_CONFLICT_METHOD_UPDATE
from mage_ai.io.export_utils import PandasTypes, insert_rows, to_pandas_frame
from mage_ai.io.sql import BaseSQL
from mage_ai.shared.parsers import encode_complex
from mage_ai.shared.utils import clean_name

QUERY_ROW_LIMIT = 10_000_000
# A UNIQUE key cannot cover a TEXT column. Text key columns are VARCHAR(255): 1,020 bytes
# in utf8mb4, so three fit in InnoDB's 3,072-byte key limit.
KEY_VARCHAR_LENGTH = 255

MAX_TIME = pd.Timedelta(hours=838, minutes=59, seconds=59, microseconds=999999)


def _fits_time(column: Series) -> bool:
    """Whether every value of a timedelta column fits MySQL's TIME(6)."""
    try:
        values = pd.to_timedelta(column.dropna())
    except (TypeError, ValueError):
        return False
    return bool(len(values) == 0 or values.abs().max() <= MAX_TIME)

class MySQL(BaseSQL):
    def __init__(
        self,
        database: str,
        host: str,
        password: str,
        user: str,
        port: int = 3306,
        allow_local_infile: bool = False,
        verbose: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(
            database=database,
            host=host,
            password=password,
            port=port or 3306,
            user=user,
            verbose=verbose,
            allow_local_infile=allow_local_infile,
            **kwargs,
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'MySQL':
        conn_kwargs = dict(
            database=config[ConfigKey.MYSQL_DATABASE],
            host=config[ConfigKey.MYSQL_HOST],
            password=config[ConfigKey.MYSQL_PASSWORD],
            port=config[ConfigKey.MYSQL_PORT],
            user=config[ConfigKey.MYSQL_USER],
        )
        if config[ConfigKey.MYSQL_ALLOW_LOCAL_INFILE] is not None:
            conn_kwargs['allow_local_infile'] = config[ConfigKey.MYSQL_ALLOW_LOCAL_INFILE]
        return cls(**conn_kwargs)

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
    ) -> str:
        if unique_constraints is None:
            unique_constraints = []
        key_columns = {
            clean_name(col, case_sensitive=case_sensitive) if auto_clean_name else col
            for col in unique_constraints
        }
        columns_and_types = []
        for cname in dtypes:
            if overwrite_types is not None and cname in overwrite_types.keys():
                dtypes[cname] = overwrite_types[cname]
            if auto_clean_name:
                cleaned_col_name = clean_name(cname, case_sensitive=case_sensitive)
            else:
                cleaned_col_name = cname
            column_type = dtypes[cname]
            if cleaned_col_name in key_columns and column_type in ('TEXT', 'LONGTEXT'):
                column_type = f'VARCHAR({KEY_VARCHAR_LENGTH})'
            columns_and_types.append(f'`{cleaned_col_name}` {column_type} NULL')

        if unique_constraints:
            unique_constraints = [
                clean_name(col, case_sensitive=case_sensitive) if auto_clean_name else col
                for col in unique_constraints
            ]
            index_name = '_'.join([
                clean_name(table_name, case_sensitive=case_sensitive),
            ] + unique_constraints)
            index_name = f'unique{index_name}'[:64]
            key = ', '.join(f'`{col}`' for col in unique_constraints)
            columns_and_types.append(f'CONSTRAINT `{index_name}` UNIQUE ({key})')

        name = self._full_table_name(schema_name, table_name)
        return f'CREATE TABLE {name} (' + ','.join(columns_and_types) + ');'

    def build_create_schema_command(self, schema_name: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS `{schema_name.replace('`', '``')}`;"

    def _full_table_name(self, schema_name: Union[str, None], table_name: str) -> str:
        # Unquoted, reserved words such as long, and names with a dash or a space, failed.
        name = f"`{table_name.replace('`', '``')}`"
        if schema_name:
            return f"`{schema_name.replace('`', '``')}`.{name}"
        return name

    def open(self) -> None:
        with self.printer.print_msg('Opening connection to MySQL database'):
            self._ctx = connect(**self.settings)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM information_schema.tables '
                'WHERE table_schema = %s AND table_name = %s LIMIT 1',
                (schema_name or self.settings['database'], table_name),
            )
            return len(cur.fetchall()) >= 1

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
        Load the result of a query.

        Args:
            exact_types (bool): Return pyarrow-backed columns built from the MySQL column
                types: integers with NULLs stay integers, DECIMAL stays decimal, and
                TIMESTAMP is in UTC. Defaults to False, which uses pandas read_sql:
                integer columns with NULLs become float64, DECIMAL becomes float, and
                TIMESTAMP is naive, in the session time zone.
            polars (bool): Return a Polars DataFrame with the same types.
            nullable_integers (bool): read_sql's types, except that integer columns stay
                integers, as nullable Int64 or UInt64. SQL blocks load with it.
            **kwargs: Passed to pandas read_sql. With exact_types or polars, only params
                is used.
        """
        # The query runs in a transaction, which keeps its snapshot and holds metadata
        # locks on the tables it read. A load that starts the transaction commits it:
        # left open, a second load on the client read the first one's snapshot, and other
        # sessions' ALTER and DROP on those tables waited.
        started_transaction = not self.conn.in_transaction
        try:
            frame = self.__load(
                query_string,
                limit=limit,
                display_query=display_query,
                verbose=verbose,
                exact_types=exact_types,
                polars=polars,
                nullable_integers=nullable_integers,
                **kwargs,
            )
        except Exception:
            if started_transaction:
                self.conn.rollback()
            raise
        if started_transaction:
            self.conn.commit()
        return frame

    def __load(
        self,
        query_string: str,
        limit: int,
        display_query: Union[str, None],
        verbose: bool,
        exact_types: bool,
        polars: bool,
        nullable_integers: bool = False,
        **kwargs,
    ) -> Union[DataFrame, pl.DataFrame]:
        if nullable_integers and not (exact_types or polars):
            query = self._enforce_limit(self._clean_query(query_string), limit)

            def __load_nullable():
                with self.conn.cursor() as cursor:
                    cursor.execute(query, kwargs.get('params'))
                    return mysql_types.frame_with_nullable_integers(cursor)

            if verbose:
                message = f'Loading data with query\n\n{display_query or query_string}\n\n'
                with self.printer.print_msg(message):
                    return __load_nullable()
            return __load_nullable()
        if not (exact_types or polars):
            return super().load(
                query_string,
                limit=limit,
                display_query=display_query,
                verbose=verbose,
                **kwargs,
            )

        query = self._enforce_limit(self._clean_query(query_string), limit)

        def __load():
            with self.conn.cursor() as cursor:
                cursor.execute('SELECT @@session.time_zone')
                time_zone = cursor.fetchone()[0]
                # TIMESTAMP values are returned in the session time zone.
                cursor.execute("SET time_zone = '+00:00'")
                try:
                    cursor.execute(query, kwargs.get('params'))
                    table = mysql_types.arrow_table_from_cursor(cursor)
                finally:
                    cursor.execute('SET time_zone = %s', (time_zone,))
            if polars:
                # Polars decimals hold 38 digits and it panics on wider Arrow decimals.
                # DECIMAL columns up to 65 digits reach Polars as their exact text.
                return pl.from_arrow(table.cast(pa.schema([
                    field.with_type(pa.string()) if pa.types.is_decimal256(field.type)
                    else field
                    for field in table.schema
                ])))
            return table.to_pandas(types_mapper=pd.ArrowDtype)

        if verbose:
            message = f'Loading data with query\n\n{display_query or query_string}\n\n'
            with self.printer.print_msg(message):
                return __load()
        return __load()

    def export(
        self,
        df,
        schema_name: str = None,
        table_name: str = None,
        if_exists: str = 'replace',
        allow_reserved_words: bool = False,
        auto_clean_name: bool = True,
        case_sensitive: bool = False,
        drop_table_on_replace: bool = False,
        query_string: Union[str, None] = None,
        **kwargs,
    ) -> None:
        """
        Export a frame. Columns of an existing table are matched by name: the cleaned name,
        which prefixes reserved words with an underscore, the plain name, or the name
        cleaned without the prefix. The cleaned name alone was tried, so a frame column c
        or order did not match a table created outside Mage.
        """
        if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            df = to_pandas_frame(df)
        if (
            isinstance(df, DataFrame)
            and table_name
            and not query_string
            and not ('replace' == if_exists and drop_table_on_replace)
            and self.table_exists(schema_name, table_name)
        ):
            existing = self.__column_names(schema_name, table_name)
            mapping = {}
            for column in df.columns:
                names = [
                    self._clean_column_name(
                        str(column),
                        allow_reserved_words=allow_reserved_words,
                        case_sensitive=case_sensitive,
                    ) if auto_clean_name else str(column),
                    str(column),
                    clean_name(str(column), case_sensitive=case_sensitive),
                ]
                match = next((name for name in names if name in existing), None)
                if match is None:
                    raise ValueError(
                        f'Column {column!r} is not in table {table_name}, which has '
                        f'{sorted(existing)}. Tried {list(dict.fromkeys(names))}.',
                    )
                mapping[column] = match
            df = df.rename(columns=mapping)
            auto_clean_name = False

        super().export(
            df,
            schema_name=schema_name,
            table_name=table_name,
            if_exists=if_exists,
            allow_reserved_words=allow_reserved_words,
            auto_clean_name=auto_clean_name,
            case_sensitive=case_sensitive,
            drop_table_on_replace=drop_table_on_replace,
            query_string=query_string,
            **kwargs,
        )

    def __column_names(self, schema_name: str, table_name: str) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                'SELECT column_name FROM information_schema.columns '
                'WHERE table_schema = %s AND table_name = %s',
                (schema_name or self.settings['database'], table_name),
            )
            return [row[0] for row in cur.fetchall()]

    def upload_dataframe(
        self,
        cursor: MySQLCursor,
        df: DataFrame,
        db_dtypes: List[str],
        dtypes: List[str],
        full_table_name: str,
        buffer: Union[IO, None] = None,
        case_sensitive: bool = False,
        unique_constraints: List[str] = None,
        unique_conflict_method: str = None,
        **kwargs,
    ) -> None:
        def serialize_obj(val):
            if isinstance(val, (set, frozenset)):
                # A JSON array, in a fixed order.
                try:
                    val = sorted(val)
                except TypeError:
                    val = sorted(val, key=repr)
            if isinstance(val, (dict, list, tuple, np.ndarray)):
                return simplejson.dumps(
                    val,
                    default=encode_complex,
                    ignore_nan=True,
                )
            return val

        frame = df.copy(deep=False)
        for column in frame.columns:
            series = frame[column]
            if isinstance(series.dtype, pd.ArrowDtype) and pa.types.is_nested(
                series.dtype.pyarrow_dtype,
            ):
                # Arrow lists and structs reach the driver as NumPy arrays and dicts of
                # them; their Python values are stored as JSON.
                frame[column] = pd.Series(
                    [
                        None if value is None else serialize_obj(value)
                        for value in pa.array(series.array).to_pylist()
                    ],
                    index=series.index,
                    dtype=object,
                )
                continue
            if isinstance(series.dtype, pd.DatetimeTZDtype) or (
                isinstance(series.dtype, pd.ArrowDtype)
                and pa.types.is_timestamp(series.dtype.pyarrow_dtype)
                and series.dtype.pyarrow_dtype.tz
            ):
                # Zoned timestamps are stored in DATETIME columns in UTC. A text value with
                # an offset was converted to the session time zone.
                frame[column] = series.dt.tz_convert('UTC').dt.tz_localize(None)

        values_placeholder = ', '.join(["%s" for i in range(len(frame.columns))])
        columns = frame.columns
        # strip_quotes removed the double quotes around text values such as '"a"'.
        values = insert_rows(frame, serialize=serialize_obj, stringify_timestamps=True)

        cleaned_columns = [
            clean_name(col, case_sensitive=case_sensitive)
            if kwargs.get('auto_clean_name', True) else col
            for col in columns
        ]
        insert_columns = ', '.join([f'`{col}`'for col in cleaned_columns])

        query = [
            f'INSERT INTO {full_table_name} ({insert_columns})',
            f'VALUES ({values_placeholder})',
        ]

        if unique_constraints and unique_conflict_method:
            if UNIQUE_CONFLICT_METHOD_UPDATE == unique_conflict_method:
                update_command = [f'`{col}` = new.`{col}`' for col in cleaned_columns]
                query += [
                    'AS new',
                    f"ON DUPLICATE KEY UPDATE {', '.join(update_command)}",
                ]

        sql = '\n'.join(query)

        cursor.executemany(sql, values)

    def clean(self, column: Series, dtype: str) -> Series:
        # Durations that fit TIME(6) stay durations; BaseSQL turns them into nanoseconds.
        if dtype in (PandasTypes.TIMEDELTA, PandasTypes.TIMEDELTA64) and _fits_time(column):
            return column
        return super().clean(column, dtype)

    def get_type(self, column: Series, dtype: str) -> str:
        """
        The MySQL type of a new column. FLOAT and DECIMAL columns used to be DECIMAL, which
        MySQL reads as DECIMAL(10, 0), so 0.25 was stored as 0. Booleans were CHAR(52),
        bytes VARBINARY(255), text TEXT (64 KB), and timestamps TIMESTAMP, which drops
        microseconds, ends in 2038, and converts through the session time zone.
        """
        values = column.dropna()
        if dtype in (
            PandasTypes.MIXED,
            PandasTypes.UNKNOWN_ARRAY,
            PandasTypes.COMPLEX,
        ):
            if len(values) and all(
                isinstance(value, (dict, list, tuple, set, frozenset, np.ndarray))
                for value in values
            ):
                return 'JSON'
            return 'LONGTEXT'
        elif dtype in (PandasTypes.DATETIME, PandasTypes.DATETIME64):
            return 'DATETIME(6)'
        elif dtype == PandasTypes.TIME:
            return 'TIME(6)'
        elif dtype == PandasTypes.DATE:
            return 'DATE'
        elif dtype in (PandasTypes.STRING, PandasTypes.CATEGORICAL, PandasTypes.PERIOD):
            return 'LONGTEXT'
        elif dtype == PandasTypes.BYTES:
            return 'LONGBLOB'
        elif dtype in (PandasTypes.FLOATING, PandasTypes.MIXED_INTEGER_FLOAT):
            return 'DOUBLE'
        elif dtype == PandasTypes.DECIMAL:
            decimals = [value for value in values if isinstance(value, decimal.Decimal)]
            return mysql_types.decimal_column_type(decimals) or 'LONGTEXT'
        elif dtype == PandasTypes.INTEGER:
            if not len(values):
                return 'BIGINT'
            low = min(int(value) for value in values)
            high = max(int(value) for value in values)
            if np.iinfo(np.int64).min <= low and high <= np.iinfo(np.int64).max:
                return 'BIGINT'
            if 0 <= low and high <= np.iinfo(np.uint64).max:
                return 'BIGINT UNSIGNED'
            # 128-bit integers, such as Polars Int128.
            return 'DECIMAL(39, 0)'
        elif dtype == PandasTypes.BOOLEAN:
            return 'BOOLEAN'
        elif dtype in (PandasTypes.TIMEDELTA, PandasTypes.TIMEDELTA64):
            # TIME(6) holds -838:59:59 to 838:59:59. Longer durations were cleaned to
            # integer nanoseconds.
            return 'TIME(6)' if _fits_time(column) else 'BIGINT'
        elif dtype == PandasTypes.EMPTY:
            return 'LONGTEXT'
        else:
            print(f'Invalid datatype provided: {dtype}')

        return 'LONGTEXT'

    def _enforce_limit(self, query: str, limit: int = QUERY_ROW_LIMIT) -> str:
        query = query.strip(';')

        return f"""
SELECT *
FROM (
    {query}
) a
LIMIT {limit}
"""
