
import functools
import json
import logging
import uuid
from typing import IO, Any, Dict, List, Tuple, Union

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from pandas import DataFrame, Series

from mage_ai.io.base import QUERY_ROW_LIMIT, ExportWritePolicy
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.constants import (
    UNIQUE_CONFLICT_METHOD_IGNORE,
    UNIQUE_CONFLICT_METHOD_UPDATE,
)
from mage_ai.io.export_utils import PandasTypes
from mage_ai.io.sql import BaseSQL
from mage_ai.shared.parsers import encode_complex
from mage_ai.shared.utils import clean_name

logger = logging.getLogger(__name__)


class DuckDB(BaseSQL):
    def __init__(
            self,
            database: str,
            motherduck_token: str = None,
            schema: str = None,
            verbose: bool = True,
            **kwargs,) -> None:
        """
        Initializes settings to connect to duck.
        """
        super().__init__(
            database=database,
            motherduck_token=motherduck_token,
            schema=schema,
            verbose=verbose,
            **kwargs
        )
        self.open()

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'DuckDB':
        return cls(
            database=config[ConfigKey.DUCKDB_DATABASE],
            motherduck_token=config[ConfigKey.MOTHERDUCK_TOKEN],
            schema=config[ConfigKey.DUCKDB_SCHEMA],
        )

    def default_schema(self) -> str:
        return self.settings.get('schema') or 'main'

    def close(self) -> None:
        """
        Close the underlying connection to the SQL data source if open. Else will do nothing.
        """
        self._ctx.close()

    def open(self) -> None:
        with self.printer.print_msg('Opening connection to DuckDB'):
            conn_kwargs = dict(
                read_only=False,
            )
            database_url = self.settings['database']
            if database_url and database_url.startswith('md:'):
                config = dict(
                    autoload_known_extensions=False,
                    custom_user_agent='MAGE',
                )
                if self.settings.get('motherduck_token'):
                    config['motherduck_token'] = self.settings.get('motherduck_token')
                conn_kwargs['config'] = config
            self._ctx = duckdb.connect(
                database_url,
                **conn_kwargs,
            )

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
        Loads the result of a query.

        nullable_integers=True returns read_sql's types, except that integer columns stay
        integers: nullable Int64 or UInt64, and Python ints for HUGEINT and UHUGEINT.
        read_sql turned integer columns with NULL into float64. SQL blocks load with it.

        By default the result goes through pandas.read_sql, as before. exact_types=True
        returns pyarrow-backed pandas columns built from DuckDB's Arrow result, which keep
        integer widths, integers with NULL, decimals, lists, intervals and UUIDs.
        polars=True returns a Polars frame. In both, zoned timestamps are in UTC; DuckDB
        exports them in the session time zone. UHUGEINT columns, and HUGEINT columns in
        Polars, are read as text and returned as exact integers. The Arrow paths read the
        result in columns instead of building Python rows: 0.04 s instead of 4.3 s for
        1,000,000 rows.
        """
        if nullable_integers and not (exact_types or polars):
            return self.__load_nullable_integers(
                query_string, limit, display_query, verbose, kwargs.get('params') or None,
            )
        if not (exact_types or polars):
            return super().load(
                query_string,
                limit=limit,
                display_query=display_query,
                verbose=verbose,
                **kwargs,
            )
        query = self._enforce_limit(self._clean_query(query_string), limit)
        message = 'Loading data'
        if verbose:
            message += f' with query\n\n{display_query or query}\n\n'
        params = kwargs.get('params') or None
        with self.printer.print_msg(message):
            relation = self.conn.sql(query, params=params)
            # DuckDB exports HUGEINT and UHUGEINT as decimal128(38, 0). UHUGEINT values
            # above 2**127 wrap around (the largest arrives as -1), and Polars rounds
            # HUGEINT values of 39 digits to 38. These columns are read as text and
            # rebuilt as exact integers.
            wide = {
                name: str(kind) for name, kind in zip(relation.columns, relation.types)
                if str(kind) == 'UHUGEINT' or (polars and str(kind) == 'HUGEINT')
            }
            if wide:
                replaced = ', '.join(f'{_quote(n)}::VARCHAR AS {_quote(n)}' for n in wide)
                relation = self.conn.sql(
                    f'SELECT * REPLACE ({replaced}) FROM ({query}) AS mage_query',
                    params=params,
                )
            table = _zoned_timestamps_in_utc(relation.to_arrow_table())
        if polars:
            frame = pl.from_arrow(_intervals_for_polars(table))
            return frame.with_columns(
                pl.Series(
                    name,
                    [None if v is None else int(v) for v in frame[name].to_list()],
                    dtype=pl.UInt128 if kind == 'UHUGEINT' else pl.Int128,
                )
                for name, kind in wide.items()
            )
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        for name in wide:
            frame[name] = pd.Series(
                [None if v is None or v is pd.NA else int(v) for v in frame[name].tolist()],
                index=frame.index,
                dtype=object,
            )
        return frame

    def __load_nullable_integers(
        self,
        query_string: str,
        limit: int,
        display_query: Union[str, None],
        verbose: bool,
        params,
    ) -> DataFrame:
        query = self._enforce_limit(self._clean_query(query_string), limit)
        message = 'Loading data'
        if verbose:
            message += f' with query\n\n{display_query or query}\n\n'
        with self.printer.print_msg(message):
            relation = self.conn.sql(query, params=params)
            names, kinds = relation.columns, [str(kind) for kind in relation.types]
            rows = relation.fetchall()
        # read_sql builds its frame this way from a DBAPI cursor.
        frame = pd.DataFrame.from_records(rows, columns=names, coerce_float=True)
        for position, kind in enumerate(kinds):
            if kind in INTEGER_TYPES:
                frame.isetitem(position, pd.array(
                    [row[position] for row in rows],
                    dtype='UInt64' if kind.startswith('U') else 'Int64',
                ))
            elif kind in ('HUGEINT', 'UHUGEINT'):
                frame.isetitem(position, pd.Series(
                    [row[position] for row in rows], index=frame.index, dtype=object,
                ))
        return frame

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        if schema_name is None or len(schema_name) == 0:
            schema_name = 'main'
        with self.conn.cursor() as cur:
            cur.execute('\n'.join([
                'SELECT * FROM information_schema.tables',
                f'WHERE table_schema = \'{schema_name}\' AND table_name = \'{table_name}\'',
            ]))
            result = cur.fetchall()
            return len(result) >= 1

    def export(
        self,
        df: Union[DataFrame, pl.DataFrame, pl.LazyFrame, pa.Table, Dict, List],
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
        **kwargs,
    ) -> None:
        """
        Exports a pandas or Polars frame, a LazyFrame or an Arrow table to a DuckDB table.

        DuckDB reads the frame through Arrow, so new tables get the frame's own types:
        integers of each width, nullable integers, floats, booleans, decimals, zoned
        timestamps, intervals, bytes, lists and structs. Dicts and nested values in pandas
        object columns are stored as JSON. Rows are inserted by column name, and the
        export runs in one transaction, so a failure leaves the table unchanged.

        Args:
            unique_conflict_method (str): UPDATE or IGNORE, in any case. UPDATE raises
                ValueError when two rows share a key; DuckDB would keep the first of them
                without an error. IGNORE keeps the first of them.
            unique_constraints (List[str]): Columns of the unique constraint ON CONFLICT
                uses. New tables are created with it.
        """
        if query_string:
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
        if table_name is None:
            raise Exception('Please provide a table_name argument in the export method.')
        schema_name = schema_name or self.default_schema()
        full_table_name = f'{_quote(schema_name)}.{_quote(table_name)}'

        method = _conflict_method(unique_conflict_method) if unique_constraints else None
        frame = df
        if isinstance(frame, dict):
            frame = DataFrame([frame])
        elif isinstance(frame, list):
            frame = DataFrame(frame)
        elif isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        if isinstance(frame, DataFrame) and index:
            frame = frame.reset_index()
        table, column_types = duckdb_arrow(frame)

        def clean(column: str) -> str:
            return self._clean_column_name(
                str(column),
                allow_reserved_words=allow_reserved_words,
                auto_clean_name=auto_clean_name,
                case_sensitive=case_sensitive,
            )

        def candidates(column: str) -> List[str]:
            # Mage prefixes reserved words with an underscore. Tables Mage created have the
            # prefixed name; tables created elsewhere usually have the plain one.
            names = [clean(column), str(column)]
            if auto_clean_name:
                names.append(clean_name(str(column), case_sensitive=case_sensitive))
            return list(dict.fromkeys(names))

        def __process():
            existing = self.table_exists(schema_name, table_name)
            create = not existing
            self.conn.execute('BEGIN TRANSACTION')
            try:
                self.conn.execute(f'CREATE SCHEMA IF NOT EXISTS {_quote(schema_name)}')
                if existing:
                    if ExportWritePolicy.FAIL == if_exists:
                        raise ValueError(f'Table {full_table_name} already exists.')
                    if ExportWritePolicy.REPLACE == if_exists:
                        if drop_table_on_replace:
                            cascade = ' CASCADE' if cascade_on_drop else ''
                            self.conn.execute(f'DROP TABLE {full_table_name}{cascade}')
                            create = True
                        else:
                            self.conn.execute(f'DELETE FROM {full_table_name}')

                target = {} if create else self._table_column_types(schema_name, table_name)
                if create:
                    names = [clean(c) for c in table.column_names]
                else:
                    names = []
                    for column in table.column_names:
                        match = next((n for n in candidates(column) if n in target), None)
                        if match is None:
                            raise ValueError(
                                f'Column {column!r} is not in table {full_table_name}, which '
                                f'has {sorted(target)}. Tried {candidates(column)}.'
                            )
                        names.append(match)
                if len(set(names)) != len(names):
                    raise ValueError(
                        'Columns would have the same name after cleaning, and one would '
                        f'overwrite the other: {table.column_names} -> {names}.',
                    )
                renamed = table.rename_columns(names)
                final = dict(zip(table.column_names, names))
                expressions = {final[k]: v for k, v in column_types.items()}
                for name, field in zip(names, renamed.schema):
                    timestamp = TIMESTAMP_TYPES.get(getattr(field.type, 'unit', None))
                    if (
                        pa.types.is_timestamp(field.type)
                        and target.get(name) in TIMESTAMP_TYPES.values()
                        and target[name] != timestamp
                    ):
                        # DuckDB has no cast between TIMESTAMP_S, _MS and _NS; it goes
                        # through TIMESTAMP.
                        expressions.setdefault(
                            name, f'CAST(CAST({{}} AS TIMESTAMP) AS {target[name]})',
                        )
                keys = [
                    names[table.column_names.index(k)] if k in table.column_names else clean(k)
                    for k in (unique_constraints or [])
                ]
                if method == UNIQUE_CONFLICT_METHOD_UPDATE:
                    _raise_on_duplicate_keys(renamed, keys)

                view = f'mage_export_{uuid.uuid4().hex}'
                null_keys = None
                if method and keys:
                    # A NULL key never conflicts, but DuckDB's ON CONFLICT DO UPDATE keeps
                    # only one of several rows with a NULL key. Those rows are inserted
                    # without ON CONFLICT.
                    complete = functools.reduce(
                        pc.and_kleene, [pc.is_valid(renamed[k]) for k in keys],
                    )
                    null_keys = renamed.filter(pc.invert(complete))
                    renamed = renamed.filter(complete)
                self.conn.register(view, renamed)
                try:
                    select = ', '.join(
                        f'{expressions[n].format(_quote(n))} AS {_quote(n)}'
                        if n in expressions else _quote(n)
                        for n in names
                    )
                    if create:
                        described = self.conn.execute(
                            f'DESCRIBE SELECT {select} FROM {view}',
                        ).fetchall()
                        definitions = [
                            f'{_quote(name)} {(overwrite_types or {}).get(name, kind)}'
                            for name, kind, *_ in described
                        ]
                        if keys:
                            definitions.append(
                                'UNIQUE ({})'.format(', '.join(_quote(k) for k in keys)),
                            )
                        self.conn.execute(
                            f'CREATE TABLE {full_table_name} ({", ".join(definitions)})',
                        )
                    command = f'INSERT INTO {full_table_name} BY NAME SELECT {select} FROM {view}'
                    if method:
                        command += ' ON CONFLICT ({})'.format(', '.join(_quote(k) for k in keys))
                        if method == UNIQUE_CONFLICT_METHOD_UPDATE:
                            updates = ', '.join(
                                f'{_quote(n)} = EXCLUDED.{_quote(n)}'
                                for n in names if n not in keys
                            )
                            command += f' DO UPDATE SET {updates}' if updates else ' DO NOTHING'
                        else:
                            command += ' DO NOTHING'
                    self.conn.execute(command)
                    if null_keys is not None and null_keys.num_rows:
                        self.conn.unregister(view)
                        self.conn.register(view, null_keys)
                        self.conn.execute(
                            f'INSERT INTO {full_table_name} BY NAME SELECT {select} FROM {view}',
                        )
                finally:
                    self.conn.unregister(view)
                self.conn.execute('COMMIT')
            except Exception:
                self.conn.execute('ROLLBACK')
                raise

        if verbose:
            with self.printer.print_msg(f'Exporting data to {full_table_name}'):
                __process()
        else:
            __process()

    def _table_column_types(self, schema_name: str, table_name: str) -> Dict[str, str]:
        rows = self.conn.execute(
            'SELECT column_name, data_type FROM information_schema.columns '
            'WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position',
            [schema_name, table_name],
        ).fetchall()
        return dict(rows)

    def upload_dataframe(
        self,
        cursor,
        df: DataFrame,
        db_dtypes: List[str],
        dtypes: List[str],
        full_table_name: str,
        buffer: Union[IO, None] = None,
        **kwargs,
    ) -> None:
        sql = f'INSERT INTO {full_table_name} SELECT * FROM df'
        cursor.execute(sql)

    def get_type(self, column: Series, dtype: str) -> str:
        if dtype in (
            PandasTypes.MIXED,
            PandasTypes.UNKNOWN_ARRAY,
            PandasTypes.COMPLEX,
        ):
            return 'TEXT'
        elif dtype in (PandasTypes.DATETIME, PandasTypes.DATETIME64):
            try:
                if column.dt.tz:
                    return 'TIMESTAMP'
            except AttributeError:
                pass
            return 'TIMESTAMP'
        elif dtype == PandasTypes.TIME:
            try:
                if column.dt.tz:
                    return 'TIME'
            except AttributeError:
                pass
            return 'TIME'
        elif dtype == PandasTypes.DATE:
            return 'DATE'
        elif dtype == PandasTypes.STRING:
            return 'TEXT'
        elif dtype == PandasTypes.CATEGORICAL:
            return 'TEXT'
        elif dtype == PandasTypes.BYTES:
            return 'VARBINARY(255)'
        elif dtype in (PandasTypes.FLOATING, PandasTypes.DECIMAL, PandasTypes.MIXED_INTEGER_FLOAT):
            return 'DECIMAL'
        elif dtype == PandasTypes.INTEGER:
            # Every width mapped to the same type, and numpy 2 raises OverflowError
            # when narrowing a Python int that does not fit.
            return 'BIGINT'
        elif dtype == PandasTypes.BOOLEAN:
            return 'CHAR(52)'
        elif dtype in (PandasTypes.TIMEDELTA, PandasTypes.TIMEDELTA64, PandasTypes.PERIOD):
            return 'BIGINT'
        elif dtype == PandasTypes.EMPTY:
            return 'CHAR(255)'
        else:
            print(f'Invalid datatype provided: {dtype}')

        return 'CHAR(255)'


def _zoned_timestamps_in_utc(table: pa.Table) -> pa.Table:
    """Mark zoned timestamps as UTC. The instants do not change, only the display zone."""
    fields = [
        field.with_type(pa.timestamp(field.type.unit, 'UTC'))
        if pa.types.is_timestamp(field.type) and field.type.tz is not None
        else field
        for field in table.schema
    ]
    return table.cast(pa.schema(fields, metadata=table.schema.metadata))


TIMESTAMP_TYPES = {
    's': 'TIMESTAMP_S', 'ms': 'TIMESTAMP_MS', 'us': 'TIMESTAMP', 'ns': 'TIMESTAMP_NS',
}
POLARS_INTERVAL_FIELDS = {'months': pl.Int32, 'days': pl.Int32, 'nanoseconds': pl.Int64}
INTERVAL_STRUCT = pa.struct([
    ('months', pa.int32()), ('days', pa.int32()), ('nanoseconds', pa.int64()),
])


def _intervals_for_polars(table: pa.Table) -> pa.Table:
    """
    Polars cannot import Arrow's month_day_nano_interval, the type of DuckDB INTERVAL
    columns. Intervals without months become exact nanosecond durations; months have no
    fixed length, so a column with months becomes a struct of months, days and
    nanoseconds.
    """
    for index, field in enumerate(table.schema):
        if not pa.types.is_interval(field.type):
            continue
        values = table.column(index).to_pylist()
        if all(v is None or v.months == 0 for v in values):
            column = pa.array(
                [None if v is None else v.days * 86_400 * 10**9 + v.nanoseconds for v in values],
                type=pa.duration('ns'),
            )
        else:
            column = pa.array(
                [None if v is None else dict(months=v.months, days=v.days,
                                             nanoseconds=v.nanoseconds) for v in values],
                type=INTERVAL_STRUCT,
            )
        table = table.set_column(index, field.name, column)
    return table


# DuckDB's integer types that fit 64 bits.
INTEGER_TYPES = {
    'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT',
    'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
}


def _quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _conflict_method(method: Union[str, None]) -> Union[str, None]:
    if method is None:
        return None
    normalized = str(method).upper()
    if normalized not in (UNIQUE_CONFLICT_METHOD_UPDATE, UNIQUE_CONFLICT_METHOD_IGNORE):
        raise ValueError(
            f'unique_conflict_method must be {UNIQUE_CONFLICT_METHOD_UPDATE} or '
            f'{UNIQUE_CONFLICT_METHOD_IGNORE}, not {method!r}',
        )
    return normalized


def _raise_on_duplicate_keys(table: pa.Table, keys: List[str]) -> None:
    if not keys:
        return
    frame = pl.from_arrow(table.select(keys))
    if not isinstance(frame, pl.DataFrame):
        frame = frame.to_frame()
    frame = frame.drop_nulls()
    duplicated = frame.filter(frame.is_duplicated()).unique(maintain_order=True)
    if duplicated.height:
        raise ValueError(
            f'{duplicated.height} keys appear more than once in the rows to export, and '
            'UPDATE would keep one of them silently. Examples: '
            f'{duplicated.head(5).rows()}',
        )


def _is_json_value(value: Any) -> bool:
    return isinstance(value, dict) or (
        isinstance(value, (list, tuple)) and any(isinstance(v, (dict, list, tuple)) for v in value)
    )


def duckdb_arrow(frame: Any) -> Tuple[pa.Table, Dict[str, str]]:
    """
    An Arrow table DuckDB reads with exact types, and the SQL expression, with {} for the
    column, that gives the DuckDB value of columns Arrow cannot type: JSON for dicts and
    nested values, UUID for UUID objects, HUGEINT and UHUGEINT for Polars 128-bit
    integers, INTERVAL for the struct a Polars load gives intervals with months.
    """
    if isinstance(frame, pa.Table):
        return frame, {}
    if isinstance(frame, pl.DataFrame):
        objects = [c for c, t in frame.schema.items() if t == pl.Object]
        if objects:
            raise ValueError(f'Polars Object columns cannot be exported: {objects}')
        # Arrow has no 128-bit integers; these go as text and are cast in DuckDB.
        wide = {c: t for c, t in frame.schema.items() if t in (pl.Int128, pl.UInt128)}
        types = {
            c: 'CAST({} AS UHUGEINT)' if t == pl.UInt128 else 'CAST({} AS HUGEINT)'
            for c, t in wide.items()
        }
        if wide:
            frame = frame.with_columns(pl.col(list(wide)).cast(pl.String))
        for column, dtype in frame.schema.items():
            if dtype == pl.Struct(POLARS_INTERVAL_FIELDS):
                # The struct a Polars load gives DuckDB intervals with months.
                types[column] = (
                    'to_months({0}.months) + to_days({0}.days) '
                    '+ to_microseconds({0}.nanoseconds // 1000)'
                )
        return frame.to_arrow(), types

    types = {}
    columns = {}
    for name in frame.columns:
        series = frame[name]
        if isinstance(series.dtype, pd.ArrowDtype):
            # pyarrow-backed columns go as they are; some, such as intervals, have no
            # NumPy equivalent for the checks below.
            columns[name] = series.array.__arrow_array__()
            continue
        if isinstance(series.dtype, pd.CategoricalDtype):
            series = series.astype(str).where(series.notna(), None)
        if pd.api.types.is_object_dtype(series.dtype):
            values = [None if v is None or v is pd.NA or (isinstance(v, float) and v != v)
                      else v for v in series.tolist()]
            present = [v for v in values if v is not None]
            if present and any(_is_json_value(v) for v in present):
                columns[name] = pa.array(
                    [None if v is None
                     else json.dumps(v, default=encode_complex, ensure_ascii=False)
                     for v in values],
                    pa.string(),
                )
                types[str(name)] = 'CAST({} AS JSON)'
                continue
            if present and all(isinstance(v, uuid.UUID) for v in present):
                columns[name] = pa.array([None if v is None else str(v) for v in values])
                types[str(name)] = 'CAST({} AS UUID)'
                continue
            try:
                columns[name] = pa.array(values)
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
                columns[name] = pa.array(
                    [None if v is None else str(v) for v in values], pa.string(),
                )
            continue
        # NaN in a NumPy float column is pandas' missing value marker and becomes NULL.
        columns[name] = pa.Array.from_pandas(series)
    # Built from the arrays directly: a pandas frame would turn date32 into datetime64.
    return pa.table(
        [columns[name] for name in frame.columns],
        names=[str(name) for name in frame.columns],
    ), types
