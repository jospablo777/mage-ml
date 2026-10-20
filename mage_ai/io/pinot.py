"""
Apache Pinot for blocks: queries through the broker's SQL API (pinotdb).

Results are typed from Pinot's own column types, not guessed by pandas: INT and LONG are
nullable integers, so a LONG keeps every digit when the column has nulls; TIMESTAMP is a
UTC datetime; BIG_DECIMAL an exact Decimal; BYTES bytes; JSON the parsed value. Queries run
with enableNullHandling, so a table that stores nulls returns them; one that does not
returns Pinot's default values in their place (-9223372036854775808 for a LONG, 'null' for
a STRING), as Pinot stores them.
"""
import json
import re
from decimal import Decimal
from typing import IO, Any, Dict, List, Union

import pandas as pd
from pandas import DataFrame, Series
from pinotdb import connect

from mage_ai.io.base import QUERY_ROW_LIMIT, ExportWritePolicy, checks_row_limit
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.sql import BaseSQL

WRITE_NOT_SUPPORTED_EXCEPTION = Exception('write operations are not supported.')


class Pinot(BaseSQL):
    """
    Handles data transfer between a Pinot data warehouse and the Mage app.
    """

    def __init__(
            self,
            host: str,
            port: int,
            username: Union[str, None] = None,
            password: Union[str, None] = None,
            path: str = '/query/sql',
            scheme: str = 'http',
            null_handling: bool = True,
            **kwargs,
    ) -> None:
        """
        Initializes the data loader.

        Args:
            host (str): The host of the pinot controller to connect to.
            port (int): Port on which the pinot controller is running.
            username (str): The user with which to connect to the server with.
            password (str): The login password for the user.
            path (str): Path to pinot sql api.
            scheme (str): The scheme identifies the protocol to be used http or https.
            null_handling (bool): Query with enableNullHandling, so tables that store nulls
                return them instead of Pinot's default values.
            **kwargs: Additional settings for creating SQLAlchemy engine and connection
        """
        self.null_handling = null_handling
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            path=path,
            scheme=scheme,
            **kwargs,
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'Pinot':
        return cls(
            host=config[ConfigKey.PINOT_HOST],
            port=config[ConfigKey.PINOT_PORT],
            username=config[ConfigKey.PINOT_USER],
            password=config[ConfigKey.PINOT_PASSWORD],
            # Unset in most profiles: None replaced the defaults, and pinotdb failed.
            path=config[ConfigKey.PINOT_PATH] or '/query/sql',
            scheme=config[ConfigKey.PINOT_SCHEME] or 'http',
        )

    def open(self) -> None:
        with self.printer.print_msg('Opening connection to Pinot warehouse'):
            self._ctx = connect(
                host=self.settings['host'],
                port=self.settings['port'],
                username=self.settings['username'],
                password=self.settings['password'],
                path=self.settings['path'],
                scheme=self.settings['scheme'],
            )

    def execute(self, query_string: str, **query_vars) -> None:
        super().execute(query_string, **query_vars)

    def execute_query_raw(self, query: str, **kwargs) -> None:
        self.execute(query)

    def execute_queries(self,
                        queries: List[str],
                        query_variables: List[Dict] = None,
                        commit: bool = False,
                        fetch_query_at_indexes: List[bool] = None) -> List:
        return super().execute_queries(queries, query_variables, commit, fetch_query_at_indexes)

    def fetch_query(self, cursor, query: str) -> Any:
        return super().fetch_query(cursor, query)

    @checks_row_limit
    def load(self,
             query_string: str,
             limit: int = QUERY_ROW_LIMIT,
             display_query: Union[str, None] = None,
             verbose: bool = True,
             **kwargs) -> DataFrame:
        """The query's rows, typed from Pinot's column types."""
        message = 'Loading data'
        if verbose:
            message += f' with query\n\n{display_query or query_string}\n\n'
        query = self._enforce_limit(self._clean_query(query_string), limit)
        with self.printer.print_msg(message):
            cursor = self.conn.cursor()
            cursor.execute(
                query,
                queryOptions='enableNullHandling=true' if self.null_handling else None,
            )
            return pinot_frame(cursor.fetchall(), cursor.schema or [])

    def clean(self, column: Series, dtype: str) -> Series:
        return super().clean(column, dtype)

    @property
    def conn(self) -> Any:
        # Opened on first use: Pinot's SQL API is stateless HTTP, and a client used
        # outside a with block had no connection and failed with a NoneType error.
        if '_ctx' not in self.__dict__:
            self.open()
        return self._ctx

    def upload_dataframe(self,
                         cursor,
                         df: DataFrame,
                         db_dtypes: List[str],
                         dtypes: List[str],
                         full_table_name: str,
                         buffer: Union[IO, None] = None,
                         **kwargs) -> None:
        raise WRITE_NOT_SUPPORTED_EXCEPTION

    def export(self,
               df: DataFrame,
               schema_name: str,
               table_name: str,
               if_exists: ExportWritePolicy = ExportWritePolicy.REPLACE,
               index: bool = False, verbose: bool = True,
               query_string: Union[str, None] = None,
               drop_table_on_replace: bool = False,
               cascade_on_drop: bool = False,
               allow_reserved_words: bool = False,
               unique_conflict_method: str = None,
               unique_constraints: List[str] = None) -> None:
        raise WRITE_NOT_SUPPORTED_EXCEPTION

    def _enforce_limit(self, query: str, limit: int = QUERY_ROW_LIMIT) -> str:
        """
        The query with a LIMIT unless it ends with one: without, Pinot's broker returns
        10 rows. Any 'limit' in the query (a credit_limit column) used to count.
        """
        query = query.strip().rstrip(';').strip()
        if ENDS_WITH_LIMIT.search(query):
            return query
        return f'{query}\nLIMIT {limit}'


# LIMIT n, LIMIT offset, n or LIMIT n OFFSET m at the end of the query.
ENDS_WITH_LIMIT = re.compile(
    r'\blimit\s+\d+(\s*,\s*\d+)?(\s+offset\s+\d+)?\s*$', re.IGNORECASE,
)
NULLABLE_INTEGERS = {'INT': 'Int32', 'LONG': 'Int64'}
FLOATS = {'FLOAT': 'float32', 'DOUBLE': 'float64'}


def _decimal(value):
    if value is None:
        return None
    if isinstance(value, str):
        # pinotdb hands BIG_DECIMAL over as JSON text: '"12.5"'.
        text = value.strip()
        if text.startswith('"'):
            text = json.loads(text)
        return Decimal(text)
    return Decimal(str(value))


def _bytes(value):
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return bytes.fromhex(value)


def pinot_frame(rows: List[List[Any]], schema: List[Dict]) -> DataFrame:
    """Rows of a Pinot query as a frame typed from the query's column types."""
    names = [column['name'] for column in schema]
    columns = list(zip(*rows)) if rows else [[] for _ in names]
    data = {}
    for column, values in zip(schema, columns):
        kind = column.get('type')
        values = list(values)
        if kind in NULLABLE_INTEGERS:
            series = pd.Series(values, dtype=NULLABLE_INTEGERS[kind])
        elif kind in FLOATS:
            series = pd.Series(
                [float('nan') if v is None else v for v in values], dtype=FLOATS[kind],
            )
        elif kind == 'BOOLEAN':
            series = pd.Series(values, dtype='boolean')
        elif kind == 'TIMESTAMP':
            series = pd.to_datetime(pd.Series(values, dtype=object), utc=True).astype(
                'datetime64[ms, UTC]',
            )
        elif kind == 'BIG_DECIMAL':
            series = pd.Series([_decimal(v) for v in values], dtype=object)
        elif kind == 'BYTES':
            series = pd.Series([_bytes(v) for v in values], dtype=object)
        else:
            series = pd.Series(values, dtype=object)
        data[column['name']] = series
    return pd.DataFrame(data, columns=names)
