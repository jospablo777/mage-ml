"""
IBM Db2 (Db2 on Cloud, Db2 Warehouse, Db2 for LUW) through IBM's ibm_db driver, the `db2`
extra. Its wheels include IBM's CLI driver, so nothing else is installed.

Db2 folds unquoted names to upper case: tables and columns are created unquoted, so SQL
written by hand needs no quotes, and loaded columns come back in lower case, as pandas frames
are exported, so a round trip keeps its names.
"""
import json
import math
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import IO, Any, Dict, List, Mapping, Optional, Union

import numpy as np
from pandas import DataFrame, Series, Timestamp, isna

from mage_ai.io.base import QUERY_ROW_LIMIT, checks_row_limit, shown_query
from mage_ai.io.config import BaseConfigLoader, ConfigKey
from mage_ai.io.constants import UNIQUE_CONFLICT_METHOD_UPDATE
from mage_ai.io.export_utils import PandasTypes
from mage_ai.io.sql import BaseSQL

# Db2's longest VARCHAR, in bytes; longer text goes to a CLOB.
VARCHAR_MAX_BYTES = 32672
# Rows per array insert.
INSERT_CHUNK = 5000
# Db2's ordinary identifiers start with a letter; one that starts with _ reads as a
# conditional compilation directive.
SIMPLE_NAME = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')


def import_ibm_db():
    try:
        import ibm_db
        import ibm_db_dbi
    except ImportError as error:
        raise ImportError(
            f'Db2 needs the ibm_db driver: install mage-ml[db2]. ({error})'
        ) from error
    return ibm_db, ibm_db_dbi


# Keywords that Db2 does not take as plain column or table names; others, such as A, are
# not quoted, so SQL written by hand needs no quotes for them.
DB2_KEYWORDS = frozenset('''
    ALL AND ANY AS ASC BETWEEN BY CASE CAST CHECK COLUMN CONSTRAINT CREATE CROSS CURRENT
    CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER DEFAULT DELETE DESC DISTINCT
    DROP ELSE END EXCEPT EXISTS FETCH FOR FOREIGN FROM FULL GRANT GROUP HAVING IN INNER INSERT
    INTERSECT INTO IS JOIN KEY LEFT LIKE NOT NULL OF ON OR ORDER OUTER PRIMARY REFERENCES
    RIGHT ROW ROWS SELECT SESSION_USER SET SOME TABLE THEN TO UNION UNIQUE UPDATE USER USING
    VALUES WHEN WHERE WITH
'''.split())


def _plain(name: str, case_sensitive: bool = False) -> bool:
    """Whether a name can be written unquoted: Db2 then stores it in upper case."""
    return (not case_sensitive and bool(SIMPLE_NAME.match(name))
            and name.upper() not in DB2_KEYWORDS)


def quote(name: str, case_sensitive: bool = False) -> str:
    """A name as written in Db2 SQL: plain when Db2 folds it the same way, else quoted."""
    if _plain(name, case_sensitive):
        return name
    return '"' + name.replace('"', '""') + '"'


def _stored_name(name: str, case_sensitive: bool = False) -> str:
    """How Db2 stores a name in its catalog."""
    return name.upper() if _plain(name, case_sensitive) else name


def _value(value):
    """A Python value the driver binds, with missing values as NULL."""
    if value is None:
        return None
    if isinstance(value, (list, dict, tuple, np.ndarray)):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        return json.dumps(value, default=str, ensure_ascii=False)
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        if isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, Timestamp):
        if value.tzinfo is not None:
            value = value.tz_convert('UTC').tz_localize(None)
        return value.to_pydatetime()
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        value = int(value)
    if isinstance(value, int) and not isinstance(value, bool) and \
            not -(2**63) <= value < 2**63:
        # The driver cannot bind integers past 64 bits; DECFLOAT takes their text exactly.
        return str(value)
    if isinstance(value, int):
        return value
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return value


def _binder(db_type: str, utf8: bool = True):
    """
    How a column's values are bound, from its Db2 type, so a column binds one type. Text goes
    as UTF-8 bytes into a UTF-8 database: ibm_db cut characters outside the BMP (emoji, some
    CJK) from the end of bound strings, and crashed binding strings to CLOBs.
    """
    db_type = db_type.upper()
    if db_type.startswith(('DECFLOAT', 'DECIMAL', 'NUMERIC')):
        return str
    if db_type.startswith(('VARCHAR', 'CHAR', 'CLOB')):
        if utf8:
            return lambda v: (v if isinstance(v, str) else str(v)).encode('utf-8')
        return lambda v: v if isinstance(v, str) else str(v)
    if db_type.startswith(('DOUBLE', 'REAL', 'FLOAT')):
        return float
    if db_type.startswith(('BIGINT', 'INTEGER', 'SMALLINT')):
        return int
    if db_type == 'BOOLEAN':
        return bool
    return lambda v: v


class Db2(BaseSQL):
    def __init__(
        self,
        database: str,
        host: str,
        port: Union[int, str] = 50000,
        user: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        ssl: bool = False,
        ssl_certificate: Optional[str] = None,
        connect_timeout: Optional[int] = None,
        verbose: bool = True,
        **kwargs,
    ) -> None:
        """
        - `database`, `host`, `port`: Db2 on Cloud's are in the service credentials
          (`connection.db2`); its TLS port is usually 30xxx or 50001
        - `ssl`: TLS on; `ssl_certificate`: the server's CA as a file, PEM, or the base64 form
          of the credentials (`certificate.certificate_base64`); without it the system's
          trusted CAs are used, which is enough for Db2 on Cloud
        - `schema`: where tables go; the user's own schema by default
        """
        super().__init__(
            database=database, host=host, port=port, user=user, password=password,
            schema=schema, ssl=ssl, ssl_certificate=ssl_certificate,
            connect_timeout=connect_timeout, verbose=verbose, **kwargs,
        )

    @classmethod
    def with_config(cls, config: BaseConfigLoader) -> 'Db2':
        return cls(
            database=config[ConfigKey.DB2_DATABASE],
            host=config[ConfigKey.DB2_HOST],
            port=config[ConfigKey.DB2_PORT] or 50000,
            user=config[ConfigKey.DB2_USER],
            password=config[ConfigKey.DB2_PASSWORD],
            schema=config[ConfigKey.DB2_SCHEMA],
            ssl=str(config[ConfigKey.DB2_SSL]).lower() in ('1', 'true', 'yes'),
            ssl_certificate=config[ConfigKey.DB2_SSL_CERTIFICATE],
            connect_timeout=config[ConfigKey.DB2_CONNECT_TIMEOUT],
        )

    def _clean_column_name(self, column_name: str, allow_reserved_words: bool = False,
                           auto_clean_name: bool = True, case_sensitive: bool = False) -> str:
        # Reserved words are quoted (see quote), not renamed with a leading underscore,
        # so a frame's columns keep their names in Db2 and back.
        return super()._clean_column_name(
            column_name, allow_reserved_words=True, auto_clean_name=auto_clean_name,
            case_sensitive=case_sensitive,
        )

    def connection_string(self) -> str:
        """The DSN; user and password go to the driver on their own, so they may hold ";"."""
        settings = self.settings
        parts = {
            'DATABASE': settings['database'],
            'HOSTNAME': settings['host'],
            'PORT': settings['port'],
            'PROTOCOL': 'TCPIP',
        }
        if settings.get('ssl') or settings.get('ssl_certificate'):
            parts['SECURITY'] = 'SSL'
            if settings.get('ssl_certificate'):
                from mage_ai.io.tls import ca_certificate_file

                parts['SSLServerCertificate'] = ca_certificate_file(settings['ssl_certificate'])
        if settings.get('connect_timeout'):
            parts['ConnectTimeout'] = settings['connect_timeout']
        for value in parts.values():
            if ';' in str(value):
                raise ValueError('Db2 database, host and port cannot contain ";".')
        return ''.join(f'{key}={value};' for key, value in parts.items())

    def open(self) -> None:
        ibm_db, ibm_db_dbi = import_ibm_db()
        with self.printer.print_msg(
            f'Opening connection to Db2 database {self.settings["database"]} on '
            f'{self.settings["host"]}:{self.settings["port"]}'
        ):
            # Column names in results in lower case, as pandas frames name them.
            self._ctx = ibm_db_dbi.connect(
                self.connection_string(), self.settings.get('user') or '',
                self.settings.get('password') or '', conn_options={
                    ibm_db.ATTR_CASE: ibm_db.CASE_LOWER,
                },
            )
            self._ctx.set_autocommit(False)
            if self.settings.get('schema'):
                # Unqualified names, as SQL blocks write their upstream tables, resolve there.
                with self._ctx.cursor() as cursor:
                    cursor.execute(f'SET SCHEMA {quote(self.settings["schema"])}')

    def default_database(self) -> str:
        return self.settings['database']

    def default_schema(self) -> str:
        if self.settings.get('schema'):
            return self.settings['schema']
        if getattr(self, '_current_schema', None) is None:
            with self.conn.cursor() as cursor:
                cursor.execute('VALUES CURRENT SCHEMA')
                self._current_schema = cursor.fetchone()[0].strip()
        return self._current_schema

    def _full_table_name(self, schema_name: Optional[str], table_name: str) -> str:
        if schema_name:
            return f'{quote(schema_name)}.{quote(table_name)}'
        return quote(table_name)

    def _enforce_limit(self, query: str, limit: int = QUERY_ROW_LIMIT) -> str:
        query = query.strip().rstrip(';')
        return f'SELECT * FROM (\n{query}\n) AS mage_query FETCH FIRST {int(limit)} ROWS ONLY'

    @checks_row_limit
    def load(
        self,
        query_string: str,
        limit: int = QUERY_ROW_LIMIT,
        display_query: Union[str, None] = None,
        verbose: bool = True,
        **kwargs,
    ) -> DataFrame:
        query_string = self._clean_query(query_string)
        message = 'Loading data'
        if verbose:
            message += f' with query\n\n{shown_query(display_query or query_string)}\n\n'
        with self.printer.print_msg(message):
            with self.conn.cursor() as cursor:
                cursor.execute(self._enforce_limit(query_string, limit))
                # Plain names come back as Db2 stores them, in upper case; frames name
                # columns in lower case. Quoted names keep their case.
                columns = [d[0].lower() if d[0] == d[0].upper() else d[0]
                           for d in cursor.description]
                rows = cursor.fetchall()
            self.conn.commit()
        return DataFrame([list(row) for row in rows], columns=columns)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT 1 FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?',
                (_stored_name(schema_name or self.default_schema()), _stored_name(table_name)),
            )
            return cursor.fetchone() is not None

    def build_create_schema_command(self, schema_name: str) -> str:
        # Db2 has no CREATE SCHEMA IF NOT EXISTS.
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT 1 FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = ?', (_stored_name(schema_name),),
            )
            if cursor.fetchone() is not None:
                return 'VALUES 1'
        return f'CREATE SCHEMA {quote(schema_name)}'

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
        unique = list(unique_constraints or [])
        columns = []
        for name, db_type in dtypes.items():
            db_type = (overwrite_types or {}).get(name, db_type)
            # Db2 needs the columns of a unique constraint to be NOT NULL.
            not_null = ' NOT NULL' if name in unique else ''
            columns.append(f'{quote(name, case_sensitive)} {db_type}{not_null}')
        if unique:
            names = ', '.join(quote(c, case_sensitive) for c in unique)
            columns.append(f'UNIQUE ({names})')
        return (
            f'CREATE TABLE {self._full_table_name(schema_name, table_name)} (\n    '
            + ',\n    '.join(columns) + '\n)'
        )

    def build_create_table_as_command(self, table_name: str, query_string: str) -> str:
        return f'CREATE TABLE {table_name} AS (\n{query_string.strip().rstrip(";")}\n) WITH DATA'

    def get_type(self, column: Series, dtype: str) -> str:
        if dtype == PandasTypes.BOOLEAN:
            return 'BOOLEAN'
        if dtype == PandasTypes.INTEGER:
            values = column.dropna()
            if len(values) and (int(values.max()) > 2**63 - 1 or int(values.min()) < -2**63):
                return 'DECFLOAT(34)'
            return 'BIGINT'
        if dtype == PandasTypes.FLOATING:
            return 'DOUBLE'
        if dtype in (PandasTypes.DECIMAL, PandasTypes.MIXED_INTEGER_FLOAT):
            # DECFLOAT(34) keeps 34 digits exactly, where DOUBLE rounds.
            return 'DECFLOAT(34)'
        if dtype in (PandasTypes.DATETIME, PandasTypes.DATETIME64):
            return 'TIMESTAMP(6)'
        if dtype == PandasTypes.DATE:
            return 'DATE'
        if dtype == PandasTypes.TIME:
            return 'TIME'
        if dtype in (PandasTypes.TIMEDELTA, PandasTypes.TIMEDELTA64, PandasTypes.PERIOD):
            return 'BIGINT'
        if dtype == PandasTypes.BYTES:
            longest = column.dropna().map(len).max() if column.notna().any() else 0
            return f'VARBINARY({max(int(longest or 0), 1)})' if longest <= 32672 else 'BLOB(2G)'
        return self._text_type(column)

    def _text_type(self, column: Series) -> str:
        values = column.dropna()
        longest = 0
        if len(values):
            longest = int(values.map(
                lambda v: len((v if isinstance(v, str) else json.dumps(
                    v.tolist() if isinstance(v, np.ndarray) else v, default=str,
                    ensure_ascii=False,
                )).encode('utf-8')),
            ).max())
        if longest > VARCHAR_MAX_BYTES:
            return 'CLOB(2G)'
        # Room to grow, in steps, so a later append of longer text fits more often.
        size = 255
        while size < longest:
            size *= 4
        return f'VARCHAR({min(size, VARCHAR_MAX_BYTES)})'

    def clean(self, column: Series, dtype: str) -> Series:
        if dtype in (PandasTypes.DECIMAL, PandasTypes.MIXED_INTEGER_FLOAT):
            return column.map(lambda v: v if v is None or isinstance(v, Decimal)
                              else Decimal(str(v)) if not isna(v) else None)
        return super().clean(column, dtype)

    def upload_dataframe(
        self,
        cursor: Any,
        df: DataFrame,
        db_dtypes: Mapping[str, str],
        dtypes: Mapping[str, str],
        full_table_name: str,
        buffer: Union[IO, None] = None,
        case_sensitive: bool = False,
        unique_conflict_method: str = None,
        unique_constraints: List[str] = None,
        **kwargs,
    ) -> None:
        columns = list(df.columns)
        names = [quote(c, case_sensitive) for c in columns]
        markers = ', '.join('?' for _ in columns)
        if unique_conflict_method and unique_constraints:
            sql = self._merge(full_table_name, names, columns, unique_constraints,
                              unique_conflict_method, case_sensitive)
        else:
            sql = f'INSERT INTO {full_table_name} ({", ".join(names)}) VALUES ({markers})'
        # Array inserts need every row of a column bound as the same Python type.
        utf8 = self._utf8_database()
        binders = [_binder(db_dtypes.get(c, ''), utf8) for c in columns]
        rows = df.itertuples(index=False, name=None)
        chunk = []
        for row in rows:
            chunk.append(tuple(
                None if (v := _value(value)) is None else bind(v)
                for value, bind in zip(row, binders)
            ))
            if len(chunk) >= INSERT_CHUNK:
                cursor.executemany(sql, chunk)
                chunk = []
        if chunk:
            cursor.executemany(sql, chunk)

    def _utf8_database(self) -> bool:
        if getattr(self, '_utf8', None) is None:
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'codeset'",
                    )
                    row = cursor.fetchone()
                self._utf8 = bool(row) and row[0].strip().upper() in ('UTF-8', 'UTF8')
            except Exception:
                # Not allowed to read the configuration: new databases are UTF-8.
                self._utf8 = True
        return self._utf8

    def _merge(self, table, names, columns, keys, method, case_sensitive) -> str:
        source = ', '.join(names)
        markers = ', '.join('?' for _ in names)
        key_names = [quote(k, case_sensitive) for k in keys]
        on = ' AND '.join(f't.{k} = s.{k}' for k in key_names)
        sql = (
            f'MERGE INTO {table} AS t USING (VALUES ({markers})) AS s ({source}) ON {on}'
        )
        others = [n for c, n in zip(columns, names) if c not in keys]
        if method == UNIQUE_CONFLICT_METHOD_UPDATE and others:
            sql += ' WHEN MATCHED THEN UPDATE SET ' + ', '.join(f't.{n} = s.{n}' for n in others)
        else:
            # Kept as it is. A row that changes nothing returns Db2's "no rows" warning,
            # which ibm_db's array execution raises as an error, so matched rows set a
            # column to itself.
            column = (others or key_names)[0]
            sql += f' WHEN MATCHED THEN UPDATE SET t.{column} = t.{column}'
        sql += (
            f' WHEN NOT MATCHED THEN INSERT ({source}) VALUES ('
            + ', '.join(f's.{n}' for n in names) + ')'
        )
        return sql
