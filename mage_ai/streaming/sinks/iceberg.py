"""
An Apache Iceberg table as a stream's sink, for editor runs (pyiceberg). Exported services
write with the engine's Iceberg sink, which takes the same settings (ADR-0042).

Each batch is appended in one commit. Messages are JSON objects: fields the table does not
have are left out, columns a message does not have are null, and a list or an object in a
text column is written as its JSON text. With create_table, a missing table is created from
the first batch: integers as long, other numbers as double, booleans, the rest as text;
columns overrides any of them, and partition_by partitions it (day(ts), bucket(16, id)).
"""
import json
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sinks.base import MESSAGE_FORMAT_V2_KEYS, BaseSink

TYPE_ALIASES = {'bigint': 'long', 'integer': 'long', 'text': 'string', 'varchar': 'string',
                'bool': 'boolean'}
PARTITION_FIELD = re.compile(
    r'^\s*(?:(?P<fn>year|month|day|hour|identity)\((?P<col>[^(),\s]+)\)'
    r'|(?P<nfn>bucket|truncate)\(\s*(?P<n>\d+)\s*,\s*(?P<ncol>[^(),\s]+)\s*\)'
    r'|(?P<plain>[^(),\s]+))\s*$'
)


@dataclass
class IcebergConfig(BaseConfig):
    catalog: Dict
    table: str
    create_table: bool = False
    columns: Dict = field(default_factory=dict)
    partition_by: List[str] = field(default_factory=list)


def rejoined(items: List[str]) -> List[str]:
    """A YAML flow list ([bucket(16, id)]) splits a call at its comma: the parts joined."""
    joined: List[str] = []
    for item in items or []:
        item = str(item).strip()
        if joined and '(' in joined[-1] and ')' not in joined[-1]:
            joined[-1] = f'{joined[-1]}, {item}'
        else:
            joined.append(item)
    return joined


def partition_field(text: str):
    """(transform name, its argument, column) of `day(ts)`, `bucket(16, id)` or `region`."""
    found = PARTITION_FIELD.match(text or '')
    if not found:
        raise ValueError(
            f'Iceberg partition_by {text!r}: a column, or year, month, day or hour of a time '
            'column (day(ts)), bucket(16, id), or truncate(4, name).'
        )
    if found['fn']:
        return found['fn'], None, found['col']
    if found['nfn']:
        return found['nfn'], int(found['n']), found['ncol']
    return 'identity', None, found['plain']


def _transform(name: str, n: Optional[int]):
    from pyiceberg import transforms

    return {
        'identity': transforms.IdentityTransform,
        'year': transforms.YearTransform,
        'month': transforms.MonthTransform,
        'day': transforms.DayTransform,
        'hour': transforms.HourTransform,
    }[name]() if n is None else {
        'bucket': transforms.BucketTransform,
        'truncate': transforms.TruncateTransform,
    }[name](n)


def _iceberg_type(column: str, text: str):
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    text = TYPE_ALIASES.get(text.strip().lower(), text.strip().lower())
    simple = {
        'long': LongType, 'int': IntegerType, 'double': DoubleType, 'float': FloatType,
        'string': StringType, 'boolean': BooleanType, 'date': DateType,
        'timestamp': TimestampType, 'timestamptz': TimestamptzType,
    }
    if text in simple:
        return simple[text]()
    decimal = re.match(r'^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$', text)
    if decimal:
        return DecimalType(int(decimal[1]), int(decimal[2]))
    raise ValueError(
        f'Iceberg column {column}: {text} is not a type; use long, int, double, float, string, '
        'boolean, date, timestamp, timestamptz, or decimal(10, 2).'
    )


def inferred_schema(rows: List[Dict], columns: Dict):
    """The table's schema from the first rows, in the order fields first appear."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType,
        DoubleType,
        LongType,
        NestedField,
        StringType,
    )

    seen: Dict[str, object] = {}
    for row in rows:
        for name, value in row.items():
            if value is None:
                continue
            if isinstance(value, bool):
                kind = BooleanType()
            elif isinstance(value, int):
                kind = LongType()
            elif isinstance(value, float):
                kind = DoubleType()
            else:
                kind = StringType()
            known = seen.get(name)
            if known is None:
                seen[name] = kind
            elif known == kind or (known == DoubleType() and kind == LongType()):
                pass
            elif known == LongType() and kind == DoubleType():
                seen[name] = kind
            else:
                seen[name] = StringType()
    for name in columns:
        seen.setdefault(name, StringType())
    if not seen:
        raise ValueError('Iceberg: the first messages have no fields to make the columns of.')
    fields = [
        NestedField(
            field_id=i + 1,
            name=name,
            field_type=_iceberg_type(name, columns[name]) if name in columns else kind,
            required=False,
        )
        for i, (name, kind) in enumerate(seen.items())
    ]
    return Schema(*fields)


class IcebergSink(BaseSink):
    config_class = IcebergConfig

    def init_client(self):
        from pyiceberg.catalog import load_catalog

        properties = {k: str(v) for k, v in (self.config.catalog or {}).items() if v is not None}
        properties.setdefault('type', 'rest')
        if len([p for p in (self.config.table or '').split('.') if p]) < 2:
            raise ValueError(
                'Iceberg settings need table with its namespace, such as analytics.events.'
            )
        self.config.partition_by = rejoined(self.config.partition_by)
        for text in self.config.partition_by:
            partition_field(text)
        for name, text in (self.config.columns or {}).items():
            _iceberg_type(name, text)
        self.catalog = load_catalog('mageml', **properties)
        self.table = None

    def _open(self, rows: List[Dict]):
        from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

        if self.table is not None:
            return self.table
        try:
            self.table = self.catalog.load_table(self.config.table)
            return self.table
        except (NoSuchTableError, NoSuchNamespaceError):
            if not self.config.create_table:
                raise ValueError(
                    f'Iceberg table {self.config.table} does not exist; create it, or set '
                    'create_table: true to create it from the first messages.'
                ) from None
        from pyiceberg.exceptions import (
            NamespaceAlreadyExistsError,
            TableAlreadyExistsError,
        )
        from pyiceberg.partitioning import PartitionField, PartitionSpec

        schema = inferred_schema(rows, self.config.columns or {})
        fields = []
        for i, text in enumerate(self.config.partition_by or []):
            name, n, column = partition_field(text)
            if column not in schema.column_names:
                raise ValueError(
                    f'Iceberg partition_by {text}: the first messages have no {column}; '
                    'give its type under columns.'
                )
            source = schema.find_field(column)
            target = column if name == 'identity' else (
                f'{column}_{"trunc" if name == "truncate" else name}'
            )
            fields.append(PartitionField(
                source_id=source.field_id, field_id=1000 + i, transform=_transform(name, n),
                name=target,
            ))
        namespace = tuple(self.config.table.split('.')[:-1])
        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        try:
            self.table = self.catalog.create_table(
                self.config.table, schema=schema, partition_spec=PartitionSpec(*fields),
            )
        except TableAlreadyExistsError:
            self.table = self.catalog.load_table(self.config.table)
        return self.table

    def _arrow(self, rows: List[Dict]):
        """The rows as the table's Arrow schema; text columns take nested values as JSON."""
        import pyarrow as pa

        schema = self.table.schema().as_arrow()
        arrays = []
        for column in schema:
            values = [row.get(column.name) for row in rows]
            if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                # Other values as their JSON text (true, 2.5, [1]), as the engine writes them.
                values = [v if v is None or isinstance(v, str) else json.dumps(v) for v in values]
                arrays.append(pa.array(values, type=column.type))
                continue
            try:
                arrays.append(pa.array(values, type=column.type))
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
                # Text such as timestamps and decimals: Arrow's cast reads them.
                arrays.append(pa.array(
                    [None if v is None else str(v) for v in values], type=pa.string(),
                ).cast(column.type))
        return pa.Table.from_arrays(arrays, schema=schema)

    def batch_write(self, messages: List[Dict]):
        rows = []
        for message in messages:
            data = message
            if isinstance(message, dict) and set(message) == MESSAGE_FORMAT_V2_KEYS:
                data = message['data']
            if not isinstance(data, dict):
                raise ValueError('Iceberg: a message is not a JSON object; tables take objects.')
            rows.append(data)
        if not rows:
            return
        self._open(rows)
        self.table.append(self._arrow(rows))
        self._print(f'Appended {len(rows)} rows to {self.config.table}.')
