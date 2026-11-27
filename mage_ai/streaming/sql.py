"""
A stream's SQL block in the editor: the same query on the same tables as the engine runs it
(pipeline_services/service/crates/service/src/stream_sql.rs), with Python's Polars SQL.

The batch's messages are the table `messages` (Mage's `{{ df_1 }}` too), one row per JSON
object; the stream's broadcast values, when there are any, the one-row table `broadcast`.
Frames are built and rows written by the engine's rules (stream_sql.rs): a column per field
in the order fields first appear, typed by its values (Boolean, Int64, Float64, String;
mixed types and nested values as their JSON text), and rows in the query's column order.
"""
import json
import math
import re
from typing import Any, Dict, List, Optional

REFERENCE = re.compile(r'\{\{(.*?)\}\}', re.S)
# A comparison with a subquery, which the engine's Polars SQL (the Rust crate, 0.55) does not
# run though Python's Polars does: refused here too, so the editor runs what services run.
SUBQUERY_COMPARISON = re.compile(r'(?:[<>]=?|!=|<>|=)\s*\(\s*SELECT\b', re.I)


class StreamSqlError(Exception):
    pass


def query_of(text: str) -> str:
    """The block's query: `{{ df_1 }}`, its one upstream, is `messages`."""

    def replace(match):
        name = match.group(1).strip()
        if name != 'df_1':
            raise StreamSqlError(
                f'{{{{ {name} }}}} is not available in a stream: a stream\'s SQL block reads '
                'the table messages (its upstream block\'s messages, also {{ df_1 }}) and '
                'broadcast'
            )
        return 'messages'

    query = REFERENCE.sub(replace, text).strip().rstrip(';').strip()
    if not query:
        raise StreamSqlError('the SQL block has no query')
    if SUBQUERY_COMPARISON.search(query):
        raise StreamSqlError(
            'services do not run comparisons with a subquery, such as > (SELECT ...); '
            'join the table instead: FROM messages CROSS JOIN broadcast WHERE amount > threshold'
        )
    return query


def _kind(value: Any) -> str:
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'bool'
    if isinstance(value, int):
        return 'int' if -2 ** 63 <= value < 2 ** 63 else 'float'
    if isinstance(value, float):
        return 'float'
    if isinstance(value, str):
        return 'text'
    return 'json'


def _merge(a: str, b: str) -> str:
    if a == 'null':
        return b
    if b == 'null' or a == b:
        return a
    if {a, b} == {'int', 'float'}:
        return 'float'
    return 'json'


def _text(value: Any) -> Optional[str]:
    """A value of a String column: text as it is, other values as their JSON text."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _frame(objects: List[Dict]):
    import polars as pl

    names: List[str] = []
    for item in objects:
        for name in item:
            if name not in names:
                names.append(name)
    columns = []
    for name in names:
        cells = [item.get(name) for item in objects]
        kind = 'null'
        for cell in cells:
            kind = _merge(kind, _kind(cell))
        if kind == 'null':
            columns.append(pl.Series(name, [None] * len(cells), dtype=pl.Null))
        elif kind == 'bool':
            columns.append(pl.Series(name, cells, dtype=pl.Boolean))
        elif kind == 'int':
            columns.append(pl.Series(name, cells, dtype=pl.Int64))
        elif kind == 'float':
            columns.append(pl.Series(
                name, [None if c is None else float(c) for c in cells], dtype=pl.Float64,
            ))
        else:
            columns.append(pl.Series(name, [_text(c) for c in cells], dtype=pl.String))
    return pl.DataFrame(columns, height=len(objects))


def _value(value: Any) -> Any:
    """A row's value as a message holds it: a float that is not finite is None, and types
    JSON has none of (dates, lists) are their text."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


def run(text: str, messages: List, broadcast: Optional[Dict] = None) -> List[Dict]:
    """The query's rows, as messages, for a batch of JSON objects."""
    import polars as pl

    query = query_of(text)
    for message in messages:
        if not isinstance(message, dict):
            raise StreamSqlError(
                f'a SQL block reads JSON objects, a row each; a message is {message!r}'
            )
    if not messages:
        return []
    context = pl.SQLContext(messages=_frame(messages))
    if broadcast:
        context.register('broadcast', _frame([dict(broadcast)]))
    out = context.execute(query).collect()
    return [{name: _value(v) for name, v in row.items()} for row in out.iter_rows(named=True)]
