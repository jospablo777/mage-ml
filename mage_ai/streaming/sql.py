"""
A stream's SQL block in the editor: the same query on the same tables as the engine runs it
(pipeline_services/service/crates/service/src/stream_sql.rs), with Python's Polars SQL.

The batch's messages are the table `messages` (Mage's `{{ df_1 }}` too), one row per JSON
object; the stream's broadcast values, when there are any, the one-row table `broadcast`.
A query that groups by a window of event time (GROUP BY key, TUMBLE(INTERVAL '1 minute'))
is a window block (window_of): the engine keeps its windows across batches, and it runs
where windows run, in exported services and the Run tab.

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
    if window_of(text) is not None:
        raise StreamSqlError(
            'a query that groups by a window runs in the engine, as window blocks do: in '
            'exported services and the Run tab, not in MageML\'s own streaming'
        )
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


WINDOWS = {'TUMBLE': 'tumbling', 'HOP': 'hopping', 'SESSION_WINDOW': 'session'}
# A window call; SESSION is a keyword to sqlglot, so it is read as SESSION_WINDOW.
WINDOW_CALL = re.compile(r'\b(TUMBLE|HOP|SESSION)\s*\(', re.I)
SESSION_CALL = re.compile(r'\bSESSION\s*\(', re.I)
UNITS = {
    'MILLISECOND': 0.001, 'SECOND': 1, 'MINUTE': 60, 'HOUR': 3600, 'DAY': 86400,
}
AGGREGATES = {'Sum': 'sum', 'Min': 'min', 'Max': 'max', 'Avg': 'avg'}


def _seconds(interval) -> float:
    from sqlglot import exp

    if isinstance(interval, exp.Interval):
        amount = interval.this.name if interval.this is not None else ''
        unit = (interval.unit.name if interval.unit is not None else '').upper()
        if not unit and ' ' in amount:
            amount, unit = amount.split(None, 1)
        unit = unit.upper().rstrip('S')
        try:
            return float(amount) * UNITS[unit]
        except (KeyError, ValueError):
            pass
    raise StreamSqlError(
        f'{interval.sql() if interval is not None else "nothing"} is not a window length: '
        "write INTERVAL '1 minute', INTERVAL '30 seconds' or INTERVAL '2 hours'"
    )


def _field(column) -> str:
    """A column as a message field: user.id reaches into objects, as window keys do."""
    return '.'.join(part.name for part in column.parts)


def window_of(text: str) -> Optional[Dict]:
    """
    A query that groups by a window of event time, as a window block's settings (the YAML
    window: of export._window_spec, with key_as); None for a query that is not one:

        SELECT card_id, count(*) AS payments, sum(amount) AS total,
               count(DISTINCT shop_id) AS shops
        FROM messages
        GROUP BY card_id, TUMBLE(INTERVAL '1 minute')

    TUMBLE(size), HOP(size, slide), SESSION(gap); count(*), count(DISTINCT f), sum, min,
    max and avg of a field, each named with AS. A window aggregates what reaches it: filter
    in a SQL block before it.
    """
    import sqlglot
    from sqlglot import exp

    query = query_of(text)
    if not WINDOW_CALL.search(query):
        return None
    try:
        tree = sqlglot.parse_one(SESSION_CALL.sub('SESSION_WINDOW(', query))
    except sqlglot.errors.ParseError as error:
        raise StreamSqlError(f'the windowed query does not parse: {error}') from None
    if not isinstance(tree, exp.Select) or not tree.args.get('group'):
        return None
    group = tree.args['group'].expressions
    windows = [g for g in group if isinstance(g, exp.Anonymous)
               and str(g.this).upper() in WINDOWS]
    if not windows:
        return None
    if len(windows) > 1:
        raise StreamSqlError('a windowed query groups by one window')
    for clause in ('where', 'having', 'order', 'limit', 'joins', 'distinct', 'qualify'):
        if tree.args.get(clause):
            raise StreamSqlError(
                f'a windowed query has no {clause.upper()}: a window aggregates every message '
                'that reaches it, so filter or join in a SQL block before it'
            )
    source = tree.args.get('from') or tree.args.get('from_')
    if source is None or not isinstance(source.this, exp.Table) or \
            source.this.name != 'messages':
        raise StreamSqlError('a windowed query reads messages (or {{ df_1 }})')
    keys = [g for g in group if g is not windows[0]]
    if len(keys) > 1 or (keys and not isinstance(keys[0], exp.Column)):
        raise StreamSqlError('a windowed query groups by the window and at most one field')
    key = _field(keys[0]) if keys else None

    call = windows[0]
    kind = WINDOWS[str(call.this).upper()]
    lengths = [_seconds(a) for a in call.expressions]
    expected = 2 if kind == 'hopping' else 1
    if len(lengths) != expected:
        raise StreamSqlError(
            'TUMBLE(size), HOP(size, slide) and SESSION(gap) take INTERVAL lengths, such as '
            "TUMBLE(INTERVAL '1 minute')"
        )
    window = dict(key=key, type=kind)
    if kind == 'tumbling':
        window['size_seconds'] = lengths[0]
    elif kind == 'hopping':
        window['size_seconds'], window['slide_seconds'] = lengths
    else:
        window['gap_seconds'] = lengths[0]

    aggregates: Dict = {}
    for item in tree.expressions:
        name = item.alias_or_name
        value = item.this if isinstance(item, exp.Alias) else item
        if isinstance(value, exp.Column):
            if key is not None and _field(value) == key:
                window['key_as'] = name
                continue
            if _field(value) in ('window_start', 'window_end') and name == _field(value):
                continue
            raise StreamSqlError(
                f'{value.sql()} is neither the window\'s key nor an aggregate: a window '
                'gives its key, window_start, window_end and its aggregates'
            )
        if not isinstance(item, exp.Alias):
            raise StreamSqlError(f'name {item.sql()} with AS, such as count(*) AS payments')
        if isinstance(value, exp.Count):
            inner = value.this
            if isinstance(inner, exp.Star):
                aggregates[name] = 'count'
                continue
            if isinstance(inner, exp.Distinct) and len(inner.expressions) == 1 \
                    and isinstance(inner.expressions[0], exp.Column):
                aggregates[name] = dict(distinct=_field(inner.expressions[0]))
                continue
        function = AGGREGATES.get(type(value).__name__)
        if function and isinstance(value.this, exp.Column):
            aggregates[name] = {function: _field(value.this)}
            continue
        raise StreamSqlError(
            f'{value.sql()} is not a window aggregate: count(*), count(DISTINCT field), and '
            'sum, min, max or avg of a field'
        )
    if not aggregates:
        raise StreamSqlError('a windowed query has at least one aggregate, such as '
                             'count(*) AS payments')
    window['aggregate'] = aggregates
    return window
