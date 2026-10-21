"""
Golden runs: the expected outputs and effect counts of a pipeline, so a run made another way
(another engine, version or machine) can be checked block by block (ROADMAP P0).

A golden file, <project>/golden/<pipeline uuid>.yaml, holds per block and output a
fingerprint of the data, not of the stored bytes, so engines that store the same data
differently compare equal:

    rows      row count (items of a list, keys of a dict)
    columns   name and type kind (integer, float, string, boolean, date, datetime,
              datetime_tz, decimal, binary, list, struct, null, other) in order
    content   a hash of the rows as a multiset: the same rows in any order give the same hash
    order     a hash of the rows in their order

Floats are compared to 12 significant digits, so a different summation order does not count
as a change; NaN equals NaN. A pandas index other than row numbers counts as columns.
Outputs that are not tables or JSON (a model, an image) are compared by the SHA-256 of
their stored files.

Effect counts: what the run left in the systems it writes to, from probes the golden file
declares and `mage golden record` fills in:

    effects:
      orders_written:
        sql: select count(*) from analytics.orders
        data_provider: postgres     # a profile of io_config.yaml
        profile: default
      export_file:
        file: exports/orders.parquet   # rows of a parquet or CSV file, lines otherwise

A probe counts the destination after the run, so a run that writes twice to an append-only
table shows a different count.
"""
import csv
import datetime as dt
import decimal
import hashlib
import json
import math
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

GOLDEN_FOLDER = 'golden'
FLOAT_DIGITS = 12
FORMAT_VERSION = 1
# The modulus of the multiset hash: rows' digests are added as 256-bit integers.
MODULUS = 1 << 256
EQUAL = 'equal'
DIFFERENT = 'different'
MISSING = 'missing'
NEW = 'new'
SQL_PROVIDERS = {
    'bigquery': 'mage_ai.io.bigquery.BigQuery',
    'clickhouse': 'mage_ai.io.clickhouse.ClickHouse',
    'duckdb': 'mage_ai.io.duckdb.DuckDB',
    'mssql': 'mage_ai.io.mssql.MSSQL',
    'mysql': 'mage_ai.io.mysql.MySQL',
    'postgres': 'mage_ai.io.postgres.Postgres',
    'redshift': 'mage_ai.io.redshift.Redshift',
    'snowflake': 'mage_ai.io.snowflake.Snowflake',
    'trino': 'mage_ai.io.trino.Trino',
}


class GoldenError(Exception):
    pass


# ---- fingerprints ---------------------------------------------------------------


def _canonical(value: Any) -> Any:
    """A value as JSON that compares equal across engines and storage formats."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        rounded = float(f'{value:.{FLOAT_DIGITS}g}')
        # 2.0 and 2 are the same number; an integer column read as float stays equal.
        return int(rounded) if rounded.is_integer() and abs(rounded) < 2 ** 53 else rounded
    if isinstance(value, decimal.Decimal):
        return _canonical(float(value)) if value.is_finite() else str(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonical(v) for v in value]
        return sorted(items, key=_dumps) if isinstance(value, (set, frozenset)) else items
    if hasattr(value, 'tolist'):
        return _canonical(value.tolist())
    if hasattr(value, 'isoformat'):
        return _canonical(value.to_pydatetime()) if hasattr(value, 'to_pydatetime') \
            else value.isoformat()
    if hasattr(value, 'item'):
        return _canonical(value.item())
    return str(value)


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _hashes(rows: Iterable[Any]) -> Tuple[int, str, str]:
    """Row count, multiset hash and ordered hash of canonical rows."""
    count = 0
    total = 0
    ordered = hashlib.sha256()
    for row in rows:
        digest = hashlib.sha256(_dumps(row).encode()).digest()
        total = (total + int.from_bytes(digest, 'big')) % MODULUS
        ordered.update(digest)
        count += 1
    return count, f'{total:064x}', ordered.hexdigest()


def _kind(arrow_type) -> str:
    import pyarrow as pa
    import pyarrow.types as t

    if t.is_dictionary(arrow_type):
        return _kind(arrow_type.value_type)
    checks = (
        (t.is_boolean, 'boolean'),
        (t.is_integer, 'integer'),
        (t.is_floating, 'float'),
        (t.is_decimal, 'decimal'),
        (lambda a: t.is_string(a) or t.is_large_string(a)
         or getattr(t, 'is_string_view', lambda _: False)(a), 'string'),
        (lambda a: t.is_binary(a) or t.is_large_binary(a) or t.is_fixed_size_binary(a),
         'binary'),
        (t.is_date, 'date'),
        (lambda a: t.is_list(a) or t.is_large_list(a) or t.is_fixed_size_list(a), 'list'),
        (t.is_struct, 'struct'),
        (t.is_null, 'null'),
    )
    if t.is_timestamp(arrow_type):
        return 'datetime_tz' if arrow_type.tz else 'datetime'
    for check, name in checks:
        if check(arrow_type):
            return name
    if arrow_type == pa.null():
        return 'null'
    return 'other'


def _arrow(data: Any):
    """A pandas, Polars or Arrow table as an Arrow table; None for anything else."""
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    module = type(data).__module__.split('.')[0]
    if module == 'polars' and hasattr(data, 'to_arrow'):
        if type(data).__name__ == 'LazyFrame':
            data = data.collect()
        return data.to_arrow()
    if module == 'pandas' and type(data).__name__ == 'DataFrame':
        import pandas as pd

        # A default index is row numbers, not data; any other (a groupby key) is a column.
        default = isinstance(data.index, pd.RangeIndex) and data.index.start == 0 and \
            data.index.step == 1
        frame = data.reset_index(drop=default)
        frame.columns = [str(c) for c in frame.columns]
        return pa.Table.from_pandas(frame, preserve_index=False)
    if module == 'pandas' and type(data).__name__ == 'Series':
        return _arrow(data.to_frame(name=str(data.name or 'value')))
    return None


def fingerprint(data: Any) -> Dict[str, Any]:
    """The fingerprint of one output: kind, rows, columns, content and order."""
    table = _arrow(data)
    if table is not None:
        columns = [[field.name, _kind(field.type)] for field in table.schema]
        rows = (
            [_canonical(row[name]) for name, _ in columns]
            for batch in table.to_batches()
            for row in batch.to_pylist()
        )
        count, content, order = _hashes(rows)
        return dict(kind='table', rows=count, columns=columns, content=content, order=order)
    if isinstance(data, (list, tuple)):
        count, content, order = _hashes(_canonical(item) for item in data)
        return dict(kind='list', rows=count, content=content, order=order)
    if isinstance(data, dict):
        canonical = _canonical(data)
        count, content, order = _hashes(sorted(canonical.items(), key=lambda kv: kv[0]))
        return dict(kind='dict', rows=count, content=content, order=order)
    if data is None or isinstance(data, (str, int, float, bool)):
        count, content, order = _hashes([_canonical(data)])
        return dict(kind='value', rows=count, content=content, order=order)
    raise GoldenError(f'{type(data).__name__} is not a table or JSON')


# ---- reading a run --------------------------------------------------------------


def run_partition(pipeline_run) -> Optional[str]:
    """Where a run stored its outputs: command line runs record it in their metrics."""
    metrics = pipeline_run.metrics or {}
    if 'command_line' in metrics:
        return metrics.get('execution_partition')
    return pipeline_run.execution_partition


def block_outputs(pipeline, block_uuid: str, partition: Optional[str]) -> Dict[str, Dict]:
    """Fingerprints of a block's stored outputs; files' SHA-256 when not a table or JSON."""
    from mage_ai.orchestration import run_records

    manager = pipeline.variable_manager
    names = manager.get_variables_by_block(
        pipeline.uuid, block_uuid, partition=partition, output_variable_only=True,
    )
    outputs = {}
    digests = None
    for name in sorted(names):
        try:
            data = manager.get_variable(
                pipeline.uuid, block_uuid, name, partition=partition, raise_exception=True,
            )
            outputs[name] = fingerprint(data)
        except Exception:
            if digests is None:
                digests = run_records.output_digests(pipeline, block_uuid, partition)
            digest = digests.get(name)
            outputs[name] = dict(kind='file', sha256=digest['sha256']) if digest else \
                dict(kind='unreadable')
    return outputs


def run_fingerprints(pipeline, pipeline_run) -> Dict[str, Dict]:
    """{block uuid: {status, outputs}} for every block run of the run."""
    partition = run_partition(pipeline_run)
    blocks = {}
    for block_run in sorted(pipeline_run.block_runs, key=lambda b: b.block_uuid):
        status = str(getattr(block_run.status, 'value', block_run.status))
        entry = dict(status=status)
        if status == 'completed':
            entry['outputs'] = block_outputs(pipeline, block_run.block_uuid, partition)
        blocks[block_run.block_uuid] = entry
    return blocks


def latest_completed_run(pipeline):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = PipelineRun.query.filter(
        PipelineRun.pipeline_uuid == pipeline.uuid,
        PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED,
    ).order_by(PipelineRun.id.desc()).first()
    if run is None:
        raise GoldenError(f'{pipeline.uuid} has no completed run: run it first (mage run).')
    return run


# ---- effects --------------------------------------------------------------------


def _count_file(path: str) -> int:
    if path.endswith('.parquet'):
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).metadata.num_rows
    with open(path, newline='', encoding='utf8') as file:
        if path.endswith('.csv'):
            return max(sum(1 for _ in csv.reader(file)) - 1, 0)
        return sum(1 for _ in file)


def _count_sql(probe: Dict, repo_path: str) -> int:
    import importlib

    from mage_ai.io.config import ConfigFileLoader
    from mage_ai.settings.repo import get_repo_config

    provider = probe.get('data_provider')
    if provider not in SQL_PROVIDERS:
        raise GoldenError(f'data_provider {provider!r} is not one of '
                          f'{", ".join(sorted(SQL_PROVIDERS))}.')
    module_name, class_name = SQL_PROVIDERS[provider].rsplit('.', 1)
    loader_class = getattr(importlib.import_module(module_name), class_name)
    config_path = os.path.join(repo_path, get_repo_config(repo_path).io_config_path or
                               'io_config.yaml')
    loader = loader_class.with_config(ConfigFileLoader(config_path,
                                                       probe.get('profile', 'default')))
    with loader:
        frame = loader.load(probe['sql'])
    value = frame.iloc[0, 0] if hasattr(frame, 'iloc') else frame[0][0]
    return int(value)


def count_effect(probe: Dict, repo_path: str) -> int:
    """What a probe counts now: rows of a SQL count or a file."""
    if 'sql' in probe:
        return _count_sql(probe, repo_path)
    if 'file' in probe:
        path = probe['file']
        path = path if os.path.isabs(path) else os.path.join(repo_path, path)
        if not os.path.exists(path):
            raise GoldenError(f'{probe["file"]} does not exist.')
        return _count_file(path)
    raise GoldenError('a probe has sql (with data_provider) or file.')


def effects(probes: Dict[str, Dict], repo_path: str) -> Dict[str, Any]:
    counted = {}
    for name, probe in sorted((probes or {}).items()):
        try:
            counted[name] = count_effect(probe, repo_path)
        except Exception as error:
            counted[name] = dict(error=str(error))
    return counted


# ---- golden files ---------------------------------------------------------------


def golden_path(pipeline) -> str:
    return os.path.join(pipeline.repo_path, GOLDEN_FOLDER, f'{pipeline.uuid}.yaml')


def load(pipeline) -> Dict[str, Any]:
    path = golden_path(pipeline)
    if not os.path.exists(path):
        raise GoldenError(f'{pipeline.uuid} has no golden file: run mage golden record first '
                          f'({os.path.relpath(path, pipeline.repo_path)}).')
    with open(path, encoding='utf8') as file:
        return yaml.safe_load(file) or {}


def record(pipeline, pipeline_run) -> Dict[str, Any]:
    """Writes the run's fingerprints and the probes' counts as the golden file."""
    path = golden_path(pipeline)
    existing = {}
    if os.path.exists(path):
        with open(path, encoding='utf8') as file:
            existing = yaml.safe_load(file) or {}
    blocks = run_fingerprints(pipeline, pipeline_run)
    failed = sorted(uuid for uuid, entry in blocks.items() if entry['status'] != 'completed')
    if failed:
        raise GoldenError(f'Run {pipeline_run.id} has blocks that did not complete: '
                          f'{", ".join(failed)}.')
    probes = {
        name: {key: value for key, value in (probe or {}).items() if key != 'count'}
        for name, probe in (existing.get('effects') or {}).items()
    }
    counted = effects(probes, pipeline.repo_path)
    errors = {name: c['error'] for name, c in counted.items() if isinstance(c, dict)}
    if errors:
        raise GoldenError('Probes failed: ' + '; '.join(f'{n}: {e}' for n, e in errors.items()))
    document = dict(
        version=FORMAT_VERSION,
        pipeline=pipeline.uuid,
        recorded_from_run=pipeline_run.id,
        blocks={uuid: entry['outputs'] for uuid, entry in blocks.items()},
        effects={name: dict(probes[name], count=counted[name]) for name in sorted(probes)},
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf8') as file:
        yaml.safe_dump(document, file, sort_keys=False, width=100)
    return document


def _output_differences(expected: Dict, actual: Dict, ordered: bool) -> List[str]:
    found = []
    if expected.get('kind') != actual.get('kind'):
        return [f"kind {expected.get('kind')} -> {actual.get('kind')}"]
    if expected.get('rows') != actual.get('rows'):
        found.append(f"rows {expected.get('rows')} -> {actual.get('rows')}")
    if expected.get('columns') != actual.get('columns'):
        before = {name: kind for name, kind in expected.get('columns') or []}
        after = {name: kind for name, kind in actual.get('columns') or []}
        for name in [n for n in after if n not in before]:
            found.append(f'column {name} added')
        for name in [n for n in before if n not in after]:
            found.append(f'column {name} removed')
        for name in [n for n in before if n in after and before[n] != after[n]]:
            found.append(f'column {name} {before[name]} -> {after[name]}')
        if not found and list(before) != list(after):
            found.append('columns reordered')
    if expected.get('content') != actual.get('content') and not found:
        found.append('values differ')
    if expected.get('sha256') != actual.get('sha256'):
        found.append('stored files differ')
    if ordered and expected.get('order') != actual.get('order') and \
            expected.get('content') == actual.get('content'):
        found.append('row order differs')
    return found


def compare(pipeline, pipeline_run, ordered: bool = False) -> Dict[str, Any]:
    """Each block of the run against the golden file: equal, different, missing or new."""
    golden = load(pipeline)
    expected_blocks = golden.get('blocks') or {}
    actual_blocks = run_fingerprints(pipeline, pipeline_run)
    blocks = []
    for uuid in sorted(set(expected_blocks) | set(actual_blocks)):
        expected = expected_blocks.get(uuid)
        actual = actual_blocks.get(uuid)
        if expected is None:
            blocks.append(dict(block_uuid=uuid, result=NEW, differences=[]))
            continue
        if actual is None or actual['status'] != 'completed':
            status = actual['status'] if actual else 'not run'
            blocks.append(dict(block_uuid=uuid, result=MISSING, differences=[status]))
            continue
        differences = []
        outputs = actual['outputs']
        for name in sorted(set(expected) | set(outputs)):
            if name not in outputs:
                differences.append(f'{name}: missing')
            elif name not in expected:
                differences.append(f'{name}: new output')
            else:
                differences += [
                    f'{name}: {d}' for d in _output_differences(expected[name], outputs[name],
                                                                 ordered)
                ]
        blocks.append(dict(
            block_uuid=uuid,
            result=DIFFERENT if differences else EQUAL,
            differences=differences,
        ))
    probes = {
        name: {key: value for key, value in (probe or {}).items() if key != 'count'}
        for name, probe in (golden.get('effects') or {}).items()
    }
    counted = effects(probes, pipeline.repo_path)
    effect_rows = []
    for name in sorted(probes):
        expected = (golden['effects'][name] or {}).get('count')
        actual = counted[name]
        if isinstance(actual, dict):
            effect_rows.append(dict(name=name, result=MISSING, expected=expected,
                                    actual=None, error=actual['error']))
        else:
            effect_rows.append(dict(name=name, result=EQUAL if actual == expected else DIFFERENT,
                                    expected=expected, actual=actual))
    passed = all(b['result'] == EQUAL for b in blocks) and \
        all(e['result'] == EQUAL for e in effect_rows)
    return dict(
        pipeline=pipeline.uuid,
        run=pipeline_run.id,
        golden_run=golden.get('recorded_from_run'),
        blocks=blocks,
        effects=effect_rows,
        passed=passed,
    )


def format_comparison(comparison: Dict[str, Any]) -> str:
    lines = [f"{comparison['pipeline']}: run {comparison['run']} against the golden file "
             f"(recorded from run {comparison['golden_run']})."]
    width = max([len(b['block_uuid']) for b in comparison['blocks']] + [5])
    for block in comparison['blocks']:
        detail = '; '.join(block['differences'])
        lines.append(f"  {block['block_uuid']:<{width}}  {block['result']}"
                     + (f'  ({detail})' if detail else ''))
    for effect in comparison['effects']:
        if effect['result'] == MISSING:
            lines.append(f"  effect {effect['name']}: {effect['error']}")
        else:
            lines.append(f"  effect {effect['name']}: {effect['result']} "
                         f"(expected {effect['expected']}, counted {effect['actual']})")
    lines.append('Equal.' if comparison['passed'] else 'Different.')
    return '\n'.join(lines)
