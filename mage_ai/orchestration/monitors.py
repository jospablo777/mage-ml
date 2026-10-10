"""
Monitors: a block's output watched across pipeline runs, against a baseline run.

    # monitors/churn_scores.yaml
    pipeline: churn_scoring
    block: score
    baseline: previous        # the run before, or run:<pipeline run id>
    columns: [score, segment] # every column by default
    drift:                    # Jensen-Shannon divergence of each column, 0 to ln 2
      warn: 0.1
      fail: 0.2
    missing:                  # share of missing values
      max: 0.2
      max_increase: 0.05      # over the baseline's share
    rows:
      min: 1000
      max_change: 0.5         # relative to the baseline's rows
    freshness:
      column: event_time
      max_age: 6h             # newest value against the run's execution date
    min_rows: 30              # fewer rows (now or in the baseline) hold the drift checks
    fail_block: false         # true: a failed check fails the block run
    notify: true              # a failed check sends the pipeline's notifications

When a block run that a monitor watches completes, the Rust engine of ColumnAtlas
profiles the whole stored output (rust/column_atlas/src/profile.rs): numeric columns are
binned on the baseline's edges, text and boolean columns by their most frequent values.
Each check passes, warns, fails, or holds when there is too little data to decide.
The profile and the checks are saved for the run, the next run's baseline, and recorded in
block_run.metrics['monitors'].
"""
import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml

MONITORS_FOLDER = 'monitors'
RECORDS_FOLDER = '.monitoring'
_NAME = re.compile(r'^[A-Za-z0-9][\w.\-]{0,127}$')
_KEYS = {
    'pipeline', 'block', 'output', 'baseline', 'columns', 'drift', 'missing', 'rows',
    'freshness', 'min_rows', 'fail_block', 'notify', 'description', 'bins', 'top',
}
_DURATION = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*$')
_SECONDS = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800}
RESULT_ORDER = {'pass': 0, 'hold': 1, 'warn': 2, 'fail': 3}


class MonitorError(Exception):
    pass


@dataclass
class Monitor:
    name: str
    pipeline: str
    block: str
    output: str = 'output_0'
    baseline: str = 'previous'
    columns: Optional[List[str]] = None
    drift: Dict[str, float] = field(default_factory=lambda: dict(warn=0.1, fail=0.2))
    missing: Dict[str, float] = field(default_factory=dict)
    rows: Dict[str, float] = field(default_factory=dict)
    freshness: Dict[str, Any] = field(default_factory=dict)
    min_rows: int = 30
    fail_block: bool = False
    notify: bool = True
    bins: int = 20
    top: int = 20


def parse_duration(value: Any, what: str) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    match = _DURATION.match(str(value or ''))
    if not match:
        raise MonitorError(f'{what} must be a duration such as 30m, 6h or 2d.')
    return float(match.group(1)) * _SECONDS[match.group(2)]


def _number(value: Any, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise MonitorError(f'{what} must be a number.')
    return float(value)


def load_monitor(name: str, repo_path: str) -> Monitor:
    folder = os.path.join(repo_path, MONITORS_FOLDER)
    path = next((
        os.path.join(folder, f'{name}{s}') for s in ('.yaml', '.yml')
        if os.path.isfile(os.path.join(folder, f'{name}{s}'))
    ), None)
    if path is None:
        raise MonitorError(f'Monitor {name} does not exist in {MONITORS_FOLDER}/.')
    try:
        with open(path, encoding='utf-8') as file:
            document = yaml.safe_load(file) or {}
    except yaml.YAMLError as error:
        raise MonitorError(f'Monitor {name} is not valid YAML: {error}')
    if not isinstance(document, dict):
        raise MonitorError(f'Monitor {name} must be a mapping.')
    unknown = set(document) - _KEYS
    if unknown:
        raise MonitorError(
            f'Monitor {name} has unknown settings: {", ".join(sorted(unknown))}.'
        )
    for required in ('pipeline', 'block'):
        if not document.get(required):
            raise MonitorError(f'Monitor {name} needs a {required}.')
    baseline = str(document.get('baseline') or 'previous')
    if baseline != 'previous' and not re.fullmatch(r'run:\d+', baseline):
        raise MonitorError(
            f'The baseline of monitor {name} must be previous or run:<pipeline run id>.'
        )
    drift = dict(warn=0.1, fail=0.2)
    drift.update({
        k: _number(v, f'drift {k} of {name}') for k, v in (document.get('drift') or {}).items()
        if k in ('warn', 'fail')
    })
    missing = {
        k: _number(v, f'missing {k} of {name}')
        for k, v in (document.get('missing') or {}).items() if k in ('max', 'max_increase')
    }
    rows = {
        k: _number(v, f'rows {k} of {name}')
        for k, v in (document.get('rows') or {}).items() if k in ('min', 'max_change')
    }
    freshness = dict(document.get('freshness') or {})
    if freshness:
        if not freshness.get('column'):
            raise MonitorError(f'The freshness of monitor {name} needs a column.')
        freshness['max_age'] = parse_duration(
            freshness.get('max_age'), f'The freshness max_age of {name}',
        )
    columns = document.get('columns')
    if columns is not None and (
        not isinstance(columns, list) or not all(isinstance(c, str) for c in columns)
    ):
        raise MonitorError(f'The columns of monitor {name} must be a list of names.')
    return Monitor(
        name=name,
        pipeline=str(document['pipeline']),
        block=str(document['block']),
        output=str(document.get('output') or 'output_0'),
        baseline=baseline,
        columns=columns,
        drift=drift,
        missing=missing,
        rows=rows,
        freshness=freshness,
        min_rows=int(document.get('min_rows', 30)),
        fail_block=bool(document.get('fail_block', False)),
        notify=bool(document.get('notify', True)),
        bins=int(document.get('bins', 20)),
        top=int(document.get('top', 20)),
    )


def monitors_for(repo_path: str, pipeline_uuid: str, block_uuid: str) -> List[Monitor]:
    folder = os.path.join(repo_path, MONITORS_FOLDER)
    if not os.path.isdir(folder):
        return []
    found = []
    for name in sorted(os.listdir(folder)):
        stem, suffix = os.path.splitext(name)
        if suffix not in ('.yaml', '.yml') or not _NAME.match(stem):
            continue
        monitor = load_monitor(stem, repo_path)
        if monitor.pipeline == pipeline_uuid and monitor.block == block_uuid:
            found.append(monitor)
    return found


# ---- comparing profiles -------------------------------------------------------------


def jensen_shannon(first: List[float], second: List[float]) -> Optional[float]:
    """Jensen-Shannon divergence (natural log) of two count vectors; None if one is empty."""
    total_a, total_b = sum(first), sum(second)
    if total_a <= 0 or total_b <= 0:
        return None
    p = [a / total_a for a in first]
    q = [b / total_b for b in second]
    m = [(a + b) / 2 for a, b in zip(p, q)]

    def kl(x, y):
        return sum(a * math.log(a / b) for a, b in zip(x, y) if a > 0)

    return max(0.0, (kl(p, m) + kl(q, m)) / 2)


def _category_counts(column: Dict, keys: List[str]) -> List[float]:
    top = {value: count for value, count in column.get('top') or []}
    listed = sum(top.values())
    other = (column.get('other') or 0) + listed - sum(top.get(k, 0) for k in keys)
    return [top.get(k, 0) for k in keys] + [other]


def _worst(results: List[str]) -> str:
    return max(results, key=lambda r: RESULT_ORDER[r]) if results else 'pass'


def _parse_time(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None
    try:
        value = datetime.fromisoformat(text.replace('Z', '+00:00').replace(' UTC', '+00:00'))
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def compare(
    monitor: Monitor,
    current: Dict,
    baseline: Optional[Dict],
    execution_date: Optional[datetime] = None,
) -> Dict:
    """Each check of the monitor for the current profile against the baseline's."""
    checks = []
    rows = current['rows']
    baseline_rows = baseline['rows'] if baseline else None
    enough = rows >= monitor.min_rows and (baseline_rows or 0) >= monitor.min_rows

    if 'min' in monitor.rows:
        checks.append(dict(
            check='rows', column=None, value=rows,
            result='fail' if rows < monitor.rows['min'] else 'pass',
            detail=f'{rows} rows; at least {int(monitor.rows["min"])}',
        ))
    if 'max_change' in monitor.rows:
        if not baseline_rows:
            checks.append(dict(check='row_change', column=None, value=None, result='hold',
                               detail='no baseline rows to compare with'))
        else:
            change = abs(rows - baseline_rows) / baseline_rows
            checks.append(dict(
                check='row_change', column=None, value=round(change, 4),
                result='fail' if change > monitor.rows['max_change'] else 'pass',
                detail=f'{rows} rows against {baseline_rows} ({change:.1%} change)',
            ))

    baseline_columns = {c['name']: c for c in (baseline or {}).get('columns') or []}
    for column in current['columns']:
        name = column['name']
        share = column['missing'] / column['rows'] if column['rows'] else 0.0
        reference = baseline_columns.get(name)
        if 'max' in monitor.missing:
            checks.append(dict(
                check='missing', column=name, value=round(share, 4),
                result='fail' if share > monitor.missing['max'] else 'pass',
                detail=f'{share:.1%} missing; at most {monitor.missing["max"]:.1%}',
            ))
        if 'max_increase' in monitor.missing and reference:
            before = reference['missing'] / reference['rows'] if reference['rows'] else 0.0
            increase = share - before
            checks.append(dict(
                check='missing_increase', column=name, value=round(increase, 4),
                result='fail' if increase > monitor.missing['max_increase'] else 'pass',
                detail=f'{share:.1%} missing against {before:.1%}',
            ))

        if column['kind'] not in ('numeric', 'string', 'boolean'):
            continue
        if reference is None:
            checks.append(dict(check='drift', column=name, value=None, result='hold',
                               detail='no baseline for this column'))
            continue
        if not enough:
            checks.append(dict(check='drift', column=name, value=None, result='hold', detail=(
                f'too few rows to compare ({rows} now, {baseline_rows or 0} in the baseline; '
                f'{monitor.min_rows} needed)'
            )))
            continue
        if column['kind'] == 'numeric':
            first, second = reference.get('counts') or [], column.get('counts') or []
        else:
            keys = [value for value, _ in reference.get('top') or []]
            first = _category_counts(reference, keys)
            second = _category_counts(column, keys)
        divergence = jensen_shannon(first, second) if len(first) == len(second) else None
        if divergence is None:
            checks.append(dict(check='drift', column=name, value=None, result='hold',
                               detail='no values to compare'))
            continue
        result = (
            'fail' if divergence >= monitor.drift['fail']
            else 'warn' if divergence >= monitor.drift['warn'] else 'pass'
        )
        checks.append(dict(
            check='drift', column=name, value=round(divergence, 4), result=result,
            detail=f'Jensen-Shannon divergence {divergence:.4f}',
        ))

    if monitor.freshness:
        name = monitor.freshness['column']
        column = next((c for c in current['columns'] if c['name'] == name), None)
        newest = _parse_time(column.get('max')) if column else None
        reference_time = execution_date or datetime.now(timezone.utc)
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)
        if newest is None:
            checks.append(dict(check='freshness', column=name, value=None, result='hold',
                               detail='no timestamps to check'))
        else:
            age = (reference_time - newest).total_seconds()
            checks.append(dict(
                check='freshness', column=name, value=round(age),
                result='fail' if age > monitor.freshness['max_age'] else 'pass',
                detail=f'newest value {age / 3600:.1f} h before the run',
            ))
    return dict(result=_worst([c['result'] for c in checks]), checks=checks)


# ---- profiles of runs ---------------------------------------------------------------


def _records_dir(pipeline, monitor: Monitor) -> str:
    return os.path.join(pipeline.variable_manager.variables_dir, RECORDS_FOLDER, monitor.name)


def saved(pipeline, monitor: Monitor) -> List[Dict]:
    """The monitor's saved records, oldest run first."""
    storage = pipeline.variable_manager.storage
    folder = _records_dir(pipeline, monitor)
    if not storage.path_exists(folder):
        return []
    ids = sorted(
        int(name[:-5]) for name in storage.listdir(folder)
        if name.endswith('.json') and name[:-5].isdigit()
    )
    return [storage.read_json_file(os.path.join(folder, f'{i}.json')) for i in ids]


def _baseline(pipeline, monitor: Monitor, run_id: int) -> Optional[Dict]:
    records = [r for r in saved(pipeline, monitor) if r['pipeline_run_id'] != run_id]
    if monitor.baseline == 'previous':
        earlier = [r for r in records if r['pipeline_run_id'] < run_id]
        return earlier[-1] if earlier else None
    wanted = int(monitor.baseline.split(':', 1)[1])
    return next((r for r in records if r['pipeline_run_id'] == wanted), None)


def _output_file(pipeline, block_uuid: str, output: str, partition: Optional[str]):
    from mage_ai.data_preparation import contracts

    class _Block:
        pass

    block = _Block()
    block.pipeline = pipeline
    block.uuid = block_uuid
    path, variable = contracts._stored_file(
        block, contracts.Binding(name='monitor', output=output), partition, None,
    )
    return path, variable


def check_block_run(pipeline, block_uuid: str, pipeline_run, block_run_id: int) -> List[Dict]:
    """Runs the monitors that watch this block; returns their records."""
    import tempfile

    import column_atlas_native

    from mage_ai.data_preparation import contracts

    records = []
    for monitor in monitors_for(pipeline.repo_path, pipeline.uuid, block_uuid):
        baseline = _baseline(pipeline, monitor, pipeline_run.id)
        edges = {
            c['name']: c['edges'] for c in (baseline or {}).get('profile', {}).get('columns', [])
            if c.get('edges')
        }
        request = dict(bins=monitor.bins, top=monitor.top, edges=edges)
        if monitor.columns:
            columns = list(monitor.columns)
            if monitor.freshness and monitor.freshness['column'] not in columns:
                columns.append(monitor.freshness['column'])
            request['columns'] = columns
        with tempfile.TemporaryDirectory(prefix='mage_monitor_') as directory:
            path, variable = _output_file(
                pipeline, block_uuid, monitor.output, pipeline_run.execution_partition,
            )
            if path is None and variable is not None:
                path = contracts._write_frame(variable.read_data(), directory)
            if path is None:
                raise MonitorError(
                    f'Monitor {monitor.name}: {monitor.output} of block {block_uuid} is not a '
                    'stored DataFrame.'
                )
            try:
                profile = json.loads(column_atlas_native.profile_columns(path, json.dumps(request)))
            except ValueError as error:
                raise MonitorError(f'Monitor {monitor.name}: {error}')
        result = compare(
            monitor, profile, (baseline or {}).get('profile'), pipeline_run.execution_date,
        )
        record = dict(
            monitor=monitor.name,
            pipeline_run_id=pipeline_run.id,
            block_run_id=block_run_id,
            baseline_run_id=(baseline or {}).get('pipeline_run_id'),
            checked_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            result=result['result'],
            checks=result['checks'],
            fail_block=monitor.fail_block,
            notify=monitor.notify,
            profile=profile,
        )
        storage = pipeline.variable_manager.storage
        folder = _records_dir(pipeline, monitor)
        storage.makedirs(folder, exist_ok=True)
        storage.write_json_file(os.path.join(folder, f'{pipeline_run.id}.json'), record)
        records.append(record)
    return records


def summary(record: Dict) -> str:
    baseline = record.get('baseline_run_id')
    lines = [
        f'Monitor {record["monitor"]}: {record["result"]} '
        f'(against {f"run {baseline}" if baseline else "no baseline"}).'
    ]
    for check in record['checks']:
        if check['result'] == 'pass':
            continue
        where = f' {check["column"]}' if check.get('column') else ''
        lines.append(f'- {check["check"]}{where}: {check["result"]}; {check["detail"]}')
    return '\n'.join(lines)


def without_profile(record: Dict) -> Dict:
    """The record without its profile, for block run metrics."""
    return {key: value for key, value in record.items() if key != 'profile'}
