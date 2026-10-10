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
    'freshness', 'min_rows', 'fail_block', 'notify', 'description', 'bins', 'top', 'rolling',
    'performance', 'evidently', 'nannyml', 'environment',
}
# The engines' versions tested with this fork (pandas 3, NumPy 2, Python 3.12).
ENGINE_REQUIREMENTS = {'evidently': 'evidently==0.7.23', 'nannyml': 'nannyml==0.13.1'}
# What an engine's failed test counts as: Evidently's default tests are bands around the
# baseline in both directions, so a better accuracy fails them too.
ENGINE_SEVERITY = {'evidently': 'warn', 'nannyml': 'fail'}
ENGINE_TIMEOUT_SECONDS = int(os.getenv('MAGE_MONITOR_ENGINE_TIMEOUT_SECONDS') or 1800)
ENGINE_WORKER = os.path.join(os.path.dirname(__file__), 'monitor_engines.py')
DRIFT_METHODS = {
    # Jensen-Shannon divergence, natural log: 0 to ln 2.
    'js': dict(warn=0.1, fail=0.2),
    # Population stability index: under 0.1 stable, 0.1 to 0.25 moderate, above large.
    'psi': dict(warn=0.1, fail=0.25),
}
CLASSIFICATION_METRICS = ('accuracy', 'precision', 'recall', 'f1', 'roc_auc', 'log_loss')
REGRESSION_METRICS = ('mae', 'rmse', 'mape', 'r2')
# Metrics where a larger value is worse.
LOWER_IS_BETTER = {'log_loss', 'mae', 'rmse', 'mape'}
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
    # True: every failed run notifies; on_change: only a run whose result turns to fail.
    notify: Any = True
    bins: int = 20
    top: int = 20
    # Per column: {column: {drift: {...}, missing: {...}}}, over the monitor's settings.
    column_rules: Dict[str, Dict] = field(default_factory=dict)
    # The baseline is the sum of this many earlier runs (previous baseline only).
    rolling: int = 1
    performance: Dict[str, Any] = field(default_factory=dict)
    evidently: Dict[str, Any] = field(default_factory=dict)
    nannyml: Dict[str, Any] = field(default_factory=dict)
    # The requirements the engines run with; ENGINE_REQUIREMENTS by default.
    environment: Dict[str, Any] = field(default_factory=dict)


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
    drift = _drift_settings(document.get('drift'), name)
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
    column_rules: Dict[str, Dict] = {}
    if isinstance(columns, dict):
        for column, rules in columns.items():
            rules = rules or {}
            if not isinstance(rules, dict) or set(rules) - {'drift', 'missing'}:
                raise MonitorError(
                    f'The rules of column {column} in monitor {name} take drift and missing.'
                )
            column_rules[str(column)] = dict(
                drift=_drift_settings(rules.get('drift'), name, drift) if 'drift' in rules
                else None,
                missing={
                    k: _number(v, f'missing {k} of {column} in {name}')
                    for k, v in (rules.get('missing') or {}).items()
                    if k in ('max', 'max_increase')
                } if 'missing' in rules else None,
            )
        columns = list(columns)
    elif columns is not None and (
        not isinstance(columns, list) or not all(isinstance(c, str) for c in columns)
    ):
        raise MonitorError(f'The columns of monitor {name} must be a list or a mapping.')
    notify = document.get('notify', True)
    if notify not in (True, False, 'on_change'):
        raise MonitorError(f'The notify of monitor {name} is true, false or on_change.')
    rolling = document.get('rolling', 1)
    if isinstance(rolling, bool) or not isinstance(rolling, int) or rolling < 1:
        raise MonitorError(f'The rolling of monitor {name} is a number of runs, 1 or more.')
    performance = _performance_settings(document.get('performance'), name)
    engines = {}
    for engine in ('evidently', 'nannyml'):
        settings = document.get(engine)
        if settings in (None, False):
            engines[engine] = {}
            continue
        if settings is True:
            settings = {}
        if not isinstance(settings, dict):
            raise MonitorError(f'The {engine} settings of monitor {name} must be a mapping.')
        severity = settings.get('severity', ENGINE_SEVERITY[engine])
        if severity not in ('warn', 'fail'):
            raise MonitorError(f'The {engine} severity of monitor {name} is warn or fail.')
        engines[engine] = dict(settings, severity=severity)
    environment = document.get('environment') or {}
    if not isinstance(environment, dict) or set(environment) - {'requirements', 'python'}:
        raise MonitorError(
            f'The environment of monitor {name} takes requirements and python.'
        )
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
        notify=notify,
        bins=int(document.get('bins', 20)),
        top=int(document.get('top', 20)),
        column_rules=column_rules,
        rolling=rolling,
        performance=performance,
        evidently=engines['evidently'],
        nannyml=engines['nannyml'],
        environment=environment,
    )


def _drift_settings(raw: Any, name: str, defaults: Optional[Dict] = None) -> Dict:
    raw = raw or {}
    if not isinstance(raw, dict) or set(raw) - {'method', 'warn', 'fail'}:
        raise MonitorError(f'The drift of monitor {name} takes method, warn and fail.')
    method = raw.get('method') or (defaults or {}).get('method') or 'js'
    if method not in DRIFT_METHODS:
        raise MonitorError(
            f'The drift method of monitor {name} is one of {", ".join(DRIFT_METHODS)}.'
        )
    settings = dict(DRIFT_METHODS[method])
    if defaults and defaults.get('method') == method:
        settings.update(warn=defaults['warn'], fail=defaults['fail'])
    settings.update({
        k: _number(v, f'drift {k} of {name}') for k, v in raw.items() if k in ('warn', 'fail')
    })
    settings['method'] = method
    return settings


def _performance_settings(raw: Any, name: str) -> Dict:
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise MonitorError(f'The performance of monitor {name} must be a mapping.')
    unknown = set(raw) - {
        'task', 'target', 'prediction', 'score', 'positive', 'metrics', 'min', 'max',
        'max_drop',
    }
    if unknown:
        raise MonitorError(
            f'The performance of monitor {name} has unknown settings: '
            f'{", ".join(sorted(unknown))}.'
        )
    task = raw.get('task') or 'classification'
    if task not in ('classification', 'regression'):
        raise MonitorError(f'The performance task of {name} is classification or regression.')
    for required in ('target', 'prediction'):
        if not raw.get(required):
            raise MonitorError(f'The performance of monitor {name} needs a {required} column.')
    allowed = CLASSIFICATION_METRICS if task == 'classification' else REGRESSION_METRICS
    metrics = list(raw.get('metrics') or (
        ['accuracy', 'f1'] + (['roc_auc'] if raw.get('score') else [])
        if task == 'classification' else ['mae', 'rmse', 'r2']
    ))
    unknown = [m for m in metrics if m not in allowed]
    if unknown:
        raise MonitorError(
            f'Monitor {name} cannot compute {", ".join(unknown)} for {task}; '
            f'the metrics are {", ".join(allowed)}.'
        )
    if any(m in ('roc_auc', 'log_loss') for m in metrics) and not raw.get('score'):
        raise MonitorError(f'roc_auc and log_loss of monitor {name} need a score column.')
    bounds = {}
    for key in ('min', 'max', 'max_drop'):
        values = raw.get(key) or {}
        if not isinstance(values, dict):
            raise MonitorError(f'The performance {key} of {name} maps metrics to numbers.')
        bounds[key] = {m: _number(v, f'performance {key} {m} of {name}') for m, v in values.items()}
    return dict(
        task=task,
        target=str(raw['target']),
        prediction=str(raw['prediction']),
        score=raw.get('score'),
        positive=raw.get('positive', 1),
        metrics=metrics,
        **bounds,
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


def population_stability(first: List[float], second: List[float]) -> Optional[float]:
    """
    Population stability index of two count vectors. Empty bins count as 0.0001 of their
    side, the usual guard against log(0); None if one side is empty.
    """
    total_a, total_b = sum(first), sum(second)
    if total_a <= 0 or total_b <= 0:
        return None
    psi = 0.0
    for a, b in zip(first, second):
        p = max(a / total_a, 1e-4)
        q = max(b / total_b, 1e-4)
        psi += (q - p) * math.log(q / p)
    return psi


DIVERGENCES = {'js': jensen_shannon, 'psi': population_stability}
DIVERGENCE_NAMES = {'js': 'Jensen-Shannon divergence', 'psi': 'population stability index'}


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
        rules = monitor.column_rules.get(name) or {}
        missing_rules = rules.get('missing') if rules.get('missing') is not None \
            else monitor.missing
        drift_rules = rules.get('drift') or monitor.drift
        share = column['missing'] / column['rows'] if column['rows'] else 0.0
        reference = baseline_columns.get(name)
        if 'max' in missing_rules:
            checks.append(dict(
                check='missing', column=name, value=round(share, 4),
                result='fail' if share > missing_rules['max'] else 'pass',
                detail=f'{share:.1%} missing; at most {missing_rules["max"]:.1%}',
            ))
        if 'max_increase' in missing_rules and reference:
            before = reference['missing'] / reference['rows'] if reference['rows'] else 0.0
            increase = share - before
            checks.append(dict(
                check='missing_increase', column=name, value=round(increase, 4),
                result='fail' if increase > missing_rules['max_increase'] else 'pass',
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
        method = drift_rules.get('method', 'js')
        divergence = DIVERGENCES[method](first, second) if len(first) == len(second) else None
        if divergence is None:
            checks.append(dict(check='drift', column=name, value=None, result='hold',
                               detail='no values to compare'))
            continue
        result = (
            'fail' if divergence >= drift_rules['fail']
            else 'warn' if divergence >= drift_rules['warn'] else 'pass'
        )
        checks.append(dict(
            check='drift', column=name, value=round(divergence, 4), result=result,
            method=method,
            detail=f'{DIVERGENCE_NAMES[method]} {divergence:.4f}',
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
    checks.extend(_performance_checks(
        monitor, current.get('performance'), (baseline or {}).get('performance'),
    ))
    return dict(result=_worst([c['result'] for c in checks]), checks=checks)


def _performance_checks(monitor: Monitor, current: Optional[Dict], baseline: Optional[Dict]):
    settings = monitor.performance
    if not settings:
        return []
    if not current or current.get('error'):
        return [dict(check='performance', column=None, value=None, result='hold',
                     detail=(current or {}).get('error') or 'no labeled rows')]
    checks = []
    for metric in settings['metrics']:
        value = current.get(metric)
        if value is None or not math.isfinite(value):
            checks.append(dict(check=metric, column=None, value=None, result='hold',
                               detail=f'{metric} could not be computed'))
            continue
        result, reasons = 'pass', []
        if metric in settings['min'] and value < settings['min'][metric]:
            result = 'fail'
            reasons.append(f'below the min {settings["min"][metric]}')
        if metric in settings['max'] and value > settings['max'][metric]:
            result = 'fail'
            reasons.append(f'above the max {settings["max"][metric]}')
        before = (baseline or {}).get(metric)
        if metric in settings['max_drop'] and before is not None:
            drop = (value - before) if metric in LOWER_IS_BETTER else (before - value)
            if drop > settings['max_drop'][metric]:
                result = 'fail'
                reasons.append(f'{drop:.4g} worse than the baseline ({before:.4g})')
        checks.append(dict(
            check=metric, column=None, value=round(value, 6), result=result,
            detail=f'{metric} {value:.4g} on {current.get("rows", 0)} labeled rows'
                   + (f'; {"; ".join(reasons)}' if reasons else ''),
        ))
    return checks


def performance(settings: Dict, path: str) -> Dict:
    """The performance metrics of a model's predictions in an output with their labels."""
    import numpy as np
    import pyarrow.parquet as pq
    from sklearn import metrics as sk

    columns = [settings['target'], settings['prediction']]
    if settings.get('score'):
        columns.append(settings['score'])
    try:
        frame = pq.read_table(path, columns=list(dict.fromkeys(columns))).to_pandas()
    except Exception as error:
        return dict(error=f'the performance columns could not be read: {error}')
    frame = frame.dropna(subset=columns)
    if frame.empty:
        return dict(error='no rows have both a label and a prediction', rows=0)
    target, prediction = frame[settings['target']], frame[settings['prediction']]
    result: Dict[str, Any] = dict(rows=int(len(frame)))
    for metric in settings['metrics']:
        try:
            if metric == 'accuracy':
                value = sk.accuracy_score(target, prediction)
            elif metric in ('precision', 'recall', 'f1'):
                binary = target.nunique() <= 2 and settings.get('positive') in set(target)
                kwargs = dict(pos_label=settings['positive'], average='binary') if binary \
                    else dict(average='macro')
                function = dict(
                    precision=sk.precision_score, recall=sk.recall_score, f1=sk.f1_score,
                )[metric]
                value = function(target, prediction, zero_division=0, **kwargs)
            elif metric == 'roc_auc':
                value = sk.roc_auc_score(target == settings['positive'], frame[settings['score']])
            elif metric == 'log_loss':
                value = sk.log_loss(
                    target == settings['positive'], frame[settings['score']], labels=[False, True],
                )
            elif metric == 'mae':
                value = sk.mean_absolute_error(target, prediction)
            elif metric == 'rmse':
                value = float(np.sqrt(sk.mean_squared_error(target, prediction)))
            elif metric == 'mape':
                value = sk.mean_absolute_percentage_error(target, prediction)
            else:
                value = sk.r2_score(target, prediction)
            result[metric] = float(value)
        except Exception:
            result[metric] = None
    return result


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


def _merged(records: List[Dict]) -> Dict:
    """One baseline record from several, whose profiles share their bin edges."""
    if len(records) == 1:
        return records[0]
    newest = records[-1]
    columns = {}
    for record in records:
        for column in record['profile']['columns']:
            merged = columns.get(column['name'])
            if merged is None:
                columns[column['name']] = json.loads(json.dumps(column))
                continue
            merged['rows'] += column['rows']
            merged['missing'] += column['missing']
            if column.get('counts') and merged.get('edges') == column.get('edges'):
                merged['counts'] = [a + b for a, b in zip(merged['counts'], column['counts'])]
            if 'top' in column:
                tally = {value: count for value, count in merged.get('top') or []}
                for value, count in column['top']:
                    tally[value] = tally.get(value, 0) + count
                merged['top'] = sorted(tally.items(), key=lambda item: (-item[1], item[0]))
                merged['other'] = merged.get('other', 0) + column.get('other', 0)
    profile = dict(
        rows=sum(r['profile']['rows'] for r in records),
        columns=list(columns.values()),
    )
    performances = [r['profile'].get('performance') for r in records]
    if all(p and not p.get('error') for p in performances):
        keys = {k for p in performances for k, v in p.items() if isinstance(v, (int, float))}
        weights = [p.get('rows', 0) for p in performances]
        profile['performance'] = {
            key: sum((p.get(key) or 0) * w for p, w in zip(performances, weights))
            / max(sum(weights), 1)
            for key in keys if key != 'rows'
        }
    return dict(newest, profile=profile,
                pipeline_run_id=newest['pipeline_run_id'],
                baseline_runs=[r['pipeline_run_id'] for r in records],
                baseline_records=[dict(partition=r.get('partition')) for r in records])


def _baseline(pipeline, monitor: Monitor, run_id: int) -> Optional[Dict]:
    records = [r for r in saved(pipeline, monitor) if r['pipeline_run_id'] != run_id]
    if monitor.baseline == 'previous':
        earlier = [r for r in records if r['pipeline_run_id'] < run_id]
        return _merged(earlier[-monitor.rolling:]) if earlier else None
    wanted = int(monitor.baseline.split(':', 1)[1])
    return next((r for r in records if r['pipeline_run_id'] == wanted), None)


def _reference_edges(pipeline, monitor: Monitor, baseline: Optional[Dict]) -> Dict:
    """
    The bin edges every run of the monitor is counted in: the baseline run's for a fixed
    baseline, else the first checked run's, so all runs compare and roll up.
    """
    if monitor.baseline != 'previous':
        source = baseline
    else:
        records = saved(pipeline, monitor)
        source = records[0] if records else None
    return {
        c['name']: c['edges'] for c in (source or {}).get('profile', {}).get('columns', [])
        if c.get('edges')
    }


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


def run_engines(
    pipeline, monitor: Monitor, block_uuid: str, current_path: str, baseline: Optional[Dict],
    output_dir: str,
) -> Dict:
    """
    Evidently and NannyML on the current output and the baseline runs' outputs, in an
    environment uv builds for them (data_preparation/environments.py), so their packages
    never enter Mage's. Returns each engine's checks, reports and error.
    """
    import subprocess

    from mage_ai.data_preparation import environments

    engines = {k: v for k, v in (('evidently', monitor.evidently),
                                 ('nannyml', monitor.nannyml)) if v}
    if not engines:
        return {}
    baseline_paths = []
    for record in (baseline or {}).get('baseline_records') or ([baseline] if baseline else []):
        path, _ = _output_file(
            pipeline, block_uuid, monitor.output, record.get('partition'),
        )
        if path:
            baseline_paths.append(path)
    if not baseline_paths:
        return {name: dict(checks=[dict(
            check=name, column=None, value=None, result='hold',
            detail='no stored baseline output to compare with',
        )], reports=[]) for name in engines}

    requirements = monitor.environment.get('requirements') or [
        ENGINE_REQUIREMENTS[name] for name in engines
    ]
    if isinstance(requirements, str):
        requirements = [requirements]
    environment = environments.environment_for(requirements, monitor.environment.get('python'))
    python = environments.ensure(environment)
    os.makedirs(output_dir, exist_ok=True)
    request_path = os.path.join(output_dir, 'request.json')
    with open(request_path, 'w', encoding='utf-8') as file:
        json.dump(dict(
            current=[current_path], baseline=baseline_paths, output_dir=output_dir,
            **{name: {k: v for k, v in settings.items() if k != 'severity'}
               for name, settings in engines.items()},
        ), file)
    # -I: the script's folder (mage_ai/orchestration, with a queue package) stays off
    # the import path, so the standard library's queue is found.
    completed = subprocess.run(
        [str(python), '-I', ENGINE_WORKER, request_path],
        capture_output=True, text=True, timeout=ENGINE_TIMEOUT_SECONDS,
        env={k: v for k, v in os.environ.items() if k not in ('PYTHONPATH', 'VIRTUAL_ENV')},
    )
    result_path = os.path.join(output_dir, 'result.json')
    if not os.path.exists(result_path):
        message = (completed.stderr or completed.stdout or '').strip()[-2000:]
        raise MonitorError(f'Monitor {monitor.name}: the engines stopped: {message}')
    with open(result_path, encoding='utf-8') as file:
        result = json.load(file)
    for name, outcome in result.items():
        severity = engines[name]['severity']
        for check in outcome.get('checks') or []:
            if check['result'] == 'fail':
                check['result'] = severity
        if outcome.get('error'):
            outcome['checks'] = [dict(
                check=name, column=None, value=None, result='hold',
                detail=f'{name} failed: {outcome["error"]}',
            )]
    return result


def check_block_run(pipeline, block_uuid: str, pipeline_run, block_run_id: int) -> List[Dict]:
    """Runs the monitors that watch this block; returns their records."""
    import tempfile

    import column_atlas_native

    from mage_ai.data_preparation import contracts

    records = []
    for monitor in monitors_for(pipeline.repo_path, pipeline.uuid, block_uuid):
        baseline = _baseline(pipeline, monitor, pipeline_run.id)
        edges = _reference_edges(pipeline, monitor, baseline)
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
            if monitor.performance:
                profile['performance'] = performance(monitor.performance, path)
            engines = run_engines(
                pipeline, monitor, block_uuid, path, baseline,
                os.path.join(_records_dir(pipeline, monitor), str(pipeline_run.id)),
            ) if (monitor.evidently or monitor.nannyml) else {}
        result = compare(
            monitor, profile, (baseline or {}).get('profile'), pipeline_run.execution_date,
        )
        for outcome in engines.values():
            result['checks'].extend(outcome.get('checks') or [])
        result['result'] = _worst([c['result'] for c in result['checks']])
        previous = saved(pipeline, monitor)
        earlier = [r for r in previous if r['pipeline_run_id'] < pipeline_run.id]
        turned = result['result'] == 'fail' and (
            not earlier or earlier[-1].get('result') != 'fail'
        )
        record = dict(
            monitor=monitor.name,
            pipeline_run_id=pipeline_run.id,
            block_run_id=block_run_id,
            baseline_run_id=(baseline or {}).get('pipeline_run_id'),
            baseline_runs=(baseline or {}).get('baseline_runs'),
            partition=pipeline_run.execution_partition,
            engines={name: dict(reports=o.get('reports') or [], error=o.get('error'))
                     for name, o in engines.items()},
            should_notify=result['result'] == 'fail' and (
                monitor.notify is True or (monitor.notify == 'on_change' and turned)
            ),
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
