"""
Datasets (top_5 #1, ADR-0046): block outputs a project names and maintains, with an owner,
a description and how fresh their data must be, kept in datasets.yaml beside the code:

    datasets:
      - id: daily_sales
        name: Daily sales
        description: Revenue per store and day.
        owner: ana@example.com
        pipeline: sales_daily
        block: aggregate_sales
        output: output_0          # the default
        time_column: day          # optional: the data's own time, for freshness
        max_age: 1d               # optional: 30m, 6h, 1d, 7d

Each dataset's status comes from what MageML records, read without scanning its rows: the
latest completed run of its block (published), the stored output's footer (rows, columns,
and the time column's largest value: data through), whether the block's latest run failed,
and its monitors. Freshness compares the data's time with max_age (the publication time
when there is no time column, and says so); the states stay apart: data can be fresh and
its checks failing.

Dependencies: a dataset depends on the registered datasets its block reads, through the
blocks between them, and on those of the pipelines that feed its pipeline
(pipeline_dependencies.py).
"""
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml

FILE = 'datasets.yaml'
ID = re.compile(r'^[a-z][a-z0-9_]{0,63}$')
AGE = re.compile(r'^\s*(\d+)\s*([mhdw])\s*$')
UNITS = dict(m='minutes', h='hours', d='days', w='weeks')
FIELDS = ('id', 'name', 'description', 'owner', 'pipeline', 'block', 'output', 'time_column',
          'max_age')


class DatasetError(Exception):
    pass


def _path(repo_path: str) -> str:
    return os.path.join(repo_path, FILE)


def max_age(text: Optional[str]) -> Optional[timedelta]:
    if not text:
        return None
    found = AGE.match(str(text))
    if not found:
        raise DatasetError(f'max_age {text!r}: a number and m, h, d or w, such as 1d.')
    return timedelta(**{UNITS[found[2]]: int(found[1])})


def load(repo_path: str) -> List[Dict[str, Any]]:
    """The registered datasets, as written."""
    path = _path(repo_path)
    if not os.path.isfile(path):
        return []
    with open(path) as file:
        document = yaml.safe_load(file) or {}
    entries = document.get('datasets') or []
    if not isinstance(entries, list):
        raise DatasetError(f'{FILE}: datasets is a list.')
    return [dict(e, output=e.get('output') or 'output_0') for e in entries if isinstance(e, dict)]


def _save(repo_path: str, entries: List[Dict[str, Any]]) -> None:
    cleaned = [
        {k: e[k] for k in FIELDS if e.get(k) not in (None, '') and
         not (k == 'output' and e[k] == 'output_0')}
        for e in entries
    ]
    path = _path(repo_path)
    temporary = f'{path}.tmp'
    with open(temporary, 'w') as file:
        yaml.safe_dump(dict(datasets=cleaned), file, sort_keys=False, allow_unicode=True)
    os.replace(temporary, path)


def validate(repo_path: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    from mage_ai.data_preparation.models.pipeline import Pipeline

    unknown = sorted(set(entry) - set(FIELDS))
    if unknown:
        raise DatasetError(f'Unknown dataset fields {", ".join(unknown)}.')
    if not ID.match(str(entry.get('id') or '')):
        raise DatasetError('id is lowercase letters, digits and _, starting with a letter.')
    for key in ('pipeline', 'block'):
        if not entry.get(key):
            raise DatasetError(f'A dataset needs its {key}.')
    pipeline = Pipeline.get(entry['pipeline'], repo_path=repo_path, check_if_exists=True)
    if pipeline is None:
        raise DatasetError(f"Pipeline {entry['pipeline']} does not exist.")
    if pipeline.get_block(entry['block']) is None:
        raise DatasetError(f"{entry['pipeline']} has no block {entry['block']}.")
    max_age(entry.get('max_age'))
    return dict(entry, name=entry.get('name') or entry['id'], output=entry.get('output') or
                'output_0')


def register(repo_path: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """Adds or replaces a dataset by id."""
    entry = validate(repo_path, entry)
    entries = [e for e in load(repo_path) if e.get('id') != entry['id']]
    taken = [e['id'] for e in entries
             if (e.get('pipeline'), e.get('block'), e.get('output')) ==
             (entry['pipeline'], entry['block'], entry['output'])]
    if taken:
        raise DatasetError(f'That output is the dataset {taken[0]} already.')
    _save(repo_path, entries + [entry])
    return entry


def unregister(repo_path: str, dataset_id: str) -> None:
    entries = load(repo_path)
    if not any(e.get('id') == dataset_id for e in entries):
        raise DatasetError('Unknown dataset.')
    _save(repo_path, [e for e in entries if e.get('id') != dataset_id])


def suggestions(repo_path: str) -> List[Dict[str, Any]]:
    """Global data products' outputs not registered yet, as drafts to register."""
    from mage_ai.data_preparation.models.global_data_product import GlobalDataProduct

    registered = {(e.get('pipeline'), e.get('block')) for e in load(repo_path)}
    drafts = []
    for product in GlobalDataProduct.load_all(repo_path=repo_path):
        object_type = str(getattr(product.object_type, 'value', product.object_type))
        if object_type != 'pipeline':
            continue
        for block_uuid in (product.settings or {}):
            if (product.object_uuid, block_uuid) in registered:
                continue
            drafts.append(dict(
                id=re.sub(r'[^a-z0-9_]+', '_', f'{product.uuid}_{block_uuid}'.lower())[:64],
                name=f'{product.uuid}: {block_uuid}',
                pipeline=product.object_uuid,
                block=block_uuid,
                source=f'global data product {product.uuid}',
            ))
    return drafts


def _latest_runs(pipeline_uuid: str, block_uuid: str, limit: int = 10):
    from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun

    return (
        BlockRun.query.join(PipelineRun, PipelineRun.id == BlockRun.pipeline_run_id)
        .filter(PipelineRun.pipeline_uuid == pipeline_uuid, BlockRun.block_uuid == block_uuid)
        .order_by(BlockRun.id.desc())
        .limit(limit)
        .all()
    )


def _status(block_run) -> str:
    return str(getattr(block_run.status, 'value', block_run.status))


def _footer(repo_path: str, entry: Dict[str, Any], block_run) -> Optional[Dict[str, Any]]:
    """Rows, columns and the time column's largest value, from the Parquet footer."""
    import pyarrow.parquet as pq

    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(entry['pipeline'], repo_path=repo_path)
    variable = pipeline.variable_manager.get_variable_object(
        entry['pipeline'], entry['block'], entry['output'],
        partition=block_run.pipeline_run.execution_partition,
    )
    path = os.path.join(variable.variable_path, 'data.parquet')
    if not os.path.isfile(path):
        return None
    metadata = pq.read_metadata(path)
    schema = metadata.schema.to_arrow_schema()
    columns = [dict(name=f.name, type=str(f.type)) for f in schema if not
               f.name.startswith('__index_level_')]
    through = None
    column = entry.get('time_column')
    if column and column in schema.names:
        index = schema.get_field_index(column)
        values = []
        for group in range(metadata.num_row_groups):
            statistics = metadata.row_group(group).column(index).statistics
            if statistics is not None and statistics.has_min_max:
                values.append(statistics.max)
        if values:
            through = max(values)
    return dict(rows=metadata.num_rows, columns=columns, through=through)


def _as_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if hasattr(value, 'isoformat') and not isinstance(value, str):
        # A date: its end.
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc) + \
            timedelta(days=1)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def freshness(entry: Dict[str, Any], published: Optional[datetime],
              through: Optional[datetime], now: Optional[datetime] = None) -> Dict[str, Any]:
    """fresh, stale or unknown, with what it is measured on and why."""
    now = now or datetime.now(timezone.utc)
    limit = max_age(entry.get('max_age'))
    if limit is None:
        return dict(state='unknown', reason='No max_age is set.')
    measured, basis = (through, 'data') if entry.get('time_column') else (published, 'published')
    if measured is None:
        return dict(state='unknown', basis=basis, reason=(
            f"The output has no {entry['time_column']} values." if basis == 'data' else
            'The block has not completed a run.'
        ))
    if measured > now + timedelta(minutes=5):
        return dict(state='unknown', basis=basis, reason='Its time is in the future.')
    age = now - measured
    return dict(
        state='fresh' if age <= limit else 'stale', basis=basis,
        age_seconds=int(age.total_seconds()), max_age=entry['max_age'],
    )


def status(repo_path: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """The dataset as it is now: published run, rows, columns, data through, freshness and
    checks."""
    runs = _latest_runs(entry['pipeline'], entry['block'])
    completed = next((r for r in runs if _status(r) == 'completed'), None)
    latest = runs[0] if runs else None
    footer = None
    if completed is not None:
        try:
            footer = _footer(repo_path, entry, completed)
        except Exception:
            footer = None
    published = _as_time(getattr(completed, 'completed_at', None))
    through = _as_time((footer or {}).get('through'))
    failing = []
    if latest is not None and _status(latest) == 'failed':
        failing.append(f'its latest run failed (run {latest.pipeline_run_id})')
    for monitor in ((getattr(completed, 'metrics', None) or {}).get('monitors') or []):
        if monitor.get('result') in ('fail', 'error'):
            failing.append(f"monitor {monitor.get('monitor')} {monitor.get('result')}ed")
    return dict(
        published_at=published.isoformat() if published else None,
        block_run_id=completed.id if completed else None,
        pipeline_run_id=completed.pipeline_run_id if completed else None,
        rows=(footer or {}).get('rows'),
        columns=(footer or {}).get('columns'),
        data_through=through.isoformat() if through else None,
        freshness=freshness(entry, published, through),
        checks=dict(state='failing' if failing else ('passing' if completed else 'unknown'),
                    failing=failing),
        runs=[dict(block_run_id=r.id, pipeline_run_id=r.pipeline_run_id, status=_status(r),
                   at=(_as_time(r.completed_at or r.updated_at) or datetime.now(timezone.utc))
                   .isoformat())
              for r in runs],
    )


def _ancestors(pipeline, block_uuid: str) -> List[str]:
    seen, stack, found = set(), [block_uuid], []
    while stack:
        block = pipeline.get_block(stack.pop())
        for upstream in (block.upstream_blocks or []) if block else []:
            if upstream.uuid not in seen:
                seen.add(upstream.uuid)
                found.append(upstream.uuid)
                stack.append(upstream.uuid)
    return found


def dependencies(repo_path: str, entries: List[Dict[str, Any]],
                 entry: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    """The registered datasets this one reads (upstream) and those that read it
    (downstream): through its pipeline's blocks, and the pipelines that feed or follow it."""
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration import pipeline_dependencies

    def ancestors_of(dataset) -> List[str]:
        pipeline = Pipeline.get(dataset['pipeline'], repo_path=repo_path)
        return _ancestors(pipeline, dataset['block'])

    try:
        edges = pipeline_dependencies.build(repo_path)['edges']
    except Exception:
        edges = []
    feeds = {(e['source'], e['target']) for e in edges}
    upstream, downstream = [], []
    mine = ancestors_of(entry)
    for other in entries:
        if other['id'] == entry['id']:
            continue
        if other['pipeline'] == entry['pipeline']:
            if other['block'] in mine:
                upstream.append(dict(id=other['id'], via='block'))
            elif entry['block'] in ancestors_of(other):
                downstream.append(dict(id=other['id'], via='block'))
        elif (other['pipeline'], entry['pipeline']) in feeds:
            upstream.append(dict(id=other['id'], via=f"pipeline {other['pipeline']}"))
        elif (entry['pipeline'], other['pipeline']) in feeds:
            downstream.append(dict(id=other['id'], via=f"pipeline {other['pipeline']}"))
    return dict(upstream=upstream, downstream=downstream)


def search(entries: List[Dict[str, Any]], query: Optional[str]) -> List[Dict[str, Any]]:
    """Datasets whose id, name, description, owner or pipeline contain every word."""
    words = [w for w in (query or '').lower().split() if w]
    found = []
    for entry in entries:
        text = ' '.join(str(entry.get(k) or '') for k in
                        ('id', 'name', 'description', 'owner', 'pipeline', 'block')).lower()
        if all(word in text for word in words):
            found.append(entry)
    return sorted(found, key=lambda e: e['id'])


def summary(repo_path: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """A search result: the entry with its status, without its runs."""
    found = status(repo_path, entry)
    return dict(entry, **{k: v for k, v in found.items() if k not in ('runs', 'columns')})
