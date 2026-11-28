"""
Change previews (ADR-0044): what the pipeline's current code does to its data, against a
completed run (the baseline), before it ships.

1. The blocks whose code or settings differ from the baseline's run record, and the blocks
   after them, are the candidates. A change to a shared file (not a block's, not the
   pipeline's settings) makes every block a candidate.
2. The candidates run in a partition of their own, `previews/<id>`, on the baseline's
   inputs: every other block's stored outputs are linked there (hard links, no copy), so a
   candidate reads what the baseline's blocks read. Exporters and sensors never run (they
   write to or wait on the outside), nor do SQL blocks (they run in the warehouse) or
   dynamic blocks; the blocks after them read their baseline outputs.
3. Each candidate's outputs are compared with the baseline's: stored DataFrames in Rust
   (column_atlas_native.diff_outputs: schema, then rows added, removed and changed by key,
   or as multisets without one), other values as fusion verification compares them.

A block names its key with `preview_key` in its configuration; without one, a column `id`
in both outputs is the key. A preview is kept as JSON with the run records
(`.previews/<pipeline>/<id>.json`); the last KEEP previews of a pipeline and their outputs
are kept.
"""
import json
import os
import shutil
import subprocess
import sys
import traceback
import uuid as uuid_lib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import yaml

from mage_ai.data_preparation.models.constants import (
    VARIABLE_DIR,
    BlockLanguage,
    BlockType,
)
from mage_ai.orchestration import run_records
from mage_ai.shared.utils import clean_name

PARTITION_PREFIX = 'previews'
FOLDER = '.previews'
KEEP = 10
DATAFRAME_FILE = 'data.parquet'
NOT_RUN_TYPES = {
    BlockType.DATA_EXPORTER: 'writes to a destination; previews never run exporters',
    BlockType.SENSOR: 'waits on the outside; previews never run sensors',
}


class PreviewError(Exception):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _folder(pipeline) -> str:
    return os.path.join(pipeline.variable_manager.variables_dir, FOLDER, pipeline.uuid)


def _path(pipeline, preview_id: str) -> str:
    if not preview_id.isalnum():
        raise PreviewError('Unknown preview.')
    return os.path.join(_folder(pipeline), f'{preview_id}.json')


def partition(preview_id: str) -> str:
    return f'{PARTITION_PREFIX}/{preview_id}'


def _partition_dir(pipeline, name: str) -> str:
    return os.path.join(
        pipeline.variable_manager.pipeline_path(pipeline.uuid), VARIABLE_DIR, name,
    )


def read(pipeline, preview_id: str) -> Dict[str, Any]:
    path = _path(pipeline, preview_id)
    if not os.path.isfile(path):
        raise PreviewError('Unknown preview.')
    with open(path) as file:
        return json.load(file)


def write(pipeline, preview: Dict[str, Any]) -> None:
    path = _path(pipeline, preview['id'])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f'{path}.tmp'
    with open(temporary, 'w') as file:
        json.dump(preview, file, default=str)
    os.replace(temporary, path)


def listed(pipeline) -> List[Dict[str, Any]]:
    """The pipeline's previews, newest first, without their block results."""
    folder = _folder(pipeline)
    if not os.path.isdir(folder):
        return []
    found = []
    for name in os.listdir(folder):
        if name.endswith('.json'):
            try:
                with open(os.path.join(folder, name)) as file:
                    preview = json.load(file)
            except (OSError, ValueError):
                continue
            found.append({k: v for k, v in preview.items() if k != 'blocks'})
    return sorted(found, key=lambda p: p.get('created_at') or '', reverse=True)


def _block_settings(metadata_text: Optional[str]) -> Dict[str, Any]:
    """Each block's entry in a pipeline's metadata.yaml, by uuid."""
    try:
        document = yaml.safe_load(metadata_text or '') or {}
    except yaml.YAMLError:
        return {}
    entries = (document.get('blocks') or []) + (document.get('callbacks') or [])
    return {e.get('uuid'): e for e in entries if isinstance(e, dict) and e.get('uuid')}


def changes(pipeline, baseline_run) -> Tuple[Dict[str, str], List[str]]:
    """The blocks that changed since the baseline, each with why, and the files changed."""
    record = run_records.manifest(pipeline, baseline_run.id)
    before: Dict[str, str] = record['code']['files']
    files, _ = run_records.source_files(pipeline)
    now = {relative: run_records._file_digest(path) for relative, path in files.items()}
    changed_files = sorted(
        path for path in set(before) | set(now) if before.get(path) != now.get(path)
    )
    repo_path = os.path.realpath(pipeline.repo_path)
    block_files = {}
    for block in pipeline.blocks_by_uuid.values():
        path = getattr(block, 'file_path', None)
        if path:
            block_files[os.path.relpath(os.path.realpath(path), repo_path)] = block.uuid
    pipeline_dir = os.path.relpath(os.path.realpath(pipeline.dir_path), repo_path)
    metadata = os.path.join(pipeline_dir, 'metadata.yaml')
    changed: Dict[str, str] = {}
    shared: List[str] = []
    for path in changed_files:
        if path in block_files:
            changed[block_files[path]] = f'its code changed ({path})'
        elif path == metadata:
            try:
                snapshot = run_records.code_snapshot(pipeline, record['code']['digest'])
                old = run_records._content(snapshot[path]).decode() if path in snapshot else ''
            except run_records.RunRecordError:
                old = None
            if old is None:
                shared.append(path)
                continue
            with open(os.path.join(repo_path, path)) as file:
                new = file.read()
            settings_before, settings_now = _block_settings(old), _block_settings(new)
            for uuid in pipeline.blocks_by_uuid:
                if settings_before.get(uuid) != settings_now.get(uuid):
                    changed.setdefault(uuid, 'its settings changed (metadata.yaml)')
        elif not path.startswith(pipeline_dir + os.sep):
            shared.append(path)
    if shared:
        for uuid in pipeline.blocks_by_uuid:
            changed.setdefault(uuid, f'shared files changed ({", ".join(shared[:3])})')
    return changed, changed_files


def _ordered(pipeline) -> List:
    from mage_ai.orchestration.fusion_verify import _ordered_blocks

    return _ordered_blocks(pipeline)


def _not_run(block) -> Optional[str]:
    if block.type in NOT_RUN_TYPES:
        return NOT_RUN_TYPES[block.type]
    if block.language == BlockLanguage.SQL:
        return 'runs in the warehouse; previews do not run SQL blocks'
    configuration = block.configuration or {}
    if configuration.get('dynamic') or getattr(block, 'is_dynamic_child', False):
        return 'is dynamic; previews do not run dynamic blocks'
    return None


def plan(pipeline, changed: Dict[str, str]) -> List[Dict[str, Any]]:
    """Each block in order: run (a candidate), not run (with why), or unchanged."""
    candidates: Set[str] = set()
    # Changed, or reading a change, but not run: what reads them has no new input.
    unpreviewed: Set[str] = set()
    rows = []
    for block in _ordered(pipeline):
        upstream = [u.uuid for u in (block.upstream_blocks or [])]
        after = [u for u in upstream if u in candidates]
        stale = [u for u in upstream if u in unpreviewed]
        why = changed.get(block.uuid) or (f'it reads {after[0]}, which changed' if after else None)
        if why is None and stale:
            unpreviewed.add(block.uuid)
            rows.append(dict(block_uuid=block.uuid, status='not run', reason=(
                f'it reads {stale[0]}, which changed and was not previewed'
            )))
            continue
        if why is None:
            rows.append(dict(block_uuid=block.uuid, status='unchanged', reason=''))
            continue
        skip = _not_run(block)
        if skip:
            unpreviewed.add(block.uuid)
            rows.append(dict(block_uuid=block.uuid, status='not run', reason=f'{why}; it {skip}'))
            continue
        if stale:
            why = f"{why}; it reads {stale[0]}'s output from the baseline run, as {stale[0]} " \
                'was not previewed'
        candidates.add(block.uuid)
        rows.append(dict(block_uuid=block.uuid, status='run', reason=why))
    return rows


def _link_tree(source: str, destination: str) -> None:
    def link(src, dst):
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)

    shutil.copytree(source, destination, copy_function=link, dirs_exist_ok=True)


def _key(block, keys: Optional[Dict[str, List[str]]]) -> List[str]:
    if keys and keys.get(block.uuid):
        return list(keys[block.uuid])
    configured = (block.configuration or {}).get('preview_key')
    if isinstance(configured, str):
        return [configured]
    if isinstance(configured, list):
        return [str(c) for c in configured]
    return []


def _parquet(pipeline, block_uuid: str, name: str, part: str) -> Optional[str]:
    variable = pipeline.variable_manager.get_variable_object(
        pipeline.uuid, block_uuid, name, partition=part,
    )
    path = os.path.join(variable.variable_path, DATAFRAME_FILE)
    return path if os.path.isfile(path) else None


def _schema_columns(path: str) -> List[str]:
    import pyarrow.parquet as pq

    return list(pq.read_schema(path).names)


def compare_outputs(pipeline, block, baseline_part: str, candidate_part: str,
                    keys: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
    """Each output of the block, the baseline's against the candidate's."""
    import column_atlas_native

    from mage_ai.orchestration.fusion_verify import compare_values

    manager = pipeline.variable_manager
    names = [
        set(manager.get_variables_by_block(
            pipeline.uuid, block.uuid, partition=part, output_variable_only=True,
        ))
        for part in (baseline_part, candidate_part)
    ]
    outputs = {}
    for name in sorted(names[0] | names[1]):
        if name not in names[0]:
            outputs[name] = dict(result='added', detail='The baseline has no such output.')
            continue
        if name not in names[1]:
            outputs[name] = dict(result='removed', detail='The candidate has no such output.')
            continue
        base = _parquet(pipeline, block.uuid, name, baseline_part)
        cand = _parquet(pipeline, block.uuid, name, candidate_part)
        if base and cand:
            key = _key(block, keys)
            if not key and 'id' in _schema_columns(base) and 'id' in _schema_columns(cand):
                key = ['id']
            diff = json.loads(column_atlas_native.diff_outputs(
                base, cand, json.dumps(dict(keys=key)),
            ))
            if key and diff.get('duplicate_keys') and not diff.get('complete') and \
                    not (keys and keys.get(block.uuid)):
                # An inferred key that is not unique: compared as multisets instead.
                diff = json.loads(column_atlas_native.diff_outputs(
                    base, cand, json.dumps(dict(keys=[])),
                ))
            same = (
                diff.get('complete') and not any(diff['schema'].values())
                and not diff.get('added') and not diff.get('removed')
                and not diff.get('changed')
            )
            outputs[name] = dict(result='same' if same else 'differs', diff=diff)
            continue
        try:
            expected = manager.get_variable(
                pipeline.uuid, block.uuid, name, partition=baseline_part, raise_exception=True,
            )
            actual = manager.get_variable(
                pipeline.uuid, block.uuid, name, partition=candidate_part, raise_exception=True,
            )
            result, detail = compare_values(expected, actual)
        except Exception as error:
            result, detail = 'not compared', f'Reading the outputs failed: {error}'
        outputs[name] = dict(result=result, detail=detail)
    return outputs


def create(pipeline, baseline_run=None, keys: Optional[Dict[str, List[str]]] = None,
           user: Optional[str] = None) -> Dict[str, Any]:
    """A queued preview of the pipeline's current code against a completed run."""
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    if baseline_run is None:
        baseline_run = PipelineRun.query.filter(
            PipelineRun.pipeline_uuid == pipeline.uuid,
            PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED,
        ).order_by(PipelineRun.id.desc()).first()
        if baseline_run is None:
            raise PreviewError(
                f'{pipeline.uuid} has no completed run to compare with; run it once first.'
            )
    elif baseline_run.status != PipelineRun.PipelineRunStatus.COMPLETED:
        raise PreviewError(f'Run {baseline_run.id} did not complete; choose a completed run.')
    preview = dict(
        id=uuid_lib.uuid4().hex[:16],
        pipeline_uuid=pipeline.uuid,
        baseline_run_id=baseline_run.id,
        baseline_partition=baseline_run.execution_partition,
        state='queued',
        created_at=_now(),
        created_by=user,
        keys=keys or {},
        blocks=[],
    )
    write(pipeline, preview)
    return preview


def run(pipeline, preview_id: str, log: Callable[[str], None] = print) -> Dict[str, Any]:
    """Runs a queued preview to its end; the preview's file says where it is."""
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.incidents import error_line

    preview = read(pipeline, preview_id)
    preview.update(state='running', started_at=_now())
    write(pipeline, preview)
    try:
        baseline = PipelineRun.get(preview['baseline_run_id'])
        changed, files = changes(pipeline, baseline)
        rows = plan(pipeline, changed)
        preview.update(changed_files=files, blocks=rows)
        write(pipeline, preview)
        base_part = baseline.execution_partition
        part = partition(preview_id)
        candidates = {r['block_uuid'] for r in rows if r['status'] == 'run'}
        # Every block's baseline outputs, linked: the candidates' inputs. A candidate's own
        # outputs are written over its link when it runs.
        base_dir = _partition_dir(pipeline, base_part)
        needed = {
            u.uuid for c in candidates for u in (pipeline.get_block(c).upstream_blocks or [])
        } - candidates
        missing = []
        for block in pipeline.blocks_by_uuid.values():
            folder = os.path.join(base_dir, clean_name(block.uuid))
            if os.path.isdir(folder):
                _link_tree(folder, os.path.join(_partition_dir(pipeline, part),
                                                clean_name(block.uuid)))
            elif block.uuid in needed:
                missing.append(block.uuid)
        if missing:
            raise PreviewError(
                f'The baseline run {baseline.id} has no stored outputs of '
                f'{", ".join(sorted(missing))}; run the pipeline again, then preview.'
            )
        variables = baseline.get_variables()
        baseline_block_runs = {b.block_uuid: b.id for b in baseline.block_runs}
        failed: Set[str] = set()
        for row in rows:
            if row['status'] != 'run':
                continue
            block = pipeline.get_block(row['block_uuid'])
            upstream_failed = [u.uuid for u in (block.upstream_blocks or []) if u.uuid in failed]
            if upstream_failed:
                row.update(status='not run', reason=f'{upstream_failed[0]} failed')
                failed.add(block.uuid)
                continue
            # The candidate's outputs, not the linked baseline's.
            shutil.rmtree(os.path.join(_partition_dir(pipeline, part), clean_name(block.uuid)),
                          ignore_errors=True)
            log(f'Running {block.uuid}: {row["reason"]}')
            try:
                block.execute_sync(
                    execution_partition=part,
                    global_vars=dict(variables),
                    run_all_blocks=True,
                    update_status=False,
                    analyze_outputs=False,
                    verify_output=False,
                )
            except Exception as error:
                text = ''.join(traceback.format_exception(error))
                row.update(status='failed', error=error_line(text) or str(error))
                failed.add(block.uuid)
                write(pipeline, preview)
                continue
            row.update(
                status='previewed',
                outputs=compare_outputs(pipeline, block, base_part, part, preview.get('keys')),
                # The run's outputs of the block, to open next to the preview's.
                baseline_block_run_id=baseline_block_runs.get(block.uuid),
            )
            write(pipeline, preview)
        preview.update(state='done', finished_at=_now())
    except Exception as error:
        preview.update(state='failed', finished_at=_now(), error=str(error))
    write(pipeline, preview)
    _prune(pipeline)
    return preview


def _prune(pipeline) -> None:
    """The pipeline's previews past the newest KEEP, with their outputs, removed."""
    for old in listed(pipeline)[KEEP:]:
        shutil.rmtree(_partition_dir(pipeline, partition(old['id'])), ignore_errors=True)
        try:
            os.remove(_path(pipeline, old['id']))
        except OSError:
            pass


def _describe(diff: Dict[str, Any]) -> List[str]:
    lines = []
    schema = diff.get('schema') or {}
    parts = [f"+ {c['name']} ({c['type']})" for c in schema.get('added') or []]
    parts += [f"- {c['name']} ({c['type']})" for c in schema.get('removed') or []]
    parts += [f"~ {c['name']} {c['from']} -> {c['to']}" for c in schema.get('changed') or []]
    if parts:
        lines.append('schema: ' + ', '.join(parts))
    if diff.get('duplicate_keys') and not diff.get('complete'):
        dup = diff['duplicate_keys']
        lines.append(
            f"{', '.join(diff['keys'])} is not unique (baseline {dup['baseline']}, "
            f"candidate {dup['candidate']} repeated keys); rows not compared"
        )
        return lines
    rows = diff.get('rows') or {}
    if diff.get('keys'):
        columns = ', '.join(f'{c} {n}' for c, n in (diff.get('columns') or {}).items())
        lines.append(
            f"{diff['added']} added, {diff['removed']} removed, {diff['changed']} changed"
            + (f' ({columns})' if columns else '')
            + f", {diff['unchanged']} unchanged, by {', '.join(diff['keys'])}"
        )
    elif diff.get('complete'):
        lines.append(
            f"{diff['added']} rows only in the candidate, {diff['removed']} only in the "
            f"baseline (no key: compared as sets of rows)"
        )
    lines.append(f"rows: {rows.get('baseline')} before, {rows.get('candidate')} now")
    return lines


def format_preview(preview: Dict[str, Any]) -> str:
    """The preview as text, a line or more per block."""
    head = (
        f"Preview of {preview['pipeline_uuid']} against run {preview['baseline_run_id']}: "
        f"{preview['state']}"
    )
    lines = [head]
    if preview.get('error'):
        lines.append(f"  {preview['error']}")
    width = max([len(b['block_uuid']) for b in preview.get('blocks') or []] + [0])
    for block in preview.get('blocks') or []:
        status = block['status']
        reason = block.get('error') or block.get('reason') or ''
        lines.append(
            f"  {block['block_uuid']:<{width}}  {status}" + (f': {reason}' if reason else '')
        )
        for name, output in (block.get('outputs') or {}).items():
            details = _describe(output['diff']) if output.get('diff') else [
                f"{output['result']}" + (f": {output['detail']}" if output.get('detail') else '')
            ]
            lines.append(f"  {'':<{width}}    {name}: {details[0]}")
            lines.extend(f"  {'':<{width}}      {d}" for d in details[1:])
    return '\n'.join(lines)


def start(pipeline, preview_id: str) -> subprocess.Popen:
    """Runs the preview in a process of its own; its file says where it is."""
    log_path = os.path.join(_folder(pipeline), f'{preview_id}.log')
    with open(log_path, 'a') as log:
        return subprocess.Popen(
            [sys.executable, '-m', 'mage_ai.orchestration.previews', pipeline.repo_path,
             pipeline.uuid, preview_id],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )


def main(argv: List[str]) -> int:
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.settings.repo import set_repo_path

    repo_path, pipeline_uuid, preview_id = argv
    set_repo_path(repo_path)
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
        sys.path.insert(0, os.path.dirname(repo_path))
    from mage_ai.orchestration.db import db_connection

    db_connection.start_session()
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    preview = run(pipeline, preview_id)
    return 0 if preview['state'] == 'done' else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
