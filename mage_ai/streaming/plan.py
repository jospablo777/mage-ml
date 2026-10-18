"""
How a stream runs on the engine, for the editor's compile panel (ADR-0035): the source and the
positions a development run reads with, the runs of blocks in one language (each runs in one
process per batch), the boundaries between runs where messages cross as JSON, the sinks with
the block each follows, the batch settings, and the problems that would stop the stream before
Start. The plan comes from the export of the pipeline, as the engine reads it.
"""
import copy
import threading
from typing import Dict, List, Tuple

RUNS_IN = dict(
    python='one call to the Python worker per batch',
    rust='one Rust program, kept running',
    r='one R session, kept running',
)

_cache: Dict[Tuple[str, str], Dict] = {}
_lock = threading.Lock()


def _runs(blocks: List[Dict]) -> List[Dict]:
    """Consecutive blocks of one language, as the engine chains them."""
    runs: List[Dict] = []
    for block in blocks:
        language = str(block.get('language') or 'python')
        if runs and runs[-1]['language'] == language:
            runs[-1]['blocks'].append(block['uuid'])
        else:
            runs.append(dict(language=language, blocks=[block['uuid']],
                             runs_in=RUNS_IN.get(language, 'its own process')))
    return runs


def stream_plan(pipeline) -> Dict:
    """The plan of a streaming pipeline, kept until its code changes."""
    from mage_ai.orchestration.compilation import code_fingerprint

    key = (f'{pipeline.repo_path}:{pipeline.uuid}', code_fingerprint(pipeline))
    with _lock:
        if key in _cache:
            return _cache[key]
    plan = _build(pipeline)
    with _lock:
        _cache[key] = plan
    return plan


def _build(pipeline) -> Dict:
    from mage_ai.pipeline_services import export
    from mage_ai.streaming.development import for_development_manifest

    try:
        captured = export.capture(pipeline.repo_path, [pipeline.uuid])
    except export.ExportError as error:
        return dict(problems=list(error.problems))
    manifest = captured.manifest
    [found] = [p for p in manifest['pipelines'] if p.get('uuid') == pipeline.uuid] or [{}]
    stream = found.get('stream') or {}
    source = stream.get('source') or {}
    development = for_development_manifest(copy.deepcopy(manifest))
    runs = _runs(found.get('blocks') or [])
    dead_letter = (stream.get('errors') or {}).get('dead_letter')
    return dict(
        source=dict(block=source.get('name'), kind=source.get('kind')),
        development=[note.split(': ', 1)[-1] for note in development],
        runs=runs,
        boundaries=max(0, len(runs) - 1),
        sinks=[dict(block=s.get('name'), kind=s.get('kind'),
                    after=s.get('after') or 'the source') for s in stream.get('sinks') or []],
        dead_letter=dict(block=dead_letter.get('name'), kind=dead_letter.get('kind'))
        if dead_letter else None,
        batch=stream.get('batch') or {},
        problems=[],
    )
