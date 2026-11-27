"""
Performance advice from measured runs: what the pipeline's last completed runs say about
where its time and data go. Each advice names the measurement it comes from, so it can be
checked, and none is given without one.

- A block that holds most of the run's time: the place to work first.
- A switch between languages that moves a lot of data each run: each switch converts the
  data (Arrow files between Python, R and Rust), so one language there, or the later block
  reading the source itself, skips it.
- A slow block whose output is the same, byte for byte, run after run (the outputs' SHA-256
  that run records keep): its work could be skipped while its source does not change.
- A block whose measured memory peak nears the memory it declares (resources.py): the next
  larger input fails; or one that declares far more than it uses, which holds memory other
  block runs wait for.

The numbers are medians over the last completed runs (RUNS), so one slow run does not make
advice.
"""
import statistics
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

RUNS = 10
# Advice needs at least this many runs.
MIN_RUNS = 3
# A block holding this share of a run of at least BOTTLENECK_MIN_SECONDS.
BOTTLENECK_SHARE = 0.5
BOTTLENECK_MIN_SECONDS = 10.0
# A language switch passing at least this much a run.
SWITCH_MIN_BYTES = 100 * 1024 * 1024
# A block taking at least this long with the same output in every run looked at.
REPEATED_MIN_SECONDS = 30.0
CACHE_SECONDS = 60
# A peak at least this share of the declared memory; a declaration at least this many times
# the peak, of at least MEMORY_UNUSED_MIN_BYTES.
MEMORY_NEAR_SHARE = 0.9
MEMORY_UNUSED_FACTOR = 4
MEMORY_UNUSED_MIN_BYTES = 2 * 1024**3


@dataclass
class BlockMeasure:
    seconds: float
    output_bytes: int = 0
    # The SHA-256 of its outputs, joined; None when not recorded.
    digest: Optional[str] = None
    # Its process tree's memory peak (track_memory_peak); 0 when not measured.
    memory_peak: int = 0


@dataclass
class RunMeasure:
    seconds: float
    blocks: Dict[str, BlockMeasure] = field(default_factory=dict)


def _size(n: float) -> str:
    for unit in ('bytes', 'KB', 'MB', 'GB'):
        if n < 1024 or unit == 'GB':
            return f'{n:.0f} {unit}' if unit == 'bytes' else f'{n:.1f} {unit}'
        n /= 1024
    return ''


def _duration(seconds: float) -> str:
    if seconds < 60:
        return f'{seconds:.1f} s'
    minutes, rest = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f'{minutes}m {rest:02d}s'
    hours, minutes = divmod(minutes, 60)
    return f'{hours}h {minutes:02d}m'


def advise(
    runs: List[RunMeasure],
    languages: Dict[str, str],
    edges: List[Tuple[str, str]],
    declared_memory: Optional[Dict[str, int]] = None,
) -> List[Dict]:
    """The advice the runs (newest first) give, most important first."""
    if len(runs) < MIN_RUNS:
        return []
    count = len(runs)
    measured = f'median of the last {count} completed runs'
    advice = []

    run_seconds = statistics.median(r.seconds for r in runs)
    blocks = sorted({uuid for r in runs for uuid in r.blocks})

    def median(uuid: str, attribute: str) -> float:
        values = [getattr(r.blocks[uuid], attribute) for r in runs if uuid in r.blocks]
        return statistics.median(values) if values else 0.0

    if run_seconds >= BOTTLENECK_MIN_SECONDS:
        for uuid in blocks:
            seconds = median(uuid, 'seconds')
            if seconds >= BOTTLENECK_SHARE * run_seconds:
                share = round(100 * seconds / run_seconds)
                advice.append(dict(
                    kind='bottleneck',
                    blocks=[uuid],
                    message=(
                        f'{uuid} holds {share}% of the run\'s time ({_duration(seconds)} of '
                        f'{_duration(run_seconds)}, {measured}): work on it first.'
                    ),
                    measurement=dict(block_seconds=seconds, run_seconds=run_seconds,
                                     runs=count),
                ))

    for upstream, downstream in edges:
        source, target = languages.get(upstream), languages.get(downstream)
        if not source or not target or source == target or source == 'sql' \
                or target == 'sql':
            continue
        passed = median(upstream, 'output_bytes')
        if passed >= SWITCH_MIN_BYTES:
            advice.append(dict(
                kind='language_switch',
                blocks=[upstream, downstream],
                message=(
                    f'The switch from {source.capitalize()} ({upstream}) to '
                    f'{target.capitalize()} ({downstream}) passes {_size(passed)} a run '
                    f'({measured}). One language for both, or {downstream} reading the '
                    f'source itself, skips converting it.'
                ),
                measurement=dict(output_bytes=passed, runs=count),
            ))

    for uuid in blocks:
        digests = [r.blocks[uuid].digest for r in runs if uuid in r.blocks]
        seconds = median(uuid, 'seconds')
        if len(digests) == count and digests[0] and len(set(digests)) == 1 \
                and seconds >= REPEATED_MIN_SECONDS:
            advice.append(dict(
                kind='repeated_output',
                blocks=[uuid],
                message=(
                    f'{uuid} gave the same output in each of the last {count} completed runs '
                    f'(the same SHA-256), taking {_duration(seconds)} each (median). While '
                    f'its source does not change, its work could be skipped: a trigger on '
                    f'the source\'s changes, or an upstream condition.'
                ),
                measurement=dict(block_seconds=seconds, runs=count, sha256=digests[0]),
            ))
    for uuid, declared in (declared_memory or {}).items():
        peaks = [r.blocks[uuid].memory_peak for r in runs
                 if uuid in r.blocks and r.blocks[uuid].memory_peak]
        if not declared or len(peaks) < MIN_RUNS:
            continue
        peak = statistics.median(peaks)
        if peak >= MEMORY_NEAR_SHARE * declared:
            advice.append(dict(
                kind='memory_near_limit',
                blocks=[uuid],
                message=(
                    f'{uuid} peaks at {_size(peak)} of the {_size(declared)} it declares '
                    f'({round(100 * peak / declared)}%, median of the last {len(peaks)} '
                    f'measured runs): a larger input runs it out of memory. Declare more.'
                ),
                measurement=dict(memory_peak=peak, declared=declared, runs=len(peaks)),
            ))
        elif declared >= MEMORY_UNUSED_MIN_BYTES and declared >= MEMORY_UNUSED_FACTOR * peak:
            advice.append(dict(
                kind='memory_unused',
                blocks=[uuid],
                message=(
                    f'{uuid} declares {_size(declared)} and peaks at {_size(peak)} (median '
                    f'of the last {len(peaks)} measured runs): the rest is held from other '
                    f'block runs, which wait for it. Declaring less lets them start.'
                ),
                measurement=dict(memory_peak=peak, declared=declared, runs=len(peaks)),
            ))
    order = dict(bottleneck=0, memory_near_limit=1, language_switch=2, repeated_output=3,
                 memory_unused=4)
    return sorted(advice, key=lambda a: order[a['kind']])


def measures(pipeline) -> List[RunMeasure]:
    """The pipeline's last completed runs, newest first, from the run history."""
    from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun

    runs = (
        PipelineRun.query.filter(
            PipelineRun.pipeline_uuid == pipeline.uuid,
            PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED,
            PipelineRun.started_at.isnot(None),
            PipelineRun.completed_at.isnot(None),
        )
        .order_by(PipelineRun.id.desc())
        .limit(RUNS)
        .all()
    )
    if not runs:
        return []
    block_runs = BlockRun.query.filter(
        BlockRun.pipeline_run_id.in_([r.id for r in runs]),
        BlockRun.status == BlockRun.BlockRunStatus.COMPLETED,
    ).all()
    by_run: Dict[int, Dict[str, BlockMeasure]] = {}
    for block_run in block_runs:
        if not block_run.started_at or not block_run.completed_at:
            continue
        outputs = (block_run.metrics or {}).get('outputs') or {}
        digest = '|'.join(
            f'{name}:{outputs[name].get("sha256")}' for name in sorted(outputs)
        ) if outputs else None
        by_run.setdefault(block_run.pipeline_run_id, {})[block_run.block_uuid] = BlockMeasure(
            seconds=(block_run.completed_at - block_run.started_at).total_seconds(),
            output_bytes=sum(int(o.get('bytes') or 0) for o in outputs.values()),
            digest=digest,
            memory_peak=int((block_run.metrics or {}).get('memory_peak') or 0),
        )
    return [
        RunMeasure(
            seconds=(r.completed_at - r.started_at).total_seconds(),
            blocks=by_run.get(r.id, {}),
        )
        for r in runs
    ]


_cache: Dict[str, Tuple[float, List[Dict]]] = {}


def advice_for(pipeline) -> List[Dict]:
    """The pipeline's advice, kept a minute: the compile status is polled."""
    cached = _cache.get(pipeline.uuid)
    if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]
    blocks = pipeline.blocks_by_uuid
    languages = {
        uuid: str(getattr(b.language, 'value', b.language)) for uuid, b in blocks.items()
    }
    edges = [
        (uuid, down) for uuid, b in blocks.items()
        for down in (b.downstream_block_uuids or []) if down in blocks
    ]
    from mage_ai.orchestration import resources

    declared = {}
    for uuid, block in blocks.items():
        try:
            declared[uuid] = resources.request_of(block).memory
        except Exception:
            continue
    found = advise(measures(pipeline), languages, edges, declared)
    _cache[pipeline.uuid] = (time.monotonic(), found)
    return found
