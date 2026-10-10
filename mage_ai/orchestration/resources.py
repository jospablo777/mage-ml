"""
Resource-aware scheduling: a block declares the memory, CPUs, GPUs and shared limits it
needs, and the scheduler starts its block run only when they are free.

The project declares what there is, in its metadata.yaml:

    resources:
      limits:            # shared across every pipeline of the project
        warehouse: 4     # at most 4 block runs that use the warehouse at a time
        gpu_host: 1
      memory: 48GiB      # for the block runs of one scheduler; 80% of its RAM by default
      gpus: [0, 1]       # devices block runs get one by one (or a count: 2)

and a block, in its configuration (block settings):

    resources:
      memory: 8GiB
      cpu: 2             # thread pools of NumPy, BLAS, Polars and Rayon get 2 threads
      gpu: 1             # CUDA_VISIBLE_DEVICES holds one device of its own
      uses: [warehouse]

What block runs hold is recorded with them (block_run.metrics['resources']) when they are
queued, so the accounting is the queued and running block runs in the database: a block
run that ends, fails, is cancelled or reset by crash detection stops holding without a
release, and a scheduler restart loses nothing. Memory and GPUs are per scheduler process
(they are the host's); shared limits count every scheduler of the project. A block run
that waits records why (block_run.metrics['waiting']).
"""
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

THREAD_VARIABLES = (
    'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_MAX_THREADS',
    'POLARS_MAX_THREADS', 'RAYON_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS',
)
_UNITS = {
    '': 1, 'b': 1,
    'k': 1000, 'kb': 1000, 'm': 1000**2, 'mb': 1000**2, 'g': 1000**3, 'gb': 1000**3,
    't': 1000**4, 'tb': 1000**4,
    'ki': 1024, 'kib': 1024, 'mi': 1024**2, 'mib': 1024**2, 'gi': 1024**3, 'gib': 1024**3,
    'ti': 1024**4, 'tib': 1024**4,
}
_KEY = re.compile(r'^[A-Za-z0-9][\w.\-]{0,99}$')
_REQUEST_KEYS = {'memory', 'cpu', 'gpu', 'uses'}
_POLICY_KEYS = {'limits', 'memory', 'gpus'}


class ResourceError(Exception):
    pass


def parse_bytes(value: Any, what: str) -> int:
    """8GiB, 512MB, 1.5G or a number of bytes."""
    if isinstance(value, bool):
        raise ResourceError(f'{what} must be a size such as 8GiB.')
    if isinstance(value, (int, float)):
        number, unit = float(value), ''
    else:
        match = re.fullmatch(r'\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*', str(value or ''))
        if not match or match.group(2).lower() not in _UNITS:
            raise ResourceError(f'{what} must be a size such as 8GiB or 512MB, not {value!r}.')
        number, unit = float(match.group(1)), match.group(2).lower()
    size = int(number * _UNITS[unit])
    if size <= 0:
        raise ResourceError(f'{what} must be more than 0.')
    return size


def format_bytes(size: int) -> str:
    for unit, factor in (('GiB', 1024**3), ('MiB', 1024**2), ('KiB', 1024)):
        if size >= factor:
            return f'{size / factor:.1f} {unit}'
    return f'{size} B'


@dataclass(frozen=True)
class Request:
    memory: int = 0
    cpu: Optional[int] = None
    gpu: int = 0
    uses: Tuple[str, ...] = ()

    @property
    def empty(self) -> bool:
        return not (self.memory or self.cpu or self.gpu or self.uses)


@dataclass
class Policy:
    limits: Dict[str, int] = field(default_factory=dict)
    memory: int = 0
    gpus: List[str] = field(default_factory=list)


def request_of(block) -> Request:
    """What the block declares it needs; an empty request when it declares nothing."""
    config = (getattr(block, 'configuration', None) or {}).get('resources')
    if not config:
        return Request()
    where = f'The resources of block {block.uuid}'
    if not isinstance(config, dict):
        raise ResourceError(f'{where} must be a mapping of memory, cpu, gpu and uses.')
    unknown = set(config) - _REQUEST_KEYS
    if unknown:
        raise ResourceError(
            f'{where} have unknown settings: {", ".join(sorted(unknown))}; the settings are '
            'memory, cpu, gpu and uses.'
        )
    memory = parse_bytes(config['memory'], f'{where}: memory') if config.get('memory') else 0
    cpu = config.get('cpu')
    if cpu is not None and (isinstance(cpu, bool) or not isinstance(cpu, int) or cpu < 1):
        raise ResourceError(f'{where}: cpu must be a whole number of CPUs, 1 or more.')
    gpu = config.get('gpu') or 0
    if isinstance(gpu, bool) or not isinstance(gpu, int) or gpu < 0:
        raise ResourceError(f'{where}: gpu must be a whole number of devices.')
    uses = config.get('uses') or []
    if isinstance(uses, str):
        uses = [uses]
    if not isinstance(uses, list) or not all(isinstance(u, str) and _KEY.match(u) for u in uses):
        raise ResourceError(f'{where}: uses must be a list of shared limit names.')
    return Request(memory=memory, cpu=cpu, gpu=gpu, uses=tuple(dict.fromkeys(uses)))


def _host_memory() -> int:
    try:
        import psutil

        return int(psutil.virtual_memory().total * 0.8)
    except Exception:
        return 0


def policy_of(repo_config) -> Policy:
    """The resources the project declares, from its metadata.yaml."""
    config = (repo_config or {}).get('resources') if isinstance(repo_config, dict) else (
        getattr(repo_config, 'resources', None)
    )
    config = config or {}
    if not isinstance(config, dict):
        raise ResourceError('The resources of the project must be a mapping.')
    unknown = set(config) - _POLICY_KEYS
    if unknown:
        raise ResourceError(
            f'The resources of the project have unknown settings: {", ".join(sorted(unknown))}.'
        )
    limits = {}
    for key, limit in (config.get('limits') or {}).items():
        if not _KEY.match(str(key)):
            raise ResourceError(f'The shared limit name {key!r} is not valid.')
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ResourceError(f'The shared limit {key} must be a whole number, 1 or more.')
        limits[str(key)] = limit
    memory = parse_bytes(config['memory'], 'The memory of the project') if config.get(
        'memory',
    ) else _host_memory()
    gpus = config.get('gpus') or []
    if isinstance(gpus, int) and not isinstance(gpus, bool):
        gpus = list(range(gpus))
    if not isinstance(gpus, list):
        raise ResourceError('The gpus of the project must be a list of devices or a count.')
    return Policy(limits=limits, memory=memory, gpus=[str(g) for g in gpus])


@dataclass
class Decision:
    admitted: bool
    reason: Optional[str] = None
    held: Optional[Dict[str, Any]] = None
    # The request can never fit: the block run fails with this reason.
    impossible: bool = False


def held_by(active_block_runs) -> List[Dict[str, Any]]:
    return [
        (b.metrics or {}).get('resources') for b in active_block_runs
        if (b.metrics or {}).get('resources')
    ]


def decide(
    request: Request,
    policy: Policy,
    held: List[Dict[str, Any]],
    project: str,
    launcher: Optional[str],
) -> Decision:
    """Whether the request fits next to what the active block runs hold."""
    mine = [h for h in held if h.get('project') == project]
    for key in request.uses:
        if key not in policy.limits:
            return Decision(False, (
                f'the shared limit {key} is not declared in the project\'s resources'
            ), impossible=True)
        in_use = sum(1 for h in mine if key in (h.get('uses') or []))
        if in_use >= policy.limits[key]:
            return Decision(False, f'waiting for {key}: {in_use} of {policy.limits[key]} in use')
    local = [h for h in mine if h.get('launched_by') == launcher]
    if request.memory:
        if policy.memory and request.memory > policy.memory:
            return Decision(False, (
                f'it needs {format_bytes(request.memory)} of memory and the scheduler has '
                f'{format_bytes(policy.memory)} for block runs'
            ), impossible=True)
        used = sum(int(h.get('memory') or 0) for h in local)
        if policy.memory and used + request.memory > policy.memory:
            return Decision(False, (
                f'waiting for memory: needs {format_bytes(request.memory)}, '
                f'{format_bytes(policy.memory - used)} of {format_bytes(policy.memory)} free'
            ))
    gpus: List[str] = []
    if request.gpu:
        if request.gpu > len(policy.gpus):
            return Decision(False, (
                f'it needs {request.gpu} GPU(s) and the project declares {len(policy.gpus)}'
            ), impossible=True)
        taken = {g for h in local for g in (h.get('gpus') or [])}
        free = [g for g in policy.gpus if g not in taken]
        if len(free) < request.gpu:
            return Decision(False, (
                f'waiting for GPUs: needs {request.gpu}, {len(free)} of {len(policy.gpus)} free'
            ))
        gpus = free[:request.gpu]
    return Decision(True, held=dict(
        project=project,
        launched_by=launcher,
        memory=request.memory,
        cpu=request.cpu,
        gpus=gpus,
        uses=list(request.uses),
    ))


def environment_for(held: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Environment variables that keep a block run inside what it holds."""
    env: Dict[str, str] = {}
    if not held:
        return env
    if held.get('cpu'):
        for name in THREAD_VARIABLES:
            if not os.environ.get(name):
                env[name] = str(held['cpu'])
    if held.get('gpus'):
        env['CUDA_VISIBLE_DEVICES'] = ','.join(held['gpus'])
    return env


def apply_environment(held: Optional[Dict[str, Any]]) -> None:
    os.environ.update(environment_for(held))
