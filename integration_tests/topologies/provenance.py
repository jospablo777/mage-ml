"""
Where the compilation study's measurements come from, kept with them.

Each pytest session is one study session with an id (UTC time and a random suffix). Its
results go to MAGE_STUDY_RESULTS_DIR as <experiment>-<session>.jsonl, appended one line per
run and never rewritten, next to <session>.meta.json: the commit and whether the tree had
changes, the machine, the CPU governor, the limits the study ran under, and the versions of
the languages and libraries. Every line repeats the session id and has the state of the
machine just before the run: temperatures, load and free memory.

Without MAGE_STUDY_RESULTS_DIR nothing is written: the tests still run and check.
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[2]
SESSION = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:6]
PACKAGES = ('pandas', 'polars', 'pyarrow', 'numpy', 'scikit-learn', 'onnxruntime',
            'onnxruntime-gpu', 'skl2onnx', 'catboost', 'scipy')
_written_meta = False


def _run(command, timeout=20) -> str:
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout,
                              cwd=ROOT).stdout.strip()
    except Exception:
        return ''


def _read(path: str) -> Optional[str]:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def _cgroup_limits() -> Dict[str, Optional[str]]:
    own = _read('/proc/self/cgroup') or ''
    group = own.split('::')[-1] if '::' in own else ''
    base = f'/sys/fs/cgroup{group}'
    return {name: _read(f'{base}/{name}') for name in ('memory.max', 'memory.swap.max',
                                                       'cpu.max')}


def session_metadata() -> Dict:
    versions = {}
    for name in PACKAGES:
        try:
            versions[name] = version(name)
        except PackageNotFoundError:
            pass
    return dict(
        session=SESSION,
        started_utc=datetime.now(timezone.utc).isoformat(),
        commit=_run(['git', 'rev-parse', 'HEAD']),
        branch=_run(['git', 'rev-parse', '--abbrev-ref', 'HEAD']),
        dirty_files=[line for line in _run(['git', 'status', '--porcelain']).splitlines()
                     if not line.startswith('??')],
        host=platform.node(),
        os=_run(['sh', '-c', '. /etc/os-release && echo $PRETTY_NAME']) or platform.platform(),
        kernel=platform.release(),
        machine=platform.machine(),
        cpu=_run(['sh', '-c', "lscpu | sed -n 's/^Model name: *//p'"]),
        cpus=os.cpu_count(),
        memory_gb=round(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1e9, 1),
        gpu=_run(['nvidia-smi', '--query-gpu=name,driver_version,memory.total',
                  '--format=csv,noheader'])
        if os.getenv('MAGE_STUDY_GPU') == '1' and shutil.which('nvidia-smi') else None,
        governor=_read('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'),
        ptrace_scope=_read('/proc/sys/kernel/yama/ptrace_scope'),
        cgroup=_cgroup_limits(),
        threads={k: os.environ.get(k) for k in ('POLARS_MAX_THREADS', 'RAYON_NUM_THREADS',
                                                'OMP_NUM_THREADS', 'MAGE_STUDY_THREADS')},
        study={k: v for k, v in os.environ.items() if k.startswith('MAGE_STUDY_')},
        python=platform.python_version(),
        rustc=_run(['rustc', '--version']) if shutil.which('rustc') else None,
        r=(_run(['R', '--version']).splitlines() or [None])[0] if shutil.which('R') else None,
        packages=versions,
    )


def machine_snapshot() -> Dict:
    from integration_tests.topologies.conftest import cpu_temperature, gpu_temperature

    load1, load5, _ = os.getloadavg()
    try:
        import psutil

        free = round(psutil.virtual_memory().available / 1e9, 2)
    except Exception:
        free = None
    return dict(utc=datetime.now(timezone.utc).isoformat(), cpu_c=cpu_temperature(),
                gpu_c=gpu_temperature(), load1=round(load1, 2), load5=round(load5, 2),
                free_gb=free)


def wait_quiet(max_load_per_cpu: float = None, timeout: float = 300) -> Dict:
    """
    Waits until the 1-minute load per CPU is below the limit; returns the snapshot. Only full
    study runs wait: a smoke run checks outputs, and on CI it waited 300 s before every run.
    """
    if os.getenv('MAGE_STUDY_FULL') != '1':
        timeout = 0
    limit = max_load_per_cpu if max_load_per_cpu is not None else float(
        os.getenv('MAGE_STUDY_MAX_LOAD', '0.25'))
    deadline = time.monotonic() + timeout
    snapshot = machine_snapshot()
    while snapshot['load1'] / (os.cpu_count() or 1) > limit and time.monotonic() < deadline:
        time.sleep(5)
        snapshot = machine_snapshot()
    snapshot['quiet'] = snapshot['load1'] / (os.cpu_count() or 1) <= limit
    return snapshot


def record(experiment: str, entry: Dict, before: Optional[Dict] = None):
    """Appends one run to <experiment>-<session>.jsonl, with the session and the machine."""
    global _written_meta
    folder = os.getenv('MAGE_STUDY_RESULTS_DIR')
    if not folder:
        return
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    if not _written_meta:
        (folder / f'{SESSION}.meta.json').write_text(
            json.dumps(session_metadata(), indent=1, default=str) + '\n')
        _written_meta = True
    line = dict(entry, experiment=experiment, session=SESSION, before=before,
                after=machine_snapshot())
    with open(folder / f'{experiment}-{SESSION}.jsonl', 'a') as file:
        file.write(json.dumps(line, default=str) + '\n')
