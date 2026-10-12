"""
Guards for the compilation study's runs, so that measuring never overloads the machine.

Before each test: enough free memory and disk, and the CPU package and the GPU below their
temperature limits. A hot machine pauses the study until it cools; a test that still lacks
room is skipped with the reason. Thread pools of the libraries blocks use are capped, and
scripts/study/run.sh runs the study in a systemd scope with a memory and CPU limit, where a
runaway case is killed in its own cgroup.

MAGE_STUDY_MIN_FREE_GB (8), MAGE_STUDY_MIN_DISK_GB (10), MAGE_STUDY_CPU_MAX_C (85),
MAGE_STUDY_GPU_MAX_C (80), MAGE_STUDY_COOL_WAIT_S (600) and MAGE_STUDY_THREADS (8) change
the limits.
"""
import os
import shutil
import subprocess
import time
from typing import Optional

import pytest

THREADS = os.getenv('MAGE_STUDY_THREADS', '8')
# Thread pools of Polars, Rust (rayon), NumPy and ONNX Runtime; the processes a test starts
# inherit them. Set for the study's tests only, not for the rest of a session.
THREAD_VARIABLES = ('POLARS_MAX_THREADS', 'RAYON_NUM_THREADS', 'OMP_NUM_THREADS',
                    'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS')

MIN_FREE_GB = float(os.getenv('MAGE_STUDY_MIN_FREE_GB', '8'))
MIN_DISK_GB = float(os.getenv('MAGE_STUDY_MIN_DISK_GB', '10'))
CPU_MAX_C = float(os.getenv('MAGE_STUDY_CPU_MAX_C', '85'))
GPU_MAX_C = float(os.getenv('MAGE_STUDY_GPU_MAX_C', '80'))
COOL_WAIT_S = float(os.getenv('MAGE_STUDY_COOL_WAIT_S', '600'))

# The model experiments need the ML libraries and the r_ml environment, which a default
# integration run does not have.
collect_ignore = [] if os.getenv('MAGE_STUDY_FULL') == '1' else ['test_ml_models.py']


def cpu_temperature() -> Optional[float]:
    try:
        import psutil

        sensors = psutil.sensors_temperatures()
    except Exception:
        return None
    for entry in sensors.get('coretemp', []) + sensors.get('k10temp', []):
        if entry.label in ('Package id 0', 'Tctl', 'Tdie'):
            return entry.current
    values = [e.current for entries in sensors.values() for e in entries if e.current]
    return max(values) if values else None


def gpu_temperature() -> Optional[float]:
    if not shutil.which('nvidia-smi'):
        return None
    try:
        out = subprocess.run(
            ['nvidia-smi', '--query-gpu=temperature.gpu', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=10,
        ).stdout
        return max(float(v) for v in out.split()) if out.strip() else None
    except Exception:
        return None


def free_memory_gb() -> float:
    import psutil

    return psutil.virtual_memory().available / 1e9


def machine_state() -> dict:
    return dict(cpu_c=cpu_temperature(), gpu_c=gpu_temperature(),
                free_gb=round(free_memory_gb(), 1),
                disk_gb=round(shutil.disk_usage(os.getcwd()).free / 1e9, 1))


def too_hot(state: dict) -> Optional[str]:
    if state['cpu_c'] is not None and state['cpu_c'] > CPU_MAX_C:
        return f"CPU at {state['cpu_c']} C (limit {CPU_MAX_C})"
    if state['gpu_c'] is not None and state['gpu_c'] > GPU_MAX_C:
        return f"GPU at {state['gpu_c']} C (limit {GPU_MAX_C})"
    return None


@pytest.fixture(autouse=True)
def machine_guard(monkeypatch):
    for name in THREAD_VARIABLES:
        if name not in os.environ:
            monkeypatch.setenv(name, THREADS)
    deadline = time.monotonic() + COOL_WAIT_S
    state = machine_state()
    while (too_hot(state) or state['free_gb'] < MIN_FREE_GB) and time.monotonic() < deadline:
        time.sleep(10)
        state = machine_state()
    reason = too_hot(state)
    if reason:
        pytest.skip(f'The machine did not cool down: {reason}')
    if state['free_gb'] < MIN_FREE_GB:
        pytest.skip(f"Only {state['free_gb']} GB of memory free (need {MIN_FREE_GB})")
    if state['disk_gb'] < MIN_DISK_GB:
        pytest.skip(f"Only {state['disk_gb']} GB of disk free (need {MIN_DISK_GB})")
    yield state
