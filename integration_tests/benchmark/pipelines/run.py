"""
Benchmark of whole pipelines, run as in production, outside the editor:

- cli: `mage run <project> <pipeline>`, a new process that runs every block in it;
- scheduler: Mage's scheduler, each block run in its own process, as on a worker;
- fused: the scheduler with block_fusion: chains, each chain of blocks in one process;
- warm: the same, with the jobs on the Rust run engine's warm workers (MAGE_JOB_ENGINE);
- service: the pipeline exported with `mage export service`, run by mage-service.

Every pipeline runs the workload of workload.py: pandas, Polars, R (dplyr) and Rust
versions, and the staggered series that replaces pandas blocks with Rust one at a time.

    uv run python -m integration_tests.benchmark.pipelines \\
        --rows 100000 1000000 --repeats 5 --out integration_tests/benchmark/results/run

A warm-up round runs every pipeline once (Rust compiles, R loads its packages, files
enter the OS cache) and is not counted. The measured rounds then run every combination
once per round, in an order shuffled with a fixed seed, so drift (heat, background load)
spreads over all of them. Each run is checked: the summary it writes must equal the
pandas pipeline's. Results go to <out>/results.json and the report to <out>/report.html.
"""
import argparse
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import textwrap
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import psutil

from integration_tests.benchmark.pipelines import workload

REPO = Path(__file__).resolve().parents[3]
SERVICE_SOURCE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
SERVICE_BINARY = SERVICE_SOURCE / 'target' / 'release' / 'mage-service'
R_PROJECT = REPO / 'integration_tests' / 'r_env'
MODES = ('cli', 'scheduler', 'fused', 'warm', 'service')
SAMPLE_SECONDS = 0.02


def environment(out: Path) -> Dict[str, str]:
    """The environment of every process the benchmark starts."""
    env = dict(os.environ)
    env.pop('ENV', None)
    env.update(
        MAGE_DATABASE_CONNECTION_URL=f'sqlite:///{out / "mage.db"}',
        MAGE_DATA_DIR=str(out / 'mage_data'),
        MAGE_R_PROJECT_DIR=str(R_PROJECT),
        MAGE_R_CACHE_DIR=str(out / 'r_cache'),
        PYTHONPATH=os.pathsep.join(filter(None, [str(REPO), env.get('PYTHONPATH')])),
        MLFLOW_DISABLE_AGENT_HINT='1',
    )
    return env


# ---- preparation ----------------------------------------------------------------------

PREPARE = '''
import sys
from pathlib import Path

from mage_ai.data_preparation.repo_manager import init_repo, set_repo_path
from integration_tests.benchmark.pipelines import workload

root = sys.argv[1]
if not Path(root, 'metadata.yaml').is_file():
    init_repo(root)
set_repo_path(root)

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.block_factory import BlockFactory
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.database_manager import database_manager

import logging
database_manager.run_migrations(log_level=logging.ERROR)
for name, languages in workload.pipelines().items():
    if Path(root, 'pipelines', name).is_dir():
        continue
    pipeline = Pipeline.create(name, repo_path=root)
    upstream = []
    for uuid, block_type, language, code in workload.block_files(languages):
        extension = {'python': 'py', 'r': 'r', 'rust': 'rs'}[language]
        folder = {'data_loader': 'data_loaders', 'transformer': 'transformers',
                  'data_exporter': 'data_exporters'}[block_type]
        path = Path(root, folder, f'{uuid}.{extension}')
        if path.is_file():
            block = BlockFactory.get_block(uuid, uuid, block_type, language=language)
        else:
            block = Block.create(uuid, block_type, root, language=language)
        path.write_text(code)
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        upstream = [block]
print('prepared')
'''


def prepare(out: Path, sizes: List[int], modes: List[str], env: Dict[str, str]) -> Dict:
    inputs = {rows: workload.write_inputs(out / 'data' / str(rows), rows) for rows in sizes}
    project = out / 'project'
    subprocess.run(
        [sys.executable, '-c', PREPARE, str(project)], env=env, check=True, cwd=out,
    )
    services = {}
    if 'service' in modes:
        subprocess.run(
            ['cargo', 'build', '--release', '-q', '-p', 'mage-service'],
            cwd=SERVICE_SOURCE, check=True,
        )
        for name in workload.pipelines():
            services[name] = export_service(project, name, out / 'services' / name, env)
    return dict(inputs=inputs, project=project, services=services)


EXPORT = '''
import sys
from mage_ai.pipeline_services import export

project, pipeline, target = sys.argv[1:4]
export.write(export.capture(project, [pipeline], name=pipeline.replace('_', '-')),
             target, force=True)
'''


def export_service(project: Path, name: str, target: Path, env: Dict[str, str]) -> Path:
    if not (target / 'service.json').is_file():
        subprocess.run(
            [sys.executable, '-c', EXPORT, str(project), name, str(target)],
            env=env, check=True, cwd=project.parent,
        )
        from integration_tests.exported_services import build_rust_blocks

        build_rust_blocks(target)
    return target


# ---- one run --------------------------------------------------------------------------

DRIVER = '''
import json, os, resource, sys, time

from mage_ai.data_preparation.repo_manager import set_repo_path

mode, project, pipeline_uuid = sys.argv[1:4]
variables = json.loads(sys.argv[4])
set_repo_path(project)

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import fusion, fusion_verify
from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.shared.dates import utc_now

db_connection.start_session(force=True)
pipeline = Pipeline.get(pipeline_uuid, repo_path=project)
fusion_mode = fusion.BLOCK_FUSION_OFF if mode == 'scheduler' else fusion.BLOCK_FUSION_CHAINS
manager = None
if mode in ('scheduler', 'fused'):
    # A process per job, as these modes measure, even where an engine is installed.
    os.environ['MAGE_JOB_ENGINE'] = 'off'
if mode == 'warm':
    # The engine and its workers run all the time in production, as the scheduler does:
    # started before the measured run.
    from mage_ai.orchestration import job_manager as job_manager_module
    from mage_ai.orchestration.queue.config import QueueConfig
    from mage_ai.orchestration.queue.engine_queue import EngineQueue
    from mage_ai.pipeline_services.engine import engine_path

    manager = job_manager_module.JobManager.__new__(job_manager_module.JobManager)
    manager.queue = EngineQueue(QueueConfig(concurrency=4), engine_path())
    job_manager_module.job_manager = manager
    manager.start()
    # The first workers import MageML meanwhile.
    time.sleep(3)
run = fusion_verify._create_run(pipeline, fusion_mode, variables, utc_now())
print('START', flush=True)


def run_on_engine(run):
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler

    done = (PipelineRun.PipelineRunStatus.COMPLETED, PipelineRun.PipelineRunStatus.FAILED,
            PipelineRun.PipelineRunStatus.CANCELLED)
    PipelineScheduler(run).start(should_schedule=False)
    while True:
        run.refresh()
        if run.status in done:
            return run
        PipelineScheduler(run).schedule()
        run.refresh()
        if run.status in done:
            return run
        # As LoopTimeTrigger.wait: the scheduler runs again when a job finishes.
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and not manager.jobs_finished():
            time.sleep(0.01)
        manager.clean_up_jobs()


def cpu():
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


cpu_start, started = cpu(), time.perf_counter()
if manager is not None:
    run = run_on_engine(run)
else:
    run = fusion_verify.run_in_process(run, log=lambda text: print(text, file=sys.stderr))
seconds = time.perf_counter() - started
if manager is not None:
    manager.stop()
print('RESULT ' + json.dumps(dict(
    seconds=seconds, cpu_seconds=cpu() - cpu_start,
    status=str(run.status), failed=[b.block_uuid for b in run.block_runs
                                    if str(b.status) != 'completed'],
)), flush=True)
'''


class TreeSampler:
    """Peak resident memory of a process and its descendants, summed, while it runs."""

    def __init__(self, pid: int):
        self.process = psutil.Process(pid)
        self.peak = 0
        self.active = threading.Event()
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()

    def _sample(self):
        while not self.done.is_set():
            if self.active.is_set():
                total = 0
                try:
                    processes = [self.process] + self.process.children(recursive=True)
                except psutil.Error:
                    processes = []
                for process in processes:
                    try:
                        total += process.memory_info().rss
                    except psutil.Error:
                        pass
                self.peak = max(self.peak, total)
            time.sleep(SAMPLE_SECONDS)

    def stop(self) -> int:
        self.done.set()
        self.thread.join()
        return self.peak


def _children_cpu() -> float:
    usage = os.times()
    return usage.children_user + usage.children_system


def run_once(
    mode: str, pipeline: str, rows: int, prepared: Dict, out: Path, env: Dict[str, str],
    timeout: int,
) -> Dict:
    output = out / 'outputs' / str(rows) / mode / f'{pipeline}.parquet'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    variables = dict(prepared['inputs'][rows], output_path=str(output))
    project = prepared['project']
    if mode == 'cli':
        command = [str(Path(sys.executable).with_name('mage')), 'run', str(project), pipeline,
                   '--runtime-vars', json.dumps(variables)]
    elif mode == 'service':
        directory = prepared['services'][pipeline]
        data = out / 'service_data' / pipeline
        shutil.rmtree(data, ignore_errors=True)
        command = [str(SERVICE_BINARY), 'run', pipeline, '--json']
        for key, value in variables.items():
            command += ['--var', f'{key}={value}']
        env = dict(
            env,
            MAGE_SERVICE_DIR=str(directory),
            MAGE_SERVICE_DATA=str(data),
            MAGE_SERVICE_WORKER=str(
                directory / 'python/mage_ai/pipeline_services/runtime/worker.py',
            ),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(directory / 'python'), str(REPO)]),
        )
    else:
        command = [sys.executable, '-c', DRIVER, mode, str(project), pipeline,
                   json.dumps(variables)]

    logs = out / 'logs' / str(rows) / mode
    logs.mkdir(parents=True, exist_ok=True)
    stdout_path, stderr_path = logs / f'{pipeline}.out', logs / f'{pipeline}.err'
    scheduled = mode in ('scheduler', 'fused', 'warm')
    cpu_before = _children_cpu()
    started = time.perf_counter()
    # Files, not pipes, for what is not parsed: a full pipe would block the run.
    with open(stderr_path, 'w') as stderr_file, open(stdout_path, 'w') as stdout_file:
        process = subprocess.Popen(
            command, env=env, cwd=out, stderr=stderr_file, text=True,
            stdout=subprocess.PIPE if scheduled else stdout_file,
        )
        sampler = TreeSampler(process.pid)
        internal = None
        if scheduled:
            # The scheduler runs all the time in production: its start-up is not counted.
            for line in process.stdout:
                stdout_file.write(line)
                if line.startswith('START'):
                    sampler.active.set()
                elif line.startswith('RESULT '):
                    internal = json.loads(line[len('RESULT '):])
        else:
            sampler.active.set()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    seconds = time.perf_counter() - started
    peak = sampler.stop()
    cpu_seconds = _children_cpu() - cpu_before
    result = dict(
        mode=mode, pipeline=pipeline, rows=rows, returncode=process.returncode,
        seconds=seconds, cpu_seconds=cpu_seconds, peak_rss_bytes=peak,
    )
    if internal is not None:
        result.update(seconds=internal['seconds'], cpu_seconds=internal['cpu_seconds'])
        if internal['failed']:
            result['returncode'] = 1
    result['error'] = None
    if result['returncode'] != 0:
        result['error'] = (
            stdout_path.read_text(errors='replace')[-3000:] + '\n'
            + stderr_path.read_text(errors='replace')[-3000:]
        ).strip()
    else:
        result['check'] = check_output(output, out / 'outputs' / str(rows) / 'reference.parquet')
    return result


# ---- the check ------------------------------------------------------------------------

def check_output(path: Path, reference: Path) -> str:
    """'same' when the summary equals the reference (the first one written, from pandas),
    'close' within 1e-9 relative, otherwise what differs."""
    import pyarrow.parquet as pq

    if not path.is_file():
        return 'missing output'
    table = pq.read_table(path).to_pylist()
    rows = sorted(
        ((r['region'], r['segment'], int(r['month'])),
         (int(r['orders']), float(r['revenue']), float(r['avg_amount'])))
        for r in table
    )
    if not reference.is_file():
        shutil.copyfile(path, reference)
        return 'reference'
    expected = sorted(
        ((r['region'], r['segment'], int(r['month'])),
         (int(r['orders']), float(r['revenue']), float(r['avg_amount'])))
        for r in pq.read_table(reference).to_pylist()
    )
    if [k for k, _ in rows] != [k for k, _ in expected]:
        return f'different groups: {len(rows)} against {len(expected)}'
    worst = 0.0
    for (key, (orders, revenue, mean)), (_, (orders_ref, revenue_ref, mean_ref)) in zip(
        rows, expected,
    ):
        if orders != orders_ref:
            return f'different order count for {key}: {orders} against {orders_ref}'
        for value, wanted in ((revenue, revenue_ref), (mean, mean_ref)):
            worst = max(worst, abs(value - wanted) / max(abs(wanted), 1e-12))
    if worst == 0:
        return 'same'
    return 'close' if worst <= 1e-9 else f'values differ by {worst:.2e} relative'


# ---- the whole benchmark --------------------------------------------------------------

def _version(command: List[str]) -> Optional[str]:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return (result.stdout or result.stderr).strip().splitlines()[0] if result.returncode == 0 \
        else None


def machine() -> Dict:
    import importlib.metadata as metadata

    def package(name):
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            return None

    git = lambda *args: _version(['git', '-C', str(REPO), *args])  # noqa: E731
    dirty = subprocess.run(
        ['git', '-C', str(REPO), 'status', '--porcelain', '--untracked-files=no'],
        capture_output=True, text=True,
    ).stdout.strip()
    return dict(
        platform=platform.platform(),
        machine=platform.machine(),
        processor=_version(['sysctl', '-n', 'machdep.cpu.brand_string']) or platform.processor(),
        cpus=os.cpu_count(),
        memory_gb=round(psutil.virtual_memory().total / 2**30, 1),
        memory_available_gb=round(psutil.virtual_memory().available / 2**30, 1),
        python=platform.python_version(),
        packages={name: package(name) for name in ('pandas', 'polars', 'pyarrow', 'numpy')},
        r=_version(['Rscript', '--version']),
        rustc=_version(['rustc', '--version']),
        git_commit=git('rev-parse', 'HEAD'),
        git_dirty=bool(dirty),
    )


def source_digest() -> str:
    """
    The state of the code a run executes: path, size and modification time of MageML's
    Python, Rust and R sources. A run measured after this changes measured other code, as
    when the working tree is edited during the benchmark; run from a pinned git worktree to
    keep working while it runs.
    """
    import hashlib

    skipped = {'__pycache__', 'frontend', 'frontend_dist', 'frontend_dist_base_path_template',
               'node_modules', 'target', '.next'}
    digest = hashlib.sha256()
    for root, dirs, files in os.walk(REPO / 'mage_ai'):
        dirs[:] = sorted(d for d in dirs if d not in skipped)
        for name in sorted(files):
            if name.endswith(('.py', '.rs', '.R', '.toml')):
                stat = os.stat(os.path.join(root, name))
                digest.update(f'{root}/{name}:{stat.st_size}:{stat.st_mtime_ns}\n'.encode())
    return digest.hexdigest()


def main(argv: Optional[List[str]] = None) -> Path:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--rows', type=int, nargs='+', default=[100_000, 1_000_000])
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--modes', nargs='+', default=list(MODES), choices=MODES)
    parser.add_argument('--pipelines', nargs='+', default=list(workload.pipelines()))
    parser.add_argument('--seed', type=int, default=7)
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('--out', type=Path, default=REPO / 'integration_tests' / 'benchmark'
                        / 'results' / datetime.now().strftime('%Y%m%d-%H%M%S'))
    args = parser.parse_args(argv)

    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    env = environment(out)
    print(f'Preparing in {out}', flush=True)
    prepared = prepare(out, args.rows, args.modes, env)
    combos = [(rows, mode, pipeline) for rows in args.rows for mode in args.modes
              for pipeline in args.pipelines]
    results_path = out / 'results.json'
    results = dict(
        started_at=datetime.now(timezone.utc).isoformat(),
        settings=dict(rows=args.rows, repeats=args.repeats, modes=args.modes,
                      pipelines=args.pipelines, seed=args.seed),
        machine=machine(),
        pipelines={name: workload.pipelines()[name] for name in args.pipelines},
        runs=[],
        source_digest=source_digest(),
    )

    def record(result, round_number):
        result['round'] = round_number
        if source_digest() != results['source_digest']:
            result['source_changed'] = True
            results['source_changed_runs'] = results.get('source_changed_runs', 0) + 1
            print('  WARNING: the source changed since the benchmark started; this run '
                  'measured other code.', flush=True)
        results['runs'].append(result)
        results_path.write_text(json.dumps(results, indent=2))
        status = result.get('check') if result['returncode'] == 0 else 'FAILED'
        print(f'  round {round_number} {result["rows"]:>9,} {result["mode"]:9} '
              f'{result["pipeline"]:16} {result["seconds"]:7.2f} s  {status}', flush=True)
        if result['error']:
            print(textwrap.indent(result['error'][-1500:], '      '), flush=True)

    # The reference summary comes from pandas, block by block in one process.
    for rows in args.rows:
        record(run_once('cli', 'pandas', rows, prepared, out, env, args.timeout), 0)
    print('Warm-up round (not counted)', flush=True)
    for rows, mode, pipeline in combos:
        record(run_once(mode, pipeline, rows, prepared, out, env, args.timeout), 0)
    generator = random.Random(args.seed)
    for round_number in range(1, args.repeats + 1):
        print(f'Round {round_number} of {args.repeats}', flush=True)
        order = combos[:]
        generator.shuffle(order)
        for rows, mode, pipeline in order:
            record(run_once(mode, pipeline, rows, prepared, out, env, args.timeout), round_number)
    results['finished_at'] = datetime.now(timezone.utc).isoformat()
    results_path.write_text(json.dumps(results, indent=2))

    from integration_tests.benchmark.pipelines import report

    path = report.write(results, out)
    print(f'Report: {path}', flush=True)
    return path
