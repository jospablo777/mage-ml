"""
Per-run overhead (QUALITY.md, performance): the time `mage run` takes for a fixed tiny
pipeline of five pandas blocks, where the blocks' own work is negligible, so the time is
what MageML adds per run. CI fails when a change makes it more than 15% slower than the
commit it is compared with.

    # one side: N warm runs in one process, as JSON
    python scripts/ci/run_overhead.py measure --runs 7 --out head.json

    # CI: the base commit's code (a git worktree) against this checkout, rounds alternated
    python scripts/ci/run_overhead.py compare --base /tmp/base --rounds 3 --runs 7

Both sides run in the same environment; the base side imports mage_ai from the worktree
through PYTHONPATH, which comes before the editable install.
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = 'overhead_check'
BLOCKS = [
    ('load', 'data_loader', '''
        import pandas as pd

        @data_loader
        def load(*args, **kwargs):
            return pd.DataFrame(dict(id=range(100), value=[i * 0.5 for i in range(100)]))
    '''),
    ('clean', 'transformer', '''
        @transformer
        def clean(df, *args, **kwargs):
            return df.dropna()
    '''),
    ('enrich', 'transformer', '''
        @transformer
        def enrich(df, *args, **kwargs):
            return df.assign(double=df['value'] * 2)
    '''),
    ('summarize', 'transformer', '''
        @transformer
        def summarize(df, *args, **kwargs):
            return df.groupby(df['id'] % 10)['double'].sum().reset_index()
    '''),
    ('keep', 'data_exporter', '''
        @data_exporter
        def keep(df, *args, **kwargs):
            assert len(df) == 10
    '''),
]
MAX_REGRESSION = 0.15
# Differences under this many milliseconds are noise on shared runners, whatever the share
# (a run takes about 50 ms; the same commit measured against itself varies by about 1%).
FLOOR_MS = 5.0


def make_project(folder: str) -> str:
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.repo_manager import init_repo
    from mage_ai.settings.repo import set_repo_path

    project = os.path.join(folder, 'overhead_project')
    init_repo(project)
    set_repo_path(project)
    pipeline = Pipeline.create(PIPELINE, repo_path=project)
    previous = None
    for name, kind, code in BLOCKS:
        block = Block.create(name, kind, project, language='python')
        Path(block.file_path).write_text(textwrap.dedent(code).lstrip())
        pipeline.add_block(block, upstream_block_uuids=[previous] if previous else None)
        previous = block.uuid
    return project


def measure(runs: int, warmup: int = 2) -> dict:
    from typer.testing import CliRunner

    with tempfile.TemporaryDirectory(prefix='mage-overhead-') as folder:
        project = make_project(folder)
        from mage_ai.cli.main import app

        runner = CliRunner()
        times = []
        for index in range(warmup + runs):
            started = time.perf_counter()
            # Without the blocks' tests, which mage run runs by default since it changed;
            # typer gives the base side's older --test option a --no-test too.
            result = runner.invoke(app, ['run', project, PIPELINE, '--no-test'])
            elapsed = (time.perf_counter() - started) * 1000
            if result.exit_code != 0:
                raise SystemExit(f'mage run failed:\n{result.output}\n{result.exception!r}')
            if index >= warmup:
                times.append(elapsed)
    return dict(median_ms=statistics.median(times), runs_ms=times)


def _measure_side(base: str, runs: int) -> list:
    env = dict(os.environ)
    if base:
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [base, env.get('PYTHONPATH')]))
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as out:
        path = out.name
    subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), 'measure', '--runs', str(runs),
         '--out', path],
        check=True, env=env, cwd=tempfile.gettempdir(),
    )
    with open(path) as file:
        return json.load(file)['runs_ms']


def compare(base: str, rounds: int, runs: int) -> dict:
    sides = dict(base=[], head=[])
    for _ in range(rounds):
        sides['base'] += _measure_side(base, runs)
        sides['head'] += _measure_side('', runs)
    return dict(verdict(sides['base'], sides['head']), runs=sides)


def verdict(base_runs: list, head_runs: list) -> dict:
    """Medians of both sides and whether the head regressed past the limit and the floor."""
    base_ms = statistics.median(base_runs)
    head_ms = statistics.median(head_runs)
    change = head_ms / base_ms - 1
    regressed = change > MAX_REGRESSION and head_ms - base_ms > FLOOR_MS
    return dict(base_ms=base_ms, head_ms=head_ms, change=change, regressed=regressed,
                max_regression=MAX_REGRESSION, floor_ms=FLOOR_MS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    commands = parser.add_subparsers(dest='command', required=True)
    one = commands.add_parser('measure')
    one.add_argument('--runs', type=int, default=7)
    one.add_argument('--out')
    both = commands.add_parser('compare')
    both.add_argument('--base', required=True, help='a checkout of the base commit')
    both.add_argument('--rounds', type=int, default=3)
    both.add_argument('--runs', type=int, default=7)
    both.add_argument('--out')
    args = parser.parse_args()

    if args.command == 'measure':
        result = measure(args.runs)
        if args.out:
            Path(args.out).write_text(json.dumps(result))
        print(f"mage run, tiny pipeline: median {result['median_ms']:.0f} ms over "
              f"{args.runs} runs")
        return 0

    result = compare(os.path.abspath(args.base), args.rounds, args.runs)
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=1))
    print(f"Per-run overhead: base {result['base_ms']:.0f} ms, this change "
          f"{result['head_ms']:.0f} ms ({result['change']:+.1%}).")
    if result['regressed']:
        print(f"::error::mage run of the tiny pipeline is {result['change']:.1%} slower "
              f"than the base commit (limit {MAX_REGRESSION:.0%}).")
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
