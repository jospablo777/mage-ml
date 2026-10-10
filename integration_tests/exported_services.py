"""
Running an exported pipeline service from the integration tests: export the pipelines of
the test project and run one with the mage-service debug binary, using the export's copy
of Mage's Python code and the repository for integration_tests.data.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'


def service_binary() -> Path:
    # Always: cargo returns at once when the binary is current, and a stale one would test
    # old code.
    subprocess.run(['cargo', 'build', '-q', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


def run_exported(project, pipeline: str, out: Path, variables=None, timeout: int = 600):
    """Exports the pipeline to out, runs it once and returns the run; every block must
    complete."""
    return run_service(export_service(project, pipeline, out), pipeline, out, variables, timeout)


def export_service(project, pipeline: str, out: Path) -> Path:
    from mage_ai.pipeline_services import export

    return Path(export.write(
        export.capture(project, [pipeline], name=pipeline.replace('_', '-')),
        str(out / 'service'), force=True,
    ))


def build_rust_blocks(directory: Path) -> None:
    """Builds the service's Rust blocks into bin/, as its Dockerfile does, reusing the
    compiled dependencies of Mage's own Rust builds."""
    from mage_ai.data_preparation.models.block.rust import workspace

    manifest = json.loads((directory / 'service.json').read_text())
    crates = sorted({
        Path(block['binary']).name
        for pipeline in manifest['pipelines'] for block in pipeline['blocks']
        if block.get('binary')
    })
    target = workspace.target_dir()
    subprocess.run(
        ['cargo', 'build', '--release', '--manifest-path', str(directory / 'rust/Cargo.toml'),
         *[a for crate in crates for a in ('-p', crate)]],
        check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
    )
    (directory / 'bin').mkdir(exist_ok=True)
    for crate in crates:
        shutil.copy2(target / 'release' / crate, directory / 'bin' / crate)


def run_service(directory: Path, pipeline: str, out: Path, variables=None, timeout: int = 600):
    """Runs the exported service's pipeline once and returns the run; every block must
    complete."""
    arguments = []
    for key, value in (variables or {}).items():
        arguments += ['--var', f'{key}={value}']
    result = subprocess.run(
        [str(service_binary()), 'run', pipeline, '--json', *arguments],
        capture_output=True, text=True, timeout=timeout,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(directory),
            MAGE_SERVICE_DATA=str(out / 'data'),
            MAGE_SERVICE_WORKER=str(
                directory / 'python/mage_ai/pipeline_services/runtime/worker.py',
            ),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(directory / 'python'), str(REPO)]),
        ),
    )
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]
    run = json.loads(result.stdout[result.stdout.index('{'):])
    assert {b['status'] for b in run['blocks']} == {'completed'}, run
    return run


def differences(expected: dict, actual: dict) -> dict:
    """Per table and column, the snapshots that differ."""
    found = {}
    for table in sorted(set(expected) | set(actual)):
        left, right = expected.get(table, {}), actual.get(table, {})
        for column in sorted(set(left) | set(right)):
            if left.get(column) != right.get(column):
                found[f'{table}.{column}'] = (left.get(column), right.get(column))
    return found
