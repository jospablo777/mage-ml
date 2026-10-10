"""
Running an exported pipeline service from the integration tests: export the pipelines of
the test project and run one with the mage-service debug binary, using the export's copy
of Mage's Python code and the repository for integration_tests.data.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'


def service_binary() -> Path:
    if not BINARY.is_file():
        subprocess.run(['cargo', 'build', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


def run_exported(project, pipeline: str, out: Path, variables=None, timeout: int = 600):
    """Exports the pipeline to out, runs it once and returns the run; every block must
    complete."""
    from mage_ai.pipeline_services import export

    directory = export.write(
        export.capture(project, [pipeline], name=pipeline.replace('_', '-')),
        str(out / 'service'), force=True,
    )
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
