"""
Running an exported pipeline service from the integration tests: export the pipelines of
the test project and run one with the mage-service debug binary, using the export's copy
of Mage's Python code and the repository for integration_tests.data.
"""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'


def service_binary() -> Path:
    # Always: cargo returns at once when the binary is current, and a stale one would test
    # old code.
    subprocess.run(['cargo', 'build', '-q', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


# The engine with every stream connector, in its own target folder: the other exported-service
# tests build it without them, and test workers build at the same time. CI builds it first.
STREAM_TARGET = SERVICE / 'target' / 'streams'
STREAM_FEATURES = ('kafka,rabbitmq,ibm-mq,object-store,postgres,clickhouse,nats,mongodb,sqs,pubsub,'
                   'activemq,search,influxdb')


def stream_service_binary(release: bool = False) -> Path:
    """
    The engine with every stream connector; release builds for benchmarks and timing tests.
    MAGE_TEST_STREAM_RELEASE=1 (CI) makes every test use the release build, built once.
    """
    release = release or os.getenv('MAGE_TEST_STREAM_RELEASE') == '1'
    command = ['cargo', 'build', '-q', '-p', 'mage-service', '--features', STREAM_FEATURES]
    subprocess.run(
        command + (['--release'] if release else []),
        cwd=SERVICE, check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(STREAM_TARGET)),
    )
    return STREAM_TARGET / ('release' if release else 'debug') / 'mage-service'


# The engine with object storage only (an audit store, state in a bucket), in its own target
# folder, for jobs that do not build the stream connectors.
OBJECT_STORE_TARGET = SERVICE / 'target' / 'object-store'


def object_store_service_binary() -> Path:
    subprocess.run(
        ['cargo', 'build', '-q', '-p', 'mage-service', '--features', 'object-store'],
        cwd=SERVICE, check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(OBJECT_STORE_TARGET)),
    )
    return OBJECT_STORE_TARGET / 'debug' / 'mage-service'


# The engine with the data checks (contracts and monitors, on Polars), in its own target
# folder. CI builds it outside the cached target: Polars' build would grow the shared Rust
# cache past the repository's 10 GB, which evicts the others.
CHECKS_TARGET = Path(os.getenv('MAGE_TEST_CHECKS_TARGET') or SERVICE / 'target' / 'checks')


def checks_service_binary() -> Path:
    """The engine of an exported service with a contract or a monitor: feature checks."""
    subprocess.run(
        ['cargo', 'build', '-q', '-p', 'mage-service', '--features', 'checks'],
        cwd=SERVICE, check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(CHECKS_TARGET)),
    )
    return CHECKS_TARGET / 'debug' / 'mage-service'


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
    """Builds the service's Rust binaries into bin/, as its Dockerfile does, reusing the
    compiled dependencies of Mage's own Rust builds."""
    from mage_ai.data_preparation.models.block.rust import workspace

    manifest = json.loads((directory / 'service.json').read_text())
    binaries = sorted({
        Path(block['binary']).name
        for pipeline in manifest['pipelines'] for block in pipeline['blocks']
        if block.get('binary')
    })
    if not binaries:
        return
    target = workspace.target_dir()
    subprocess.run(
        ['cargo', 'build', '--release', '--manifest-path', str(directory / 'rust/Cargo.toml'),
         *[a for binary in binaries for a in ('-p', binary)]],
        check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
    )
    (directory / 'bin').mkdir(exist_ok=True)
    for binary in binaries:
        shutil.copy2(target / 'release' / binary, directory / 'bin' / binary)


def run_service(directory: Path, pipeline: str, out: Path, variables=None, timeout: int = 600,
                binary: Path = None, complete: bool = True):
    """Runs the exported service's pipeline once and returns the run; every block must
    complete, unless complete is False."""
    arguments = []
    for key, value in (variables or {}).items():
        arguments += ['--var', f'{key}={value}']
    result = subprocess.run(
        [str(binary or service_binary()), 'run', pipeline, '--json', *arguments],
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
    if not complete:
        return json.loads(result.stdout[result.stdout.index('{'):])
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


TOKEN = 'stream-test-token'  # pragma: allowlist secret


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]



class RunningService:
    """An exported service serving its API, with its streams, for a test."""

    def __init__(self, binary: Path, directory: Path, data: Path, **env):
        self.port = free_port()
        self.log = data.parent / f'service-{self.port}.log'
        self.env = dict(
            os.environ,
            MAGE_SERVICE_DIR=str(directory),
            MAGE_SERVICE_DATA=str(data),
            MAGE_SERVICE_PORT=str(self.port),
            MAGE_SERVICE_TOKEN=TOKEN,
            MAGE_SERVICE_WORKER=str(
                directory / 'python/mage_ai/pipeline_services/runtime/worker.py',
            ),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(directory / 'python'), str(REPO)]),
            **env,
        )
        self.binary = binary
        self.process = None

    def start(self):
        # The child keeps its own copy of the file descriptor; ours closes at once.
        with self.log.open('a') as log:
            self.process = subprocess.Popen(
                [str(self.binary), 'serve'], env=self.env,
                stdout=log, stderr=subprocess.STDOUT,
            )

    def streams(self) -> list:
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}/v1/streams',
            headers={'Authorization': f'Bearer {TOKEN}'},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read())['streams']

    def control(self, kind):
        snapshot = self.get('/v1/snapshot')
        pipeline = snapshot['pipelines'][0]
        body = json.dumps(dict(
            pipeline_id=pipeline['id'], request_id=uuid.uuid4().hex,
            expected_config_revision=pipeline['config_revision'], command=dict(kind=kind),
        )).encode()
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}/v1/control', data=body, method='POST',
            headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    def get(self, path):
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}{path}', headers={'Authorization': f'Bearer {TOKEN}'},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read())

    def wait_running(self, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            assert self.process.poll() is None, self.log.read_text()[-4000:]
            try:
                streams = self.streams()
                if streams and streams[0]['state'] == 'running':
                    return streams[0]
            except OSError:
                pass
            time.sleep(0.5)
        raise AssertionError(f'the stream did not start:\n{self.log.read_text()[-4000:]}')

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        assert self.process.wait(timeout=60) == 0, self.log.read_text()[-4000:]
