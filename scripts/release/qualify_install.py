#!/usr/bin/env python3
"""
Qualify a set of MageML wheels on this machine: install them with uv into a fresh
environment outside the repository and check the installed application.

    python scripts/release/qualify_install.py WHEEL_DIR --version 0.1.0 --report report.json

Checks, in order, each recorded with its time and output:
1. uv installs mage-ml==VERSION from WHEEL_DIR (maintained packages) and the default index
   (everything else), in a new environment on Python 3.12.
2. mage_ai, polars, pandas, pyarrow and column_atlas_native import, and a native ColumnAtlas
   query reads a Parquet file.
3. mage init creates a project and mage run runs its example pipeline.
4. mage start serves the API and the UI, and stops on SIGTERM.

The exit code is 0 only when every check passes. Uses only the standard library.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

MAINTAINED = ('mage-ml', 'mage-integrations', 'singer-python', 'column-atlas-native')
IMPORT_CHECK = '''
import json, tempfile, os
import mage_ai, pandas, polars, pyarrow, pyarrow.parquet as pq, column_atlas_native
path = os.path.join(tempfile.mkdtemp(), "check.parquet")
pq.write_table(pyarrow.table({"a": [1, 2, None], "b": ["x", "y", "z"]}), path)
rows = column_atlas_native.NativeTable(path).count("{}")
assert rows == 3, rows
print(json.dumps({"pandas": pandas.__version__, "polars": polars.__version__,
                  "pyarrow": pyarrow.__version__, "native_rows": rows}))
'''


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Qualification:
    def __init__(self, wheels: Path, version: str, work: Path, extras: str):
        self.wheels = wheels.resolve()
        self.version = version
        self.work = work
        self.extras = extras
        self.checks = []
        self.env = work / 'env'
        bin_dir = 'Scripts' if os.name == 'nt' else 'bin'
        self.python = self.env / bin_dir / 'python'
        self.mage = self.env / bin_dir / 'mage'

    def run(self, name: str, command, timeout: int = 900, cwd: Path = None,
            env: dict = None) -> subprocess.CompletedProcess:
        started = time.monotonic()
        result = subprocess.run(
            [str(c) for c in command], cwd=cwd or self.work, env=env, timeout=timeout,
            capture_output=True, text=True,
        )
        self.checks.append(dict(
            name=name, passed=result.returncode == 0, seconds=round(time.monotonic() - started, 1),
            command=' '.join(str(c) for c in command), returncode=result.returncode,
            output=(result.stdout + result.stderr)[-4000:],
        ))
        if result.returncode != 0:
            raise RuntimeError(f'{name} failed:\n{result.stdout}\n{result.stderr}')
        return result

    def install(self) -> None:
        uv = shutil.which('uv')
        if uv is None:
            raise RuntimeError('uv is required: https://docs.astral.sh/uv/')
        self.run('create environment', [uv, 'venv', '--python', '3.12', self.env])
        requirement = f'mage-ml{self.extras}=={self.version}'
        # The maintained packages come only from the wheel directory, so a published
        # package with the same name can never stand in for the candidate.
        self.run('install wheels', [
            uv, 'pip', 'install', '--python', self.python, '--find-links', self.wheels,
            requirement,
        ], timeout=1800)
        self.check_sources()

    def check_sources(self) -> None:
        """The maintained packages must be the candidate wheels, not another copy."""
        code = (
            'import importlib.metadata as m, json\n'
            f'names = {list(MAINTAINED)!r}\n'
            'found = {d.metadata["Name"]: d.version for d in m.distributions()}\n'
            'print(json.dumps({n: found[n] for n in names if n in found}))'
        )
        versions = json.loads(self.run('maintained versions', [self.python, '-c', code]).stdout)
        wheel_versions = {}
        for wheel in self.wheels.glob('*.whl'):
            name, version = wheel.name.split('-')[:2]
            wheel_versions[name.replace('_', '-')] = version.replace('_', '+')
        wrong = {n: v for n, v in versions.items() if wheel_versions.get(n) not in (None, v)}
        if wrong or 'mage-ml' not in versions:
            raise RuntimeError(f'installed versions differ from the wheels: {versions}')

    def imports(self) -> None:
        self.run('imports and native query', [self.python, '-c', IMPORT_CHECK])

    def pipeline(self) -> Path:
        self.run('mage init', [self.mage, 'init', 'project'])
        self.run('mage run example_pipeline', [self.mage, 'run', 'project', 'example_pipeline'])
        return self.work / 'project'

    def server(self) -> None:
        port = free_port()
        log = open(self.work / 'server.log', 'w')
        started = time.monotonic()
        process = subprocess.Popen(
            [str(self.mage), 'start', 'project', '--port', str(port)],
            cwd=self.work, stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, 'MAGE_COMPILE_ON_START': 'off'},
        )
        base = f'http://127.0.0.1:{port}'
        status = page = None
        try:
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                try:
                    with urllib.request.urlopen(f'{base}/api/status', timeout=5) as response:
                        status = response.status
                    with urllib.request.urlopen(f'{base}/', timeout=10) as response:
                        page = response.read().decode('utf-8', 'replace')
                    break
                except OSError:
                    time.sleep(2)
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                code = process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                code = 'killed after 60 s'
            log.close()
        output = (self.work / 'server.log').read_text(errors='replace')[-4000:]
        passed = status == 200 and page is not None and '<html' in page.lower() \
            and '_next/' in page
        self.checks.append(dict(
            name='mage start serves the API and UI, stops on SIGTERM', passed=passed,
            seconds=round(time.monotonic() - started, 1), status=status,
            ui_served=bool(page and '_next/' in page), exit=code, output=output,
        ))
        if not passed:
            raise RuntimeError(f'server check failed (status {status}):\n{output}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('wheels', type=Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--extras', default='', help='for example [integrations]')
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--keep', action='store_true', help='keep the work directory')
    args = parser.parse_args()
    if not any(args.wheels.glob('mage_ml-*.whl')):
        parser.error(f'no mage_ml wheel in {args.wheels}')
    work = Path(tempfile.mkdtemp(prefix='mageml-qualify-'))
    qualification = Qualification(args.wheels, args.version, work, args.extras)
    error = None
    try:
        qualification.install()
        qualification.imports()
        qualification.pipeline()
        qualification.server()
    except Exception as exc:  # recorded in the report
        error = str(exc)[-4000:]
    report = dict(
        version=args.version, extras=args.extras, passed=error is None, error=error,
        platform=dict(system=platform.system(), machine=platform.machine(),
                      release=platform.release(), python=sys.version.split()[0]),
        wheels=sorted(p.name for p in args.wheels.glob('*.whl')),
        checks=qualification.checks,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    for check in qualification.checks:
        print(f"{'PASS' if check['passed'] else 'FAIL'}  {check['name']} ({check['seconds']} s)")
    if error:
        print(error, file=sys.stderr)
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if error is None else 1


if __name__ == '__main__':
    sys.exit(main())
