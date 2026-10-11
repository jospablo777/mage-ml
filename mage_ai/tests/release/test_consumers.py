#!/usr/bin/env python3
"""Exercise package routing with synthetic wheels and a local HTTP index.

The native package fixture contains Python only. These checks provide no evidence
about Rust compilation, native ABI compatibility, or application behavior.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parents[3]
BUILD = Path(tempfile.gettempdir()) / 'mageml-release-consumers'
REPORT = BUILD / 'consumer-validation.json'
INDEX_SCRIPT = ROOT / 'scripts/release/release_index.py'
VERSION = '99.0.0+fixture.1'
PROJECTS = ('mage-ml', 'column-atlas-native', 'mage-integrations', 'singer-python')
COMMANDS: list[dict] = []


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_wheel(directory: Path, name: str, requirements: tuple[str, ...]) -> Path:
    normalized = name.replace('-', '_')
    info = f'{normalized}-{VERSION}.dist-info'
    metadata = [
        'Metadata-Version: 2.1',
        f'Name: {name}',
        f'Version: {VERSION}',
        'Requires-Python: >=3.11,<3.14',
        'Summary: Synthetic dependency routing fixture',
    ]
    if name == 'mage-ml':
        metadata.append('Provides-Extra: integrations')
    metadata.extend(f'Requires-Dist: {value}' for value in requirements)
    files = {
        f'{info}/METADATA': ('\n'.join(metadata) + '\n\n').encode(),
        f'{info}/WHEEL': (
            'Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
        ).encode(),
        f'{normalized}/__init__.py': b'SYNTHETIC_PACKAGE = True\n',
    }
    record = io.StringIO(newline='')
    writer = csv.writer(record, lineterminator='\n')
    for filename, content in files.items():
        checksum = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b'=')
        writer.writerow((filename, f'sha256={checksum.decode()}', len(content)))
    writer.writerow((f'{info}/RECORD', '', ''))
    files[f'{info}/RECORD'] = record.getvalue().encode()
    path = directory / f'{normalized}-{VERSION}-py3-none-any.whl'
    with ZipFile(path, 'w', compression=ZIP_DEFLATED) as archive:
        for filename, content in files.items():
            member = ZipInfo(filename, date_time=(2020, 1, 1, 0, 0, 0))
            member.compress_type = ZIP_DEFLATED
            archive.writestr(member, content)
    return path


def clean_environment(root: Path, cache: Path, default_index: str) -> dict[str, str]:
    excluded = {'VIRTUAL_ENV', 'CONDA_PREFIX', 'PYTHONHOME', 'PYTHONPATH'}
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in excluded
        and not key.startswith(('UV_', 'PIP_'))
        and key.lower() not in {'http_proxy', 'https_proxy', 'all_proxy'}
    }
    directories = {
        'TMPDIR': root / 'tmp',
        'UV_CACHE_DIR': cache,
        'XDG_CACHE_HOME': root / 'xdg-cache',
        'XDG_CONFIG_HOME': root / 'xdg-config',
        'XDG_DATA_HOME': root / 'xdg-data',
    }
    for key, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        env[key] = str(path)
    env.update(
        UV_DEFAULT_INDEX=default_index,
        UV_PYTHON_DOWNLOADS='never',
        UV_NO_MANAGED_PYTHON='1',
        PIP_CONFIG_FILE=os.devnull,
        PIP_NO_INDEX='1',
        PIP_DISABLE_PIP_VERSION_CHECK='1',
        PIP_NO_INPUT='1',
        PYTHONDONTWRITEBYTECODE='1',
        NO_PROXY='127.0.0.1,localhost',
    )
    return env


def execute(
    args: list[str], cwd: Path, env: dict[str, str], *, success: bool = True
) -> subprocess.CompletedProcess:
    start = time.monotonic()
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=120)
    COMMANDS.append(
        {
            'arguments': args,
            'directory': str(cwd),
            'returncode': result.returncode,
            'elapsed_seconds': round(time.monotonic() - start, 4),
            'stdout': result.stdout,
            'stderr': result.stderr,
        }
    )
    if success and result.returncode:
        raise AssertionError(f'Command failed: {args!r}\n{result.stdout}\n{result.stderr}')
    if not success and result.returncode == 0:
        raise AssertionError(f'Command unexpectedly succeeded: {args!r}')
    return result


class ConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        BUILD.mkdir(parents=True, exist_ok=True)
        cls.work = Path(tempfile.mkdtemp(prefix='run-', dir=BUILD))
        cls.uv = shutil.which('uv')
        if cls.uv is None:
            raise RuntimeError('uv must be installed before running these checks')
        cls.site = cls.work / 'site'
        assets = cls.site / 'assets'
        assets.mkdir(parents=True)
        requirements = {
            'mage-ml': (
                f'column-atlas-native=={VERSION}',
                f'mage-integrations=={VERSION}; extra == "integrations"',
            ),
            'column-atlas-native': (),
            'mage-integrations': (f'singer-python=={VERSION}',),
            'singer-python': (),
        }
        cls.wheels = {
            name: write_wheel(assets, name, dependencies)
            for name, dependencies in requirements.items()
        }
        cls.requests = []

        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(cls.site), **kwargs)

            def log_message(self, fmt, *args):
                pass

            def do_GET(self):
                cls.requests.append(self.path)
                super().do_GET()

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.origin = f'http://127.0.0.1:{cls.server.server_port}'
        cls.index = cls.origin + '/simple/'
        spec = importlib.util.spec_from_file_location('consumer_release_index', INDEX_SCRIPT)
        assert spec is not None and spec.loader is not None
        release_index = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(release_index)
        generated = cls.work / 'generated-index'
        repository = 'example/mageml'
        tag = 'v' + VERSION
        cls.generated_manifest = release_index.generate(assets, repository, tag, generated)
        asset_prefix = f'https://github.com/{repository}/releases/download/{quote(tag, safe="")}/'
        for page in (generated / 'simple').rglob('*.html'):
            target = cls.site / page.relative_to(generated)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                page.read_text(encoding='utf-8').replace(asset_prefix, cls.origin + '/assets/'),
                encoding='utf-8',
            )
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.env = clean_environment(cls.work, cls.work / 'cache-initial', cls.origin + '/empty/')
        cls.versions = {
            'uv': execute([cls.uv, '--version'], cls.work, cls.env).stdout.strip(),
            'python': sys.version.splitlines()[0],
            'pip': execute(
                [sys.executable, '-m', 'pip', '--version'], cls.work, cls.env
            ).stdout.strip(),
        }

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=2)

    def project(self, name: str) -> Path:
        root = self.work / name
        root.mkdir()
        (root / 'pyproject.toml').write_text(
            '[project]\nname = "consumer-fixture"\nversion = "0.0.0"\n'
            'requires-python = ">=3.11,<3.14"\ndependencies = []\n\n'
            '[tool.uv]\npackage = false\n\n[tool.uv.workspace]\nmembers = []\n',
            encoding='utf-8',
        )
        return root

    def installed(
        self, interpreter: Path, names: tuple[str, ...], cwd: Path, env: dict[str, str]
    ) -> dict:
        code = (
            'import importlib.metadata,json; '
            f'print(json.dumps({{name:importlib.metadata.version(name) for name in {names!r}}}))'
        )
        return json.loads(execute([str(interpreter), '-c', code], cwd, env).stdout)

    def uv_consumer(self, name: str, extra: bool) -> None:
        project = self.project(name)
        requirement = f'mage-ml{"[integrations]" if extra else ""}=={VERSION}'
        execute(
            [
                self.uv,
                'add',
                requirement,
                '--index',
                f'mageml={self.index}',
                '--python',
                sys.executable,
            ],
            project,
            self.env,
        )
        config = tomllib.loads((project / 'pyproject.toml').read_text())
        indexes = config['tool']['uv']['index']
        self.assertTrue(
            any(item.get('name') == 'mageml' and item['url'] == self.index for item in indexes)
        )
        self.assertTrue(all(not item.get('explicit', False) for item in indexes))
        lock = tomllib.loads((project / 'uv.lock').read_text())
        expected = PROJECTS if extra else PROJECTS[:2]
        self.assertTrue(set(expected).issubset({package['name'] for package in lock['package']}))
        for package in lock['package']:
            if package['name'] in expected:
                self.assertEqual(package['source']['registry'].rstrip('/'), self.index.rstrip('/'))
                self.assertTrue(
                    all(
                        wheel['url'].startswith(self.origin + '/assets/')
                        for wheel in package['wheels']
                    )
                )
        self.assertEqual(
            self.installed(project / '.venv/bin/python', expected, project, self.env),
            {name: VERSION for name in expected},
        )
        fresh = self.work / (name + '-fresh')
        fresh.mkdir()
        for filename in ('pyproject.toml', 'uv.lock'):
            shutil.copy2(project / filename, fresh / filename)
        fresh_env = clean_environment(fresh, fresh / 'cache-empty', self.origin + '/empty/')
        fresh_env.pop('UV_DEFAULT_INDEX')
        self.assertEqual(list((fresh / 'cache-empty').iterdir()), [])
        original_lock = (fresh / 'uv.lock').read_bytes()
        first_request = len(self.requests)
        execute([self.uv, 'sync', '--locked', '--python', sys.executable], fresh, fresh_env)
        self.assertEqual((fresh / 'uv.lock').read_bytes(), original_lock)
        self.assertEqual(
            self.installed(fresh / '.venv/bin/python', expected, fresh, fresh_env),
            {name: VERSION for name in expected},
        )
        fresh_downloads = self.requests[first_request:]
        for name in expected:
            self.assertIn('/assets/' + quote(self.wheels[name].name), fresh_downloads)

    def test_uv_core_named_index_and_fresh_sync(self) -> None:
        self.uv_consumer('uv-core', False)

    def test_uv_integrations_transitive_singer_and_fresh_sync(self) -> None:
        self.uv_consumer('uv-integrations', True)

    def pip_install(
        self, name: str, requirements_url: str, *, success: bool = True
    ) -> subprocess.CompletedProcess:
        root = self.work / name
        root.mkdir()
        env = clean_environment(root, root / 'cache', self.origin + '/empty/')
        venv = root / 'venv'
        execute([sys.executable, '-m', 'venv', '--without-pip', str(venv)], root, env)
        result = execute(
            [
                sys.executable,
                '-m',
                'pip',
                '--python',
                str(venv / 'bin/python'),
                'install',
                '--no-index',
                '--no-cache-dir',
                '--only-binary=:all:',
                '-r',
                requirements_url,
            ],
            root,
            env,
            success=success,
        )
        if success:
            self.assertEqual(
                self.installed(venv / 'bin/python', PROJECTS, root, env),
                {name: VERSION for name in PROJECTS},
            )
        return result

    def test_pip_direct_urls_and_hash_failure(self) -> None:
        entries = []
        for name, wheel in self.wheels.items():
            package = 'mage-ml[integrations]' if name == 'mage-ml' else name
            sha = digest(wheel.read_bytes())
            entry = f'{package} @ {self.origin}/assets/{quote(wheel.name)}#sha256={sha}'
            if name == 'column-atlas-native':
                entry += (
                    f' ; sys_platform == "{sys.platform}"'
                    f' and platform_machine == "{platform.machine()}"'
                )
                entries.append(
                    f'{package} @ {self.origin}/unavailable/{quote(wheel.name)}'
                    ' ; platform_machine == "__unqualified_fixture__"'
                )
            entries.append(entry)
        (self.site / 'install.txt').write_text('\n'.join(entries) + '\n', encoding='utf-8')
        url = self.origin + '/install.txt'
        self.pip_install('pip-valid', url)
        self.assertFalse(any(path.startswith('/unavailable/') for path in self.requests))
        wheel = self.wheels['column-atlas-native']
        original = wheel.read_bytes()
        try:
            wheel.write_bytes(original + b'hash-tampering-fixture')
            result = self.pip_install('pip-tampered', url, success=False)
            self.assertIn('HASHES', result.stderr.upper())
            self.assertIn('sha256', result.stderr.lower())
        finally:
            wheel.write_bytes(original)


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(
        json.dumps(
            {
                'scope': 'Synthetic package routing and installation only',
                'test_source_sha256': digest(Path(__file__).read_bytes()),
                'index_generator_source_sha256': digest(INDEX_SCRIPT.read_bytes()),
                'index_generation': (
                    'Actual release index generator; '
                    'release asset URL prefix changed to loopback for delivery'
                ),
                'native_compilation_tested': False,
                'application_behavior_tested': False,
                'configured_indexes': (
                    'Loopback HTTP only; all fixture dependencies are supplied locally'
                ),
                'tools': getattr(ConsumerTests, 'versions', {}),
                'tests_run': result.testsRun,
                'failures': len(result.failures),
                'errors': len(result.errors),
                'successful': result.wasSuccessful(),
                'work_directory': str(getattr(ConsumerTests, 'work', BUILD)),
                'http_requests': getattr(ConsumerTests, 'requests', []),
                'commands': COMMANDS,
            },
            indent=2,
        )
        + '\n',
        encoding='utf-8',
    )
    sys.exit(0 if result.wasSuccessful() else 1)
