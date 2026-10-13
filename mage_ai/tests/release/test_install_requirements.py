"""Check target selection, historical reuse, and unsupported-host rejection."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts' / 'release'))
import install_requirements as installer  # noqa: E402
from pip._vendor.packaging.requirements import Requirement  # noqa: E402
from release_index import ValidationError, asset_url, wheel_identity  # noqa: E402


def artifact(project: str, version: str, tag: str = 'py3-none-any') -> dict:
    filename = f'{project.replace("-", "_")}-{version}-{tag}.whl'
    _, _, tags, _ = wheel_identity(filename)
    release = 'v0.9.79+ml.4'
    return {
        'project': project,
        'version': version,
        'filename': filename,
        'tag': release,
        'requires_python': '>=3.11,<3.14',
        'sha256': 'a' * 64,
        'size': 100,
        'tags': tags,
        'url': asset_url('example/mageml', release, filename),
    }


def records() -> list[dict]:
    result = [
        artifact('mage-ml', '1.0.0'),
        artifact('mage-integrations', '1.0.0'),
        artifact('singer-python', '5.13.0+mage.1'),
    ]
    for platform in (
        'manylinux_2_28_x86_64',
        'manylinux_2_28_aarch64',
        'macosx_12_0_arm64',
        'macosx_12_0_x86_64',
    ):
        result.append(artifact('column-atlas-native', '0.2.0', 'cp312-abi3-' + platform))
    return result


OPTIONS = dict(
    version='1.0.0',
    native_version='0.2.0',
    targets=list(installer.TARGETS),
    integrations_version='1.0.0',
    singer_version='5.13.0+mage.1',
)


class InstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(tempfile.gettempdir()) / 'mageml-release-tests'
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix='installer-', dir=root)
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_all_target_markers_choose_one_native_url(self) -> None:
        files = installer.render(records(), **OPTIONS)
        requirements = [
            Requirement(line) for line in files['base.txt'].splitlines() if not line.startswith('#')
        ]
        for system, machine in installer.TARGETS.values():
            active = [
                item
                for item in requirements
                if item.marker is None
                or item.marker.evaluate({'sys_platform': system, 'platform_machine': machine})
            ]
            native = [item for item in active if item.name == 'column-atlas-native']
            self.assertEqual(len(native), 1)
            self.assertIsNotNone(native[0].url)
            self.assertIn('#sha256=' + 'a' * 64, native[0].url)

    def test_unsupported_hosts_activate_only_empty_version_intersection(self) -> None:
        text = installer.render(records(), **OPTIONS)['base.txt']
        requirements = [
            Requirement(line)
            for line in text.splitlines()
            if line.startswith('column-atlas-native')
        ]
        for system, machine in (('win32', 'AMD64'), ('linux', 's390x'), ('darwin', 'unknown')):
            active = [
                item
                for item in requirements
                if item.marker.evaluate({'sys_platform': system, 'platform_machine': machine})
            ]
            self.assertEqual(len(active), 1)
            self.assertIsNone(active[0].url)
            for version in ('0', '0.1', '1', '1.0.0rc1', '99.0.0'):
                self.assertFalse(active[0].specifier.contains(version, prereleases=True))

    def test_excluded_intel_target_activates_guard(self) -> None:
        options = dict(OPTIONS, targets=['linux-x86_64', 'linux-aarch64', 'macos-arm64'])
        text = installer.render(records(), **options)['base.txt']
        native = [
            Requirement(line)
            for line in text.splitlines()
            if line.startswith('column-atlas-native')
        ]
        active = [
            item
            for item in native
            if item.marker.evaluate({'sys_platform': 'darwin', 'platform_machine': 'x86_64'})
        ]
        self.assertEqual(len(active), 1)
        self.assertIsNone(active[0].url)

    def test_reuses_original_release_urls_by_explicit_version(self) -> None:
        text = installer.render(records(), **OPTIONS)['base.txt']
        self.assertIn('/v0.9.79%2Bml.4/', text)
        self.assertIn('mage_ml-1.0.0', text)

    def test_integrations_profile_binds_every_maintained_distribution(self) -> None:
        text = installer.render(records(), **OPTIONS)['integrations.txt']
        self.assertIn('mage-ml[integrations] @', text)
        self.assertIn('mage-integrations @', text)
        self.assertIn('singer-python @', text)
        self.assertIn('5.13.0%2Bmage.1', text)

    def test_incomplete_or_ambiguous_native_matrix_fails(self) -> None:
        incomplete = records()[:-1]
        with self.assertRaisesRegex(ValidationError, 'missing its native wheel'):
            installer.render(incomplete, **OPTIONS)
        duplicate = records()
        duplicate.append(copy.deepcopy(duplicate[-1]))
        with self.assertRaisesRegex(ValidationError, 'ambiguous'):
            installer.render(duplicate, **OPTIONS)

    def test_unexpected_native_abi_and_universal_native_wheel_fail(self) -> None:
        for tag in (
            'py3-none-any',
            'cp312-cp312-manylinux_2_28_x86_64',
            'cp311-abi3-macosx_12_0_universal2',
            # An older stable ABI than the 3.12 one MageML builds.
            'cp311-abi3-manylinux_2_28_x86_64',
        ):
            modified = records()
            modified[3] = artifact('column-atlas-native', '0.2.0', tag)
            with self.assertRaisesRegex(ValidationError, 'unsupported native wheel tag'):
                installer.render(modified, **OPTIONS)

    def test_invalid_options_fail(self) -> None:
        for options in (
            dict(OPTIONS, targets=[]),
            dict(OPTIONS, targets=['linux-x86_64'] * 2),
            dict(OPTIONS, version='9.9.9'),
            dict(OPTIONS, singer_version=None),
        ):
            with self.assertRaises(ValidationError):
                installer.render(records(), **options)

    def test_generation_preserves_existing_output(self) -> None:
        manifest = self.root / 'manifest.json'
        manifest.write_text(
            json.dumps(
                {'schema_version': 1, 'repository': 'example/mageml', 'artifacts': records()}
            )
        )
        output = self.root / 'install'
        installer.generate(manifest, output, **OPTIONS)
        before = (output / 'base.txt').read_bytes()
        with self.assertRaisesRegex(ValidationError, 'output must not exist'):
            installer.generate(manifest, output, **OPTIONS)
        self.assertEqual((output / 'base.txt').read_bytes(), before)

    def test_pip_rejects_active_guard_without_indexes(self) -> None:
        requirements = self.root / 'unsupported.txt'
        requirements.write_text('column-atlas-native<0,>=0\n')
        env = {key: value for key, value in os.environ.items() if not key.startswith('PIP_')}
        env.update(
            PIP_CONFIG_FILE=os.devnull,
            PIP_DISABLE_PIP_VERSION_CHECK='1',
            PYTHONDONTWRITEBYTECODE='1',
            TMPDIR=str(self.root),
            XDG_CACHE_HOME=str(self.root / 'cache'),
        )
        result = subprocess.run(
            [
                sys.executable,
                '-m',
                'pip',
                'install',
                '--dry-run',
                '--ignore-installed',
                '--no-index',
                '--no-cache-dir',
                '-r',
                str(requirements),
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('column-atlas-native', result.stderr)


if __name__ == '__main__':
    unittest.main()
