"""A release refuses changed code under a published package version (I-24)."""
from __future__ import annotations

import importlib.util
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    'unchanged_sources', ROOT / 'scripts/release/unchanged_sources.py'
)
assert SPEC is not None and SPEC.loader is not None
unchanged = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(unchanged)


def git(root: Path, *args: str) -> None:
    subprocess.run(['git', *args], cwd=root, check=True, capture_output=True)


class UnchangedSourcesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        git(self.root, 'init', '-q')
        git(self.root, 'config', 'user.email', 'test@example.com')
        git(self.root, 'config', 'user.name', 'test')
        for project, (pyproject, paths) in unchanged.PACKAGES.items():
            for path in paths:
                (self.root / path).mkdir(parents=True, exist_ok=True)
                (self.root / path / 'code.txt').write_text('v1\n')
            (self.root / pyproject).write_text(
                f'[project]\nname = "{project}"\nversion = "1.0.0"\n')
        git(self.root, 'add', '-A')
        git(self.root, 'commit', '-q', '-m', 'one')
        git(self.root, 'tag', 'v1.0.0')
        self.manifest = {'artifacts': [
            {'project': project, 'version': '1.0.0', 'tag': 'v1.0.0'}
            for project in unchanged.PACKAGES
        ]}

    def commit(self, path: str, text: str) -> None:
        (self.root / path).write_text(text)
        git(self.root, 'commit', '-q', '-am', 'change')

    def test_unchanged_packages_pass(self) -> None:
        (self.root / 'other.txt').write_text('a change outside the packages\n')
        git(self.root, 'add', '-A')
        git(self.root, 'commit', '-q', '-m', 'other')
        self.assertEqual(unchanged.check(self.root, self.manifest), [])

    def test_changed_source_under_a_published_version_is_refused(self) -> None:
        self.commit('rust/column_atlas/code.txt', 'v2\n')

        [problem] = unchanged.check(self.root, self.manifest)

        self.assertIn('column-atlas-native 1.0.0 was published in v1.0.0', problem)
        self.assertIn('rust/column_atlas/code.txt', problem)
        self.assertIn('bump its version', problem)

    def test_the_engine_counts_its_crates_too(self) -> None:
        self.commit('mage_ai/pipeline_services/service/code.txt', 'v2\n')
        [problem] = unchanged.check(self.root, self.manifest)
        self.assertIn('mageml-engine', problem)

    def test_a_bumped_or_new_version_is_not_checked(self) -> None:
        self.commit('rust/column_atlas/code.txt', 'v2\n')
        self.commit('rust/column_atlas/pyproject.toml',
                    '[project]\nname = "column-atlas-native"\nversion = "1.0.1"\n')
        self.assertEqual(unchanged.check(self.root, self.manifest), [])
        self.assertEqual(unchanged.check(self.root, {'artifacts': []}), [])
