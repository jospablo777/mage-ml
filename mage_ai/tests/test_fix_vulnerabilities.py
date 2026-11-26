"""
scripts/fix_vulnerabilities.py: which version fixes a package, what it reports as held,
and the lock edits it makes. The tools (uv, cargo, yarn) are replaced by fakes; the
Dependency fixes workflow runs the real ones.
"""
import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'fix_vulnerabilities', ROOT / 'scripts' / 'fix_vulnerabilities.py',
)
fv = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fv)


def done(stdout: str = '', returncode: int = 0, stderr: str = ''):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


def uv_report(*entries):
    return json.dumps(dict(vulnerabilities=[
        dict(dependency=dict(name=name, version=version), id=id_, aliases=aliases,
             fix_versions=fixes)
        for name, version, id_, aliases, fixes in entries
    ]))


class FixVersionTest(unittest.TestCase):
    def finding(self, version, **fixed_in):
        finding = fv.Finding('python', 'uv.lock', 'pkg', version)
        finding.fixed_in = fixed_in
        finding.advisories = set(fixed_in)
        return finding

    def test_the_target_fixes_every_advisory_within_the_installed_line(self):
        finding = self.finding('1.4.0', A=['1.4.2', '2.0.1'], B=['1.5.0', '2.1.0'])
        self.assertEqual(fv.target(finding), '1.5.0')

    def test_zero_versions_change_line_with_the_minor(self):
        self.assertEqual(fv.compatible('0.39.4'), (0, 39))
        self.assertEqual(fv.compatible('1.39.4'), (1,))
        finding = self.finding('0.39.4', A=['0.41.0'])
        self.assertEqual(fv.target(finding), '0.41.0')

    def test_an_advisory_without_a_release_is_no_fix_once_the_others_are_fixed(self):
        finding = self.finding('1.0.0', A=['1.0.1'], B=[])
        fv.settle(finding, {'B'}, '1.0.1')
        self.assertEqual(finding.status, 'no fix')
        fv.settle(finding, {'A', 'B'}, '1.0.0')
        self.assertEqual(finding.status, 'held')
        fv.settle(finding, set(), '1.0.1')
        self.assertEqual(finding.status, 'fixed')

    def test_ranges_give_their_lower_bounds(self):
        self.assertEqual(fv.lower_bounds('>=1.2.3, <2.0.0'), ['1.2.3'])
        self.assertEqual(fv.lower_bounds('^1.4.2 || >=2.0.1'), ['1.4.2', '2.0.1'])
        self.assertEqual(fv.lower_bounds('<0.0.0'), [])

    def test_the_accepted_advisories_come_from_the_audit_script(self):
        accepted = fv.accepted_advisories()
        self.assertIn('RUSTSEC-2026-0194', accepted)
        self.assertIn('PYSEC-2023-235', accepted)
        self.assertIn('GHSA-f4j7-r4q5-qw2c', accepted)


class FixPythonTest(unittest.TestCase):
    def setUp(self):
        self.project = Path(tempfile.mkdtemp())
        self.versions = dict(requests='2.31.0', urllib3='1.26.20')
        self.write_lock()

    def write_lock(self):
        (self.project / 'uv.lock').write_text(''.join(
            f'[[package]]\nname = "{name}"\nversion = "{version}"\n\n'
            for name, version in self.versions.items()
        ))

    def fake_run(self, args, cwd=None, check=False):
        if args[:2] == ['uv', 'audit']:
            entries = []
            if self.versions['requests'] == '2.31.0':
                entries += [
                    ('requests', '2.31.0', 'GHSA-1', ['CVE-1', 'PYSEC-1'], ['2.32.0']),
                    ('requests', '2.31.0', 'PYSEC-1', ['CVE-1', 'GHSA-1'], ['2.32.0']),
                ]
            entries.append(('urllib3', '1.26.20', 'GHSA-2', [], ['2.5.0']))
            entries.append(('chromadb', '1.5.9', 'GHSA-3', [], []))
            return done(uv_report(*entries))
        if args[:2] == ['uv', 'lock']:
            upgrades = [a for a in args if '>=' in a]
            if any(a.startswith('urllib3') for a in upgrades):
                return done(returncode=1, stderr=(
                    'Using CPython 3.12\n'
                    'Because your project depends on urllib3<2 and urllib3>=2.5.0, ...'
                ))
            self.versions['requests'] = '2.32.4'
            self.write_lock()
            return done()
        raise AssertionError(args)

    def test_each_package_is_fixed_alone_when_one_is_held(self):
        with patch.object(fv, 'run', self.fake_run), \
                patch.object(fv, 'ROOT', self.project), \
                patch.object(fv, 'python_projects', lambda: [self.project]):
            findings = {f.package: f for f in fv.fix_python(False, {'GHSA-3'})}

        self.assertEqual(findings['requests'].status, 'fixed')
        self.assertEqual(findings['requests'].to, '2.32.4')
        self.assertEqual(findings['requests'].advisories, {'GHSA-1'}, 'PYSEC copy dropped')
        self.assertEqual(findings['urllib3'].status, 'held')
        self.assertIn('urllib3<2', findings['urllib3'].note)
        self.assertNotIn('Using CPython', findings['urllib3'].note)
        self.assertEqual(findings['chromadb'].status, 'accepted')

    def test_a_dry_run_changes_nothing(self):
        with patch.object(fv, 'run', self.fake_run), \
                patch.object(fv, 'ROOT', self.project), \
                patch.object(fv, 'python_projects', lambda: [self.project]):
            findings = {f.package: f for f in fv.fix_python(True, set())}

        self.assertEqual(findings['requests'].status, 'fixable')
        self.assertEqual(findings['chromadb'].status, 'no fix')
        self.assertEqual(self.versions['requests'], '2.31.0')


class YarnLockTest(unittest.TestCase):
    def test_only_the_named_package_entries_are_removed(self):
        folder = Path(tempfile.mkdtemp())
        (folder / 'yarn.lock').write_text(
            '# yarn lockfile v1\n\n\n'
            '"@scope/glob@^1.0.0":\n  version "1.0.0"\n\n'
            'glob@^7.1.0, glob@^7.2.0:\n  version "7.1.0"\n\n'
            '"glob-parent@^5.1.2":\n  version "5.1.2"\n'
        )
        with patch.object(fv, 'FRONTEND', folder):
            fv.remove_lock_entries('glob')
        text = (folder / 'yarn.lock').read_text()
        self.assertNotIn('glob@^7', text)
        self.assertIn('@scope/glob@^1.0.0', text)
        self.assertIn('glob-parent@^5.1.2', text)
        self.assertTrue(text.startswith('# yarn lockfile v1'))

    def test_a_transitive_package_gets_a_resolution_and_a_direct_one_its_range(self):
        folder = Path(tempfile.mkdtemp())
        (folder / 'package.json').write_text(json.dumps(
            dict(dependencies=dict(axios='~0.21.1')), indent=2,
        ) + '\n')
        with patch.object(fv, 'FRONTEND', folder):
            fv.raise_range('follow-redirects', '1.15.6')
            fv.raise_range('axios', '0.21.4')
            package = json.loads((folder / 'package.json').read_text())
            self.assertTrue(fv.is_direct('axios'))
        self.assertEqual(package['resolutions'], {'**/follow-redirects': '^1.15.6'})
        self.assertEqual(package['dependencies'], {'axios': '^0.21.4'})


class GeneratedMembersTest(unittest.TestCase):
    """A MageML project's Rust workspace has generated members git ignores (I-523)."""

    def workspace(self, members: str, lock: str) -> Path:
        root = Path(tempfile.mkdtemp())
        (root / 'Cargo.toml').write_text(
            f'[workspace]\nresolver = "3"\nmembers = [{members}]\n\n'
            '[workspace.dependencies]\nmage-block = { path = ".mage/sdk" }\nanyhow = "1"\n'
        )
        (root / 'Cargo.lock').write_text(lock)
        return root

    def test_the_sdk_and_each_locked_block_exist_for_cargo_and_go_after(self):
        root = self.workspace('".mage/blocks/*"', (
            'version = 4\n\n[[package]]\nname = "block_load_ab12"\nversion = "0.0.0"\n\n'
            '[[package]]\nname = "mage-block"\nversion = "0.1.0"\n\n'
            '[[package]]\nname = "anyhow"\nversion = "1.0.0"\n'
            'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        ))
        with fv.generated_members(root):
            self.assertTrue((root / '.mage/sdk/Cargo.toml').exists())
            manifest = (root / '.mage/blocks/block_load_ab12/Cargo.toml').read_text()
            self.assertIn('anyhow = { workspace = true }', manifest)
            self.assertEqual(sorted(p.name for p in (root / '.mage/blocks').iterdir()),
                             ['block_load_ab12'])
        self.assertFalse((root / '.mage').exists())

    def test_other_workspaces_and_existing_crates_are_left_alone(self):
        plain = self.workspace('"crates/*"', 'version = 4\n')
        with fv.generated_members(plain):
            self.assertFalse((plain / '.mage').exists())
        built = self.workspace('".mage/blocks/*"', 'version = 4\n')
        (built / '.mage/blocks/mine').mkdir(parents=True)
        with fv.generated_members(built):
            pass
        self.assertTrue((built / '.mage/blocks/mine').exists())
