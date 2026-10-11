"""Validate wheel inspection and preservation of published artifact identities."""

from __future__ import annotations

import base64
import copy
import csv
import hashlib
import importlib.util
import io
import itertools
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    'release_index', ROOT / 'scripts/release/release_index.py'
)
assert SPEC is not None and SPEC.loader is not None
index = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(index)
REPOSITORY = 'example/mageml'


def wheel(
    directory: Path,
    *,
    project: str = 'mage-ml',
    version: str = '1.0.0',
    tag: str = 'py3-none-any',
    build: str | None = None,
    payload: bytes = b'FIXTURE = True\n',
    additions: dict[str, bytes] | None = None,
    metadata_name: str | None = None,
    bad_record: bool = False,
    missing_record_entry: bool = False,
    archive_mode: int | None = None,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    distribution = project.replace('-', '_')
    info = f'{distribution}-{version}.dist-info'
    tags = [
        '-'.join(parts)
        for parts in itertools.product(*(part.split('.') for part in tag.split('-')))
    ]
    headers = ['Wheel-Version: 1.0', 'Root-Is-Purelib: true']
    if build is not None:
        headers.append(f'Build: {build}')
    headers.extend(f'Tag: {value}' for value in tags)
    files = {
        f'{distribution}/__init__.py': payload,
        f'{info}/METADATA': (
            f'Metadata-Version: 2.4\nName: {metadata_name or project}\nVersion: {version}\n'
            'Requires-Python: >=3.11,<3.14\n\n'
        ).encode(),
        f'{info}/WHEEL': ('\n'.join(headers) + '\n').encode(),
    }
    files.update(additions or {})
    record = io.StringIO(newline='')
    rows = csv.writer(record, lineterminator='\n')
    for position, (name, content) in enumerate(files.items()):
        if missing_record_entry and position == 0:
            continue
        checksum = hashlib.sha256(content).digest()
        if bad_record and position == 0:
            checksum = bytes(32)
        encoded = base64.urlsafe_b64encode(checksum).decode().rstrip('=')
        rows.writerow((name, f'sha256={encoded}', len(content)))
    rows.writerow((f'{info}/RECORD', '', ''))
    files[f'{info}/RECORD'] = record.getvalue().encode()
    filename = f'{distribution}-{version}-' + (f'{build}-' if build else '') + tag + '.whl'
    destination = directory / filename
    with ZipFile(destination, 'w') as archive:
        for position, (name, content) in enumerate(files.items()):
            member = ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            member.compress_type = ZIP_DEFLATED
            if archive_mode is not None and position == 0:
                member.external_attr = archive_mode << 16
            archive.writestr(member, content)
    return destination


class ReleaseIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary_root = Path(tempfile.gettempdir()) / 'mageml-release-tests'
        temporary_root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix='index-', dir=temporary_root)
        self.root = Path(self.temporary.name)
        self.wheels = self.root / 'wheels'
        self.output = self.root / 'site'

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def generate(self, **kwargs) -> dict:
        return index.generate(self.wheels, REPOSITORY, 'v1.0.0', self.output, **kwargs)

    def published(self) -> dict:
        wheel(self.wheels)
        return self.generate()

    def test_index_links_hashes_python_constraints_and_no_payload_copy(self) -> None:
        artifact = wheel(self.wheels, version='1.0.0+ml.1')
        manifest = self.generate()
        record = manifest['artifacts'][0]
        self.assertEqual(record['sha256'], hashlib.sha256(artifact.read_bytes()).hexdigest())
        self.assertEqual(record['size'], artifact.stat().st_size)
        self.assertIn('%2Bml.1', record['url'])
        page = (self.output / 'simple/mage-ml/index.html').read_text()
        self.assertIn('data-requires-python="&gt;=3.11,&lt;3.14"', page)
        self.assertIn(record['url'] + '#sha256=' + record['sha256'], page)
        self.assertIn('<a href="mage-ml/">', (self.output / 'simple/index.html').read_text())
        self.assertEqual(list(self.output.rglob('*.whl')), [])
        self.assertEqual(
            (self.output / 'SHA256SUMS').read_text(), f'{record["sha256"]}  {artifact.name}\n'
        )
        self.assertEqual(json.loads((self.output / 'release-manifest.json').read_text()), manifest)
        self.assertTrue((self.output / '.nojekyll').is_file())

    def test_record_hash_must_match_actual_member_bytes(self) -> None:
        wheel(self.wheels, bad_record=True)
        with self.assertRaisesRegex(index.ValidationError, 'RECORD digest mismatch'):
            self.generate()
        self.assertFalse(self.output.exists())

    def test_record_must_cover_every_file(self) -> None:
        wheel(self.wheels, missing_record_entry=True)
        with self.assertRaisesRegex(index.ValidationError, 'RECORD must list every wheel file'):
            self.generate()

    def test_archive_rejects_parent_traversal_absolute_and_backslash_paths(self) -> None:
        for name in ('../escape.py', '/absolute.py', 'package/../escape.py', 'package\\escape.py'):
            with self.subTest(name=name):
                wheel(self.wheels, additions={name: b'fixture'})
                with self.assertRaisesRegex(index.ValidationError, 'unsafe archive path'):
                    self.generate()
        self.assertFalse((self.root / 'escape.py').exists())

    def test_archive_rejects_symbolic_link_members(self) -> None:
        wheel(self.wheels, archive_mode=stat.S_IFLNK | 0o777)
        with self.assertRaisesRegex(index.ValidationError, 'link or special file'):
            self.generate()

    def test_wheel_input_must_be_regular(self) -> None:
        source = wheel(self.root / 'other')
        self.wheels.mkdir()
        (self.wheels / source.name).symlink_to(source)
        with self.assertRaisesRegex(index.ValidationError, 'regular file'):
            self.generate()

    def test_metadata_name_must_match_filename(self) -> None:
        wheel(self.wheels, metadata_name='singer-python')
        with self.assertRaisesRegex(index.ValidationError, 'METADATA Name'):
            self.generate()

    def test_existing_output_is_never_modified(self) -> None:
        wheel(self.wheels)
        self.output.mkdir()
        sentinel = self.output / 'existing.txt'
        sentinel.write_text('preserve')
        with self.assertRaisesRegex(index.ValidationError, 'output must not exist'):
            self.generate()
        self.assertEqual(sentinel.read_text(), 'preserve')
        self.assertEqual(list(self.output.iterdir()), [sentinel])

    def test_generation_failure_removes_partial_staging_directory(self) -> None:
        wheel(self.wheels)
        with patch.object(index, 'document', side_effect=OSError('simulated output failure')):
            with self.assertRaisesRegex(OSError, 'simulated output failure'):
                self.generate()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.site.*')), [])

    def test_reused_wheel_keeps_original_url_and_new_versions_preserve_history(self) -> None:
        old = self.published()
        wheel(self.wheels, version='1.1.0')
        result = index.generate(
            self.wheels,
            REPOSITORY,
            'v1.1.0',
            self.root / 'site-next',
            self.output / 'release-manifest.json',
        )
        self.assertEqual(len(result['artifacts']), 2)
        original = next(item for item in result['artifacts'] if item['version'] == '1.0.0')
        self.assertEqual(original, old['artifacts'][0])
        self.assertEqual(original['tag'], 'v1.0.0')

    def test_changed_bytes_under_published_filename_are_rejected(self) -> None:
        self.published()
        wheel(self.wheels, payload=b'FIXTURE = False\n')
        with self.assertRaisesRegex(index.ValidationError, 'published filename changed'):
            index.generate(
                self.wheels,
                REPOSITORY,
                'v1.1.0',
                self.root / 'site-next',
                self.output / 'release-manifest.json',
            )

    def test_build_number_cannot_replace_published_compatibility_identity(self) -> None:
        self.published()
        wheel(self.wheels, build='1', payload=b'CHANGED = True\n')
        with self.assertRaisesRegex(index.ValidationError, 'overlapping compatibility identity'):
            index.generate(
                self.wheels,
                REPOSITORY,
                'v1.1.0',
                self.root / 'site-next',
                self.output / 'release-manifest.json',
            )

    def test_current_wheel_set_rejects_intersecting_compressed_tags(self) -> None:
        wheel(self.wheels, tag='py2.py3-none-any')
        wheel(self.wheels, tag='py3-none-any')
        with self.assertRaisesRegex(index.ValidationError, 'overlapping compatibility identity'):
            self.generate()

    def test_equivalent_version_spellings_share_identity(self) -> None:
        cases = (('1.0', '1.0.0'), ('01.0', '1.0'), ('1.0+ml.01', '1.0.0+ml.1'))
        for left, right in cases:
            with self.subTest(left=left, right=right):
                self.assertEqual(index.version_identity(left), index.version_identity(right))
        wheel(self.wheels, version='1.0')
        wheel(self.wheels, version='1.0.0')
        with self.assertRaisesRegex(index.ValidationError, 'overlapping compatibility identity'):
            self.generate()

    def test_distinct_targets_and_new_versions_remain_allowed(self) -> None:
        wheel(self.wheels, project='column-atlas-native', tag='cp311-abi3-manylinux_2_28_x86_64')
        wheel(self.wheels, project='column-atlas-native', tag='cp311-abi3-manylinux_2_28_aarch64')
        wheel(
            self.wheels,
            project='column-atlas-native',
            version='1.0.1',
            tag='cp311-abi3-manylinux_2_28_x86_64',
        )
        self.assertEqual(len(self.generate()['artifacts']), 3)

    def test_corrupt_historical_manifest_is_rejected(self) -> None:
        original = self.published()
        cases = {
            'wrong_repository': lambda value: value.update(repository='elsewhere/project'),
            'boolean_schema': lambda value: value.update(schema_version=True),
            'duplicate_filename': lambda value: value['artifacts'].append(
                copy.deepcopy(value['artifacts'][0])
            ),
            'changed_url': lambda value: value['artifacts'][0].update(
                url='https://elsewhere.example/wheel'
            ),
            'invalid_digest': lambda value: value['artifacts'][0].update(sha256='g' * 64),
            'invalid_size': lambda value: value['artifacts'][0].update(size=True),
            'extra_field': lambda value: value.update(unexpected=True),
        }
        for name, change in cases.items():
            with self.subTest(case=name):
                modified = copy.deepcopy(original)
                change(modified)
                path = self.root / f'{name}.json'
                path.write_text(json.dumps(modified))
                with self.assertRaises(index.ValidationError):
                    index.load_previous(path, REPOSITORY)

    def test_historical_manifest_cannot_hide_overlapping_identity(self) -> None:
        original = self.published()
        duplicate = copy.deepcopy(original['artifacts'][0])
        duplicate['filename'] = 'mage_ml-1.0.0-1-py3-none-any.whl'
        duplicate['url'] = index.asset_url(REPOSITORY, duplicate['tag'], duplicate['filename'])
        original['artifacts'].append(duplicate)
        path = self.root / 'overlap.json'
        path.write_text(json.dumps(original))
        with self.assertRaisesRegex(index.ValidationError, 'overlapping compatibility identity'):
            index.load_previous(path, REPOSITORY)


if __name__ == '__main__':
    unittest.main(verbosity=2)
