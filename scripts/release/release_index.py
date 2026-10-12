#!/usr/bin/env python3
"""Build a static wheel index for GitHub Releases without copying wheel payloads.

The output directory must be absent. Historical filenames are immutable: an
identical wheel retains its first URL, and changed bytes require a new filename.
Only canonical release versions, unsigned wheels, and explicit numeric Python
version constraints are supported. Use a new package version for changed code.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html
import io
import itertools
import json
import os
import re
import shutil
import stat
import sys
import tempfile
import zipfile
from email.parser import BytesParser
from email.policy import compat32
from pathlib import Path, PurePosixPath
from urllib.parse import quote

PROJECTS = frozenset({'mage-ml', 'column-atlas-native', 'mage-integrations', 'singer-python'})
VERSION = re.compile(
    r'[0-9]+(?:\.[0-9]+)*(?:(?:a|b|rc)[0-9]+)?(?:\.post[0-9]+)?(?:\.dev[0-9]+)?(?:\+[a-z0-9]+(?:\.[a-z0-9]+)*)?\Z'
)
MAX_METADATA = 1024 * 1024
MAX_WHEEL = 2 * 1024**3 - 1
MAX_EXPANDED = 8 * 1024**3
MAX_MEMBERS = 100_000
MANIFEST_KEYS = frozenset(
    {'project', 'version', 'filename', 'tag', 'requires_python', 'sha256', 'size', 'tags', 'url'}
)


class ValidationError(ValueError):
    """An input fails the supported release contract."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def normalized(name: str) -> str:
    return re.sub(r'[-_.]+', '-', name).lower()


def check_repository(repository: str) -> None:
    require(
        bool(
            re.fullmatch(
                r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/[A-Za-z0-9][A-Za-z0-9_.-]{0,99}', repository
            )
        ),
        'repository must be owner/repository',
    )


def check_tag(tag: str) -> None:
    require(
        tag.startswith('v') and bool(VERSION.fullmatch(tag[1:])),
        'tag must be v followed by a supported version',
    )


def check_python(value: str) -> None:
    require(
        isinstance(value, str) and 0 < len(value) <= 256,
        'Requires-Python must be present and at most 256 characters',
    )
    for part in value.split(','):
        match = re.fullmatch(r'\s*(~=|==|!=|<=|>=|<|>)\s*([0-9]+(?:\.[0-9]+)*)(\.\*)?\s*', part)
        require(match is not None, 'Requires-Python must use numeric version specifiers')
        assert match is not None
        require(not match[3] or match[1] in {'==', '!='}, 'wildcards require == or !=')
        require(match[1] != '~=' or '.' in match[2], '~= requires at least two version components')


def wheel_identity(filename: str) -> tuple[str, str, list[str], str]:
    require(isinstance(filename, str) and len(filename) <= 240, 'wheel filename is invalid')
    require(
        bool(re.fullmatch(r'[A-Za-z0-9_.+\-]+\.whl', filename)),
        'wheel filename contains unsupported characters',
    )
    pieces = filename[:-4].split('-')
    require(
        len(pieces) in {5, 6}, 'wheel filename must contain distribution, version, and three tags'
    )
    distribution, version = pieces[:2]
    project = normalized(distribution)
    require(project in PROJECTS, f'unexpected project: {project}')
    require(
        distribution == project.replace('-', '_'),
        'wheel distribution must use lowercase underscores',
    )
    require(
        bool(VERSION.fullmatch(version)),
        'wheel version is outside the supported canonical version syntax',
    )
    if len(pieces) == 6:
        require(bool(re.fullmatch(r'[0-9][A-Za-z0-9_]*', pieces[2])), 'wheel build tag is invalid')
    compressed = pieces[-3:]
    for item in compressed:
        require(
            bool(re.fullmatch(r'[a-z0-9_]+(?:\.[a-z0-9_]+)*', item)),
            'wheel compatibility tag is invalid',
        )
        require(
            len(item.split('.')) == len(set(item.split('.'))),
            'wheel compatibility tags contain duplicates',
        )
    require(
        all(len(item.split('.')) <= 16 for item in compressed), 'wheel has too many compressed tags'
    )
    tags = sorted(
        '-'.join(parts) for parts in itertools.product(*(part.split('.') for part in compressed))
    )
    return project, version, tags, f'{distribution}-{version}.dist-info'


def asset_url(repository: str, tag: str, filename: str) -> str:
    tag, filename = quote(tag, safe=''), quote(filename, safe='')
    return f'https://github.com/{repository}/releases/download/{tag}/{filename}'


def version_identity(version: str) -> tuple:
    """Versions such as 1.0 and 1.0.0 compare equally during installation."""
    match = re.fullmatch(
        r'([0-9]+(?:\.[0-9]+)*)(?:(a|b|rc)([0-9]+))?'
        r'(?:\.post([0-9]+))?(?:\.dev([0-9]+))?(?:\+([a-z0-9]+(?:\.[a-z0-9]+)*))?',
        version,
    )
    require(match is not None, 'version is outside the supported syntax')
    assert match is not None
    release = [int(value) for value in match[1].split('.')]
    while len(release) > 1 and release[-1] == 0:
        release.pop()
    local = tuple(
        int(value) if value.isdigit() else value
        for value in (match[6].split('.') if match[6] else [])
    )
    return (
        tuple(release),
        match[2],
        int(match[3]) if match[3] is not None else None,
        int(match[4]) if match[4] is not None else None,
        int(match[5]) if match[5] is not None else None,
        local,
    )


def verify_compatibility_identities(artifacts: list[dict]) -> None:
    owners = {}
    for item in artifacts:
        version = version_identity(item['version'])
        for tag in item['tags']:
            identity = (item['project'], version, tag)
            previous = owners.setdefault(identity, item['filename'])
            require(
                previous == item['filename'],
                f'overlapping compatibility identity: {previous} and {item["filename"]}; '
                'bump the package version',
            )


def regular(path: Path) -> None:
    require(stat.S_ISREG(path.lstat().st_mode), f'input must be a regular file: {path.name}')


def one_header(message, key: str) -> str:
    values = message.get_all(key, [])
    require(len(values) == 1 and isinstance(values[0], str), f'expected one {key} header')
    value = values[0]
    require(
        not any(ord(char) < 32 or ord(char) == 127 for char in value),
        f'{key} contains control characters',
    )
    return value


def archive_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    members = archive.infolist()
    require(0 < len(members) <= MAX_MEMBERS, 'wheel member count exceeds the limit')
    require(
        sum(info.file_size for info in members) <= MAX_EXPANDED,
        'wheel expanded size exceeds the limit',
    )
    result = {}
    for info in members:
        name = info.filename
        require(
            name and '\\' not in name and ':' not in name and not name.startswith('/'),
            'unsafe archive path',
        )
        require(
            not any(ord(char) < 32 or ord(char) == 127 for char in name),
            'archive path contains control characters',
        )
        require(
            all(part not in {'', '.', '..'} for part in name.rstrip('/').split('/')),
            'unsafe archive path component',
        )
        require(name not in result, 'wheel contains duplicate archive members')
        require(not info.flag_bits & 1, 'encrypted wheels are unsupported')
        mode = stat.S_IFMT(info.external_attr >> 16)
        require(mode in {0, stat.S_IFREG, stat.S_IFDIR}, 'wheel contains a link or special file')
        require(
            (mode != stat.S_IFDIR or info.is_dir()) and (mode != stat.S_IFREG or not info.is_dir()),
            'archive file type disagrees with its path',
        )
        result[name] = info
    return result


def read_metadata(archive: zipfile.ZipFile, members: dict, name: str):
    require(name in members and not members[name].is_dir(), f'wheel is missing {name}')
    require(members[name].file_size <= MAX_METADATA, f'{name} exceeds the metadata size limit')
    raw = archive.read(name)
    raw.decode('utf-8', errors='strict')
    message = BytesParser(policy=compat32).parsebytes(raw)
    require(not message.defects, f'malformed headers in {name}')
    return message


def verify_record(archive: zipfile.ZipFile, members: dict, record_path: str) -> None:
    require(
        record_path in members and members[record_path].file_size <= 16 * MAX_METADATA,
        'missing or oversized RECORD',
    )
    rows = csv.reader(
        io.StringIO(archive.read(record_path).decode('utf-8'), newline=''), strict=True
    )
    recorded = set()
    for row in rows:
        require(len(row) == 3, 'RECORD rows must have three fields')
        name, digest, size = row
        require(
            name in members and not members[name].is_dir() and name not in recorded,
            'RECORD contains an absent or duplicate file',
        )
        recorded.add(name)
        if name == record_path:
            require(not digest and not size, 'RECORD must leave its own digest and size empty')
            continue
        require(
            size.isascii() and size.isdigit() and int(size) == members[name].file_size,
            'RECORD file size mismatch',
        )
        algorithm, separator, encoded = digest.partition('=')
        require(
            separator == '=' and algorithm in {'sha256', 'sha384', 'sha512'},
            'RECORD digest must use SHA-256 or stronger',
        )
        calculated = hashlib.new(algorithm)
        with archive.open(name) as stream:
            while chunk := stream.read(1024 * 1024):
                calculated.update(chunk)
        expected = base64.urlsafe_b64encode(calculated.digest()).decode('ascii').rstrip('=')
        require(encoded == expected, f'RECORD digest mismatch: {name}')
    require(
        recorded == {name for name, info in members.items() if not info.is_dir()},
        'RECORD must list every wheel file',
    )


def inspect_wheel(path: Path, repository: str, tag: str) -> dict:
    regular(path)
    require(path.stat().st_size <= MAX_WHEEL, 'wheel exceeds the GitHub release asset size limit')
    project, version, tags, info_root = wheel_identity(path.name)
    with path.open('rb') as stream:
        initial = os.fstat(stream.fileno())
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            members = archive_members(archive)
            roots = {
                PurePosixPath(name).parts[0]
                for name in members
                if PurePosixPath(name).parts[0].endswith('.dist-info')
            }
            require(roots == {info_root}, 'wheel must contain its matching dist-info directory')
            metadata = read_metadata(archive, members, f'{info_root}/METADATA')
            require(
                bool(re.fullmatch(r'2\.[1-4]', one_header(metadata, 'Metadata-Version'))),
                'supported Metadata-Version values are 2.1 through 2.4',
            )
            name = one_header(metadata, 'Name')
            require(
                bool(re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?', name))
                and normalized(name) == project,
                'METADATA Name disagrees with the filename',
            )
            require(
                one_header(metadata, 'Version') == version,
                'METADATA Version disagrees with the filename',
            )
            python = one_header(metadata, 'Requires-Python')
            check_python(python)
            wheel = read_metadata(archive, members, f'{info_root}/WHEEL')
            require(
                one_header(wheel, 'Wheel-Version') == '1.0', 'only Wheel-Version 1.0 is supported'
            )
            require(
                one_header(wheel, 'Root-Is-Purelib') in {'true', 'false'},
                'Root-Is-Purelib must be true or false',
            )
            wheel_tags = wheel.get_all('Tag', [])
            require(
                len(wheel_tags) == len(set(wheel_tags)) and sorted(wheel_tags) == tags,
                'WHEEL tags disagree with the filename',
            )
            filename_parts = path.name[:-4].split('-')
            build = wheel.get_all('Build', [])
            require(
                build == ([filename_parts[2]] if len(filename_parts) == 6 else []),
                'WHEEL Build disagrees with the filename',
            )
            verify_record(archive, members, f'{info_root}/RECORD')
        final = os.fstat(stream.fileno())
        require(
            (initial.st_size, initial.st_mtime_ns, initial.st_ctime_ns)
            == (final.st_size, final.st_mtime_ns, final.st_ctime_ns),
            'wheel changed during validation',
        )
    return {
        'project': project,
        'version': version,
        'filename': path.name,
        'tag': tag,
        'requires_python': python,
        'sha256': digest,
        'size': initial.st_size,
        'tags': tags,
        'url': asset_url(repository, tag, path.name),
    }


def load_previous(path: Path, repository: str) -> list[dict]:
    check_repository(repository)
    regular(path)
    require(path.stat().st_size <= 16 * MAX_METADATA, 'previous manifest exceeds the size limit')
    manifest = json.loads(path.read_text(encoding='utf-8'))
    require(
        isinstance(manifest, dict)
        and set(manifest) == {'schema_version', 'repository', 'artifacts'},
        'previous manifest fields are invalid',
    )
    require(
        type(manifest['schema_version']) is int and manifest['schema_version'] == 1,
        'unsupported previous manifest schema',
    )
    require(manifest['repository'] == repository, 'previous manifest belongs to another repository')
    artifacts = manifest['artifacts']
    require(
        isinstance(artifacts, list) and len(artifacts) <= 10_000,
        'previous artifact list is invalid',
    )
    seen = set()
    for item in artifacts:
        require(
            isinstance(item, dict) and set(item) == MANIFEST_KEYS,
            'previous artifact fields are invalid',
        )
        require(
            all(
                isinstance(item[key], str)
                for key in {
                    'project',
                    'version',
                    'filename',
                    'tag',
                    'requires_python',
                    'sha256',
                    'url',
                }
            ),
            'previous artifact string fields are invalid',
        )
        project, version, tags, _ = wheel_identity(item['filename'])
        check_tag(item['tag'])
        check_python(item['requires_python'])
        require(
            (item['project'], item['version'], item['tags']) == (project, version, tags),
            'previous artifact identity is invalid',
        )
        require(item['filename'] not in seen, 'previous manifest contains duplicate filenames')
        seen.add(item['filename'])
        require(
            bool(re.fullmatch(r'[0-9a-f]{64}', item['sha256'])),
            'previous artifact digest is invalid',
        )
        require(
            type(item['size']) is int and 0 < item['size'] <= MAX_WHEEL,
            'previous artifact size is invalid',
        )
        require(
            item['url'] == asset_url(repository, item['tag'], item['filename']),
            'previous artifact URL is invalid',
        )
    verify_compatibility_identities(artifacts)
    return artifacts


def merge_artifacts(previous: list[dict], current: list[dict]) -> list[dict]:
    result = {item['filename']: item for item in previous}
    for item in current:
        old = result.get(item['filename'])
        if old:
            identity = MANIFEST_KEYS - {'tag', 'url'}
            require(
                all(item[key] == old[key] for key in identity),
                f'published filename changed: {item["filename"]}; bump its package version',
            )
        else:
            result[item['filename']] = item
    artifacts = sorted(
        result.values(), key=lambda item: (item['project'], item['version'], item['filename'])
    )
    verify_compatibility_identities(artifacts)
    return artifacts


def document(title: str, links: list[str]) -> str:
    return (
        '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">'
        '<meta name="pypi:repository-version" content="1.0"><title>'
        + html.escape(title)
        + '</title></head><body><h1>'
        + html.escape(title)
        + '</h1>\n'
        + '\n'.join(links)
        + '\n</body></html>\n'
    )


def generate(
    wheel_directory: Path,
    repository: str,
    tag: str,
    output: Path,
    previous_manifest: Path | None = None,
) -> dict:
    check_repository(repository)
    check_tag(tag)
    require(
        wheel_directory.is_dir() and not wheel_directory.is_symlink(),
        'wheel directory must be a real directory',
    )
    require(not output.exists() and not output.is_symlink(), 'output must not exist')
    require(
        output.parent.is_dir() and not output.parent.is_symlink(),
        'output parent must be an existing directory',
    )
    paths = sorted(wheel_directory.iterdir())
    require(paths, 'wheel directory is empty')
    require(
        all(path.name.endswith('.whl') for path in paths),
        'wheel directory may contain only wheel files',
    )
    current = [inspect_wheel(path, repository, tag) for path in paths]
    previous = load_previous(previous_manifest, repository) if previous_manifest else []
    artifacts = merge_artifacts(previous, current)
    manifest = {'schema_version': 1, 'repository': repository, 'artifacts': artifacts}
    stage = Path(tempfile.mkdtemp(prefix=f'.{output.name}.', dir=output.parent))
    try:
        simple = stage / 'simple'
        simple.mkdir()
        projects = sorted({item['project'] for item in artifacts})
        (simple / 'index.html').write_text(
            document(
                'MageML packages',
                [f'<a href="{project}/">{project}</a><br>' for project in projects],
            ),
            encoding='utf-8',
        )
        for project in projects:
            directory = simple / project
            directory.mkdir()
            links = []
            for item in artifacts:
                if item['project'] == project:
                    url = html.escape(item['url'] + '#sha256=' + item['sha256'], quote=True)
                    python = html.escape(item['requires_python'], quote=True)
                    links.append(
                        f'<a href="{url}" data-requires-python="{python}">'
                        f'{html.escape(item["filename"])}</a><br>'
                    )
            (directory / 'index.html').write_text(document(project, links), encoding='utf-8')
        (stage / 'release-manifest.json').write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + '\n', encoding='utf-8'
        )
        (stage / 'SHA256SUMS').write_text(
            ''.join(f'{item["sha256"]}  {item["filename"]}\n' for item in artifacts),
            encoding='ascii',
        )
        (stage / '.nojekyll').write_text('', encoding='ascii')
        require(
            not output.exists() and not output.is_symlink(),
            'output appeared while generating the index',
        )
        stage.rename(output)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('wheel_directory', type=Path)
    parser.add_argument('--repository', required=True)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument(
        '--previous-manifest',
        type=Path,
        help='merge the trusted published manifest; filenames must retain their bytes and metadata',
    )
    args = parser.parse_args(argv)
    try:
        manifest = generate(
            args.wheel_directory, args.repository, args.tag, args.output, args.previous_manifest
        )
    except (
        ValidationError,
        OSError,
        zipfile.BadZipFile,
        UnicodeError,
        csv.Error,
        json.JSONDecodeError,
    ) as exc:
        parser.exit(1, f'release index: {exc}\n')
    print(f'Wrote {len(manifest["artifacts"])} artifact entries to {args.output}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
