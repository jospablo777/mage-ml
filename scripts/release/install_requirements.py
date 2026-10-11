#!/usr/bin/env python3
"""Generate platform-selective pip requirements from a trusted release manifest.

The manifest has already passed wheel inspection. Versions and qualified targets
are explicit inputs because reused wheels retain their original release URLs.
This utility does not resolve or lock third-party dependencies.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile

from release_index import ValidationError, VERSION, load_previous, require


TARGETS = {
    'linux-x86_64': ('linux', 'x86_64'),
    'linux-aarch64': ('linux', 'aarch64'),
    'macos-arm64': ('darwin', 'arm64'),
    'macos-x86_64': ('darwin', 'x86_64'),
}


def marker(target: str, inverse: bool = False) -> str:
    system, machine = TARGETS[target]
    operator, joiner = ('!=', 'or') if inverse else ('==', 'and')
    return f'sys_platform {operator} "{system}" {joiner} platform_machine {operator} "{machine}"'


def reference(item: dict, extra: str = '') -> str:
    return f'{item["project"]}{extra} @ {item["url"]}#sha256={item["sha256"]}'


def select(artifacts: list[dict], project: str, version: str) -> list[dict]:
    require(bool(VERSION.fullmatch(version)), f'unsupported version for {project}')
    found = [
        item for item in artifacts if item['project'] == project and item['version'] == version
    ]
    require(bool(found), f'manifest has no {project} {version}')
    return found


def universal(artifacts: list[dict], project: str, version: str) -> dict:
    found = select(artifacts, project, version)
    require(
        len(found) == 1 and found[0]['tags'] == ['py3-none-any'],
        f'{project} requires one py3-none-any wheel',
    )
    return found[0]


def native_target(item: dict) -> str:
    targets = set()
    for tag in item['tags']:
        match = re.fullmatch(
            r'cp311-abi3-(manylinux_2_28_(x86_64|aarch64)|macosx_[0-9]+_[0-9]+_(x86_64|arm64))', tag
        )
        require(match is not None, f'unsupported native wheel tag for installer generation: {tag}')
        assert match is not None
        targets.add('linux-' + match[2] if match[2] else 'macos-' + match[3])
    require(len(targets) == 1, 'native wheel must describe exactly one target')
    return next(iter(targets))


def render(
    artifacts: list[dict],
    version: str,
    native_version: str,
    targets: list[str],
    integrations_version: str | None = None,
    singer_version: str | None = None,
) -> dict[str, str]:
    require(
        bool(targets) and len(targets) == len(set(targets)),
        'targets must be a nonempty unique list',
    )
    require(set(targets) <= TARGETS.keys(), 'unsupported installation target')
    require(
        bool(integrations_version) == bool(singer_version),
        'integrations and Singer versions must be supplied together',
    )
    main = universal(artifacts, 'mage-ml', version)
    native = {}
    for item in select(artifacts, 'column-atlas-native', native_version):
        target = native_target(item)
        require(target not in native, f'ambiguous native wheel for {target}')
        native[target] = item
    require(set(targets) <= native.keys(), 'a qualified target is missing its native wheel')
    selected = sorted(targets)
    native_lines = [reference(native[target]) + ' ; ' + marker(target) for target in selected]
    complement = ' and '.join('(' + marker(target, inverse=True) + ')' for target in selected)
    # The empty version intersection prevents unsupported hosts from resolving a fallback wheel.
    native_lines.append('column-atlas-native<0,>=0 ; ' + complement)
    header = (
        '# Generated from inspected release artifacts. '
        'Third-party dependencies resolve separately.\n'
    )
    result = {'base.txt': header + '\n'.join([reference(main), *native_lines]) + '\n'}
    if integrations_version and singer_version:
        integrations = universal(artifacts, 'mage-integrations', integrations_version)
        singer = universal(artifacts, 'singer-python', singer_version)
        result['integrations.txt'] = (
            header
            + '\n'.join(
                [
                    reference(main, '[integrations]'),
                    *native_lines,
                    reference(integrations),
                    reference(singer),
                ]
            )
            + '\n'
        )
    return result


def generate(manifest_path: Path, output: Path, **options) -> dict[str, str]:
    require(
        manifest_path.is_file() and not manifest_path.is_symlink(),
        'manifest must be a regular file',
    )
    require(manifest_path.stat().st_size <= 16 * 1024**2, 'manifest exceeds the size limit')
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    require(
        isinstance(manifest, dict) and isinstance(manifest.get('repository'), str),
        'manifest repository is missing',
    )
    artifacts = load_previous(manifest_path, manifest['repository'])
    files = render(artifacts, **options)
    require(not output.exists() and not output.is_symlink(), 'output must not exist')
    require(
        output.parent.is_dir() and not output.parent.is_symlink(),
        'output parent must be an existing directory',
    )
    stage = Path(tempfile.mkdtemp(prefix=f'.{output.name}.', dir=output.parent))
    try:
        for filename, content in files.items():
            (stage / filename).write_text(content, encoding='utf-8')
        require(
            not output.exists() and not output.is_symlink(), 'output appeared during generation'
        )
        stage.rename(output)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--native-version', required=True)
    parser.add_argument('--integrations-version')
    parser.add_argument('--singer-version')
    parser.add_argument('--targets', nargs='+', choices=sorted(TARGETS), required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        files = generate(
            args.manifest,
            args.output,
            version=args.version,
            native_version=args.native_version,
            targets=args.targets,
            integrations_version=args.integrations_version,
            singer_version=args.singer_version,
        )
    except (ValidationError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        parser.exit(1, f'install requirements: {exc}\n')
    print(f'Wrote {len(files)} installer files to {args.output}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
