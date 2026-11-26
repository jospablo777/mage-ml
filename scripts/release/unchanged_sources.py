#!/usr/bin/env python3
"""Refuse a release that ships changed code under a published package version.

A package whose version is already in the published manifest is not published again:
the release keeps the published wheel (release_index.py --reuse-published). That is only
right when the package's source did not change since the release that published it, so
this check compares its source folders with that release's tag and fails, naming the
package and its files, when they differ: bump its version.

Run from the repository root, with the tags fetched:

    python3 scripts/release/unchanged_sources.py previous-manifest.json

With --bump it raises the patch version of each such package instead, and the exact pins
on it (name==version) in the workspace's pyproject.toml files; run `uv lock` after. The
Dependency fixes workflow does, since a lock it updates is part of a package's source.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

# Each package published besides mage-ml: where its version is, and the folders its wheel is
# built from. mage-ml takes the release's own version, which the release contract checks.
PACKAGES = {
    # With every crate it compiles in by path (test_unchanged_sources checks the list
    # against rust/column_atlas/Cargo.toml).
    'column-atlas-native': ('rust/column_atlas/pyproject.toml', [
        'rust/column_atlas',
        'mage_ai/data_preparation/models/block/rust/sdk',
        'mage_ai/pipeline_services/service/crates/checks',
        'mage_ai/pipeline_services/service/crates/log-window',
        'mage_ai/pipeline_services/service/crates/core',
    ]),
    'mage-integrations': ('mage_integrations/pyproject.toml', ['mage_integrations']),
    'singer-python': ('vendor/singer-python/pyproject.toml', ['vendor/singer-python']),
    'mageml-engine': (
        'packages/mageml-engine/pyproject.toml',
        ['packages/mageml-engine', 'mage_ai/pipeline_services/service'],
    ),
}


def published_tags(manifest: dict) -> dict[tuple[str, str], str]:
    """(project, version) to the tag of the release that published it first."""
    tags: dict[tuple[str, str], str] = {}
    for artifact in manifest.get('artifacts', []):
        tags.setdefault((artifact['project'], artifact['version']), artifact['tag'])
    return tags


def changed_files(root: Path, tag: str, paths: list[str]) -> list[str]:
    # Against the working tree: what is committed, and a change not committed yet.
    result = subprocess.run(
        ['git', 'diff', '--name-only', tag, '--', *paths],
        cwd=root, capture_output=True, text=True, check=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def stale(root: Path, manifest: dict) -> list[tuple[str, str, str, list[str]]]:
    """(project, version, tag, changed files) of each package whose published version
    ships changed source."""
    tags = published_tags(manifest)
    found = []
    for project, (pyproject, paths) in PACKAGES.items():
        version = tomllib.loads((root / pyproject).read_text())['project']['version']
        tag = tags.get((project, version))
        if tag is None:
            continue
        changed = changed_files(root, tag, paths)
        if changed:
            found.append((project, version, tag, changed))
    return found


def check(root: Path, manifest: dict) -> list[str]:
    """Problems: packages whose published version ships changed source."""
    problems = []
    for project, version, tag, changed in stale(root, manifest):
        shown = ', '.join(changed[:5]) + (f' and {len(changed) - 5} more'
                                          if len(changed) > 5 else '')
        problems.append(
            f'{project} {version} was published in {tag}, and its source changed since '
            f'({shown}): bump its version in {PACKAGES[project][0]}.'
        )
    return problems


def next_patch(version: str) -> str:
    """0.2.1 to 0.2.2; a local version such as 5.13.0+mage.2 to 5.13.0+mage.3."""
    match = re.fullmatch(r'(.*?)(\d+)', version)
    if match is None:
        raise ValueError(f'{version} does not end with a number to raise.')
    return f'{match.group(1)}{int(match.group(2)) + 1}'


# Where the workspace pins its packages exactly (mage-ml pins column-atlas-native, and so on).
PIN_FILES = ['pyproject.toml', *sorted({pyproject for pyproject, _ in PACKAGES.values()})]


def bump(root: Path, manifest: dict) -> list[str]:
    """Raises the version of each stale package and its exact pins; what changed."""
    done = []
    for project, version, _, _ in stale(root, manifest):
        new = next_patch(version)
        path = root / PACKAGES[project][0]
        text = path.read_text()
        # The [project] table's version: the first version line of the file.
        text, count = re.subn(rf'(?m)^version = "{re.escape(version)}"$',
                              f'version = "{new}"', text, count=1)
        if not count:
            raise ValueError(f'{path} has no line version = "{version}".')
        path.write_text(text)
        for pins in PIN_FILES:
            pin_path = root / pins
            if pin_path.exists():
                pinned = pin_path.read_text()
                updated = pinned.replace(f'"{project}=={version}"', f'"{project}=={new}"')
                if updated != pinned:
                    pin_path.write_text(updated)
        done.append(f'{project} {version} -> {new}')
    return done


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('previous_manifest', type=Path)
    parser.add_argument('--root', type=Path, default=Path('.'))
    parser.add_argument('--bump', action='store_true',
                        help='raise the stale packages\' versions instead of failing')
    args = parser.parse_args(argv)
    manifest = json.loads(args.previous_manifest.read_text())
    if args.bump:
        for line in bump(args.root, manifest) or ['No published package changed.']:
            print(line)
        return 0
    problems = check(args.root, manifest)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    print('Every package published under its current version is unchanged since.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
