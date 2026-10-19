#!/usr/bin/env python3
"""Refuse a release that ships changed code under a published package version.

A package whose version is already in the published manifest is not published again:
the release keeps the published wheel (release_index.py --reuse-published). That is only
right when the package's source did not change since the release that published it, so
this check compares its source folders with that release's tag and fails, naming the
package and its files, when they differ: bump its version.

Run from the repository root, with the tags fetched:

    python3 scripts/release/unchanged_sources.py previous-manifest.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from pathlib import Path

# Each package published besides mage-ml: where its version is, and the folders its wheel is
# built from. mage-ml takes the release's own version, which the release contract checks.
PACKAGES = {
    'column-atlas-native': ('rust/column_atlas/pyproject.toml', ['rust/column_atlas']),
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
    result = subprocess.run(
        ['git', 'diff', '--name-only', tag, 'HEAD', '--', *paths],
        cwd=root, capture_output=True, text=True, check=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def check(root: Path, manifest: dict) -> list[str]:
    """Problems: packages whose published version ships changed source."""
    tags = published_tags(manifest)
    problems = []
    for project, (pyproject, paths) in PACKAGES.items():
        version = tomllib.loads((root / pyproject).read_text())['project']['version']
        tag = tags.get((project, version))
        if tag is None:
            continue
        changed = changed_files(root, tag, paths)
        if changed:
            shown = ', '.join(changed[:5]) + (f' and {len(changed) - 5} more'
                                              if len(changed) > 5 else '')
            problems.append(
                f'{project} {version} was published in {tag}, and its source changed since '
                f'({shown}): bump its version in {pyproject}.'
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('previous_manifest', type=Path)
    parser.add_argument('--root', type=Path, default=Path('.'))
    args = parser.parse_args(argv)
    manifest = json.loads(args.previous_manifest.read_text())
    problems = check(args.root, manifest)
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    print('Every package published under its current version is unchanged since.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
