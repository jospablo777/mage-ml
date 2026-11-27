"""
Packages of an R or Rust environment as people write them, added to its manifest: the R
packages of rproject.toml (rv) and the crates of Cargo.toml's [workspace.dependencies],
edited with tomlkit so the file keeps its comments and layout (ADR-0040).

    R      dplyr                  a CRAN package
           tidyverse/dplyr@v1.1.4 a GitHub repository, at a tag
    Rust   regex                  the newest version on crates.io
           regex@1.11             a version requirement
           polars@0.55[lazy,sql]  with features

rv pins R versions with the repository snapshot a lock resolves against, not per package:
`with_r_snapshot` points the Posit Package Manager repository at a date. The Rust toolchain
is the channel of rust-toolchain.toml.
"""
import json
import re
import sys
import urllib.request
from typing import Callable, List, Optional, Tuple

import tomlkit

R_NAME = re.compile(r'^[A-Za-z][A-Za-z0-9.]*[A-Za-z0-9]$|^[A-Za-z]$')
GITHUB = re.compile(r'^(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+)(?:@(?P<ref>\S+))?$')
CRATE = re.compile(
    r'^(?P<name>[A-Za-z0-9_-]+)(?:@(?P<version>[^\[\]\s]+))?(?:\[(?P<features>[^\]]*)\])?$'
)
TOOLCHAIN = re.compile(r'^(stable|beta|nightly(-\d{4}-\d{2}-\d{2})?|\d+\.\d+(\.\d+)?)$')
SNAPSHOT = re.compile(r'^\d{4}-\d{2}-\d{2}$')
PPM = 'https://packagemanager.posit.co/cran'


class PackageError(Exception):
    pass


def split(text: str) -> List[str]:
    """Packages written one per line, or separated by commas or spaces."""
    return [p for p in re.split(r'[\s,]+', text or '') if p]


def _r_item(spec: str):
    found = GITHUB.match(spec)
    if found:
        item = tomlkit.inline_table()
        item['name'] = found['repo']
        item['git'] = f'https://github.com/{found["owner"]}/{found["repo"]}'
        if found['ref']:
            item['tag'] = found['ref']
        return found['repo'], item
    if not R_NAME.match(spec):
        raise PackageError(
            f'{spec!r} is not an R package: a CRAN name such as dplyr, or a GitHub '
            'repository such as tidyverse/dplyr (with @tag to pin it).'
        )
    from mage_ai.data_preparation.models.block.r import runtime

    # As the default environment writes it (runtime._dependency_line).
    if sys.platform == 'darwin' and spec in runtime.MACOS_CRAN_PACKAGES:
        item = tomlkit.inline_table()
        item['name'] = spec
        item['repository'] = 'CRAN'
        return spec, item
    return spec, spec


def _name_of(item) -> str:
    return item if isinstance(item, str) else str(item.get('name', ''))


def add_r_packages(manifest: str, specs: List[str]) -> Tuple[str, List[str]]:
    """rproject.toml with the packages added to its dependencies; returns the text and the
    names added (those listed already are left as they are)."""
    document = tomlkit.parse(manifest)
    project = document.setdefault('project', tomlkit.table())
    dependencies = project.get('dependencies')
    if dependencies is None:
        dependencies = tomlkit.array()
        project['dependencies'] = dependencies
    dependencies.multiline(True)
    listed = {_name_of(item).lower() for item in dependencies}
    added = []
    for spec in specs:
        name, item = _r_item(spec)
        if name.lower() in listed:
            continue
        dependencies.append(item)
        listed.add(name.lower())
        added.append(name)
    return tomlkit.dumps(document), added


def with_r_snapshot(manifest: str, date: str) -> str:
    """rproject.toml with Posit Package Manager's CRAN snapshot of `date` as its first
    repository: the versions of that day, with binaries for them."""
    if not SNAPSHOT.match(date or ''):
        raise PackageError(f'{date!r} is not a snapshot date such as 2026-10-12.')
    document = tomlkit.parse(manifest)
    project = document.setdefault('project', tomlkit.table())
    repositories = project.get('repositories')
    if repositories is None:
        repositories = tomlkit.array()
        project['repositories'] = repositories
    url = f'{PPM}/{date}'
    for repository in repositories:
        if str(repository.get('url', '')).startswith(PPM):
            repository['url'] = url
            return tomlkit.dumps(document)
    item = tomlkit.inline_table()
    item['alias'] = 'PPM'
    item['url'] = url
    repositories.insert(0, item)
    repositories.multiline(True)
    return tomlkit.dumps(document)


def newest_crate_version(name: str, timeout: float = 10) -> str:
    """The newest version of a crate on crates.io, as a requirement (`1.11`)."""
    request = urllib.request.Request(
        f'https://crates.io/api/v1/crates/{name}',
        headers={'User-Agent': 'mageml (https://github.com/jospablo777/mage-ml)'},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            crate = json.loads(response.read()).get('crate') or {}
    except OSError as error:
        if getattr(error, 'code', None) == 404:
            raise PackageError(f'crates.io has no crate {name}.') from None
        raise PackageError(
            f'crates.io did not answer for {name} ({error}): give its version, such as '
            f'{name}@1.'
        ) from None
    version = crate.get('max_stable_version') or crate.get('max_version')
    if not version:
        raise PackageError(f'crates.io has no version of {name}.')
    major, minor, *_ = version.split('.') + ['0']
    return f'{major}.{minor}' if major != '0' else f'0.{minor}'


def add_crates(
    cargo_toml: str,
    specs: List[str],
    newest: Optional[Callable[[str], str]] = None,
) -> Tuple[str, List[str]]:
    """Cargo.toml with the crates added to [workspace.dependencies], which every Rust block
    of the environment can use; returns the text and the crates added. A crate listed
    already is left as it is."""
    newest = newest or newest_crate_version
    document = tomlkit.parse(cargo_toml)
    workspace = document.setdefault('workspace', tomlkit.table())
    dependencies = workspace.setdefault('dependencies', tomlkit.table())
    added = []
    for spec in specs:
        found = CRATE.match(spec)
        if not found:
            raise PackageError(
                f'{spec!r} is not a crate: a name such as regex, with @version and '
                '[features] if needed, such as polars@0.55[lazy,sql].'
            )
        name = found['name']
        if name in dependencies:
            continue
        version = found['version'] or newest(name)
        features = [f.strip() for f in (found['features'] or '').split(',') if f.strip()]
        if features:
            item = tomlkit.inline_table()
            item['version'] = version
            item['features'] = features
            dependencies[name] = item
        else:
            dependencies[name] = version
        added.append(name)
    return tomlkit.dumps(document), added


def with_toolchain(toolchain_toml: str, channel: str) -> str:
    """rust-toolchain.toml with the Rust version or channel blocks build with."""
    if not TOOLCHAIN.match(channel or ''):
        raise PackageError(
            f'{channel!r} is not a Rust toolchain: a version such as 1.98.0, or stable, beta '
            'or nightly.'
        )
    document = tomlkit.parse(toolchain_toml or '')
    toolchain = document.setdefault('toolchain', tomlkit.table())
    toolchain['channel'] = channel
    return tomlkit.dumps(document)


def with_rust_essentials(cargo_toml: str) -> str:
    """A Cargo.toml people bring, with what MageML's Rust blocks need: the block crates as
    members and MageML's block SDK among the workspace dependencies."""
    document = tomlkit.parse(cargo_toml)
    workspace = document.setdefault('workspace', tomlkit.table())
    workspace.setdefault('resolver', '3')
    members = workspace.get('members')
    if members is None:
        members = tomlkit.array()
        workspace['members'] = members
    if '.mage/blocks/*' not in [str(m) for m in members]:
        members.append('.mage/blocks/*')
    dependencies = workspace.setdefault('dependencies', tomlkit.table())
    if 'mage-block' not in dependencies:
        sdk = tomlkit.inline_table()
        sdk['path'] = '.mage/sdk'
        sdk['features'] = ['postgres']
        dependencies['mage-block'] = sdk
    return tomlkit.dumps(document)
