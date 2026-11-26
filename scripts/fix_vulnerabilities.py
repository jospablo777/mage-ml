"""
Updates the dependencies that have known vulnerabilities to releases that fix every
advisory, in every lock the audit checks (scripts/audit_dependencies.sh): the root uv.lock and each
runtimes/* lock, every Cargo.lock and the frontend's yarn.lock. Each lock is audited again
afterwards, and a summary says what was fixed and what still needs a person:

    fixed     updated to a release without the advisory
    held      a fixed release exists, but another dependency's version range excludes it;
              the resolver's or cargo's message names that dependency
    no fix    no release fixes it yet
    accepted  listed with a reason in scripts/audit_dependencies.sh

Only locks change, within the version ranges MageML declares, with two exceptions in the
frontend: a transitive package whose ranges keep the vulnerable release gets a
`resolutions` entry, and a direct dependency gets its range raised, both only within the
installed major. uv and cargo move a package to the newest release its ranges allow. Run the
tests after it, as the Dependency fixes workflow does on the pull request it opens.

    uv run --no-sync python scripts/fix_vulnerabilities.py [--only python|rust|frontend]
        [--summary fixes.md] [--dry-run]

Exits 1 when a vulnerability with a fixed release is still there (held), 2 when a tool
failed, 0 otherwise.
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# The rules the Workers page uses too.
from mage_ai.shared.vulnerabilities import (  # noqa: E402
    compatible,
    first_fix,
    fix_target,
    version_key,
)

AUDIT_SCRIPT = ROOT / 'scripts' / 'audit_dependencies.sh'
FRONTEND = ROOT / 'mage_ai' / 'frontend'
NO_FIX_NPM = '<0.0.0'
DEPENDENCY_SECTIONS = ('dependencies', 'devDependencies', 'optionalDependencies')


@dataclass
class Finding:
    ecosystem: str
    lock: str
    package: str
    version: str
    advisories: Set[str] = field(default_factory=set)
    # The versions each advisory is fixed in; an empty list: no fixed release.
    fixed_in: Dict[str, List[str]] = field(default_factory=dict)
    accepted: Set[str] = field(default_factory=set)
    status: str = ''
    to: str = ''
    note: str = ''


def run(args: List[str], cwd: Path = ROOT, check: bool = False) -> subprocess.CompletedProcess:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f'{" ".join(args)} failed:\n{result.stderr or result.stdout}')
    return result


def accepted_advisories() -> Set[str]:
    """The advisories audit_dependencies.sh ignores, each with its reason there."""
    text = AUDIT_SCRIPT.read_text()
    return set(re.findall(r'--ignore(?:-until-fixed)? ((?:GHSA|PYSEC|RUSTSEC|CVE)-[\w-]+)', text))


def lower_bounds(requirement: str) -> List[str]:
    """The versions a range such as '>=1.2.3, <2' or '^1.4.2 || >=2.0.1' starts at."""
    return re.findall(r'(?:>=|\^|~|=)\s*v?(\d+(?:\.\d+)*)', requirement)


def target(finding: Finding) -> Optional[str]:
    """The smallest version that fixes every advisory with a fixed release."""
    return fix_target(finding.version.split(', ')[0], finding.fixed_in)


def unfixable(finding: Finding) -> Set[str]:
    current = finding.version.split(', ')[0]
    return {a for a, versions in finding.fixed_in.items() if not first_fix(current, versions)}


def settle(finding: Finding, remaining: Set[str], to: str) -> None:
    """The status once the lock was updated: remaining are the advisories still found."""
    without_fix = unfixable(finding)
    if remaining - without_fix:
        finding.status = 'held'
    elif remaining:
        finding.status = 'no fix'
        finding.to = to
        finding.note = (finding.note + ' ' if finding.note else '') + (
            f'{len(remaining)} advisories have no fixed release yet.'
        )
    else:
        finding.status = 'fixed'
        finding.to = to


def short(message: str, lines: int = 6) -> str:
    kept = [
        line.strip() for line in message.strip().splitlines()
        if line.strip() and not line.strip().startswith(('Using ', 'Updating ', 'Locking '))
    ]
    return ' '.join(kept[-lines:])[:600]


# Python: uv audit (OSV) on each uv project, uv lock --upgrade-package name>=fixed.

def python_projects() -> List[Path]:
    projects = [ROOT]
    projects += sorted(p.parent for p in (ROOT / 'runtimes').glob('*/uv.lock'))
    return projects


def python_findings(project: Path, accepted: Set[str]) -> Dict[str, Finding]:
    result = run([
        'uv', 'audit', '--frozen', '--preview-features', 'audit-command',
        '--project', str(project), '--output-format', 'json',
    ])
    try:
        report = json.loads(result.stdout)
    except ValueError:
        raise RuntimeError(f'uv audit failed for {project}:\n{result.stderr}')
    lock = str((project / 'uv.lock').relative_to(ROOT))
    findings: Dict[str, Finding] = {}
    aliases: Dict[str, List[Set[str]]] = {}
    for entry in report.get('vulnerabilities') or []:
        name = entry['dependency']['name']
        finding = findings.setdefault(name, Finding(
            'python', lock, name, entry['dependency']['version'],
        ))
        ids = {entry['id'], *(entry.get('aliases') or [])}
        if ids & accepted:
            finding.accepted.add(entry['id'])
            continue
        # OSV lists a GHSA advisory and its PYSEC copy as two entries: keep one.
        if any(ids & known for known in aliases.get(name, [])):
            continue
        aliases.setdefault(name, []).append(ids)
        finding.advisories.add(entry['id'])
        finding.fixed_in[entry['id']] = list(entry.get('fix_versions') or [])
    return findings


def uv_lock(project: Path, upgrades: List[Tuple[str, str]]) -> subprocess.CompletedProcess:
    args = ['uv', 'lock', '--project', str(project)]
    for name, version in upgrades:
        args += ['--upgrade-package', f'{name}>={version}']
    return run(args)


def locked_version(project: Path, name: str) -> str:
    text = (project / 'uv.lock').read_text()
    match = re.search(rf'\[\[package\]\]\nname = "{re.escape(name)}"\nversion = "([^"]+)"', text)
    return match.group(1) if match else 'removed'


def fix_python(dry_run: bool, accepted: Set[str]) -> List[Finding]:
    results = []
    for project in python_projects():
        findings = python_findings(project, accepted)
        todo = classify(findings.values(), dry_run)
        if todo and not dry_run:
            # All at once when they resolve together, else one by one so one held package
            # does not keep the others back.
            together = uv_lock(project, [(f.package, f.to) for f in todo])
            if together.returncode != 0:
                for finding in todo:
                    alone = uv_lock(project, [(finding.package, finding.to)])
                    if alone.returncode != 0:
                        finding.note = short(alone.stderr)
            after = python_findings(project, accepted)
            for finding in todo:
                remaining = after[finding.package].advisories if finding.package in after else set()
                settle(finding, remaining, locked_version(project, finding.package))
        results += findings.values()
    return results


def classify(findings: Iterable[Finding], dry_run: bool) -> List[Finding]:
    """Sets the status of the findings nothing can fix; returns those to update."""
    todo = []
    for finding in findings:
        if not finding.advisories:
            finding.status = 'accepted'
            continue
        finding.to = target(finding) or ''
        if not finding.to:
            finding.status = 'no fix'
            continue
        finding.status = 'fixable' if dry_run else ''
        todo.append(finding)
    return todo


# Rust: cargo audit on each Cargo.lock, cargo update -p name@version.

def cargo_locks() -> List[Path]:
    listed = run(['git', 'ls-files', '*Cargo.lock'], check=True).stdout.split()
    return [ROOT / path for path in listed]


def rust_findings(lock: Path, accepted: Set[str]) -> Dict[Tuple[str, str], Finding]:
    result = run(['cargo', 'audit', '--json', '--file', str(lock)])
    try:
        report = json.loads(result.stdout)
    except ValueError:
        raise RuntimeError(f'cargo audit failed for {lock}:\n{result.stderr}')
    entries = list((report.get('vulnerabilities') or {}).get('list') or [])
    for warnings in (report.get('warnings') or {}).values():
        entries += warnings
    findings: Dict[Tuple[str, str], Finding] = {}
    for entry in entries:
        package = entry['package']
        advisory = entry.get('advisory') or {}
        finding = findings.setdefault((package['name'], package['version']), Finding(
            'rust', str(lock.relative_to(ROOT)), package['name'], package['version'],
        ))
        advisory_id = advisory.get('id') or entry.get('kind', 'warning')
        if advisory_id in accepted or set(advisory.get('aliases') or []) & accepted:
            finding.accepted.add(advisory_id)
            continue
        finding.advisories.add(advisory_id)
        fixes = finding.fixed_in.setdefault(advisory_id, [])
        if entry.get('kind') == 'yanked':
            # Any newer release will do; cargo update picks the newest compatible one.
            fixes.append(bump_patch(package['version']))
        for requirement in (entry.get('versions') or {}).get('patched') or []:
            fixes += lower_bounds(requirement)
    return findings


def bump_patch(version: str) -> str:
    parts = [*version_key(version).release, 0, 0][:3]
    return f'{parts[0]}.{parts[1]}.{parts[2] + 1}'


def cargo_locked_version(lock: Path, name: str, old: str) -> str:
    versions = re.findall(
        rf'\[\[package\]\]\nname = "{re.escape(name)}"\nversion = "([^"]+)"', lock.read_text(),
    )
    newer = [v for v in versions if version_key(v) > version_key(old)]
    return ', '.join(sorted(newer, key=version_key)) or 'removed'


def _rust_workspace_module():
    """
    MageML's Rust workspace module, by its file: it needs only the standard library, and
    its package imports all of MageML's dependencies, which the workflow does not install.
    """
    import importlib.util

    path = ROOT / 'mage_ai/data_preparation/models/block/rust/workspace.py'
    spec = importlib.util.spec_from_file_location('mageml_rust_workspace', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextmanager
def generated_members(root: Path):
    """
    A MageML project's Rust workspace (<project>/rust) lists generated crates as members:
    MageML's SDK in .mage/sdk and one crate per block in .mage/blocks, which git ignores, so
    cargo cannot load the workspace of a fresh checkout. They are written for cargo here, as
    MageML writes them (each block depends on every workspace crate), and removed after.
    """
    try:
        members = tomllib.loads((root / 'Cargo.toml').read_text()).get(
            'workspace', {}).get('members') or []
    except (OSError, tomllib.TOMLDecodeError):
        members = []
    generated = root / '.mage'
    if '.mage/blocks/*' not in members or generated.exists():
        yield
        return
    ws = _rust_workspace_module()
    try:
        workspace = ws.Workspace(root)
        ws.sync_sdk(workspace)
        lock = tomllib.loads((root / 'Cargo.lock').read_text())
        for package in lock.get('package') or []:
            # The workspace's own crates have no source; mage-block is the SDK.
            if 'source' not in package and package['name'] != 'mage-block':
                ws.write_block_crate(workspace, package['name'], {'main.rs': 'fn main() {}\n'})
        yield
    finally:
        shutil.rmtree(generated, ignore_errors=True)


def fix_rust(dry_run: bool, accepted: Set[str]) -> List[Finding]:
    results = []
    for lock in cargo_locks():
        findings = rust_findings(lock, accepted)
        todo = classify(findings.values(), dry_run)
        if todo and not dry_run:
            with generated_members(lock.parent):
                update_rust(lock, todo, accepted)
        results += findings.values()
    return results


def update_rust(lock: Path, todo: List[Finding], accepted: Set[str]) -> None:
    manifest = lock.parent / 'Cargo.toml'
    for finding in todo:
        spec = f'{finding.package}@{finding.version}'
        update = ['cargo', 'update', '--manifest-path', str(manifest), '-p', spec]
        updated = run(update)
        still = rust_findings(lock, accepted).get((finding.package, finding.version))
        if updated.returncode == 0 and still is not None and still.advisories:
            # The newest compatible release was not enough: ask for the fixed one.
            updated = run(update + ['--precise', finding.to])
        if updated.returncode != 0:
            finding.note = short(updated.stderr)
    after = rust_findings(lock, accepted)
    for finding in todo:
        remaining = set()
        for (name, _), found in after.items():
            if name == finding.package:
                remaining |= found.advisories & finding.advisories
        settle(finding, remaining,
               cargo_locked_version(lock, finding.package, finding.version))
        if finding.status == 'held' and not finding.note:
            finding.note = (
                f'A version range excludes the fixed release (cargo tree -i '
                f'{finding.package}@{finding.version}).'
            )


# Frontend: yarn audit; a transitive package is resolved again from its ranges, then
# pinned with a resolution within its line when its ranges keep the vulnerable release.

def frontend_findings(accepted: Set[str]) -> Dict[str, Finding]:
    run(['yarn', 'install', '--frozen-lockfile', '--ignore-scripts', '--non-interactive'],
        cwd=FRONTEND)
    result = run(['yarn', 'audit', '--json'], cwd=FRONTEND)
    findings: Dict[str, Finding] = {}
    for line in result.stdout.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get('type') != 'auditAdvisory':
            continue
        advisory = entry['data']['advisory']
        name = advisory['module_name']
        installed = sorted(
            {f['version'] for f in advisory.get('findings') or []}, key=version_key,
        )
        finding = findings.setdefault(name, Finding(
            'frontend', 'mage_ai/frontend/yarn.lock', name, ', '.join(installed),
        ))
        if len(installed) > len(finding.version.split(', ')):
            finding.version = ', '.join(installed)
        advisory_id = advisory.get('github_advisory_id') or str(advisory['id'])
        if advisory_id in accepted:
            finding.accepted.add(advisory_id)
            continue
        finding.advisories.add(advisory_id)
        patched = advisory.get('patched_versions') or NO_FIX_NPM
        finding.fixed_in[advisory_id] = [] if patched == NO_FIX_NPM else lower_bounds(patched)
    return findings


def remove_lock_entries(name: str) -> None:
    lock = FRONTEND / 'yarn.lock'
    blocks = lock.read_text().split('\n\n')

    def names(block: str) -> Set[str]:
        header = block.lstrip('\n').split('\n', 1)[0]
        if header.startswith('#') or not header.endswith(':'):
            return set()
        specs = [s.strip().strip('"') for s in header[:-1].split(',')]
        return {spec.rsplit('@', 1)[0] for spec in specs}

    lock.write_text('\n\n'.join(b for b in blocks if name not in names(b)))


def is_direct(name: str) -> bool:
    package = json.loads((FRONTEND / 'package.json').read_text())
    return any(name in package.get(s, {}) for s in DEPENDENCY_SECTIONS)


def raise_range(name: str, version: str) -> None:
    """A direct dependency's range is raised to ^version; a transitive one is resolved to
    it."""
    path = FRONTEND / 'package.json'
    package = json.loads(path.read_text())
    sections = [s for s in DEPENDENCY_SECTIONS if name in package.get(s, {})]
    for section in sections:
        package[section][name] = f'^{version}'
    if not sections:
        package.setdefault('resolutions', {})[f'**/{name}'] = f'^{version}'
    path.write_text(json.dumps(package, indent=2) + '\n')


def frontend_version(name: str) -> str:
    result = run(['yarn', 'list', '--pattern', name, '--depth=100', '--json'], cwd=FRONTEND)
    versions = set(re.findall(rf'"{re.escape(name)}@([^"]+)"', result.stdout))
    return ', '.join(sorted(versions, key=version_key)) or 'removed'


def fix_frontend(dry_run: bool, accepted: Set[str]) -> List[Finding]:
    findings = frontend_findings(accepted)
    todo = classify(findings.values(), dry_run)
    if dry_run or not todo:
        return list(findings.values())

    install = ['yarn', 'install', '--ignore-scripts', '--non-interactive']
    # First within the ranges the dependents declare.
    for finding in todo:
        remove_lock_entries(finding.package)
    run(install, cwd=FRONTEND, check=True)
    after = frontend_findings(accepted)
    pinned = False
    for finding in todo:
        if finding.package not in after or not (
            after[finding.package].advisories - unfixable(finding)
        ):
            continue
        installed = after[finding.package].version.split(', ')
        if {compatible(v) for v in installed} == {compatible(finding.to)}:
            raise_range(finding.package, finding.to)
            pinned = True
        elif is_direct(finding.package):
            finding.note = (
                f'A direct dependency: the fix ({finding.to}) is in another line, so its '
                'range in package.json needs raising by hand, with a check of the code.'
            )
        else:
            finding.note = (
                f'Installed as {", ".join(installed)}; the fix ({finding.to}) is in another '
                'line, so a resolution would change what the other dependents get.'
            )
    if pinned:
        run(install, cwd=FRONTEND, check=True)
        after = frontend_findings(accepted)
    for finding in todo:
        remaining = after[finding.package].advisories if finding.package in after else set()
        settle(finding, remaining, frontend_version(finding.package))
    return list(findings.values())


def summary(findings: List[Finding]) -> str:
    order = ['fixed', 'held', 'fixable', 'no fix', 'accepted']
    lines = ['| Status | Lock | Package | From | To | Advisories | Note |',
             '|---|---|---|---|---|---|---|']
    for finding in sorted(findings, key=lambda f: (order.index(f.status), f.lock, f.package)):
        lines.append('| {} | {} | {} | {} | {} | {} | {} |'.format(
            finding.status, finding.lock, finding.package, finding.version, finding.to or '-',
            ', '.join(sorted(finding.advisories or finding.accepted)) or '-',
            finding.note.replace('|', '/') or '',
        ))
    counts = {s: sum(f.status == s for f in findings) for s in order}
    head = ', '.join(f'{n} {s}' for s, n in counts.items() if n) or 'No vulnerabilities'
    return head + '.\n\n' + ('\n'.join(lines) if findings else '')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--only', choices=['python', 'rust', 'frontend'], action='append')
    parser.add_argument('--summary', help='Also write the summary to this Markdown file.')
    parser.add_argument('--dry-run', action='store_true', help='Report; change nothing.')
    options = parser.parse_args()

    accepted = accepted_advisories()
    fixers = dict(python=fix_python, rust=fix_rust, frontend=fix_frontend)
    findings: List[Finding] = []
    for name in options.only or list(fixers):
        tool = dict(python='uv', rust='cargo', frontend='yarn')[name]
        if shutil.which(tool) is None:
            print(f'{tool} not found: {name} skipped.', file=sys.stderr)
            continue
        print(f'== {name}', file=sys.stderr)
        try:
            findings += fixers[name](options.dry_run, accepted)
        except RuntimeError as error:
            print(error, file=sys.stderr)
            return 2

    text = summary(findings)
    print(text)
    if options.summary:
        Path(options.summary).write_text(text + '\n')
    return 1 if any(f.status == 'held' for f in findings) else 0


if __name__ == '__main__':
    sys.exit(main())
