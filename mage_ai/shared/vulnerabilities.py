"""
Known vulnerabilities in a uv project's lock, from `uv audit` (OSV), and the release that
fixes them: shared by the Workers page (data_preparation/python_workers.py) and the
repository's fixer (scripts/fix_vulnerabilities.py).

A package's fix is the smallest release that fixes every advisory with a fixed release,
preferring the package's current line (1.4.x before 2.x); one advisory's fix alone could
leave another open (I-454).
"""
import json
import os
import subprocess
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from packaging.version import InvalidVersion, Version


def version_key(value: str) -> Version:
    try:
        return Version(value)
    except InvalidVersion:
        return Version('0')


def compatible(version: str) -> Tuple[int, ...]:
    """The semver line a version belongs to: 1.4.2 -> (1,), 0.39.4 -> (0, 39)."""
    release = version_key(version).release
    return release[:1] if release[:1] != (0,) else release[:2]


def first_fix(current: str, candidates: Iterable[str]) -> Optional[str]:
    """The smallest fixed version above current, preferring one in current's line."""
    newer = sorted(
        {c for c in candidates if version_key(c) > version_key(current)}, key=version_key,
    )
    if not newer:
        return None
    same_line = [c for c in newer if compatible(c) == compatible(current)]
    return (same_line or newer)[0]


def fix_target(current: str, fixed_in: Dict[str, List[str]]) -> Optional[str]:
    """The smallest version that fixes every advisory with a fixed release."""
    fixes = [first_fix(current, versions) for versions in fixed_in.values()]
    fixes = [f for f in fixes if f]
    return max(fixes, key=version_key) if fixes else None


@dataclass
class Advisory:
    id: str
    aliases: List[str]
    summary: str
    link: Optional[str]
    fix_versions: List[str]


@dataclass
class VulnerablePackage:
    package: str
    version: str
    advisories: List[Advisory] = field(default_factory=list)

    @property
    def fix(self) -> Optional[str]:
        """The release that fixes every advisory that has a fix; None when none has one."""
        return fix_target(self.version.split(', ')[0],
                          {a.id: a.fix_versions for a in self.advisories})

    @property
    def unfixed(self) -> List[str]:
        """The advisories no release fixes yet."""
        current = self.version.split(', ')[0]
        return [a.id for a in self.advisories if not first_fix(current, a.fix_versions)]


class AuditError(Exception):
    """uv could not audit the lock; offline is True when it could not reach OSV."""

    def __init__(self, message: str, offline: bool = False):
        super().__init__(message)
        self.offline = offline


def audit(project: str, uv: str = 'uv', timeout: int = 300) -> List[VulnerablePackage]:
    """The vulnerable packages of the uv project's lock, by name; OSV lists an advisory
    and its PYSEC copy as two entries, which count once."""
    result = subprocess.run(
        [uv, 'audit', '--frozen', '--preview-features', 'audit-command', '--project', project,
         '--output-format', 'json'],
        capture_output=True, text=True, timeout=timeout,
        # Without colors, which FORCE_COLOR turns on: the error is shown as text.
        env=dict(os.environ, NO_COLOR='1'),
    )
    try:
        report = json.loads(result.stdout)
    except ValueError:
        text = (result.stderr or result.stdout or '').strip()
        offline = any(s in text.lower() for s in (
            'failed to fetch', 'error sending request', 'dns error', 'network is unreachable',
            'offline', 'timed out'))
        raise AuditError(text.splitlines()[-1] if text else 'uv audit printed nothing.',
                         offline=offline)
    return parse(report)


def parse(report: Dict) -> List[VulnerablePackage]:
    packages: Dict[str, VulnerablePackage] = {}
    seen: Dict[str, List[set]] = {}
    for entry in report.get('vulnerabilities') or []:
        name = entry['dependency']['name']
        package = packages.setdefault(name, VulnerablePackage(
            name, entry['dependency']['version']))
        ids = {entry['id'], *(entry.get('aliases') or [])}
        if any(ids & known for known in seen.get(name, [])):
            continue
        seen.setdefault(name, []).append(ids)
        package.advisories.append(Advisory(
            id=entry.get('display_id') or entry['id'],
            aliases=list(entry.get('aliases') or []),
            summary=(entry.get('summary') or '').strip(),
            link=entry.get('link'),
            fix_versions=list(entry.get('fix_versions') or []),
        ))
    return sorted(packages.values(), key=lambda p: p.package)
