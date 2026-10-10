"""
Fails when the installed package's Python files differ from the source tree.

The Docker image installs Mage as a wheel. A cached wheel from an earlier build once
gave images old code; this check stops such a build.

    python scripts/check_installed_source.py mage_ai
"""
import hashlib
import importlib.util
import sys
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(package: str) -> int:
    source = Path(package)
    spec = importlib.util.find_spec(package)
    if spec is None or not spec.submodule_search_locations:
        print(f'{package} is not installed.')
        return 1
    installed = Path(list(spec.submodule_search_locations)[0])
    if installed.resolve() == source.resolve():
        print(f'{package} is installed from the source tree; nothing to compare.')
        return 0
    different, missing = [], []
    for path in sorted(source.rglob('*.py')):
        relative = path.relative_to(source)
        if 'frontend' in relative.parts or 'tests' in relative.parts:
            continue
        target = installed / relative
        if not target.exists():
            missing.append(str(relative))
        elif digest(target) != digest(path):
            different.append(str(relative))
    if different or missing:
        print(f'The installed {package} does not match its source ({installed}):')
        for name in different[:20]:
            print(f'  differs: {name}')
        for name in missing[:20]:
            print(f'  missing: {name}')
        return 1
    print(f'The installed {package} matches its source.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else 'mage_ai'))
