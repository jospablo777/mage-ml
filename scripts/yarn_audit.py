"""
Fails on frontend advisories that have a fixed version, and reports those with no fixed
version as warnings: yarn audit fails on both, so an advisory nobody can fix would keep
the check red and hide new ones.

    cd mage_ai/frontend && yarn audit --json | python ../../scripts/yarn_audit.py
"""
import json
import sys

NO_FIX = '<0.0.0'


def main(lines) -> int:
    advisories = {}
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get('type') != 'auditAdvisory':
            continue
        data = entry['data']
        advisory = data['advisory']
        key = advisory.get('github_advisory_id') or advisory['id']
        found = advisories.setdefault(key, dict(advisory=advisory, paths=set()))
        found['paths'].add(data['resolution']['path'])
    fixable = 0
    for key, found in sorted(advisories.items()):
        advisory = found['advisory']
        path = sorted(found['paths'])[0]
        text = (
            f"{advisory['severity']} {advisory['module_name']} {advisory['vulnerable_versions']}: "
            f"{advisory['title']} ({key}), through {path}"
        )
        if advisory.get('patched_versions') == NO_FIX:
            print(f'::warning title=No fixed version::{text}')
        else:
            fixable += 1
            print(f"::error title=Fixed in {advisory['patched_versions']}::{text}")
    print(f'{fixable} advisories with a fix, {len(advisories) - fixable} without.')
    return 1 if fixable else 0


if __name__ == '__main__':
    sys.exit(main(sys.stdin))
