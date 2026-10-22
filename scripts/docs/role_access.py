"""
Writes the table of who may do what through the API into
docs/production/authentication/access.mdx, between its markers, from the policies as
recorded in contracts/policy_matrix.json. The contract test fails when the page and the
matrix differ.

    .venv/bin/python scripts/docs/role_access.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / 'docs' / 'production' / 'authentication' / 'access.mdx'
MATRIX = ROOT / 'contracts' / 'policy_matrix.json'
START = '{/* access:start (scripts/docs/role_access.py) */}'
END = '{/* access:end */}'

ORDER = ['anonymous', 'none', 'viewer', 'editor', 'admin', 'owner']
LABELS = dict(
    anonymous='Anyone', none='Signed in', viewer='Viewer', editor='Editor', admin='Admin',
    owner='Owner',
)
ACTIONS = ['list', 'detail', 'create', 'update', 'delete']


def label(roles) -> str:
    """The lowest role allowed, as roles above it are allowed too; the list otherwise."""
    if not roles:
        return 'No one'
    lowest = min(ORDER.index(r) for r in roles)
    if set(roles) == set(ORDER[lowest:]):
        return LABELS[ORDER[lowest]]
    return ', '.join(LABELS[r] for r in sorted(roles, key=ORDER.index))


def table(matrix) -> str:
    cells = {}
    for resource, action, modes, roles in matrix['actions']:
        for mode in modes:
            cells.setdefault((resource, action), {})[mode] = label(roles)
    lines = [
        '| Resource | ' + ' | '.join(a.capitalize() for a in ACTIONS) + ' |',
        '| --- |' + ' --- |' * len(ACTIONS),
    ]
    for resource in sorted({r for r, _ in cells}):
        row = []
        for action in ACTIONS:
            by_mode = cells.get((resource, action), {})
            default = by_mode.get(0, 'No one')
            changed = any(by_mode.get(m, default) != default for m in (1, 2))
            row.append(f'{default} *' if changed else default)
        lines.append(f'| `{resource}` | ' + ' | '.join(row) + ' |')
    return '\n'.join(lines) + '\n'


def written(text: str, content: str) -> str:
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    return f'{before}{START}\n\n{content}\n{END}{after}'


def main() -> int:
    matrix = json.loads(MATRIX.read_text())
    PAGE.write_text(written(PAGE.read_text(), table(matrix)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
