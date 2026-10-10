"""
Prints the failed, errored and skipped tests of JUnit XML files as GitHub Actions
annotations, so a failed CI run shows which tests failed and why on the run's page and
through the checks API.

    python scripts/junit_annotations.py integration_tests/results.xml
"""
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# GitHub shows at most 10 annotations of each level per step.
LIMIT = 10
MESSAGE_CHARS = 2000


def _escape(text: str) -> str:
    return text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')


def _property(text: str) -> str:
    return _escape(text).replace(':', '%3A').replace(',', '%2C')


def _cases(path: Path):
    root = ET.parse(path).getroot()
    for case in root.iter('testcase'):
        name = '::'.join(filter(None, [case.get('classname'), case.get('name')]))
        for kind in ('failure', 'error', 'skipped'):
            outcome = case.find(kind)
            if outcome is not None:
                detail = (outcome.get('message') or '') + '\n' + (outcome.text or '')
                yield kind, name, case.get('file'), case.get('line'), detail.strip()


def main(paths) -> int:
    counts = {'failure': 0, 'error': 0, 'skipped': 0}
    for path in map(Path, paths):
        if not path.is_file():
            print(f'::warning title=No test results::{_escape(str(path))} does not exist')
            continue
        for kind, name, file, line, detail in _cases(path):
            counts[kind] += 1
            level = 'warning' if kind == 'skipped' else 'error'
            if counts[kind] > LIMIT:
                continue
            location = ''
            if file:
                location = f'file={_property(file)},'
                if line and line.isdigit():
                    location += f'line={int(line) + 1},'
            print(f'::{level} {location}title={_property(name)}::'
                  f'{_escape(detail[-MESSAGE_CHARS:])}')
    failed = counts['failure'] + counts['error']
    print(f'::notice title=Test results::{failed} failed, {counts["skipped"]} skipped')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
