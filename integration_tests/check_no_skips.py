"""
Fail when a JUnit report lists skipped tests.

Integration tests skip when their service is not configured. In CI every service is
configured, so a skip means a test did not run.
"""

import sys
import xml.etree.ElementTree as ElementTree


def main(path: str) -> int:
    skipped = [
        f'{case.get("classname")}::{case.get("name")}'
        for case in ElementTree.parse(path).iter('testcase')
        if case.find('skipped') is not None
    ]
    for name in skipped:
        print(f'skipped: {name}')
    return 1 if skipped else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
