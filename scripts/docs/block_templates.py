"""
Writes the coverage table of the block templates into docs/guides/blocks/block-templates.mdx,
between its markers, from the template records (mage_ai/data_preparation/templates). The
catalog test fails when the page and the records differ.

    .venv/bin/python scripts/docs/block_templates.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / 'docs' / 'guides' / 'blocks' / 'block-templates.mdx'
START = '{/* coverage:start (scripts/docs/block_templates.py) */}'
END = '{/* coverage:end */}'


def written(text: str, table: str) -> str:
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    return f'{before}{START}\n\n{table}\n{END}{after}'


def main() -> int:
    sys.path.insert(0, str(ROOT))
    from mage_ai.data_preparation.templates.catalog import coverage_markdown

    PAGE.write_text(written(PAGE.read_text(), coverage_markdown()))
    return 0


if __name__ == '__main__':
    sys.exit(main())
