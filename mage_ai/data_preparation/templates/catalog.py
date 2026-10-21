"""
The block templates as data (ADR-0026): each record of TEMPLATES (templates/constants.py)
is a language, a block type, a family (its groups), a source name and the file it renders.
The editor's menus come from the records; this module checks each one and builds the
coverage table, block type by family and language, that the docs page shows
(docs/guides/blocks/block-templates.mdx, written by scripts/docs/block_templates.py).
"""
import ast
import os
import re
from collections import defaultdict
from typing import Dict, List

from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.data_preparation.templates.constants import TEMPLATES

TEMPLATES_DIR = os.path.dirname(__file__)
LANGUAGES = (BlockLanguage.PYTHON, BlockLanguage.R, BlockLanguage.RUST)
LANGUAGE_NAMES = {
    BlockLanguage.PYTHON: 'Python',
    BlockLanguage.R: 'R',
    BlockLanguage.RUST: 'Rust',
}
BLOCK_TYPE_NAMES = {
    BlockType.DATA_LOADER: 'Data loaders',
    BlockType.TRANSFORMER: 'Transformers',
    BlockType.DATA_EXPORTER: 'Data exporters',
    BlockType.SENSOR: 'Sensors',
    BlockType.CUSTOM: 'Custom',
}
OTHER_FAMILY = 'Other'
# The decorator a Python block of the type uses, when it is not the type's name.
DECORATORS = {
    'conditional': 'condition',
}
# The function a Rust block of the type defines.
RUST_FUNCTIONS = {
    BlockType.DATA_LOADER: 'load_data',
    BlockType.TRANSFORMER: 'transform',
    BlockType.DATA_EXPORTER: 'export_data',
}


def _value(value) -> str:
    return str(getattr(value, 'value', value))


def render(record: Dict) -> str:
    """The code a block created from the record starts with."""
    from mage_ai.data_preparation.templates.template import fetch_template_source

    if record.get('data_source'):
        # Made with the source, as the menus make it.
        return fetch_template_source(
            record['block_type'], dict(data_source=record['data_source']),
            language=record['language'],
        )
    return fetch_template_source(
        record['block_type'], dict(template_path=record['path']), language=record['language'],
    )


def problems(record: Dict) -> List[str]:
    """What is wrong with a record; empty when its file renders into a block of its type."""
    name = f"{_value(record.get('language'))} {_value(record.get('block_type'))} " \
        f"{record.get('name')!r}"
    found = [
        f'{name} has no {field}.'
        for field in ('block_type', 'description', 'language', 'name', 'path')
        if not record.get(field)
    ]
    if found:
        return found
    if not os.path.isfile(os.path.join(TEMPLATES_DIR, record['path'])):
        return [f"{name}: {record['path']} does not exist in the templates folder."]
    if record.get('data_source') and not record.get('groups'):
        return [f"{name}: {record['path']} has no name and family in PYTHON_SOURCES "
                '(templates/constants.py).']
    try:
        code = render(record)
    except Exception as error:
        return [f"{name}: {record['path']} does not render: {error}"]
    language = _value(record['language'])
    block_type = _value(record['block_type'])
    if language == 'python':
        try:
            tree = ast.parse(code)
        except SyntaxError as error:
            return [f"{name}: {record['path']} is not valid Python: {error}"]
        decorators = {
            getattr(d, 'id', None) or getattr(getattr(d, 'func', None), 'id', None)
            for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
            for d in node.decorator_list
        }
        decorator = DECORATORS.get(block_type, block_type)
        if decorator not in decorators:
            return [f"{name}: {record['path']} has no function decorated with @{decorator}."]
    elif language == 'r':
        if not re.search(rf'#\*\s*@{block_type}\b', code):
            return [f"{name}: {record['path']} has no #* @{block_type} function."]
    elif language == 'rust':
        function = RUST_FUNCTIONS.get(record['block_type'])
        if function and not re.search(rf'\bfn\s+{function}\s*\(', code):
            return [f"{name}: {record['path']} defines no fn {function}."]
    return []


def coverage(records: List[Dict] = None) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """Per block type, per family, per language: the sources a template covers."""
    table: Dict[str, Dict[str, Dict[str, List[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)),
    )
    for record in records if records is not None else TEMPLATES:
        block_type = _value(record['block_type'])
        language = _value(record['language'])
        for family in record.get('groups') or [OTHER_FAMILY]:
            table[block_type][family][language].append(record['name'])
    return table


def coverage_markdown(records: List[Dict] = None) -> str:
    """The coverage table the docs page shows, one table per block type."""
    table = coverage(records)
    lines = []
    for block_type, title in BLOCK_TYPE_NAMES.items():
        families = table.get(_value(block_type))
        if not families:
            continue
        lines += [
            f'### {title}',
            '',
            '| Family | ' + ' | '.join(LANGUAGE_NAMES[lang] for lang in LANGUAGES) + ' |',
            '| --- |' + ' --- |' * len(LANGUAGES),
        ]
        for family in sorted(families, key=lambda f: (f == OTHER_FAMILY, f.lower())):
            cells = []
            for language in LANGUAGES:
                names = sorted(set(families[family].get(_value(language), [])), key=str.lower)
                cells.append(', '.join(names) if names else 'none yet')
            lines.append(f'| {family} | ' + ' | '.join(cells) + ' |')
        lines.append('')
    return '\n'.join(lines).rstrip() + '\n'
