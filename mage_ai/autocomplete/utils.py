import ast
import asyncio
import os
import pathlib
import re
from functools import reduce

import aiofiles

from mage_ai.cache.mem_lru import MemorySizeLRUCache, async_cache_file_read
from mage_ai.shared.utils import files_in_path

root_path = '/'.join(str(pathlib.Path(__file__).parent.resolve()).split('/')[:-2])


FILE_EXTENSIONS_TO_INCLUDE = [
    '.py',
]
PATHS_TO_TRAVERSE = [
    f'{root_path}/mage_ai/io',
]
FILES_TO_READ = [
    f'{root_path}/mage_ai/data_cleaner/transformer_actions/constants.py',
    f'{root_path}/mage_ai/data_cleaner/transformer_actions/utils.py',
]


def add_file(acc, path):
    files = files_in_path(path)

    def __should_include(file_name):
        tup = os.path.splitext(file_name)
        if len(tup) >= 2:
            file_extension = tup[1]
            return file_extension in FILE_EXTENSIONS_TO_INCLUDE

        return True

    return acc + list(filter(__should_include, files))


def extract_all_classes(file_content):
    regex_base = r'([A-Za-z_]+)\(*[A-Za-z_, ]*\)*:'
    regex = re.compile(f'^class {regex_base}|\nclass {regex_base}')
    return [t[0] or t[1] for t in re.findall(regex, file_content)]


def extract_all_constants(file_content):
    regex_base = '([A-Z_]+)[ ]*=[ ]*'
    regex = re.compile(f'^{regex_base}|\n{regex_base}')
    return [t[0] or t[1] for t in re.findall(regex, file_content)]


def extract_all_functions(file_content):
    regex_base = r'([A-Za-z_]+)\('
    regex = re.compile(f'^def {regex_base}|\ndef {regex_base}')
    return [t[0] or t[1] for t in re.findall(regex, file_content)]


def extract_all_imports(file_content, ignore_nesting=False):
    base_regexes = [
        r'import [\w.]+ as [\w.]+',
        r'import [\w.]+',
        r'from [\w.]+ import [\w.]+ as [\w.]+',
        r'from [\w.]+ import [\w.]+',
        r'from [\w.]+ import \(',
    ]
    regexes = []

    if ignore_nesting:
        for regex in base_regexes:
            regexes.append(f'^{regex}')
            regexes.append(f'\n{regex}')
    else:
        regexes += base_regexes

    regex = re.compile(f'({"|".join(regexes)})')
    return [s.strip() for s in re.findall(regex, file_content)]


autocomplete_file_cache = MemorySizeLRUCache(maxsize=100_000)


def class_members(file_content: str) -> dict:
    """
    Each class's public methods and attributes, and its bases, read from the source
    without importing it: importing ran the module's code (a user's block, or an io module
    that imports a heavy SDK) on the server, and held every request for seconds.
    """
    try:
        tree = ast.parse(file_content)
    except (SyntaxError, ValueError):
        return {}
    members = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        names = []
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.append(item.name)
            elif isinstance(item, ast.Assign):
                names += [t.id for t in item.targets if isinstance(t, ast.Name)]
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                names.append(item.target.id)
        bases = [
            b.id if isinstance(b, ast.Name) else b.attr
            for b in node.bases if isinstance(b, (ast.Name, ast.Attribute))
        ]
        members[node.name] = dict(
            names=[n for n in names if not n.startswith('_')], bases=bases,
        )
    return members


@async_cache_file_read(autocomplete_file_cache)
async def extract_classes_from_file(file_path: str, cwd: str = None, mode: str = 'r'):
    cwd = cwd or os.getcwd()
    async with aiofiles.open(file_path, mode='r') as f:
        file_content = await f.read()

    file_name = file_path.replace(f'{cwd}/', '').replace(f'{root_path}/', '')
    files = []
    parts = file_name.split('/')

    if '__init__.py' == parts[-1]:
        path_sub = '/'.join(parts[: len(parts) - 1])
        files += [fn for fn in reduce(add_file, [path_sub], []) if fn != file_name]

    members = await asyncio.to_thread(class_members, file_content)
    return dict(
        classes=extract_all_classes(file_content),
        constants=extract_all_constants(file_content),
        files=files,
        functions=extract_all_functions(file_content),
        imports=extract_all_imports(file_content),
        class_members=members,
    )


def _methods_for_class(class_name: str, index: dict, seen=None) -> list:
    """A class's public names with those of its bases found among the files read."""
    seen = seen or set()
    if class_name in seen or class_name not in index:
        return []
    seen.add(class_name)
    names = list(index[class_name]['names'])
    for base in index[class_name]['bases']:
        names += _methods_for_class(base, index, seen)
    return sorted(set(names))


async def build_file_content_mapping(paths, files):
    file_content_mapping = {}
    file_names = reduce(add_file, paths, files)

    cwd = os.getcwd()
    for file_name in file_names:
        file_content_mapping[file_name] = dict(
            await extract_classes_from_file(file_name, cwd=cwd),
        )

    index = {}
    for info in file_content_mapping.values():
        for class_name, members in (info.get('class_members') or {}).items():
            index.setdefault(class_name, members)
    for info in file_content_mapping.values():
        members = info.pop('class_members', None) or {}
        info['methods_for_class'] = {
            class_name: _methods_for_class(class_name, index) for class_name in members
        }

    return file_content_mapping
