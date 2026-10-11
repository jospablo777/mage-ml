"""
Error messages that say what failed and what to do about it.

A block's exception keeps its type; MageML adds notes to it (PEP 678), which Python prints
after the error message in the notebook, in run logs and in the CLI.
"""
import linecache
import os
import shlex
import sys
from typing import Optional

# Modules whose package has another name on PyPI.
PACKAGE_NAMES = {
    'bs4': 'beautifulsoup4',
    'cv2': 'opencv-python',
    'dateutil': 'python-dateutil',
    'dotenv': 'python-dotenv',
    'google.cloud.bigquery': 'google-cloud-bigquery',
    'jwt': 'pyjwt',
    'MySQLdb': 'mysqlclient',
    'PIL': 'pillow',
    'psycopg2': 'psycopg2-binary',
    'skimage': 'scikit-image',
    'sklearn': 'scikit-learn',
    'yaml': 'pyyaml',
}

_ANNOTATED = '_mage_annotated'


def package_for(module: str) -> str:
    for name in (module, module.split('.')[0]):
        if name in PACKAGE_NAMES:
            return PACKAGE_NAMES[name]
    return module.split('.')[0].replace('_', '-')


def hint_for(error: BaseException) -> Optional[str]:
    """What to do about a common error, or None when there is nothing useful to add."""
    if isinstance(error, ModuleNotFoundError) and error.name:
        if error.name.split('.')[0] == 'mage_ai':
            return None
        package = package_for(error.name)
        return (
            f'{package} is not installed in the Python environment MageML runs in '
            f'({sys.executable}). Install it with `uv pip install --python '
            f'{shlex.quote(sys.executable)} {package}` and add it to the project\'s '
            'requirements.txt, so deployments get it too.'
        )
    if isinstance(error, ImportError) and 'cannot import name' in str(error):
        module = error.name or 'the package'
        return (
            f'The installed version of {module} may not have this name. Check it with '
            f'`uv pip show {package_for(module)}` and pin the version the code expects in '
            'requirements.txt.'
        )
    if isinstance(error, MemoryError):
        return (
            'The block ran out of memory. Read fewer columns or rows, process the data in '
            'chunks, use Polars or a Rust block for large tables, or give MageML more memory.'
        )
    if isinstance(error, FileNotFoundError) and error.filename and not os.path.isabs(
        str(error.filename),
    ):
        return (
            f'Relative paths start from the directory MageML runs in ({os.getcwd()}), not the '
            'project. Build paths from the project: '
            'os.path.join(get_repo_path(), ...) with '
            '`from mage_ai.settings.repo import get_repo_path`.'
        )
    if isinstance(error, ConnectionRefusedError) or (
        type(error).__name__ == 'OperationalError'
        and any(text in str(error).lower() for text in (
            'could not connect', 'connection refused', 'could not translate host',
            "can't connect", 'timeout expired',
        ))
    ):
        return (
            'MageML could not reach the server. Check the host, port and credentials in the '
            'io_config.yaml profile the block uses, and that the server is running and '
            'reachable from where MageML runs.'
        )
    return None


def annotate_block_error(error: BaseException, block) -> BaseException:
    """Notes on a block's exception: which block failed and, when known, what to do."""
    if getattr(error, _ANNOTATED, False) or not hasattr(error, 'add_note'):
        return error
    try:
        setattr(error, _ANNOTATED, True)
    except (AttributeError, TypeError):
        return error
    where = getattr(block, 'uuid', None)
    path = getattr(block, 'file_path', None)
    repo_path = getattr(block, 'repo_path', None)
    if path and repo_path:
        try:
            path = os.path.relpath(path, repo_path)
        except ValueError:
            pass
    if where:
        error.add_note(f'Block {where}{f" ({path})" if path else ""} failed.')
    hint = hint_for(error)
    if hint:
        error.add_note(hint)
    return error


def compile_source(code: str, filename: str):
    """
    Compiles code under its file name, so tracebacks name the block's file and show its
    lines. The source goes into linecache: the code run may differ from the saved file.
    """
    linecache.cache[filename] = (len(code), None, code.splitlines(True), filename)
    return compile(code, filename, 'exec')


def describe_exit(code: int, process: str = 'the process') -> str:
    """How a child process ended, in words: signals name their usual cause."""
    import signal

    number = -code if code < 0 else code - 128 if 128 < code < 160 else None
    if number == signal.SIGKILL:
        return (
            f'{process} was killed (SIGKILL), most often by the system when it runs out of '
            'memory'
        )
    if number == signal.SIGSEGV:
        return f'{process} crashed in native code (segmentation fault)'
    if number in (signal.SIGTERM, signal.SIGINT):
        return f'{process} was stopped ({signal.Signals(number).name})'
    if number is not None and code < 0:
        try:
            name = signal.Signals(number).name
        except ValueError:
            name = f'signal {number}'
        return f'{process} was stopped by {name}'
    return f'{process} exited with code {code}'


def r_hint(message: str) -> Optional[str]:
    """What to do about a common R error, or None."""
    import re

    missing = re.search(r"there is no package called [‘'\"]([A-Za-z][\w.]*)[’'\"]", message or '')
    if missing:
        package = missing.group(1)
        return (
            f'{package} is not in the project\'s R environment. Add it with `rv add {package}` '
            'in the project\'s r folder, which installs it and pins it in rv.lock.'
        )
    return None
