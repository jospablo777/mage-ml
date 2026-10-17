"""
Error messages that say what failed and what to do about it.

A block's exception keeps its type; MageML adds notes to it (PEP 678), which Python prints
after the error message in the notebook, in run logs and in the CLI.
"""
import errno
import importlib.util
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


def _module_location(name: str) -> Optional[str]:
    """Where Python finds the top-level module name: its file or folder, or None."""
    module = sys.modules.get(name)
    origin = getattr(module, '__file__', None)
    if origin is None:
        try:
            spec = importlib.util.find_spec(name)
        except (ImportError, ValueError):
            return None
        if spec is None:
            return None
        origin = spec.origin if spec.has_location else None
        if origin is None and spec.submodule_search_locations:
            origin = next(iter(spec.submodule_search_locations), None)
    if origin is None:
        return None
    return os.path.dirname(origin) if os.path.basename(origin) == '__init__.py' else origin


def _inside(path: str, folder: str) -> bool:
    path, folder = os.path.realpath(path), os.path.realpath(folder)
    return path == folder or path.startswith(folder + os.sep)


def _project_path(repo_path: Optional[str]) -> Optional[str]:
    if repo_path:
        return repo_path
    try:
        from mage_ai.settings.repo import get_repo_path

        return get_repo_path()
    except Exception:
        return None


def _missing_submodule_hint(name: str, repo_path: Optional[str]) -> Optional[str]:
    """
    The hint for a.b missing while a is found: installing a package named a would not help.
    A project named like another module, such as test, json or email, is the usual cause.
    """
    top = name.split('.')[0]
    location = _module_location(top)
    if location is None:
        return None
    project = _project_path(repo_path)
    if project and top == os.path.basename(os.path.normpath(project)):
        if _inside(location, project):
            return f'The project has no module {name}. Check the file and folder names.'
        return (
            f'The project folder {top} has the name of the module at {location}, which '
            f'Python finds first, so {name} is looked up there. Rename the project folder, '
            f'and the imports that start with {top}., to a name no installed module has.'
        )
    return (
        f'{top} is installed but has no module {name}. Check the module name, and the '
        f'installed version with `uv pip show {package_for(top)}`.'
    )


def _project_module_hint(name: str, repo_path: Optional[str]) -> Optional[str]:
    """
    The hint for a missing module that is a folder or file of the project: Python looks for
    it on its import path, where the project folder is not. Installing a package with that
    name from PyPI would install someone else's code under it.
    """
    project = _project_path(repo_path)
    if not project:
        return None
    top = name.split('.')[0]
    for candidate in (os.path.join(project, top), os.path.join(project, f'{top}.py')):
        if os.path.exists(candidate):
            break
    else:
        return None
    project_name = os.path.basename(os.path.normpath(project))
    kind = 'folder' if os.path.isdir(candidate) else 'module'
    return (
        f'{top} is a {kind} in the project ({candidate}), but the project folder is not on '
        f"Python's import path, so `import {name}` does not find it. Import it through the "
        f'project\'s name (`from {project_name}.{name} import ...`), or make the shared code '
        'a package installed in the environment (a pyproject.toml and `uv pip install -e .`). '
        f'Do not install a package named {package_for(name)} from PyPI: it is not this code.'
    )


def hint_for(error: BaseException, repo_path: Optional[str] = None) -> Optional[str]:
    """What to do about a common error, or None when there is nothing useful to add."""
    if isinstance(error, ModuleNotFoundError) and error.name:
        if error.name.split('.')[0] == 'mage_ai':
            return None
        if '.' in error.name:
            hint = _missing_submodule_hint(error.name, repo_path)
            if hint:
                return hint
        hint = _project_module_hint(error.name, repo_path)
        if hint:
            return hint
        package = package_for(error.name)
        return (
            f'{package} is not installed in the Python environment MageML runs in '
            f'({sys.executable}). If {package} is the PyPI package you mean (check the name: '
            'installing a package runs its code), install it with `uv pip install --python '
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
    if isinstance(error, OSError) and error.errno == errno.ENOSPC:
        return (
            f'The disk is full{f" ({error.filename})" if error.filename else ""}. Block outputs '
            'are stored under MAGE_DATA_DIR; free space there, or keep fewer outputs with '
            'variables_retention_period in the project\'s metadata.yaml and '
            '`mage clean-cached-variables`.'
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
    hint = hint_for(error, repo_path)
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
