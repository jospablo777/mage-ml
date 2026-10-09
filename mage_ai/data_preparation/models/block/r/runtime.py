"""
The R environment that R blocks run in.

A Mage project's R environment is an rv project (https://github.com/A2-ai/rv): an
rproject.toml that lists the packages, an rv.lock that pins them, and a library that
`rv sync` installs. R blocks run with Rscript --vanilla and that library alone, besides
R's base packages and Mage's mageml package, so they see exactly the locked packages.

Settings, read from the environment:

- MAGE_R_PROJECT_DIR: the rv project. Defaults to <project>/r, then to the project
  directory, wherever an rproject.toml is.
- MAGE_RSCRIPT, MAGE_RV: the Rscript and rv executables. Default to the ones on PATH.
- MAGE_R_SYNC: 'check' (default) fails a block when the library is not synced with the
  lock file, 'auto' runs `rv sync` first, 'off' skips the check.
- MAGE_R_TIMEOUT: seconds after which an R block is stopped. No limit by default.
- MAGE_R_CACHE_DIR: where Mage installs its mageml R package. Defaults to
  ~/.cache/mage-ml/r.
- MAGE_R_TZ: the time zone of R sessions. Defaults to UTC.

Without an rv project, R blocks use the R library of the Rscript they run with. Either
way, they load the mageml package, which Mage installs into its own library.
"""
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_R_VERSION = '4.6'
R_PROJECT_DIRECTORY = 'r'
# Packages Mage needs to exchange data with R blocks.
EXCHANGE_PACKAGES = ['arrow', 'bit64', 'jsonlite', 'tibble']
DEFAULT_PACKAGES = ['tidyverse'] + EXCHANGE_PACKAGES
DEFAULT_REPOSITORIES = [
    # Binary packages for Linux, macOS and Windows; CRAN builds Linux packages from source.
    dict(alias='PPM', url='https://packagemanager.posit.co/cran/latest'),
    dict(alias='CRAN', url='https://cloud.r-project.org/'),
]
SYNC_MODES = ('check', 'auto', 'off')


class REnvironmentError(RuntimeError):
    """The R environment cannot run R blocks as configured."""


@dataclass
class RConfig:
    project_dir: Optional[Path]
    rscript: str
    rv: str
    sync: str = 'check'
    timeout: Optional[float] = None

    @property
    def uses_rv(self) -> bool:
        return self.project_dir is not None


def r_config(repo_path: Optional[str] = None) -> RConfig:
    project_dir = os.getenv('MAGE_R_PROJECT_DIR')
    if project_dir:
        path = Path(project_dir)
        if not path.is_absolute() and repo_path:
            path = Path(repo_path) / path
        if not (path / 'rproject.toml').exists():
            raise REnvironmentError(
                f'MAGE_R_PROJECT_DIR is {path}, which has no rproject.toml. Create the R '
                f'environment with `mage r init {repo_path or "."}`.',
            )
        resolved = path.resolve()
    else:
        resolved = None
        for candidate in (
            Path(repo_path) / R_PROJECT_DIRECTORY if repo_path else None,
            Path(repo_path) if repo_path else None,
        ):
            if candidate is not None and (candidate / 'rproject.toml').exists():
                resolved = candidate.resolve()
                break

    sync = os.getenv('MAGE_R_SYNC', 'check').lower()
    if sync not in SYNC_MODES:
        raise REnvironmentError(f'MAGE_R_SYNC must be one of {SYNC_MODES}, not {sync!r}.')
    timeout = os.getenv('MAGE_R_TIMEOUT')
    return RConfig(
        project_dir=resolved,
        rscript=os.getenv('MAGE_RSCRIPT') or 'Rscript',
        rv=os.getenv('MAGE_RV') or 'rv',
        sync=sync,
        timeout=float(timeout) if timeout else None,
    )


def _executable(name: str, what: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise REnvironmentError(
            f'{what} ({name}) was not found. Install it, or set its path in '
            f'{"MAGE_RSCRIPT" if what == "Rscript" else "MAGE_RV"}.',
        )
    return path


def _run(args: List[str], cwd: Optional[Path] = None, timeout: float = 600) -> str:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise REnvironmentError(
            f'{" ".join(args)} failed with exit code {result.returncode}:\n'
            f'{(result.stderr or result.stdout).strip()}',
        )
    return result.stdout


def project_r_version(project_dir: Path) -> Optional[str]:
    with open(project_dir / 'rproject.toml', 'rb') as file:
        return str(tomllib.load(file).get('project', {}).get('r_version') or '') or None


def rscript_version(config: RConfig) -> str:
    output = _run([
        _executable(config.rscript, 'Rscript'), '--vanilla', '-e',
        'cat(R.version$major, ".", R.version$minor, sep = "")',
    ])
    return output.strip()


def plan(config: RConfig) -> Dict:
    """What `rv sync` would install and remove."""
    output = _run([_executable(config.rv, 'rv'), 'plan', '--json'], cwd=config.project_dir)
    return json.loads(output)


def sync(config: RConfig) -> Dict:
    output = _run(
        [_executable(config.rv, 'rv'), 'sync', '--json'], cwd=config.project_dir,
        timeout=3600,
    )
    return json.loads(output) if output.strip() else {}


def library_path(config: RConfig) -> Path:
    output = _run(
        [_executable(config.rv, 'rv'), 'library', '--json'], cwd=config.project_dir,
    )
    path = Path(json.loads(output)['directory'])
    return path if path.is_absolute() else (config.project_dir / path).resolve()


# The source of the mageml R package, which runs R blocks.
MAGEML_DIRECTORY = Path(__file__).parent / 'mageml'


@dataclass
class RLibraries:
    """The libraries R blocks load packages from."""

    # The rv environment's library, or None for the R installation's own.
    rv: Optional[Path]
    # The library that holds the mageml package.
    mageml: Path


# Checked environments: the key holds what a check depends on.
_prepared: Dict[tuple, RLibraries] = {}


def _modified(path: Path) -> Optional[int]:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def _prepare_key(config: RConfig) -> tuple:
    project = config.project_dir
    files = ()
    if project is not None:
        library_root = project / 'rv' / 'library'
        libraries = sorted(library_root.glob('*/*')) if library_root.exists() else []
        files = (
            _modified(project / 'rproject.toml'),
            _modified(project / 'rv.lock'),
            # Installing or removing a package changes its library's modification time.
            tuple((str(path), _modified(path)) for path in libraries),
        )
    return (
        str(project),
        shutil.which(config.rscript),
        shutil.which(config.rv),
        config.sync,
        str(r_cache_directory()),
        *files,
    )


def prepare(config: RConfig) -> RLibraries:
    """
    Check the R environment before a block runs, install the mageml package when its
    version is not installed, and return the libraries R blocks use. A check is
    repeated when rproject.toml, rv.lock, the library or the executables change; it
    takes about 0.3 seconds.
    """
    key = _prepare_key(config)
    libraries = _prepared.get(key)
    if libraries is not None and (libraries.mageml / 'mageml' / 'DESCRIPTION').exists():
        return libraries
    libraries = _prepare(config)
    # rv sync in auto mode changes the files the key holds.
    _prepared[_prepare_key(config)] = libraries
    return libraries


def _prepare(config: RConfig) -> RLibraries:
    actual = rscript_version(config)
    rv_library = None
    if config.uses_rv:
        expected = project_r_version(config.project_dir)
        if expected and not (actual == expected or actual.startswith(f'{expected}.')):
            raise REnvironmentError(
                f'The R environment in {config.project_dir} is for R {expected}, but '
                f'{_executable(config.rscript, "Rscript")} runs R {actual}. Set '
                f'MAGE_RSCRIPT to an R {expected} Rscript, or change r_version in '
                'rproject.toml and run `mage r sync`.',
            )

        if config.sync != 'off':
            pending = plan(config)
            changes = (pending.get('installed') or []) + (pending.get('removed') or [])
            if changes:
                if config.sync == 'auto':
                    sync(config)
                else:
                    names = sorted({change.get('name', '?') for change in changes})
                    raise REnvironmentError(
                        f'The R library in {config.project_dir} is not synced with '
                        f'rv.lock: {", ".join(names)} would change. Run `mage r sync` or '
                        '`rv sync` there, or set MAGE_R_SYNC=auto.',
                    )

        rv_library = library_path(config)
        if not rv_library.exists():
            raise REnvironmentError(
                f'The R library {rv_library} does not exist. Run `mage r sync` in '
                f'{config.project_dir}.',
            )
        missing = [
            name for name in EXCHANGE_PACKAGES
            if not (rv_library / name / 'DESCRIPTION').exists()
        ]
        if missing:
            raise REnvironmentError(
                f'R blocks need the R packages {", ".join(missing)}. Add them with '
                f'`rv add {" ".join(missing)}` in {config.project_dir}.',
            )
    return RLibraries(rv=rv_library, mageml=install_mageml(config, actual, rv_library))


def r_cache_directory() -> Path:
    """Where Mage installs the mageml package, one library per version and R version."""
    directory = os.getenv('MAGE_R_CACHE_DIR')
    if directory:
        return Path(directory)
    return Path.home() / '.cache' / 'mage-ml' / 'r'


def mageml_hash() -> str:
    """A hash of the mageml package's source, without its tests."""
    digest = hashlib.sha256()
    for path in sorted(MAGEML_DIRECTORY.rglob('*')):
        relative = path.relative_to(MAGEML_DIRECTORY)
        if path.is_file() and relative.parts[0] != 'tests':
            digest.update(str(relative.as_posix()).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


INSTALL_MAGEML = """
args <- commandArgs(trailingOnly = TRUE)
source_dir <- args[[1]]
staging <- args[[2]]
if (length(args) > 2) {
  .libPaths(c(staging, args[[3]]), include.site = FALSE)
} else {
  .libPaths(c(staging, .libPaths()))
}
install.packages(source_dir, repos = NULL, type = "source", lib = staging)
if (!requireNamespace("mageml", lib.loc = staging, quietly = TRUE)) {
  quit(status = 1, save = "no")
}
"""


def install_mageml(config: RConfig, r_version: str, rv_library: Optional[Path]) -> Path:
    """
    Install the mageml package, unless this version of it is installed for this R
    version, and return its library. The rv library is left alone: rv sync replaces it
    with exactly the locked packages.
    """
    minor = '.'.join(r_version.split('.')[:2])
    cache = r_cache_directory()
    target = cache / f'mageml-{mageml_hash()}-R{minor}'
    if (target / 'mageml' / 'DESCRIPTION').exists():
        return target
    cache.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.mageml-', dir=cache))
    try:
        args = [str(MAGEML_DIRECTORY), str(staging)]
        if rv_library is not None:
            args.append(str(rv_library))
        result = subprocess.run(
            [_executable(config.rscript, 'Rscript'), '--vanilla', '-e', INSTALL_MAGEML, *args],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            output = '\n'.join((result.stdout + result.stderr).strip().splitlines()[-20:])
            raise REnvironmentError(
                'The mageml R package, which runs R blocks, failed to install. It needs '
                f'the R packages {", ".join(EXCHANGE_PACKAGES)}'
                + (f' in {rv_library}' if rv_library else ' in R\'s library')
                + f'. R said:\n{output}',
            )
        try:
            os.replace(staging, target)
        except OSError:
            # Another process installed it first.
            if not (target / 'mageml' / 'DESCRIPTION').exists():
                raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return target


def project_dependencies(project_dir: Path) -> List[str]:
    with open(project_dir / 'rproject.toml', 'rb') as file:
        dependencies = tomllib.load(file).get('project', {}).get('dependencies') or []
    return [d if isinstance(d, str) else d.get('name', '') for d in dependencies]


def status(config: RConfig) -> Dict:
    """What R blocks run with, and the problems that stop them."""
    details = dict(project=str(config.project_dir))
    problems = []
    try:
        expected = project_r_version(config.project_dir)
        details['R version of the environment'] = expected
        actual = rscript_version(config)
        details['Rscript'] = f'{shutil.which(config.rscript) or config.rscript} (R {actual})'
        if expected and not (actual == expected or actual.startswith(f'{expected}.')):
            problems.append(f'The environment is for R {expected}, but Rscript runs R {actual}.')

        missing = sorted(set(EXCHANGE_PACKAGES) - set(project_dependencies(config.project_dir)))
        if missing:
            problems.append(
                f'R blocks need {", ".join(missing)}. Add them with `rv add {" ".join(missing)}`.',
            )

        pending = plan(config)
        changes = (pending.get('installed') or []) + (pending.get('removed') or [])
        if changes:
            names = sorted({change.get('name', '?') for change in changes})
            problems.append(f'The library is not synced: {", ".join(names)} would change.')

        library = library_path(config)
        details['library'] = str(library)
        if not library.exists():
            problems.append(f'The library {library} does not exist.')
    except REnvironmentError as error:
        problems.append(str(error))
    return dict(details=details, problems=problems)


def init_project(
    directory: Path,
    packages: Optional[List[str]] = None,
    r_version: str = DEFAULT_R_VERSION,
    config: Optional[RConfig] = None,
) -> Path:
    """
    Create the rv project of a Mage project, with the tidyverse and the packages Mage
    exchanges data with, and install them.
    """
    config = config or r_config()
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / 'rproject.toml').exists():
        raise REnvironmentError(f'{directory} already has an rproject.toml.')
    rv = _executable(config.rv, 'rv')
    _run([rv, 'init', '--r-version', r_version, '--no-repositories', str(directory)])

    dependencies = list(dict.fromkeys((packages or DEFAULT_PACKAGES) + EXCHANGE_PACKAGES))
    toml_path = directory / 'rproject.toml'
    text = toml_path.read_text()
    repositories = ',\n'.join(
        f'    {{alias = "{r["alias"]}", url = "{r["url"]}"}}' for r in DEFAULT_REPOSITORIES
    )
    text = _replace_list(text, 'repositories', repositories)
    text = _replace_list(text, 'dependencies', ',\n'.join(f'    "{p}"' for p in dependencies))
    toml_path.write_text(text)
    sync(RConfig(project_dir=directory.resolve(), rscript=config.rscript, rv=config.rv))
    return directory


def _replace_list(text: str, key: str, items: str) -> str:
    start = text.index(f'{key} = [')
    end = text.index(']', start)
    return text[:start] + f'{key} = [\n{items},\n]' + text[end + 1:]


def run_rscript(
    config: RConfig,
    script: Path,
    args: List[str],
    env: Dict[str, str],
    cwd: Path,
) -> int:
    """
    Run an R script, printing its output as it comes so it reaches the block's logs, and
    return its exit code. A run longer than the timeout is stopped with its child
    processes.
    """
    new_session = hasattr(os, 'killpg')
    process = subprocess.Popen(
        [_executable(config.rscript, 'Rscript'), '--vanilla', str(script), *args],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        start_new_session=new_session,
    )
    timed_out = []
    timer = None
    if config.timeout:
        import threading

        def stop():
            timed_out.append(True)
            try:
                if new_session:
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass

        timer = threading.Timer(config.timeout, stop)
        timer.start()
    try:
        for line in process.stdout:
            sys.stdout.write(line)
        code = process.wait()
    finally:
        if timer is not None:
            timer.cancel()
    if timed_out:
        raise TimeoutError(f'The R block ran longer than {config.timeout:g} seconds.')
    return code
