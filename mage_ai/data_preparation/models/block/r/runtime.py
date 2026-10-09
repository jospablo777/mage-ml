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
  When the Rscript on PATH is not the R version of the rv project, Mage uses the
  matching R version that rig (https://github.com/r-lib/rig) installed.
- MAGE_RIG: the rig executable. Defaults to the one on PATH.
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
# The packages of a new environment: the tidyverse, and what the R block templates use to
# read and write databases (DBI and its drivers), files and S3 (arrow, readr) and APIs
# (httr2).
DEFAULT_PACKAGES = [
    'tidyverse', *EXCHANGE_PACKAGES, 'DBI', 'RPostgres', 'RMariaDB', 'duckdb', 'RSQLite',
    'httr2',
]
# Packages that Posit Package Manager has no binary of for macOS, unlike CRAN.
MACOS_CRAN_PACKAGES = ['RMariaDB']
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
    expected = project_r_version(resolved) if resolved is not None else None
    if sync not in SYNC_MODES:
        raise REnvironmentError(f'MAGE_R_SYNC must be one of {SYNC_MODES}, not {sync!r}.')
    timeout = os.getenv('MAGE_R_TIMEOUT')
    return RConfig(
        project_dir=resolved,
        rscript=choose_rscript(expected),
        rv=os.getenv('MAGE_RV') or 'rv',
        sync=sync,
        timeout=float(timeout) if timeout else None,
    )


def _version_matches(version: Optional[str], expected: str) -> bool:
    return bool(version) and (version == expected or version.startswith(f'{expected}.'))


def _rscript_version_of(path: str) -> Optional[str]:
    try:
        result = subprocess.run(
            [path, '--vanilla', '-e', 'cat(R.version$major, ".", R.version$minor, sep = "")'],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def rig_versions() -> List[Dict]:
    """The R versions rig installed, from `rig list --json`; empty without rig."""
    rig = shutil.which(os.getenv('MAGE_RIG') or 'rig')
    if rig is None:
        return []
    try:
        result = subprocess.run(
            [rig, 'list', '--json'], capture_output=True, text=True, timeout=60,
        )
        versions = json.loads(result.stdout) if result.returncode == 0 else []
    except (OSError, subprocess.SubprocessError, ValueError):
        return []
    return versions if isinstance(versions, list) else []


def _rig_candidates(expected: str) -> List[tuple]:
    """(version parts, Rscript) of the R versions rig installed that match expected."""
    candidates = []
    for entry in rig_versions():
        version = str(entry.get('version') or '')
        binary = entry.get('binary')
        if not binary or not _version_matches(version, expected):
            continue
        rscript = Path(binary).with_name('Rscript.exe' if os.name == 'nt' else 'Rscript')
        if rscript.exists():
            parts = tuple(int(p) for p in version.split('.') if p.isdigit())
            candidates.append((parts, str(rscript)))
    return sorted(candidates, reverse=True)


def rig_rscript(expected: str) -> Optional[str]:
    """
    The Rscript of the newest R version rig installed that matches expected and runs
    it. On macOS, R versions share one framework, and every version's Rscript runs the
    default version, which `rig default` sets; such an Rscript is not used.
    """
    for _, rscript in _rig_candidates(expected):
        if _version_matches(_rscript_version_of(rscript), expected):
            return rscript
    return None


# The Rscript chosen for each R version, PATH Rscript and rig.
_rscripts: Dict[tuple, str] = {}


def choose_rscript(expected: Optional[str]) -> str:
    """
    The Rscript that R blocks run with: MAGE_RSCRIPT, else the Rscript on PATH when it
    runs the R version of the rv project, else the matching R version of rig.
    """
    explicit = os.getenv('MAGE_RSCRIPT')
    if explicit or not expected:
        return explicit or 'Rscript'
    on_path = shutil.which('Rscript')
    key = (expected, on_path, shutil.which(os.getenv('MAGE_RIG') or 'rig'))
    if key not in _rscripts:
        choice = 'Rscript'
        if not (on_path and _version_matches(_rscript_version_of(on_path), expected)):
            choice = rig_rscript(expected) or 'Rscript'
        _rscripts[key] = choice
    return _rscripts[key]


def has_r_version(version: str) -> bool:
    """Whether an Rscript of the R version can be found, on PATH or from rig."""
    rscript = os.getenv('MAGE_RSCRIPT') or choose_rscript(version)
    path = shutil.which(rscript) or rscript
    return _version_matches(_rscript_version_of(path), version)


def install_r(version: str) -> None:
    """Install an R version with rig, which asks for an administrator password on macOS."""
    rig = shutil.which(os.getenv('MAGE_RIG') or 'rig')
    if rig is None:
        raise REnvironmentError(
            f'Installing R {version} needs rig: see https://github.com/r-lib/rig#installation.'
        )
    result = subprocess.run([rig, 'add', version])
    if result.returncode != 0:
        raise REnvironmentError(f'`rig add {version}` failed with exit code {result.returncode}.')
    _rscripts.clear()


# How to install the tools of R blocks on each platform, from their documentation:
# https://a2-ai.github.io/rv-docs/intro/installation/ and https://rig.r-lib.org/install.html
INSTALL_COMMANDS = {
    'rig': {
        'darwin': ['brew install r-rig'],
        'linux': [
            'curl -Ls https://github.com/r-lib/rig/releases/download/latest/'
            'rig-linux-$(arch)-latest.tar.gz | `which sudo` tar xz -C /usr/local',
        ],
        'win32': ['winget install posit.rig'],
    },
    'rv': {
        'darwin': ['brew install rv-r'],
        'linux': [
            'curl -sSL https://raw.githubusercontent.com/A2-ai/rv/refs/heads/main/scripts/'
            'install.sh | bash',
        ],
        'win32': [
            'Download the x86_64-pc-windows-msvc zip from https://github.com/a2-ai/rv/'
            'releases/latest and add its directory to PATH',
        ],
    },
}


# The Debian and Ubuntu libraries that source builds of the default packages need, as
# rv sysdeps lists them, for packages without a Posit Package Manager binary for the
# distribution, as on Debian arm64. The Dockerfile installs the same list.
LINUX_BUILD_LIBRARIES = [
    'cmake', 'libcurl4-openssl-dev', 'libfontconfig1-dev', 'libfreetype6-dev',
    'libfribidi-dev', 'libharfbuzz-dev', 'libicu-dev', 'libjpeg-dev', 'libmariadb-dev',
    'libpng-dev', 'libpq-dev', 'libssl-dev', 'libtiff-dev', 'libuv1-dev', 'libwebp-dev',
    'libxml2-dev', 'make', 'xz-utils', 'zlib1g-dev',
]


def missing_linux_libraries() -> Optional[List[str]]:
    """The build libraries dpkg does not list as installed; None without dpkg."""
    dpkg = shutil.which('dpkg-query')
    if not dpkg:
        return None
    result = subprocess.run(
        [dpkg, '-W', '-f=${Package} ${db:Status-Status}\n', *LINUX_BUILD_LIBRARIES],
        capture_output=True, text=True,
    )
    installed = {
        line.split()[0].split(':')[0] for line in result.stdout.splitlines()
        if line.endswith(' installed')
    }
    return [p for p in LINUX_BUILD_LIBRARIES if p not in installed]


def _platform() -> str:
    if sys.platform.startswith('linux'):
        return 'linux'
    return 'win32' if sys.platform.startswith('win') else 'darwin'


def setup_steps(r_version: str = DEFAULT_R_VERSION) -> List[Dict]:
    """
    The tools R blocks need, whether they are installed, and the commands that install
    the missing ones on this platform: rig, R through rig, and rv.
    """
    platform = _platform()
    rig = shutil.which(os.getenv('MAGE_RIG') or 'rig')
    rv = shutil.which(os.getenv('MAGE_RV') or 'rv')
    rscript = os.getenv('MAGE_RSCRIPT') or choose_rscript(r_version)
    version = _rscript_version_of(shutil.which(rscript) or rscript)
    has_r = _version_matches(version, r_version)
    r_commands = [f'rig add {r_version}']
    if not has_r and _rig_candidates(r_version):
        r_commands = [f'rig default {r_version}']
    steps = [
        dict(
            name='rig, the R installation manager (optional, recommended)',
            found=rig,
            optional=True,
            commands=INSTALL_COMMANDS['rig'][platform],
        ),
        dict(
            name=f'R {r_version}',
            found=f'{shutil.which(rscript) or rscript} (R {version})' if has_r else None,
            commands=r_commands if rig else (
                INSTALL_COMMANDS['rig'][platform] + r_commands
            ),
        ),
        dict(
            name='rv, the R package manager',
            found=rv,
            commands=INSTALL_COMMANDS['rv'][platform],
        ),
    ]
    # Without dpkg, such as on Fedora, the names differ; r-blocks.md lists the libraries.
    missing = missing_linux_libraries() if platform == 'linux' else None
    if missing is not None:
        steps.append(dict(
            name='The system libraries R packages build with',
            found='installed' if not missing else None,
            commands=['sudo apt-get install -y ' + ' '.join(missing)],
        ))
    return steps


def tool_env(config: 'RConfig', base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    The environment of rv and Rscript: an Rscript that is not the one on PATH, such as
    one of rig's, comes first on PATH, so rv builds packages with the same R; arrow
    builds with S3 and GCS.
    """
    env = dict(base if base is not None else os.environ)
    # arrow built from source, as on Linux arm64, leaves out S3 and GCS, which
    # read_s3 and write_s3 use, unless this is false.
    env.setdefault('LIBARROW_MINIMAL', 'false')
    path = shutil.which(config.rscript)
    if path and path != shutil.which('Rscript'):
        env['PATH'] = os.pathsep.join([str(Path(path).parent), env.get('PATH', '')])
    return env


def _r_version_hint(expected: str) -> str:
    installed = [str(v.get('version')) for v in rig_versions()]
    if _rig_candidates(expected):
        # Installed, but it runs only as the default version, as on macOS.
        return (
            f'rig installed R {expected}, which runs only as the default R version here; '
            f'make it the default with `rig default {expected}`'
        )
    if shutil.which(os.getenv('MAGE_RIG') or 'rig') is None:
        return (
            f'Install R {expected} with rig (https://github.com/r-lib/rig), which Mage uses '
            f'when Rscript is another version: `rig add {expected}`'
        )
    return (
        f'rig has R {", ".join(installed) or "no version"}; install R {expected} with '
        f'`rig add {expected}` or `mage r init --install-r`'
    )


def _executable(name: str, what: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise REnvironmentError(
            f'{what} ({name}) was not found. Install it, or set its path in '
            f'{"MAGE_RSCRIPT" if what == "Rscript" else "MAGE_RV"}.',
        )
    return path


def _run(
    args: List[str],
    cwd: Optional[Path] = None,
    timeout: float = 600,
    env: Optional[Dict[str, str]] = None,
) -> str:
    result = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, timeout=timeout, env=env,
    )
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
    output = _run(
        [_executable(config.rv, 'rv'), 'plan', '--json'], cwd=config.project_dir,
        env=tool_env(config),
    )
    return json.loads(output)


def sync(config: RConfig) -> Dict:
    output = _run(
        [_executable(config.rv, 'rv'), 'sync', '--json'], cwd=config.project_dir,
        timeout=3600, env=tool_env(config),
    )
    return json.loads(output) if output.strip() else {}


def library_path(config: RConfig) -> Path:
    output = _run(
        [_executable(config.rv, 'rv'), 'library', '--json'], cwd=config.project_dir,
        env=tool_env(config),
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
                f'{_executable(config.rscript, "Rscript")} runs R {actual}. '
                f'{_r_version_hint(expected)}. Or set MAGE_RSCRIPT to an R {expected} '
                'Rscript, or change r_version in rproject.toml and run `mage r sync`.',
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
            problems.append(
                f'The environment is for R {expected}, but Rscript runs R {actual}. '
                f'{_r_version_hint(expected)}.',
            )
        versions = rig_versions()
        details['rig'] = ', '.join(
            f'{v.get("version")}{" (default)" if v.get("default") else ""}' for v in versions
        ) if versions else 'not installed or no R versions'

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
    rscript = os.getenv('MAGE_RSCRIPT') or choose_rscript(r_version)
    actual = _rscript_version_of(shutil.which(rscript) or rscript)
    if not _version_matches(actual, r_version):
        raise REnvironmentError(
            f'The R environment is for R {r_version}, but '
            f'{"no Rscript was found" if actual is None else f"Rscript runs R {actual}"}. '
            f'{_r_version_hint(r_version)}.',
        )
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
    text = _replace_list(
        text, 'dependencies', ',\n'.join(_dependency_line(p) for p in dependencies),
    )
    toml_path.write_text(text)
    sync(RConfig(
        project_dir=directory.resolve(),
        rscript=os.getenv('MAGE_RSCRIPT') or choose_rscript(r_version),
        rv=config.rv,
    ))
    return directory


def _dependency_line(package: str) -> str:
    if sys.platform == 'darwin' and package in MACOS_CRAN_PACKAGES:
        return f'    {{name = "{package}", repository = "CRAN"}}'
    return f'    "{package}"'


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
        env=tool_env(config, env),
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
