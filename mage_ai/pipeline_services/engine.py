"""
The service engine (mage-service) on this machine, for `mage service run`.

In order: MAGE_ENGINE_PATH, the binary the mageml-engine wheel installs next to Python
(ADR-0017), and in a source checkout the engine cargo built. Without one, the error names each
way to get it.
"""
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

ENGINE_NAME = 'mage-service'
SOURCE_ENGINE = Path(__file__).resolve().parent / 'service' / 'target'


class EngineNotFound(RuntimeError):
    pass


def _executable(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


def engine_path() -> Path:
    configured = os.getenv('MAGE_ENGINE_PATH')
    if configured:
        path = Path(configured).expanduser()
        if not _executable(path):
            raise EngineNotFound(f'MAGE_ENGINE_PATH is {configured}, which is not an executable.')
        return path
    for candidate in _candidates():
        if candidate is not None and _executable(candidate):
            return candidate
    raise EngineNotFound(
        'The service engine is not installed. Install it with `uv pip install "mage-ml[services]"` '
        '(Linux x86_64 and aarch64 with glibc 2.28 or later, macOS on Apple silicon); '
        'elsewhere build it with `cargo build --release -p mage-service` in '
        'mage_ai/pipeline_services/service and set MAGE_ENGINE_PATH to the binary, or run '
        'the service in Docker with `mage service build <name> --image`.'
    )


def installed_engine_path() -> Optional[Path]:
    """
    An installed engine: MAGE_ENGINE_PATH, the wheel's binary next to Python, or
    mage-service on PATH; not the builds of a source checkout. None without one.
    """
    configured = os.getenv('MAGE_ENGINE_PATH')
    if configured:
        path = Path(configured).expanduser()
        return path if _executable(path) else None
    beside = Path(sys.executable).parent / ENGINE_NAME
    if _executable(beside):
        return beside
    found = shutil.which(ENGINE_NAME)
    return Path(found) if found and _executable(Path(found)) else None


def _candidates():
    # The wheel's binary sits in the scripts folder of the environment running MageML.
    yield Path(sys.executable).parent / ENGINE_NAME
    found = shutil.which(ENGINE_NAME)
    yield Path(found) if found else None
    # The newer of cargo's builds: a stale release build must not win over today's debug one.
    built = [SOURCE_ENGINE / profile / ENGINE_NAME for profile in ('release', 'debug')]
    yield from sorted((p for p in built if p.is_file()), key=lambda p: p.stat().st_mtime,
                      reverse=True)


def engine_version(path: Optional[Path] = None) -> Optional[str]:
    import subprocess

    try:
        result = subprocess.run(
            [str(path or engine_path()), '--version'], capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError, EngineNotFound):
        return None
    return result.stdout.strip() or None
