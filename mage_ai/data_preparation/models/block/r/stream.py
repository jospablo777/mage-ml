"""
The command that starts a stream's resident R program (mageml::serve_stream), with the R
environment R blocks run in: the project's rv library, Mage's mageml package, and UTC as
the session's time zone. The service engine asks its Python worker for it, so the R
setup lives in one place; MageML's editor starts the same command.
"""
import os
from pathlib import Path
from typing import Dict, Optional

from mage_ai.shared.processes import supervised_command

from . import runtime

RUNNER = Path(__file__).parent / 'stream_runner.R'


def stream_command(repo_path: Optional[str], supervised: bool = True) -> Dict:
    """
    `command` (argv without the directory), `env` and `cwd` of the R stream program.
    `supervised`: R stops once this process is gone; the service engine, which starts the
    program itself and supervises it, asks without.
    """
    config = runtime.r_config(repo_path)
    libraries = runtime.prepare(config)
    env = dict(os.environ)
    env.pop('MAGE_R_LIBRARY', None)
    if libraries.rv is not None:
        env['MAGE_R_LIBRARY'] = str(libraries.rv)
    env['MAGE_R_MAGEML_LIBRARY'] = str(libraries.mageml)
    # R reads naive timestamps as UTC and shows them in the session's time zone.
    env['TZ'] = os.getenv('MAGE_R_TZ') or 'UTC'
    command = [runtime._executable(config.rscript, 'Rscript'), '--vanilla', str(RUNNER)]
    if supervised:
        command = supervised_command(command)
    cwd = repo_path if repo_path and os.path.isdir(repo_path) else os.getcwd()
    return dict(command=command, env=runtime.tool_env(config, env), cwd=cwd)
