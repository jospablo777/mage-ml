"""
Whether R and Rust blocks can run on this machine, for the editor's notice when one is
created. MageML runs Python blocks out of the box; R and Rust are installed outside Python,
so a missing setup is explained with the commands and the guide that fix it.
"""
import shutil
from typing import Dict, List, Optional

DOCS = 'https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks'
# The extras add the Python side: database clients such as postgres. R and Rust themselves
# are installed outside Python; their extras install nothing and only name the intent.
INSTALL = 'uv pip install "mage-ml[r,rust,postgres]"'
LANGUAGES = ('r', 'rust')


def language_setup(language: str, repo_path: Optional[str] = None) -> Dict:
    if language == 'r':
        problems = _r_problems(repo_path)
        commands = [
            'mage r setup   # checks R and rv, and says what to install',
            'mage r init    # creates the project\'s R environment',
        ]
        name, guide = 'R', f'{DOCS}/r-setup.md'
    elif language == 'rust':
        problems = _rust_problems()
        commands = [
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
            'mage rust init   # creates the project\'s Rust workspace',
        ]
        name, guide = 'Rust', f'{DOCS}/rust-setup.md'
    else:
        raise ValueError(f'Setup is checked for {", ".join(LANGUAGES)}, not {language!r}.')
    return dict(
        commands=commands,
        guide=guide,
        install=INSTALL,
        language=language,
        name=name,
        problems=problems,
        ready=not problems,
    )


def _r_problems(repo_path: Optional[str]) -> List[str]:
    from mage_ai.data_preparation.models.block.r import runtime

    try:
        config = runtime.r_config(repo_path)
    except runtime.REnvironmentError as error:
        return [str(error)]
    if shutil.which(config.rscript) is None:
        return ['R is not installed, or Rscript is not on the PATH. R blocks need R '
                f'{runtime.DEFAULT_R_VERSION}.']
    try:
        if config.uses_rv:
            if shutil.which(config.rv) is None:
                return [f'The project has an R environment in {config.project_dir}, but rv, '
                        'which installs its packages, is not installed.']
            library = runtime.library_path(config)
            missing = [name for name in runtime.EXCHANGE_PACKAGES
                       if not (library / name / 'DESCRIPTION').exists()]
            if missing:
                return [f'The project\'s R library lacks {", ".join(missing)}. Run '
                        '`mage r sync` to install the packages of rv.lock.']
            return []
        missing = runtime.missing_packages(config, runtime.EXCHANGE_PACKAGES)
    except runtime.REnvironmentError as error:
        return [str(error)]
    if missing:
        quoted = ', '.join(f'"{name}"' for name in missing)
        return [f'R is installed, but its library lacks {", ".join(missing)}, which R blocks '
                f'use to exchange data. Run `mage r init` for a project environment, or '
                f'install.packages(c({quoted})) in R.']
    return []


def _rust_problems() -> List[str]:
    if shutil.which('cargo') is None:
        return ['Rust is not installed, or cargo is not on the PATH. Rust blocks compile with '
                'cargo.']
    return []
