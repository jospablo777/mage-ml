"""
Named R and Rust environments (ADR-0040): which environment an R or Rust block runs with.

- `default` is the project's environment as before: the rv project in `r/` and the Cargo
  workspace in `rust/`.
- Others are folders: `r_environments/<name>/` (an rv project) and
  `rust_environments/<name>/` (a Cargo workspace).

A block selects one with `configuration.r_environment` or `configuration.rust_environment`,
else its pipeline's `environment.r` or `environment.rust`, else `default`, as Python blocks
select workers.
"""
import re
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT = 'default'
LANGUAGES = ('r', 'rust')
FOLDERS = {'r': 'r_environments', 'rust': 'rust_environments'}
# The file that makes a folder an environment.
MANIFESTS = {'r': 'rproject.toml', 'rust': 'Cargo.toml'}
BLOCK_KEYS = {'r': 'r_environment', 'rust': 'rust_environment'}
LABELS = {'r': 'R', 'rust': 'Rust'}
_NAME = re.compile(r'^[a-z0-9][a-z0-9_-]{0,63}$')


class EnvironmentNameError(ValueError):
    pass


def _language(language: str) -> str:
    language = (language or '').strip().lower()
    if language not in LANGUAGES:
        raise EnvironmentNameError(
            f'{language or "No language"} has no named environments: R and Rust do.',
        )
    return language


def normalized(name: Optional[str]) -> str:
    """The environment a name means: `default` for none; others lowercase, with letters,
    digits, `_` and `-`, so the name is also a folder and a Docker-safe word."""
    name = (name or '').strip().lower()
    if not name or name == DEFAULT:
        return DEFAULT
    if not _NAME.match(name):
        raise EnvironmentNameError(
            f'{name!r} is not an environment name: use up to 64 lowercase letters, digits, '
            '`_` and `-`, starting with a letter or digit.',
        )
    return name


def folder(repo_path: str, language: str, name: str) -> Path:
    """The folder of a named environment (not `default`, whose folder the language's runtime
    finds: `r/` or `rust/`, or their MAGE_*_PROJECT_DIR override)."""
    language = _language(language)
    name = normalized(name)
    if name == DEFAULT:
        raise EnvironmentNameError('The default environment has no folder under '
                                   f'{FOLDERS[language]}/.')
    return Path(repo_path) / FOLDERS[language] / name


def named(repo_path: str, language: str) -> List[str]:
    """The named environments of a language that exist, without `default`, sorted."""
    language = _language(language)
    root = Path(repo_path) / FOLDERS[language]
    if not root.is_dir():
        return []
    return sorted(
        path.name for path in root.iterdir()
        if path.is_dir() and _NAME.match(path.name) and (path / MANIFESTS[language]).is_file()
    )


def require(repo_path: str, language: str, name: Optional[str]) -> str:
    """The environment's name, when it exists (`default` always does); otherwise an error
    that names the ones that exist and how to create it."""
    language = _language(language)
    name = normalized(name)
    if name == DEFAULT or name in named(repo_path, language):
        return name
    others = ', '.join([DEFAULT, *named(repo_path, language)])
    command = f'mage {language} init --environment {name}'
    raise EnvironmentNameError(
        f'There is no {LABELS[language]} environment named {name}: the project has {others}. '
        f'Create it on the Workers page or with `{command}`.',
    )


def selection_of(
    language: str,
    configuration: Optional[Dict],
    pipeline_environment: Optional[Dict],
) -> str:
    """The environment a block's configuration selects, else its pipeline's, else default."""
    language = _language(language)
    selected = (configuration or {}).get(BLOCK_KEYS[language])
    if not selected and isinstance(pipeline_environment, dict):
        selected = pipeline_environment.get(language)
    return normalized(selected if isinstance(selected, str) else None)


def selection(block, language: Optional[str] = None) -> str:
    """The environment an R or Rust block selects (its language unless given)."""
    language = _language(language or str(getattr(block, 'language', '') or ''))
    pipeline = getattr(block, 'pipeline', None)
    return selection_of(
        language,
        getattr(block, 'configuration', None),
        getattr(pipeline, 'environment', None),
    )


def usage(repo_path: str, language: str) -> Dict[str, List[Dict]]:
    """
    The pipelines whose blocks of the language use each environment, from their
    metadata.yaml (blocks are not loaded): {environment: [{pipeline, name, blocks,
    default}]}, default when the pipeline's own setting chose it. A name that is not valid
    is listed as it is written, so the page can show the blocks it breaks.
    """
    import yaml

    language = _language(language)
    found: Dict[str, List[Dict]] = {}
    root = Path(repo_path) / 'pipelines'
    if not root.is_dir():
        return found
    for metadata in sorted(root.glob('*/metadata.yaml')):
        try:
            config = yaml.safe_load(metadata.read_text()) or {}
        except (OSError, yaml.YAMLError):
            continue
        environment = config.get('environment')
        environment = environment if isinstance(environment, dict) else None
        by_environment: Dict[str, List[str]] = {}
        for block in config.get('blocks') or []:
            if not isinstance(block, dict) or block.get('language') != language:
                continue
            try:
                chosen = selection_of(language, block.get('configuration'), environment)
            except EnvironmentNameError:
                chosen = str((block.get('configuration') or {}).get(BLOCK_KEYS[language])
                             or (environment or {}).get(language))
            by_environment.setdefault(chosen, []).append(block.get('uuid'))
        pipeline_default = (environment or {}).get(language)
        for name, blocks in by_environment.items():
            found.setdefault(name, []).append(dict(
                pipeline=metadata.parent.name,
                name=config.get('name') or metadata.parent.name,
                blocks=blocks,
                default=bool(pipeline_default) and str(pipeline_default).strip().lower() == name,
            ))
    return found


def of_block_files(repo_path: str, language: str) -> Dict[str, List[str]]:
    """The environments each block uuid runs in across the project's pipelines; a block in
    no pipeline is not listed (callers use `default` for it)."""
    found: Dict[str, List[str]] = {}
    for name, pipelines in usage(repo_path, language).items():
        for pipeline in pipelines:
            for uuid in pipeline['blocks']:
                if name not in found.setdefault(uuid, []):
                    found[uuid].append(name)
    return {uuid: sorted(names) for uuid, names in found.items()}
