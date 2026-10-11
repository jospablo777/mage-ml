"""
Services saved in the project: pipeline_services.yaml lists each service's pipelines, the
after-run triggers that start one pipeline when a run of another ends, and its settings.
The app's Services page edits it, and `mage export service <project> --service <name>`
exports one.

    services:
      shop-live:
        pipelines: [live_traffic, live_minute_kpis]
        after_runs:
          - run: live_minute_kpis
            after: live_traffic
            when: completed      # completed, failed or any
        max_runs: 2              # optional: runs of each pipeline at once
        include: [models/churn.onnx]
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml

FILE_NAME = 'pipeline_services.yaml'
HEADER = (
    '# Services exported from this project, edited by the Services page in MageML.\n'
    '# Export one with: mage export service <project> --service <name> --out <folder>\n'
)
_NAME = re.compile(r'^[a-z0-9][a-z0-9-]*$')


class DefinitionError(ValueError):
    pass


def definitions_path(repo_path: str) -> Path:
    return Path(repo_path) / FILE_NAME


def load_definitions(repo_path: str) -> Dict[str, Dict]:
    path = definitions_path(repo_path)
    if not path.is_file():
        return {}
    try:
        content = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as error:
        raise DefinitionError(f'{FILE_NAME} is not valid YAML: {error}')
    services = content.get('services') or {}
    if not isinstance(services, dict):
        raise DefinitionError(f'{FILE_NAME}: services must map each name to its settings.')
    return {str(name): normalize(name, value or {}) for name, value in services.items()}


def load_definition(repo_path: str, name: str) -> Dict:
    definitions = load_definitions(repo_path)
    if name not in definitions:
        known = ', '.join(sorted(definitions)) or 'none'
        raise DefinitionError(
            f'{FILE_NAME} has no service {name}; the services are: {known}.'
        )
    return definitions[name]


def normalize(name: str, value: Dict) -> Dict:
    """A service's settings with every key, checked for types."""
    if not isinstance(value, dict):
        raise DefinitionError(f'Service {name}: its settings must be a mapping.')
    pipelines = value.get('pipelines') or []
    after_runs = value.get('after_runs') or []
    include = value.get('include') or []
    max_runs = value.get('max_runs')
    if not isinstance(pipelines, list) or not all(isinstance(p, str) for p in pipelines):
        raise DefinitionError(f'Service {name}: pipelines must be a list of pipeline names.')
    if not isinstance(after_runs, list) or not all(isinstance(a, dict) for a in after_runs):
        raise DefinitionError(
            f'Service {name}: after_runs must be a list of {{run, after, when}} entries.'
        )
    if not isinstance(include, list) or not all(isinstance(i, str) for i in include):
        raise DefinitionError(f'Service {name}: include must be a list of project paths.')
    if max_runs is not None and (not isinstance(max_runs, int) or max_runs < 1):
        raise DefinitionError(f'Service {name}: max_runs must be a whole number above 0.')
    return dict(
        after_runs=[
            dict(
                after=a.get('after'),
                run=a.get('run'),
                when=a.get('when') or 'completed',
                **({'variables': a['variables']} if a.get('variables') else {}),
            )
            for a in after_runs
        ],
        description=value.get('description') or '',
        include=list(include),
        max_runs=max_runs,
        pipelines=list(dict.fromkeys(pipelines)),
    )


def save_definition(
    repo_path: str,
    name: str,
    value: Dict,
    previous_name: Optional[str] = None,
) -> Dict:
    """Saves a service, renaming it from previous_name when given, and keeps the others."""
    if not isinstance(name, str) or not _NAME.match(name):
        raise DefinitionError(
            f'{name!r} is not a service name: use lowercase letters, digits and -.'
        )
    definitions = load_definitions(repo_path)
    if previous_name and previous_name != name:
        if name in definitions:
            raise DefinitionError(f'A service named {name} already exists.')
        definitions.pop(previous_name, None)
    definitions[name] = normalize(name, value)
    _write(repo_path, definitions)
    return definitions[name]


def delete_definition(repo_path: str, name: str) -> bool:
    definitions = load_definitions(repo_path)
    if name not in definitions:
        return False
    definitions.pop(name)
    _write(repo_path, definitions)
    return True


def _compact(value: Dict) -> Dict:
    """The settings as written: empty and default values left out."""
    out = dict(pipelines=value['pipelines'])
    if value.get('description'):
        out['description'] = value['description']
    if value.get('after_runs'):
        out['after_runs'] = value['after_runs']
    if value.get('max_runs'):
        out['max_runs'] = value['max_runs']
    if value.get('include'):
        out['include'] = value['include']
    return out


def _write(repo_path: str, definitions: Dict[str, Dict]) -> None:
    path = definitions_path(repo_path)
    body = yaml.safe_dump(
        dict(services={n: _compact(v) for n, v in sorted(definitions.items())}),
        sort_keys=False,
        default_flow_style=False,
    )
    temporary = path.with_suffix('.yaml.tmp')
    temporary.write_text(HEADER + body)
    os.replace(temporary, path)


def summarize(name: str, value: Dict) -> Dict:
    return dict(name=name, **value)


def names(repo_path: str) -> List[str]:
    return sorted(load_definitions(repo_path))
