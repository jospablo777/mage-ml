"""
Services saved in the project: pipeline_services.yaml lists each service's pipelines and
settings. The app's Services page edits it, and `mage export service <project> --service
<name>` exports one. Pipelines that start after another's runs do it with their own
after-run triggers, which MageML and the service both run.

    services:
      shop-live:
        pipelines: [live_traffic, live_minute_kpis]
        max_runs: 2              # optional: runs of each pipeline at once
        include: [models/churn.onnx]
        model_registry: http://mlflow:5000   # optional: where models are pulled from
        models:                  # optional: models embedded in the image
          - name: churn
            uri: models:/churn@champion
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


# Addresses MLflow reads models from: a server, or a database or folder it opens itself.
REGISTRY_URL = re.compile(
    r'^(https?://[^\s/:]+|(databricks(-uc)?|uc|sqlite|postgresql|mysql|mssql|file):)\S*$', re.I,
)


def normalize(name: str, value: Dict) -> Dict:
    """A service's settings with every key, checked for types."""
    if not isinstance(value, dict):
        raise DefinitionError(f'Service {name}: its settings must be a mapping.')
    pipelines = value.get('pipelines') or []
    include = value.get('include') or []
    max_runs = value.get('max_runs')
    models = value.get('models') or []
    registry = value.get('model_registry') or None
    if not isinstance(pipelines, list) or not all(isinstance(p, str) for p in pipelines):
        raise DefinitionError(f'Service {name}: pipelines must be a list of pipeline names.')
    if not isinstance(include, list) or not all(isinstance(i, str) for i in include):
        raise DefinitionError(f'Service {name}: include must be a list of project paths.')
    if max_runs is not None and (not isinstance(max_runs, int) or max_runs < 1):
        raise DefinitionError(f'Service {name}: max_runs must be a whole number above 0.')
    if not isinstance(models, list) or not all(
        isinstance(m, dict) and isinstance(m.get('name'), str) and isinstance(m.get('uri'), str)
        and m['name'] and m['uri']
        for m in models
    ):
        raise DefinitionError(
            f'Service {name}: models must be a list of {{name, uri}}, such as '
            '{name: churn, uri: "models:/churn@champion"}.'
        )
    if registry is not None and (
        not isinstance(registry, str) or not REGISTRY_URL.match(registry.strip())
    ):
        raise DefinitionError(
            f'Service {name}: model_registry must be the address of an MLflow tracking '
            f'server, such as http://mlflow:5000; got {registry!r}.'
        )
    return dict(
        description=value.get('description') or '',
        include=list(include),
        max_runs=max_runs,
        model_registry=registry.strip() if registry else None,
        models=[dict(name=m['name'], uri=m['uri']) for m in models],
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
    if value.get('max_runs'):
        out['max_runs'] = value['max_runs']
    if value.get('include'):
        out['include'] = value['include']
    if value.get('model_registry'):
        out['model_registry'] = value['model_registry']
    if value.get('models'):
        out['models'] = value['models']
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
