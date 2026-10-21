"""
Services saved in the project, one folder each, tracked in Git with the pipelines:

    services/
      shop-live/
        service.yaml          # this module reads and writes it
        environments/dev.yaml # where and how it runs (see SERVICES_UX.md)
        service.lock          # written by `mage service build`

service.yaml:

    name: shop-live
    pipelines: [live_traffic, live_minute_kpis]
    max_runs: 2              # optional: runs of each pipeline at once
    include: [models/churn.onnx]
    model_registry: http://mlflow:5000   # optional: where models are pulled from
    models:                  # optional: models embedded in the image
      - name: churn
        uri: models:/churn@champion
        challengers:         # optional: scored in the shadow, compared with the champion
          - name: v4
            uri: models:/churn/4
        shadow:
          sample: 0.25       # the share of inputs the challengers score (default 0.1)
        audit:               # optional: a record of every prediction
          features: values   # values, hash (one keyed hash of them) or none
          mask: [ssn]        # columns stored as ***
          hash: [email]      # columns stored as their keyed hash
          reasons: 3         # the top features behind each score (tree models)

The Services page edits these files, and the `mage service` commands use them. Services in
the earlier single file, pipeline_services.yaml, are still read; saving one moves it to its
folder, and `mage service migrate` moves them all. Pipelines that start after another's runs
do it with their own after-run triggers, which MageML and the service both run.
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml

SERVICES_DIR = 'services'
SERVICE_FILE = 'service.yaml'
LOCK_FILE = 'service.lock'
ENVIRONMENTS_DIR = 'environments'
# The earlier single file, read for one more release.
LEGACY_FILE = 'pipeline_services.yaml'
FILE_NAME = LEGACY_FILE
HEADER = (
    '# Services exported from this project, edited by the Services page in MageML.\n'
    '# Export one with: mage export service <project> --service <name> --out <folder>\n'
)
SERVICE_HEADER = (
    '# A MageML service. Edit here or on the Services page; secrets never go in this folder.\n'
    '# mage service check|build|export <name>\n'
)
DEV_ENVIRONMENT = """# Where and how this service runs during development (mage service run).
# Values here are not secret; secrets are references, never values.
target: docker
schedules: off        # on runs the time triggers; keep off where MageML runs them
variables: {}
secrets: {}
"""
# Generated builds, local runs, development runs from the editor (I-204), deploy state and
# local secret files.
GITIGNORE_LINES = ('.mage/build/', '.mage/run/', '.mage/dev/', '.mage/deploy/',
                   'services/*/.env*')
_NAME = re.compile(r'^[a-z0-9][a-z0-9-]*$')


class DefinitionError(ValueError):
    pass


def definitions_path(repo_path: str) -> Path:
    """The earlier single file."""
    return Path(repo_path) / LEGACY_FILE


def services_path(repo_path: str) -> Path:
    return Path(repo_path) / SERVICES_DIR


def service_path(repo_path: str, name: str) -> Path:
    return services_path(repo_path) / name


def service_file(repo_path: str, name: str) -> Path:
    return service_path(repo_path, name) / SERVICE_FILE


def _load_legacy(repo_path: str) -> Dict[str, Dict]:
    path = definitions_path(repo_path)
    if not path.is_file():
        return {}
    try:
        content = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as error:
        raise DefinitionError(f'{LEGACY_FILE} is not valid YAML: {error}')
    services = content.get('services') or {}
    if not isinstance(services, dict):
        raise DefinitionError(f'{LEGACY_FILE}: services must map each name to its settings.')
    return {str(name): normalize(name, value or {}) for name, value in services.items()}


def _load_folder(repo_path: str) -> Dict[str, Dict]:
    definitions = {}
    root = services_path(repo_path)
    if not root.is_dir():
        return definitions
    for path in sorted(root.glob(f'*/{SERVICE_FILE}')):
        name = path.parent.name
        label = f'{SERVICES_DIR}/{name}/{SERVICE_FILE}'
        try:
            content = yaml.safe_load(path.read_text()) or {}
        except yaml.YAMLError as error:
            raise DefinitionError(f'{label} is not valid YAML: {error}')
        if not isinstance(content, dict):
            raise DefinitionError(f'{label} must be a mapping of settings.')
        written = content.pop('name', name)
        if written != name:
            raise DefinitionError(
                f'{label} says name: {written}, but its folder is {name}; rename one to match.'
            )
        definitions[name] = normalize(name, content)
    return definitions


def load_definitions(repo_path: str) -> Dict[str, Dict]:
    """Every service: those in services/<name>/service.yaml, then those still only in the
    earlier pipeline_services.yaml."""
    definitions = _load_legacy(repo_path)
    definitions.update(_load_folder(repo_path))
    return dict(sorted(definitions.items()))


def location(repo_path: str, name: str) -> str:
    """Where a service is saved, relative to the project."""
    if service_file(repo_path, name).is_file():
        return f'{SERVICES_DIR}/{name}/{SERVICE_FILE}'
    return LEGACY_FILE


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
    for model in models:
        problem = _challenger_problem(model) or _audit_problem(model)
        if problem:
            raise DefinitionError(f"Service {name}: model {model['name']}: {problem}")
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
        models=[_model(m) for m in models],
        pipelines=list(dict.fromkeys(pipelines)),
    )


# The share of inputs challengers score when the definition does not say.
DEFAULT_SHADOW_SAMPLE = 0.1


def _challenger_problem(model: Dict) -> Optional[str]:
    challengers = model.get('challengers') or []
    if not isinstance(challengers, list) or not all(
        isinstance(c, dict) and isinstance(c.get('name'), str) and isinstance(c.get('uri'), str)
        and re.match(r'^[A-Za-z0-9_.-]+$', c['name']) and c['uri']
        for c in challengers
    ):
        return ('challengers must be a list of {name, uri}, names of letters, digits, ".", "_" '
                'and "-", such as {name: v4, uri: "models:/churn/4"}.')
    if len({c['name'] for c in challengers}) != len(challengers):
        return 'two challengers have the same name.'
    shadow = model.get('shadow') or {}
    sample = shadow.get('sample', DEFAULT_SHADOW_SAMPLE) if isinstance(shadow, dict) else None
    if isinstance(sample, bool) or not isinstance(sample, (int, float)) or not 0 < sample <= 1:
        return 'shadow.sample must be a share of inputs above 0 and at most 1, such as 0.25.'
    return None


def _audit_problem(model: Dict) -> Optional[str]:
    audit = model.get('audit')
    if audit is None:
        return None
    if not isinstance(audit, dict):
        return 'audit must be a mapping, such as {features: values, mask: [ssn], reasons: 3}.'
    if audit.get('features', 'values') not in ('values', 'hash', 'none'):
        return 'audit.features is values, hash or none.'
    for field in ('mask', 'hash'):
        columns = audit.get(field) or []
        if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
            return f'audit.{field} must be a list of column names.'
    reasons = audit.get('reasons', 0)
    if isinstance(reasons, bool) or not isinstance(reasons, int) or not 0 <= reasons <= 50:
        return 'audit.reasons is the number of top features per score, 0 to 50.'
    return None


def _model(model: Dict) -> Dict:
    out = dict(name=model['name'], uri=model['uri'])
    if model.get('audit') is not None:
        audit = model['audit']
        out['audit'] = dict(features=audit.get('features', 'values'),
                            mask=list(audit.get('mask') or []),
                            hash=list(audit.get('hash') or []),
                            reasons=int(audit.get('reasons', 0)))
    if model.get('challengers'):
        out['challengers'] = [dict(name=c['name'], uri=c['uri']) for c in model['challengers']]
        out['shadow'] = dict(sample=float(
            (model.get('shadow') or {}).get('sample', DEFAULT_SHADOW_SAMPLE)))
    return out


def save_definition(
    repo_path: str,
    name: str,
    value: Dict,
    previous_name: Optional[str] = None,
) -> Dict:
    """
    Saves a service to services/<name>/service.yaml, renaming it (its folder, environments
    and lock included) from previous_name when given. A service still in the earlier
    pipeline_services.yaml moves to its folder.
    """
    if not isinstance(name, str) or not _NAME.match(name):
        raise DefinitionError(
            f'{name!r} is not a service name: use lowercase letters, digits and -.'
        )
    definitions = load_definitions(repo_path)
    renaming = previous_name and previous_name != name
    if renaming and name in definitions:
        raise DefinitionError(f'A service named {name} already exists.')
    normalized = normalize(name, value)
    if renaming and service_path(repo_path, previous_name).is_dir():
        os.replace(service_path(repo_path, previous_name), service_path(repo_path, name))
    _write_service(repo_path, name, normalized)
    _remove_legacy(repo_path, [name] + ([previous_name] if renaming else []))
    return normalized


def delete_definition(repo_path: str, name: str) -> bool:
    """Deletes a service's definition and lock; other files in its folder stay."""
    found = _remove_legacy(repo_path, [name])
    folder = service_path(repo_path, name)
    for file_name in (SERVICE_FILE, LOCK_FILE):
        path = folder / file_name
        if path.is_file():
            path.unlink()
            found = True
    environments = folder / ENVIRONMENTS_DIR
    if (environments / 'dev.yaml').is_file() and \
            (environments / 'dev.yaml').read_text() == DEV_ENVIRONMENT:
        (environments / 'dev.yaml').unlink()
    for directory in (environments, folder):
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()
    return found


def migrate(repo_path: str) -> List[str]:
    """Moves every service of pipeline_services.yaml to its folder; returns their names."""
    legacy = _load_legacy(repo_path)
    folder = _load_folder(repo_path)
    moved = []
    for name, value in legacy.items():
        if name not in folder:
            _write_service(repo_path, name, value)
            moved.append(name)
    _remove_legacy(repo_path, list(legacy))
    return moved


def scaffold(repo_path: str, name: str) -> List[Path]:
    """The files a new service starts with besides service.yaml; existing files are kept."""
    written = []
    dev = service_path(repo_path, name) / ENVIRONMENTS_DIR / 'dev.yaml'
    if not dev.exists():
        dev.parent.mkdir(parents=True, exist_ok=True)
        dev.write_text(DEV_ENVIRONMENT)
        written.append(dev)
    return written


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


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(text)
    os.replace(temporary, path)


def _write_service(repo_path: str, name: str, value: Dict) -> None:
    body = yaml.safe_dump(
        dict(name=name, **_compact(value)), sort_keys=False, default_flow_style=False,
    )
    _atomic_write(service_file(repo_path, name), SERVICE_HEADER + body)
    ensure_gitignore(repo_path)


def _remove_legacy(repo_path: str, names: List[str]) -> bool:
    """Removes services from pipeline_services.yaml; deletes the file when none is left."""
    path = definitions_path(repo_path)
    if not path.is_file():
        return False
    legacy = _load_legacy(repo_path)
    removed = False
    for name in names:
        if name in legacy:
            legacy.pop(name)
            removed = True
    if not removed:
        return False
    if legacy:
        body = yaml.safe_dump(
            dict(services={n: _compact(v) for n, v in sorted(legacy.items())}),
            sort_keys=False,
            default_flow_style=False,
        )
        _atomic_write(path, HEADER + body)
    else:
        path.unlink()
    return True


def ensure_gitignore(repo_path: str) -> None:
    """Generated builds and local secret files stay out of Git."""
    path = Path(repo_path) / '.gitignore'
    existing = path.read_text().splitlines() if path.is_file() else []
    missing = [line for line in GITIGNORE_LINES if line not in existing]
    if missing:
        text = path.read_text() if path.is_file() else ''
        if text and not text.endswith('\n'):
            text += '\n'
        path.write_text(text + '# MageML services and streams: generated files and local secrets\n'
                        + '\n'.join(missing) + '\n')


def summarize(name: str, value: Dict, repo_path: Optional[str] = None) -> Dict:
    summary = dict(name=name, **value)
    if repo_path:
        summary['path'] = location(repo_path, name)
    return summary


def names(repo_path: str) -> List[str]:
    return sorted(load_definitions(repo_path))
