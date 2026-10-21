"""
Models in exported services: which models the blocks use, and their files and metadata
from MLflow.
"""
import ast
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

NAME_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$')
HELPERS = ('load_model', 'model_path', 'model_metadata', 'predict')


def parse_model_option(text: str) -> Tuple[str, str]:
    """`NAME=URI` from --model."""
    if '=' not in text:
        raise ValueError(f'--model {text!r} is not NAME=URI, such as churn=models:/churn/3')
    name, uri = (part.strip() for part in text.split('=', 1))
    if not NAME_PATTERN.match(name):
        raise ValueError(f'--model name {name!r} must use letters, digits, ".", "_" and "-".')
    if not uri:
        raise ValueError(f'--model {name} has no uri.')
    return name, uri


def detect(path: Path) -> List[Tuple[str, str]]:
    """
    (name, uri) of each `load_model('name', uri='...')` call with literal arguments, and of
    `predict('name', data, uri='...')`.
    """
    try:
        tree = ast.parse(path.read_text(), str(path))
    except (SyntaxError, UnicodeDecodeError):
        return []
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, 'id', None)
        if name not in HELPERS:
            continue
        args = [a.value for a in node.args if isinstance(a, ast.Constant)]
        keywords = {
            k.arg: k.value.value for k in node.keywords
            if k.arg and isinstance(k.value, ast.Constant)
        }
        model = keywords.get('name') or (args[0] if args else None)
        if name == 'predict':
            # predict(name, data, uri=...): only a uri keyword, as any model's predict
            # method has this name too.
            uri = keywords.get('uri')
        else:
            uri = keywords.get('uri') or (args[1] if len(args) > 1 else None)
        if isinstance(model, str) and isinstance(uri, str):
            found.append((model, uri))
    return found


def _sha256(directory: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(p for p in directory.rglob('*') if p.is_file()):
        digest.update(str(path.relative_to(directory)).encode())
        digest.update(hashlib.sha256(path.read_bytes()).hexdigest().encode())
    return digest.hexdigest()


def _registry_details(uri: str) -> Dict:
    """Version details of a models:/ uri from the registry; empty for other uris."""
    match = re.match(r'^models:/([^/@]+)(?:/([^/]+)|@(.+))$', uri)
    if not match:
        return {}
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    name, version, alias = match.groups()
    if alias:
        model_version = client.get_model_version_by_alias(name, alias)
    elif version and version.isdigit():
        model_version = client.get_model_version(name, version)
    else:
        # A stage name, such as models:/churn/Production.
        versions = client.get_latest_versions(name, stages=[version])
        if not versions:
            raise ValueError(f'The registry has no version of {name} in stage {version}.')
        model_version = versions[0]
    return dict(
        registered_name=model_version.name,
        version=str(model_version.version),
        aliases=list(getattr(model_version, 'aliases', []) or []),
        run_id=model_version.run_id,
        source=model_version.source,
        description=model_version.description or None,
        tags=dict(model_version.tags or {}),
        created_at=model_version.creation_timestamp,
    )


def _run_details(run_id: Optional[str]) -> Dict:
    if not run_id:
        return {}
    from mlflow.tracking import MlflowClient

    run = MlflowClient().get_run(run_id)
    return dict(
        run_id=run_id,
        experiment_id=run.info.experiment_id,
        params=dict(run.data.params),
        metrics=dict(run.data.metrics),
        run_tags={k: v for k, v in run.data.tags.items() if not k.startswith('mlflow.log-model')},
    )


def _mlmodel(directory: Path) -> Dict:
    path = directory / 'MLmodel'
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text()) or {}
    return dict(
        flavors=sorted((data.get('flavors') or {}).keys()),
        signature=data.get('signature'),
        model_uuid=data.get('model_uuid'),
        mlflow_version=data.get('mlflow_version'),
        utc_time_created=data.get('utc_time_created'),
    )


def describe(name: str, uri: str, directory: Optional[Path] = None) -> Dict:
    details = dict(name=name, uri=uri)
    details.update(_registry_details(uri))
    run_id = details.get('run_id')
    if not run_id:
        match = re.match(r'^runs:/([^/]+)/', uri)
        run_id = match.group(1) if match else None
    details.update(_run_details(run_id))
    if directory is not None:
        details.update(_mlmodel(directory))
    return details


def requirements(directory: Path) -> List[str]:
    """The model's pinned Python requirements, as MLflow logged them."""
    path = directory / 'requirements.txt'
    if not path.is_file():
        return []
    lines = []
    for line in path.read_text().splitlines():
        line = line.split('#', 1)[0].strip()
        if line and not line.startswith('-'):
            lines.append(line)
    return lines


def explain(error: Exception, uri: str, registry: Optional[str] = None) -> str:
    """
    What went wrong reaching a model and what to do, for people: MLflow's own messages name
    its internals ("Registered model alias retired not found").
    """
    text = str(error).strip()
    code = getattr(error, 'error_code', None) or ''
    where = registry or _safe_tracking_uri() or 'the registry'
    match = re.match(r'^models:/([^/@]+)(?:/([^/]+)|@(.+))$', uri)
    name, version, alias = match.groups() if match else (None, None, None)
    lowered = text.lower()

    if type(error).__name__ == 'UnsupportedModelRegistryStoreURIException' or \
            'unsupported uri' in lowered:
        return (f'{where} is not a registry address. Use http:// or https:// with the host '
                'and port, such as http://mlflow:5000.')
    if 'max retries exceeded' in lowered or 'timed out' in lowered or \
            'connection refused' in lowered or 'name or service not known' in lowered or \
            'nodename nor servname' in lowered or type(error).__name__ in (
                'ConnectionError', 'ConnectTimeout', 'ReadTimeout'):
        return (f'The registry at {where} did not answer. Check the address, and that the '
                'MageML server can reach it (VPN, firewall, cluster DNS).')
    if code in ('UNAUTHENTICATED', 'PERMISSION_DENIED') or ' 401' in text or ' 403' in text \
            or 'unauthorized' in lowered or 'forbidden' in lowered:
        return (f'The registry at {where} refused the request. Set MLFLOW_TRACKING_TOKEN, or '
                'MLFLOW_TRACKING_USERNAME and MLFLOW_TRACKING_PASSWORD, where MageML runs, '
                'with an account that can read the model.')
    if name and 'registered model with name' in lowered and 'not found' in lowered:
        known = _registered_names()
        hint = f' The registry has: {", ".join(known)}.' if known else ''
        return f'The registry has no model named {name}; names are case-sensitive.{hint}'
    if name and alias and 'alias' in lowered and 'not found' in lowered:
        aliases = _aliases(name)
        hint = (' Its aliases: ' + ', '.join(f'{a} (version {v})' for a, v in aliases.items())
                + '.') if aliases else ' It has no aliases; pick a version instead.'
        return f'{name} has no alias {alias}.{hint}'
    if name and version and 'not found' in lowered:
        latest = _latest_version(name)
        hint = f' Its latest version is {latest}.' if latest else ''
        return f'{name} has no version {version}.{hint}'
    if type(error).__name__ in ('NoCredentialsError', 'PartialCredentialsError') or \
            'accessdenied' in lowered.replace(' ', '') or 'invalidaccesskeyid' in lowered:
        return ('The model files are in object storage the MageML server cannot read. Give '
                'it the storage credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, '
                'and MLFLOW_S3_ENDPOINT_URL for an S3-compatible store such as MinIO).')
    if 'no space left' in lowered:
        return 'The MageML server ran out of disk space while downloading the model.'
    if 'failures occurred while downloading' in lowered and (
            'no such artifact' in lowered or ' 404 ' in f' {text} '):
        files = re.findall(r'##### File (.+?) #####', text)
        missing = ', '.join(files) if files else 'the model files'
        return (f'The registry has this model version, but {missing} is missing from the '
                f'artifact store of {where}: the files were deleted or never logged. Log the '
                'model again, or pick another version.')
    # MLflow's messages often end their first line with a colon and give the cause below.
    lines = [line.strip() for line in text.splitlines()
             if line.strip() and not line.strip().startswith('#####')]
    first = lines[0] if lines else ''
    if first.endswith(':') and len(lines) > 1:
        first = f'{first} {lines[1]}'
    first = first[:300]
    return f'{type(error).__name__}: {first}' if first else type(error).__name__


def _registered_names(limit: int = 12) -> List[str]:
    try:
        from mlflow.tracking import MlflowClient

        return sorted(m.name for m in MlflowClient().search_registered_models(
            max_results=limit))
    except Exception:
        return []


def _aliases(name: str) -> Dict[str, str]:
    try:
        from mlflow.tracking import MlflowClient

        return dict(MlflowClient().get_registered_model(name).aliases or {})
    except Exception:
        return {}


def _latest_version(name: str) -> Optional[str]:
    try:
        from mlflow.tracking import MlflowClient

        versions = MlflowClient().search_model_versions(
            f"name='{name}'", max_results=1, order_by=['version_number DESC'],
        )
        return str(versions[0].version) if versions else None
    except Exception:
        return None


def download_uri(uri: str) -> str:
    """
    What to download for a uri: a models:/ uri gives its version's source, since MLflow
    downloads nothing for one whose source is a single file, such as a model.cbm logged as
    an artifact.
    """
    if not uri.startswith('models:/'):
        return uri
    source = _registry_details(uri).get('source')
    return source or uri


def download(name: str, uri: str, target: Path) -> Dict:
    """Downloads the model into target/artifacts and writes its metadata; returns it."""
    import mlflow

    if target.exists():
        shutil.rmtree(target)
    artifacts = target / 'artifacts'
    artifacts.parent.mkdir(parents=True, exist_ok=True)
    downloaded = Path(mlflow.artifacts.download_artifacts(
        artifact_uri=download_uri(uri), dst_path=str(target / '.download'),
    ))
    if downloaded.is_file():
        artifacts.mkdir()
        shutil.move(str(downloaded), artifacts / downloaded.name)
    else:
        shutil.move(str(downloaded), artifacts)
    shutil.rmtree(target / '.download', ignore_errors=True)
    metadata = describe(name, uri, artifacts)
    metadata['sha256'] = _sha256(artifacts)
    metadata['size_bytes'] = sum(p.stat().st_size for p in artifacts.rglob('*') if p.is_file())
    metadata['requirements'] = requirements(artifacts)
    metadata['tracking_uri'] = _safe_tracking_uri()
    (target / 'mage-model.json').write_text(json.dumps(metadata, indent=2, default=str) + '\n')
    return metadata


def _safe_tracking_uri() -> Optional[str]:
    """The tracking server, without credentials in the URL."""
    uri = os.environ.get('MLFLOW_TRACKING_URI')
    if not uri:
        return None
    return re.sub(r'//[^/@]+@', '//', uri)
