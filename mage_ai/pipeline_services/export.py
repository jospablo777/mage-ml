"""
`mage export service`: pipelines of a Mage project as a standalone Docker service.

The export reads the project and writes a build context:

    <out>/
      service.json         what the service runs (see rust/mage_service/crates/core)
      <project>/           the block files, shared code, io_config.yaml
      python/mage_ai/      the part of Mage that blocks import, and the block worker
      rust/                the Rust blocks' crates, built in the image
      build/mage_service/  the service's source, built in the image
      requirements.txt     the Python packages the blocks need, pinned
      Dockerfile, compose.yaml, .dockerignore, README.md, report.txt

It only reads the project: it never syncs triggers or writes the metadata database. A
block or setting the service does not support stops the export with a reason; nothing is
dropped silently.
"""
import ast
import csv
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import string
import subprocess
import sys
import textwrap
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yaml

from mage_ai.pipeline_services.secrets import (
    CLOUD_SECRET_FUNCTIONS,
    cloud_secret_source,
    cloud_secret_variable_name,
    secret_variable_name,
)

SCHEMA_VERSION = 2
MAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = MAGE_ROOT.parent
# The service's Rust workspace ships inside the package, as the Rust block SDK does.
SERVICE_SOURCE = Path(__file__).resolve().parent / 'service'
TEMPLATES = Path(__file__).resolve().parent / 'templates'
SUPPORTED_TYPES = ('data_loader', 'transformer', 'data_exporter', 'custom')
# Notebook-only blocks; they never run in a pipeline run.
NOTEBOOK_TYPES = ('chart', 'markdown', 'scratchpad')
# Packages every Python service needs: the worker exchanges tables as Arrow.
BASE_PACKAGES = ('pyarrow',)
# Distributions Mage brings that a service never needs.
EXCLUDED_DISTRIBUTIONS = {'mage-ml', 'mage-ai', 'mage-integrations', 'pip', 'setuptools'}
# Parts of mage_ai blocks never import.
MAGE_EXCLUDED_DIRS = {
    'frontend', 'tests', 'frontend_dist', 'frontend_dist_base_path_template', '__pycache__',
}
# Rust sources the image builds separately.
MAGE_EXCLUDED_PATHS = (
    Path('pipeline_services/service'),
    Path('data_preparation/models/block/rust/sdk'),
)
# Mage's R runner and its mageml R package: every file, whatever its suffix.
R_RUNNER = Path('data_preparation/models/block/r')
MAGE_DATA_SUFFIXES = ('.py', '.yaml', '.yml', '.json', '.jinja', '.sql', '.txt')
SKIPPED_PROJECT_DIRS = {
    '.variables', '.logs', '.file_versions', '__pycache__', '.mage_temp_profiles', 'pipelines',
    'rust', 'r', '.git', 'node_modules',
}


class ExportError(Exception):
    def __init__(self, problems: List[str]):
        super().__init__('\n'.join(problems))
        self.problems = problems


@dataclass
class Capture:
    name: str
    project: Path
    manifest: Dict
    files: Set[Path] = field(default_factory=set)
    rust_blocks: List[Tuple[str, str]] = field(default_factory=list)  # (block uuid, crate)
    # Crates of Rust stream transformers: resident programs (mage::serve_stream).
    rust_stream_crates: Set[str] = field(default_factory=set)
    requirements: Dict[str, str] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    unresolved_imports: Set[str] = field(default_factory=set)
    # (name, uri) of each ML model to embed.
    models: List[Tuple[str, str]] = field(default_factory=list)
    # Whether the copied Cargo.lock matches the exported crates, so the image builds --locked.
    rust_locked: bool = False
    # The rv environment R blocks run with (rproject.toml and rv.lock).
    r_project: Optional[Path] = None
    # The pipeline environment (metadata.yaml `environment`) the Python blocks run in.
    environment: Optional[Any] = None
    # Modules that read the included model files (.cbm: catboost), for the requirements.
    artifact_modules: Set[str] = field(default_factory=set)
    # Stream source and sink YAML files: their settings are in the manifest and only the
    # engine reads them, so they count in the source digest but are not copied.
    stream_files: Set[Path] = field(default_factory=set)

    @property
    def streams(self) -> List[Dict]:
        return [p['stream'] for p in self.manifest['pipelines'] if p.get('stream')]

    @property
    def connector_kinds(self) -> Set[str]:
        kinds = set()
        for stream in self.streams:
            dead_letter = (stream.get('errors') or {}).get('dead_letter')
            for connector in [stream['source'], *stream['sinks'], *filter(None, [dead_letter])]:
                kinds.add(connector['kind'])
        return kinds

    @property
    def needs_kafka(self) -> bool:
        return 'kafka' in self.connector_kinds

    @property
    def engine_features(self) -> List[str]:
        """The engine's optional connectors the streams use."""
        return sorted(ENGINE_FEATURES[k] for k in self.connector_kinds if k in ENGINE_FEATURES)

    @property
    def needs_ibm_mq(self) -> bool:
        return 'ibm_mq' in self.connector_kinds

    @property
    def needs_r(self) -> bool:
        return any(
            b['language'] == 'r' for p in self.manifest['pipelines'] for b in p['blocks']
        )

    @property
    def needs_python(self) -> bool:
        # The Python worker runs R blocks too, with Mage's R runner, and SQL blocks.
        return any(
            b['language'] in ('python', 'r', 'sql')
            for p in self.manifest['pipelines'] for b in p['blocks']
        )


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _service_name(name: Optional[str], pipelines: List[str]) -> str:
    raw = name or (pipelines[0] if len(pipelines) == 1 else 'pipelines')
    slug = re.sub(r'[^a-z0-9-]+', '-', raw.lower()).strip('-')
    return slug or 'pipelines'


def capture(
    project: str,
    pipeline_uuids: List[str],
    name: Optional[str] = None,
    max_concurrent_runs: Optional[int] = None,
    models: Optional[List[str]] = None,
    includes: Optional[List[str]] = None,
) -> Capture:
    """Reads the pipelines and everything they need; raises ExportError with every problem."""
    from mage_ai.settings.repo import set_repo_path

    project_path = Path(project).resolve()
    if not (project_path / 'metadata.yaml').exists() and not (project_path / 'pipelines').is_dir():
        raise ExportError([
            f'{project_path} is not a Mage project: it has no metadata.yaml or pipelines folder.'
        ])
    set_repo_path(str(project_path))
    if str(project_path.parent) not in sys.path:
        sys.path.append(str(project_path.parent))

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.repo_manager import get_repo_config

    problems: List[str] = []
    notes: List[str] = []
    service = _service_name(name, pipeline_uuids)
    repo_config = get_repo_config(str(project_path))
    capture_ = Capture(name=service, project=project_path, manifest={}, notes=notes)
    pipelines = []
    environments = {}
    for uuid in dict.fromkeys(pipeline_uuids):
        try:
            pipeline = Pipeline.get(uuid, repo_path=str(project_path), check_if_exists=True)
        except Exception as error:
            problems.append(f'Pipeline {uuid}: {error}')
            continue
        if pipeline is None:
            problems.append(f'Pipeline {uuid} does not exist in {project_path}.')
            continue
        entry = _pipeline_entry(pipeline, project_path, repo_config, capture_, problems,
                                max_concurrent_runs)
        if entry:
            pipelines.append(entry)
        environments[uuid] = _pipeline_environment(pipeline, problems)
    _check_after_runs(pipelines, notes, problems)
    for entry in pipelines:
        _check_integers(entry, problems)
    if problems:
        raise ExportError(problems)
    capture_.environment = _shared_environment(environments, problems)
    if problems:
        raise ExportError(problems)

    _include_shared_code(capture_, problems)
    _include_artifacts(capture_, includes or [], problems)
    for extra in ('io_config.yaml', 'requirements.txt', '__init__.py'):
        if (project_path / extra).is_file():
            capture_.files.add(project_path / extra)
    if problems:
        raise ExportError(problems)

    digest = hashlib.sha256()
    for path in sorted(capture_.files | capture_.stream_files):
        digest.update(str(path.relative_to(project_path)).encode())
        digest.update(_digest(path).encode())
    capture_.manifest = dict(
        schema_version=SCHEMA_VERSION,
        service=dict(
            name=service,
            project=project_path.name,
            exported_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            mage_version=_mage_version(),
            source_sha256=digest.hexdigest(),
        ),
        pipelines=pipelines,
    )
    capture_.manifest['environment'] = _environment(capture_)
    if capture_.needs_r:
        capture_.r_project = _r_project(project_path)
    capture_.models = _models(capture_, models or [])
    if capture_.needs_python:
        capture_.requirements = _requirements(capture_)
        if capture_.environment is not None:
            _apply_environment(capture_)
    return capture_


def _oversized(value, path: str):
    """(path, integer) for each integer that does not fit in 64 bits."""
    if isinstance(value, bool):
        return
    if isinstance(value, int) and not -2**63 <= value <= 2**64 - 1:
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _oversized(item, f'{path}.{key}' if path else str(key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _oversized(item, f'{path}[{index}]')


def _check_integers(entry: Dict, problems: List[str]) -> None:
    """
    The service holds variables as JSON numbers of 64 bits: a larger integer would lose
    digits, so it stops the export with the variable's name.
    """
    sources = [('variable', entry.get('variables') or {})]
    sources += [
        (f"trigger {t.get('name')} variable", t.get('variables') or {})
        for t in entry.get('triggers') or []
    ]
    for what, variables in sources:
        for path, value in _oversized(variables, ''):
            problems.append(
                f"Pipeline {entry['uuid']}, {what} {path}: {value} is too large for a 64-bit "
                'integer and the service would lose digits; write it as a string.'
            )


def _check_after_runs(entries: List[Dict], notes: List[str], problems: List[str]) -> None:
    """
    After-run triggers in the service: one that follows a pipeline left out of the service
    could never start, so it is left out with a note; a loop stops the export.
    """
    from mage_ai.orchestration.after_runs import trigger_problems

    exported = {e['uuid'] for e in entries}
    pairs = []
    for entry in entries:
        kept = []
        for trigger in entry['triggers']:
            if trigger['kind'] == 'after_run' and trigger['after_pipeline'] not in exported:
                notes.append(
                    f"Pipeline {entry['uuid']}, trigger {trigger['name']}: it follows "
                    f"{trigger['after_pipeline']}, which is not in the service; left out."
                )
                continue
            kept.append(trigger)
            if trigger['kind'] == 'after_run' and trigger['active']:
                pairs.append((entry['uuid'], dict(
                    after_pipeline=trigger['after_pipeline'],
                    after_status=trigger['after_status'],
                )))
        entry['triggers'] = kept
    for index, (uuid, settings) in enumerate(pairs):
        others = pairs[:index] + pairs[index + 1:]
        found = trigger_problems(uuid, settings, others)
        if found:
            problems.append(f'Pipeline {uuid}: {found[0]}')
            break


def _pipeline_environment(pipeline, problems: List[str]):
    from mage_ai.data_preparation import environments

    try:
        return environments.pipeline_environment(pipeline)
    except environments.PipelineEnvironmentError as error:
        problems.append(str(error))
        return None


def _shared_environment(environments: Dict[str, Any], problems: List[str]):
    """
    The one environment the exported pipelines run in. A service image has one Python
    environment, so pipelines that declare different ones are exported separately.
    """
    declared = {uuid: env for uuid, env in environments.items() if env is not None}
    if not declared:
        return None
    if len(declared) < len(environments) or len(set(declared.values())) > 1:
        described = ', '.join(
            f'{uuid} ({kind})' for uuid, kind in (
                (uuid, 'its own environment' if env else "Mage's packages")
                for uuid, env in environments.items()
            )
        )
        problems.append(
            'The pipelines run in different Python environments and a service has one: '
            f'{described}. Export them as separate services.'
        )
        return None
    return next(iter(declared.values()))


def _apply_environment(capture_: Capture) -> None:
    """
    Pins the packages of the pipeline environment over the versions resolved from Mage's
    own: the blocks run with the environment's packages in Mage, so the service does too.
    The environment is built first when it does not exist yet.
    """
    from mage_ai.data_preparation import environments

    environment = capture_.environment
    try:
        environments.ensure(environment, log=lambda _line: None)
    except environments.PipelineEnvironmentError as error:
        raise ExportError([f'Building the pipeline environment failed: {error}'])
    marker = json.loads((environment.directory / environments.MARKER).read_text())
    for line in marker.get('packages') or []:
        match = re.match(r'\s*([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)\s*$', line)
        if match is None:
            capture_.notes.append(
                f'The pipeline environment installs {line.strip()}, which is not a pinned '
                'release; add it to the project requirements.txt if the blocks need it.'
            )
            continue
        name = match.group(1).lower().replace('_', '-')
        if name not in EXCLUDED_DISTRIBUTIONS:
            capture_.requirements[name] = match.group(2)
    capture_.requirements = dict(sorted(capture_.requirements.items()))
    capture_.notes.append(
        f'The Python blocks run in the pipeline environment {environment.directory.name} '
        f'(Python {environment.python}); the image installs its packages.'
    )


SQL_PROVIDERS = ('postgres', 'mysql', 'duckdb', 'clickhouse')


def _sql_unsupported(block) -> Optional[str]:
    configuration = block.configuration or {}
    provider = configuration.get('data_provider')
    if provider not in SQL_PROVIDERS:
        return (
            f'a SQL block on {provider or "no data provider"}; services run SQL blocks on '
            'PostgreSQL, MySQL, DuckDB and ClickHouse.'
        )
    if not configuration.get('data_provider_profile'):
        return 'a SQL block without a data provider profile (io_config.yaml).'
    if 'block_output' in (block.content or ''):
        return 'a SQL block that uses block_output(), which services do not support yet.'
    return None


def _sql_table_name(pipeline, block) -> str:
    """The table of a block's output in SQL, as Block.table_name in a pipeline run."""
    from mage_ai.shared.utils import clean_name

    configured = (block.configuration or {}).get('data_provider_table')
    if configured:
        return configured
    return f'{pipeline.uuid}_{clean_name(block.uuid)}_{pipeline.version_name}'


def _sql_details(pipeline, block) -> Dict:
    """What the service's SQL runner needs from the block and its upstream blocks."""
    upstream = []
    for parent in block.upstream_blocks:
        language = str(getattr(parent.language, 'value', parent.language))
        configuration = _json_safe(parent.configuration or {})
        entry = dict(
            uuid=parent.uuid,
            type=str(getattr(parent.type, 'value', parent.type)),
            language=language,
            configuration=configuration,
            table_name=_sql_table_name(pipeline, parent),
        )
        if language == 'sql' and configuration.get('use_raw_sql'):
            # A raw SELECT on the same database is inlined into the query.
            entry['content'] = parent.content or ''
        upstream.append(entry)
    return dict(table_name=_sql_table_name(pipeline, block), upstream=upstream)


def _r_project(project: Path) -> Path:
    """
    The project's rv environment, which the image installs as rv.lock pins it. Found as
    Mage finds it (MAGE_R_PROJECT_DIR, <project>/r, the project); R need not be installed.
    """
    configured = os.getenv('MAGE_R_PROJECT_DIR')
    if configured:
        path = Path(configured)
        candidates = [path if path.is_absolute() else project / path]
    else:
        candidates = [project / 'r', project]
    directory = next((c for c in candidates if (c / 'rproject.toml').is_file()), None)
    if directory is None:
        raise ExportError([
            'R blocks need an R environment managed by rv, so the image installs the same '
            f'packages: run `mage r init {project}`.'
        ])
    if not (directory / 'rv.lock').is_file():
        raise ExportError([
            f'{directory} has no rv.lock; run `mage r sync {project}` so the image installs '
            'the versions the blocks ran with.'
        ])
    r_system_packages(directory)
    return directory.resolve()


R_SYSTEM_PACKAGES = 'system-packages.txt'
DEBIAN_PACKAGE = re.compile(r'^[a-z0-9][a-z0-9+.\-]+$')


def r_system_packages(directory: Path) -> List[str]:
    """
    The Debian packages that the R packages of the rv project need to build, beyond those
    the image installs, from system-packages.txt next to rproject.toml: one name per line,
    with # comments. For example libgdal-dev for sf.
    """
    path = directory / R_SYSTEM_PACKAGES
    if not path.is_file():
        return []
    names = []
    invalid = []
    for line in path.read_text(encoding='utf-8').splitlines():
        for name in line.split('#', 1)[0].split():
            (names if DEBIAN_PACKAGE.match(name) else invalid).append(name)
    if invalid:
        raise ExportError([
            f'{path} lists names that are not Debian package names: {", ".join(invalid)}.'
        ])
    return list(dict.fromkeys(names))


def _models(capture_: Capture, options: List[str]) -> List[Tuple[str, str]]:
    """The models from --model, and those the code loads with literal names and uris."""
    from mage_ai.pipeline_services import model_export

    problems = []
    explicit: Dict[str, str] = {}
    for option in options:
        try:
            name, uri = model_export.parse_model_option(option)
        except ValueError as error:
            problems.append(str(error))
            continue
        explicit[name] = uri
    detected: Dict[str, str] = {}
    for path in sorted(capture_.files):
        if path.suffix != '.py':
            continue
        for name, uri in model_export.detect(path):
            where = path.relative_to(capture_.project)
            if name in explicit:
                continue
            if name in detected and detected[name] != uri:
                problems.append(
                    f'Model {name} is loaded with two uris, {detected[name]} and {uri} (in '
                    f'{where}); choose one with --model {name}=<uri>.'
                )
                continue
            if name not in detected:
                capture_.notes.append(f'Model {name} ({uri}) is loaded in {where}; it is embedded.')
            detected[name] = uri
    if problems:
        raise ExportError(problems)
    chosen = {**detected, **explicit}
    return sorted(chosen.items())


def _mage_version() -> str:
    try:
        return importlib.metadata.version('mage-ml')
    except importlib.metadata.PackageNotFoundError:
        return 'unknown'


def _pipeline_entry(pipeline, project: Path, repo_config, capture_: Capture,
                    problems: List[str], max_runs: Optional[int]) -> Optional[Dict]:
    prefix = f'Pipeline {pipeline.uuid}'
    if str(pipeline.type) in ('streaming', 'PipelineType.STREAMING'):
        return _stream_pipeline_entry(pipeline, project, repo_config, capture_, problems)
    if str(pipeline.type) not in ('python', 'PipelineType.PYTHON'):
        problems.append(
            f'{prefix} is a {pipeline.type} pipeline; services run batch and streaming pipelines.'
        )
        return None
    blocks = []
    conditionals: Dict[str, Dict] = {}
    for block in pipeline.blocks_by_uuid.values():
        entry = _block_entry(pipeline, block, project, repo_config, capture_, problems,
                             conditionals)
        if entry is not None:
            blocks.append(entry)
    # Upstream blocks that were left out (notebook blocks) are not dependencies.
    kept = {b['uuid'] for b in blocks}
    for entry in blocks:
        entry['upstream'] = [u for u in entry['upstream'] if u in kept]
    concurrency = (getattr(pipeline, 'concurrency_config', None) or {})
    limit = max_runs or concurrency.get('pipeline_run_limit_all_triggers') or \
        concurrency.get('pipeline_run_limit') or 1
    pipeline_entry = dict(
        uuid=pipeline.uuid,
        name=pipeline.name or pipeline.uuid,
        description=pipeline.description or None,
        max_concurrent_runs=int(limit),
        variables=_json_safe(getattr(pipeline, 'variables', None) or {}),
        blocks=blocks,
        triggers=_triggers(pipeline, capture_.notes, problems),
    )
    if conditionals:
        pipeline_entry['conditionals'] = list(conditionals.values())
    return pipeline_entry


# Stream connectors services run, by Mage's connector_type: the manifest kind.
STREAM_SOURCES = {
    'kafka': 'kafka', 'webhook': 'http', 'rabbitmq': 'rabbitmq', 'ibm_mq': 'ibm_mq',
}
STREAM_SINKS = {
    'kafka': 'kafka', 'dummy': 'stdout', 'http': 'http', 'feast': 'feast', 'rabbitmq': 'rabbitmq',
    'ibm_mq': 'ibm_mq', 'object_storage': 'object_storage', 'amazon_s3': 'object_storage',
    'google_cloud_storage': 'object_storage', 'postgres': 'postgres',
    'clickhouse': 'clickhouse',
}
# Engine features (cargo) by manifest kind, for the connectors that are optional.
ENGINE_FEATURES = {
    'kafka': 'kafka', 'rabbitmq': 'rabbitmq', 'ibm_mq': 'ibm-mq',
    'object_storage': 'object-store', 'postgres': 'postgres', 'clickhouse': 'clickhouse',
}
# What the ClickHouse sink takes from the export config of MageML's sink, besides its
# io_config.yaml profile.
CLICKHOUSE_EXPORT_SETTINGS = {
    'table_name', 'database', 'auto_clean_name', 'case_sensitive', 'allow_reserved_words',
}
# MageML's PostgreSQL stream sink settings, and TLS.
POSTGRES_SINK_SETTINGS = {
    'host', 'port', 'database', 'username', 'password', 'schema', 'table',
    'unique_constraints', 'unique_conflict_method', 'auto_clean_name', 'case_sensitive',
    'allow_reserved_words', 'sslmode', 'sslrootcert', 'connect_timeout',
}
# Object storage sinks: the generic one, and Mage's S3 and GCS sinks, which name a bucket.
OBJECT_STORAGE_SETTINGS = {
    'url', 'format', 'date_partition_format', 'endpoint_url', 'region', 'buffer_size_mb',
    'buffer_timeout_seconds',
}
MAGE_OBJECT_SINK_SETTINGS = {
    'bucket', 'prefix', 'file_type', 'buffer_size_mb', 'buffer_timeout_seconds',
    'date_partition_format', 'path_to_credentials_json_file',
}
IBM_MQ_SETTINGS = {
    'queue_manager', 'channel', 'connection_name', 'host', 'port', 'queue', 'user', 'password',
    'application_name', 'cipher_spec', 'key_repository', 'certificate_label',
}
IBM_MQ_SOURCE_SETTINGS = IBM_MQ_SETTINGS | {'connections', 'batch_size', 'batch_timeout'}
# IBM's redistributable MQ client for exported images: Linux x86-64 only (IBM publishes no
# arm64 one). Pinned and checked.
IBM_MQ_CLIENT_VERSION = '9.4.5.1'
IBM_MQ_CLIENT_SHA256 = 'd01fa65721639040755ab155291d1f6327bd429e4f503748abe0f3b3a04fa853'
IBM_MQ_CLIENT_URL = (
    'https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/'
    f'{IBM_MQ_CLIENT_VERSION}-IBM-MQC-Redist-LinuxX64.tar.gz'
)
# Mage's RabbitMQ settings the engine's connector reads.
RABBITMQ_SETTINGS = {
    'connection_host', 'connection_port', 'queue_name', 'username', 'password',
    'amqp_url_virtual_host', 'url_protocol',
}
RABBITMQ_SOURCE_SETTINGS = RABBITMQ_SETTINGS | {
    'prefetch_count', 'batch_size', 'batch_timeout', 'configure_consume', 'consume_config',
}
RABBITMQ_SINK_SETTINGS = RABBITMQ_SETTINGS | {'exchange', 'routing_key'}
FEAST_SINK_SETTINGS = {
    'url', 'push_source_name', 'to', 'headers', 'timeout_seconds', 'allow_private',
}
HTTP_SINK_SETTINGS = {'url', 'headers', 'timeout_seconds', 'format', 'allow_private'}
# Header names whose values are credentials: written as env_var() references.
CREDENTIAL_HEADER = re.compile(r'auth|token|key|secret|password|cookie', re.IGNORECASE)
# What comes next, for the message that names an unsupported connector.
STREAM_PLANNED = 'NATS, Pulsar and PostgreSQL change capture come next'
# A webhook source's settings: requests signed with an HMAC of the body, and how long a
# caller waits and a retry's key is remembered.
WEBHOOK_SETTINGS = {'signature', 'answer_within_seconds', 'dedupe_window_seconds'}
SIGNATURE_SETTINGS = {'header', 'prefix', 'secret'}
# Mage's Kafka settings the engine's connector reads; others stop the export, as Mage's own
# settings class refuses unknown keys.
KAFKA_SETTINGS = {
    'bootstrap_server', 'bootstrap_servers', 'security_protocol', 'sasl_config', 'ssl_config',
    'api_version', 'librdkafka',
}
KAFKA_SOURCE_SETTINGS = KAFKA_SETTINGS | {
    'topic', 'topics', 'consumer_group', 'auto_offset_reset', 'batch_size', 'timeout_ms',
    'include_metadata', 'serde_config', 'offset', 'max_partition_fetch_bytes',
}
KAFKA_SINK_SETTINGS = KAFKA_SETTINGS | {'topic', 'batch_size', 'timeout_ms'}
SASL_SETTINGS = {'mechanism', 'username', 'password'}
SSL_SETTINGS = {'cafile', 'certfile', 'keyfile', 'password', 'check_hostname'}
# Settings that hold secrets: written as env_var() references, never as values.
SECRET_SETTINGS = {
    ('sasl_config', 'password'), ('ssl_config', 'password'), ('signature', 'secret'),
    ('serde_config', 'schema_registry_password'),
}
# Top-level settings that hold secrets (RabbitMQ's password).
SECRET_KEYS = {'password'}


def compile_stream_config(text: str, variables: Dict) -> Dict:
    """
    A stream block's YAML settings with env_var() and mage_secret_var() kept as references
    the engine fills in at run time. variables() reads the pipeline's variables, as Mage does.
    Raises ValueError for templates a service can't fill in.
    """
    import jinja2
    import yaml

    def unsupported(name):
        def function(*args, **kwargs):
            raise ValueError(
                f'it uses {name}(), which services can\'t fill in for stream connectors; read '
                'the value with env_var() and set it in the service\'s environment.'
            )
        return function

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    try:
        rendered = environment.from_string(text).render(
            env_var=_variable_mark,
            mage_secret_var=lambda name: _variable_mark(secret_variable_name(name)),
            **_cloud_secret_marks(),
            variables=lambda name: (variables or {}).get(name),
            json_value=unsupported('json_value'),
        )
    except jinja2.TemplateError as error:
        raise ValueError(f'it has a template services can\'t fill in: {error}')
    try:
        config = yaml.safe_load(rendered)
    except yaml.YAMLError as error:
        raise ValueError(f'its YAML is not valid: {error}')
    if not isinstance(config, dict):
        raise ValueError('its settings are not a YAML mapping.')
    return _io_config_parts(config)


def _literal(value) -> bool:
    """A setting written as a value, not as a reference to the environment."""
    return value not in (None, '') and not (isinstance(value, dict) and '$parts' in value)


def _kafka_settings(config: Dict, allowed: Set[str], where: str,
                    problems: List[str]) -> Dict:
    unknown = sorted(set(config) - allowed)
    if unknown:
        problems.append(f'{where}: unknown Kafka settings {", ".join(unknown)}.')
    for section, keys in (('sasl_config', SASL_SETTINGS), ('ssl_config', SSL_SETTINGS)):
        value = config.get(section)
        if value is None:
            continue
        if not isinstance(value, dict):
            problems.append(f'{where}: {section} must be a mapping.')
            continue
        extra = sorted(set(value) - keys)
        if extra:
            problems.append(
                f'{where}: {section} settings {", ".join(extra)} are not supported by services '
                'yet (OAuth sign-in comes later).'
            )
    for section, key in SECRET_SETTINGS:
        if _literal((config.get(section) or {}).get(key)):
            problems.append(
                f"{where}: {section}.{key} is written in the file; use "
                f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
            )
    settings = {k: v for k, v in config.items() if k not in ('batch_size', 'timeout_ms')}
    if 'max_partition_fetch_bytes' in settings:
        extra = dict(settings.get('librdkafka') or {})
        extra['max.partition.fetch.bytes'] = settings.pop('max_partition_fetch_bytes')
        settings['librdkafka'] = extra
    # librdkafka finds the broker's protocol version itself.
    settings.pop('api_version', None)
    return settings


def _webhook_settings(config: Dict, where: str, problems: List[str]) -> Dict:
    unknown = sorted(set(config) - WEBHOOK_SETTINGS)
    if unknown:
        problems.append(f'{where}: unknown webhook settings {", ".join(unknown)}.')
    signature = config.get('signature')
    if signature is not None:
        if not isinstance(signature, dict):
            problems.append(f'{where}: signature must be a mapping (header, prefix, secret).')
        else:
            extra = sorted(set(signature) - SIGNATURE_SETTINGS)
            if extra:
                problems.append(f'{where}: unknown signature settings {", ".join(extra)}.')
            secret = signature.get('secret')
            if not secret:
                problems.append(f'{where}: signature needs a secret.')
            elif _literal(secret):
                problems.append(
                    f"{where}: signature.secret is written in the file; use "
                    f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
                )
    for key in ('answer_within_seconds', 'dedupe_window_seconds'):
        value = config.get(key)
        if value is not None and not (isinstance(value, int) and value > 0):
            problems.append(f'{where}: {key} must be a whole number of seconds above 0.')
    return {k: v for k, v in config.items() if k in WEBHOOK_SETTINGS}


def _http_sink_settings(config: Dict, where: str, problems: List[str]) -> Dict:
    unknown = sorted(set(config) - HTTP_SINK_SETTINGS)
    if unknown:
        problems.append(f'{where}: unknown HTTP sink settings {", ".join(unknown)}.')
    url = config.get('url')
    if not url:
        problems.append(f'{where}: an HTTP sink needs a url.')
    elif isinstance(url, str) and not url.startswith(('https://', 'http://')):
        problems.append(f'{where}: the url must start with https:// or http://.')
    headers = config.get('headers') or {}
    if not isinstance(headers, dict):
        problems.append(f'{where}: headers must be a mapping of names to values.')
        headers = {}
    for name, value in headers.items():
        if CREDENTIAL_HEADER.search(str(name)) and _literal(value):
            problems.append(
                f"{where}: the {name} header is written in the file; use "
                f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
            )
    if config.get('format') not in (None, 'json', 'ndjson'):
        problems.append(f"{where}: format is json (an array per batch) or ndjson.")
    return {k: v for k, v in config.items() if k in HTTP_SINK_SETTINGS}


def _rabbitmq_connector(block, role: str, config: Dict, where: str,
                        problems: List[str]) -> Dict:
    allowed = RABBITMQ_SOURCE_SETTINGS if role == 'source' else RABBITMQ_SINK_SETTINGS
    unknown = sorted(set(config) - allowed)
    if unknown:
        problems.append(f'{where}: unknown RabbitMQ settings {", ".join(unknown)}.')
    if not config.get('connection_host'):
        problems.append(f'{where}: RabbitMQ settings need connection_host.')
    if role == 'source' and not config.get('queue_name'):
        problems.append(f'{where}: a RabbitMQ source needs queue_name.')
    if _literal(config.get('password')) and config.get('password') != 'guest':
        problems.append(
            f"{where}: password is written in the file; use "
            f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
        )
    if (config.get('consume_config') or {}).get('auto_ack'):
        problems.append(
            f'{where}: consume_config.auto_ack acknowledges messages as they arrive, before '
            'any sink took them; services acknowledge after the sinks, so remove it.'
        )
    settings = {k: v for k, v in config.items() if k in RABBITMQ_SETTINGS
                or k in ('prefetch_count', 'exchange', 'routing_key')}
    connector = dict(name=block.uuid, kind='rabbitmq', config=settings)
    if role == 'source':
        # Mage's RabbitMQ source batches by count and seconds.
        if config.get('batch_size'):
            connector['batch_size'] = config['batch_size']
        if config.get('batch_timeout'):
            connector['timeout_ms'] = int(float(config['batch_timeout']) * 1000)
    return connector


def _ibm_mq_connector(block, role: str, config: Dict, where: str,
                      problems: List[str]) -> Dict:
    allowed = IBM_MQ_SOURCE_SETTINGS if role == 'source' else IBM_MQ_SETTINGS
    unknown = sorted(set(config) - allowed)
    if unknown:
        problems.append(f'{where}: unknown IBM MQ settings {", ".join(unknown)}.')
    if not config.get('channel'):
        problems.append(f'{where}: IBM MQ settings need channel.')
    if not config.get('queue'):
        problems.append(f'{where}: IBM MQ settings need queue.')
    if not (config.get('connection_name') or config.get('host')):
        problems.append(f'{where}: IBM MQ settings need connection_name (host(port)) or host.')
    if bool(config.get('cipher_spec')) != bool(config.get('key_repository')):
        problems.append(f'{where}: IBM MQ TLS needs both cipher_spec and key_repository.')
    if _literal(config.get('password')):
        problems.append(
            f"{where}: password is written in the file; use "
            f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
        )
    settings = {k: v for k, v in config.items()
                if k in IBM_MQ_SETTINGS or k == 'connections'}
    connector = dict(name=block.uuid, kind='ibm_mq', config=settings)
    if role == 'source':
        if config.get('batch_size'):
            connector['batch_size'] = config['batch_size']
        if config.get('batch_timeout'):
            connector['timeout_ms'] = int(float(config['batch_timeout']) * 1000)
    return connector


def _object_storage_connector(block, connector_type: str, config: Dict, where: str,
                              problems: List[str]) -> Dict:
    """
    One object per batch: the generic sink names a url; Mage's amazon_s3 and
    google_cloud_storage sinks a bucket and prefix. Their buffer settings become the stream's
    batch size and wait, so objects hold what Mage's buffers held, and a batch is
    acknowledged only once its object is written.
    """
    if connector_type == 'object_storage':
        allowed = OBJECT_STORAGE_SETTINGS
    else:
        allowed = MAGE_OBJECT_SINK_SETTINGS
    unknown = sorted(set(config) - allowed)
    if unknown:
        problems.append(f'{where}: unknown object storage settings {", ".join(unknown)}.')
    settings = {}
    if connector_type == 'object_storage':
        if not config.get('url'):
            problems.append(f'{where}: object storage needs url, such as s3://bucket/prefix.')
        settings.update({k: config[k] for k in ('url', 'format', 'date_partition_format',
                                                 'endpoint_url', 'region') if config.get(k)})
    else:
        if not config.get('bucket'):
            problems.append(f'{where}: {connector_type} needs bucket.')
        scheme = 's3' if connector_type == 'amazon_s3' else 'gs'
        prefix = str(config.get('prefix') or '').strip('/')
        settings['url'] = f'{scheme}://{config.get("bucket")}' + (f'/{prefix}' if prefix else '')
        file_type = str(config.get('file_type') or 'parquet').lower()
        if file_type in ('json', 'jsonl'):
            settings['format'] = 'jsonl'
        elif file_type == 'parquet':
            settings['format'] = 'parquet'
        else:
            problems.append(
                f'{where}: file_type {file_type} would change values (CSV has no types); '
                'services write parquet or json.'
            )
        if config.get('date_partition_format'):
            settings['date_partition_format'] = config['date_partition_format']
        if config.get('path_to_credentials_json_file'):
            problems.append(
                f'{where}: path_to_credentials_json_file names a file on this machine; set '
                "GOOGLE_APPLICATION_CREDENTIALS (or use the platform's identity) in the "
                "service's environment instead."
            )
    if str(settings.get('format', 'parquet')) not in ('parquet', 'jsonl'):
        problems.append(f'{where}: format is parquet or jsonl.')
    connector = dict(name=block.uuid, kind='object_storage', config=settings)
    if config.get('buffer_size_mb'):
        connector['buffer_bytes'] = int(float(config['buffer_size_mb']) * 1024 * 1024)
    if config.get('buffer_timeout_seconds'):
        connector['buffer_ms'] = int(float(config['buffer_timeout_seconds']) * 1000)
    return connector


def _postgres_sink_settings(config: Dict, where: str, problems: List[str]) -> Dict:
    unknown = sorted(set(config) - POSTGRES_SINK_SETTINGS)
    if unknown:
        problems.append(f'{where}: unknown PostgreSQL settings {", ".join(unknown)}.')
    for key in ('host', 'database', 'username', 'table'):
        if not config.get(key):
            problems.append(f'{where}: PostgreSQL settings need {key}.')
    if _literal(config.get('password')):
        problems.append(
            f"{where}: password is written in the file; use "
            f"\"{{{{ env_var('NAME') }}}}\" and set NAME in the service's environment."
        )
    method = str(config.get('unique_conflict_method') or 'IGNORE').upper()
    if method not in ('IGNORE', 'UPDATE'):
        problems.append(f'{where}: unique_conflict_method is IGNORE or UPDATE.')
    return {k: v for k, v in config.items() if k in POSTGRES_SINK_SETTINGS}


def _clickhouse_sink_settings(config: Dict, capture_: Capture, where: str,
                              problems: List[str]) -> Dict:
    """
    MageML's ClickHouse sink names an io_config.yaml profile and export settings
    (`config: {table_name: ...}`): the profile's connection, with its variables kept as
    references, and the table become the engine's sink settings.
    """
    unknown = sorted(set(config) - {'profile', 'config'})
    if unknown:
        problems.append(f'{where}: unknown ClickHouse sink settings {", ".join(unknown)}.')
    export_config = config.get('config') or {}
    extra = sorted(set(export_config) - CLICKHOUSE_EXPORT_SETTINGS)
    if extra:
        problems.append(
            f'{where}: services do not take these ClickHouse export settings: '
            f'{", ".join(extra)}.'
        )
    if not export_config.get('table_name'):
        problems.append(f'{where}: the ClickHouse sink needs config.table_name.')
    profile = config.get('profile') or 'default'
    path = capture_.project / 'io_config.yaml'
    try:
        connection = compile_io_config(path.read_text(), profile) if path.is_file() else None
    except ValueError as error:
        problems.append(f'{where}: {error}')
        connection = None
    if connection is None:
        problems.append(f'{where}: io_config.yaml has no profile {profile}.')
        connection = {}
    interface = str(connection.get('CLICKHOUSE_INTERFACE') or 'http').lower()
    if interface not in ('http', 'https'):
        problems.append(f'{where}: CLICKHOUSE_INTERFACE {interface}: services use HTTP.')
    settings = {
        'host': connection.get('CLICKHOUSE_HOST'),
        'port': connection.get('CLICKHOUSE_PORT'),
        'username': connection.get('CLICKHOUSE_USERNAME'),
        'password': connection.get('CLICKHOUSE_PASSWORD'),
        'database': export_config.get('database') or connection.get('CLICKHOUSE_DATABASE'),
        'table': export_config.get('table_name'),
        'secure': interface == 'https',
        **{k: export_config[k] for k in ('auto_clean_name', 'case_sensitive',
                                          'allow_reserved_words') if k in export_config},
    }
    if not settings['host']:
        problems.append(f'{where}: profile {profile} has no CLICKHOUSE_HOST.')
    return {k: v for k, v in settings.items() if v is not None}


def _feast_sink_settings(config: Dict, where: str, problems: List[str]) -> Dict:
    unknown = sorted(set(config) - FEAST_SINK_SETTINGS)
    if unknown:
        problems.append(f'{where}: unknown Feast sink settings {", ".join(unknown)}.')
    if not config.get('push_source_name'):
        problems.append(f"{where}: a Feast sink needs push_source_name, the push source of "
                        'the feature view it writes.')
    if config.get('to', 'online') not in ('online', 'offline', 'online_and_offline'):
        problems.append(f'{where}: to is online, offline or online_and_offline.')
    # url, headers and the other settings are checked as for an HTTP sink.
    checked = _http_sink_settings(
        {k: v for k, v in config.items() if k in HTTP_SINK_SETTINGS}, where, problems,
    )
    return {**checked, **{k: config[k] for k in ('push_source_name', 'to') if k in config}}


def _stream_connector(block, kinds: Dict[str, str], role: str, pipeline, capture_: Capture,
                      problems: List[str]) -> Optional[Dict]:
    connector = _stream_connector_settings(block, kinds, role, pipeline, capture_, problems)
    if connector is not None and role == 'sink' and _marked_dead_letter(block, pipeline):
        connector['dead_letter'] = True
    return connector


def _marked_dead_letter(block, pipeline) -> bool:
    """Whether a YAML sink sets `dead_letter: true`."""
    try:
        config = compile_stream_config(Path(block.file_path).read_text(),
                                       getattr(pipeline, 'variables', None))
    except (OSError, ValueError):
        return False
    return bool(config.get('dead_letter'))


def _stream_connector_settings(block, kinds: Dict[str, str], role: str, pipeline,
                               capture_: Capture, problems: List[str]) -> Optional[Dict]:
    where = f'Pipeline {pipeline.uuid}, {role} {block.uuid}'
    language = str(getattr(block.language, 'value', block.language))
    if language != 'yaml':
        problems.append(
            f'{where} is written in {language}; services run stream {role}s set up in YAML '
            f'({", ".join(sorted(kinds))}). Custom Python {role}s come later.'
        )
        return None
    path = Path(block.file_path).resolve()
    if not path.is_file():
        problems.append(f'{where}: its file {path} is missing.')
        return None
    capture_.stream_files.add(path)
    try:
        config = compile_stream_config(path.read_text(), getattr(pipeline, 'variables', None))
    except ValueError as error:
        problems.append(f'{where}: {error}')
        return None
    connector_type = config.pop('connector_type', None)
    # Read by _marked_dead_letter; not a connector setting.
    config.pop('dead_letter', None)
    if connector_type not in kinds:
        problems.append(
            f'{where} uses the {connector_type or "unnamed"} connector; services run '
            f'{", ".join(sorted(kinds))} {role}s now, and {STREAM_PLANNED}.'
        )
        return None
    kind = kinds[connector_type]
    if kind == 'stdout':
        return dict(name=block.uuid, kind=kind, config={})
    if kind == 'http' and role == 'source':
        return dict(name=block.uuid, kind=kind,
                    config=_webhook_settings(config, where, problems))
    if kind == 'http':
        return dict(name=block.uuid, kind=kind,
                    config=_http_sink_settings(config, where, problems))
    if kind == 'feast':
        return dict(name=block.uuid, kind=kind,
                    config=_feast_sink_settings(config, where, problems))
    if kind == 'rabbitmq':
        return _rabbitmq_connector(block, role, config, where, problems)
    if kind == 'ibm_mq':
        return _ibm_mq_connector(block, role, config, where, problems)
    if kind == 'object_storage':
        return _object_storage_connector(block, connector_type, config, where, problems)
    if kind == 'postgres':
        return dict(name=block.uuid, kind='postgres',
                    config=_postgres_sink_settings(config, where, problems))
    if kind == 'clickhouse':
        return dict(name=block.uuid, kind='clickhouse',
                    config=_clickhouse_sink_settings(config, capture_, where, problems))
    if role == 'source':
        if config.get('include_metadata'):
            problems.append(
                f'{where}: include_metadata is not supported by services yet; the blocks get '
                'the message values.'
            )
        serde = config.get('serde_config') or {}
        method = str(serde.get('serialization_method') or 'JSON').upper()
        if method == 'AVRO':
            # Decoded by the engine with the schemas of the registry, to JSON for the blocks.
            if not serde.get('schema_registry_url'):
                problems.append(f'{where}: AVRO messages need serde_config.schema_registry_url.')
            config['serde_config'] = {
                k: v for k, v in serde.items()
                if k in ('serialization_method', 'schema_registry_url',
                         'schema_registry_username', 'schema_registry_password') and v
            }
        elif method == 'RAW_VALUE':
            # The blocks get each message's bytes, as in Mage.
            config['serde_config'] = dict(serialization_method='RAW_VALUE')
        else:
            if method != 'JSON':
                problems.append(
                    f'{where}: {method} messages are not supported by services yet; services '
                    'read JSON messages, text, raw bytes (RAW_VALUE), and Avro with a schema '
                    'registry.'
                )
            config.pop('serde_config', None)
        if config.get('offset'):
            problems.append(
                f'{where}: offset (a start position) is not supported by services yet; the '
                'consumer group\'s committed position, or auto_offset_reset, decides where the '
                'stream starts.'
            )
        config.pop('offset', None)
        config.pop('include_metadata', None)
        allowed = KAFKA_SOURCE_SETTINGS
    else:
        allowed = KAFKA_SINK_SETTINGS
    return dict(
        name=block.uuid, kind=kind, config=_kafka_settings(config, allowed, where, problems),
        batch_size=config.get('batch_size'), timeout_ms=config.get('timeout_ms'),
    )


def _stream_pipeline_entry(pipeline, project: Path, repo_config, capture_: Capture,
                           problems: List[str]) -> Optional[Dict]:
    """
    A streaming pipeline for the manifest: its YAML source and sinks become the stream's
    connectors, and its Python transformers run on each batch of messages.
    """
    prefix = f'Pipeline {pipeline.uuid}'
    sources, sinks, transformers = [], [], []
    for block in pipeline.blocks_by_uuid.values():
        block_type = str(getattr(block.type, 'value', block.type))
        if block_type in NOTEBOOK_TYPES:
            capture_.notes.append(
                f'{prefix}, block {block.uuid}: a {block_type} block; it is notebook only and '
                'left out.'
            )
        elif block_type == 'data_loader':
            sources.append(block)
        elif block_type == 'data_exporter':
            sinks.append(block)
        elif block_type == 'transformer':
            transformers.append(block)
        else:
            problems.append(
                f'{prefix}, block {block.uuid} is a {block_type} block; a stream has a source, '
                'transformers and sinks.'
            )
    if len(sources) != 1:
        problems.append(f'{prefix} is a stream with {len(sources)} sources; it needs one.')
        return None
    if not sinks:
        problems.append(f'{prefix} is a stream with no sink.')
        return None
    source_block = sources[0]
    source = _stream_connector(source_block, STREAM_SOURCES, 'source', pipeline, capture_,
                               problems)
    blocks = []
    for block in transformers:
        language = str(getattr(block.language, 'value', block.language))
        if language not in ('python', 'rust'):
            problems.append(
                f'{prefix}, block {block.uuid} is written in {language}; services run stream '
                'transformers written in Python or Rust.'
            )
            continue
        entry = _block_entry(pipeline, block, project, repo_config, capture_, problems, {},
                             stream=True)
        if entry is None:
            continue
        if entry.pop('conditions', None):
            problems.append(
                f'{prefix}, block {block.uuid} has conditions, which stream transformers do not '
                'run.'
            )
        # The source is not a block of the stream: transformers after it are its roots.
        entry['upstream'] = [u for u in entry['upstream'] if u != source_block.uuid]
        blocks.append(entry)
    sink_entries = []
    dead_letters = []
    for block in sinks:
        sink = _stream_connector(block, STREAM_SINKS, 'sink', pipeline, capture_, problems)
        if sink is None:
            continue
        if sink.pop('dead_letter', False):
            # Messages that fail go there, whichever block it hangs from.
            for key in ('batch_size', 'timeout_ms', 'buffer_bytes', 'buffer_ms'):
                sink.pop(key, None)
            dead_letters.append(sink)
            continue
        upstream = list(block.upstream_block_uuids or [])
        if len(upstream) != 1:
            problems.append(f'{prefix}, sink {block.uuid} needs one upstream block.')
            continue
        if upstream[0] == source_block.uuid:
            if blocks:
                problems.append(
                    f'{prefix}, sink {block.uuid} writes the source\'s messages next to '
                    'transformers; services do not run that yet. Put a transformer that '
                    'returns its messages before it.'
                )
                continue
        else:
            sink['after'] = upstream[0]
        sink.pop('batch_size', None)
        sink.pop('timeout_ms', None)
        sink_entries.append(sink)
    if source is None:
        return None
    batch = dict(
        max_records=int(source.pop('batch_size', None) or 100),
        max_wait_ms=int(source.pop('timeout_ms', None) or 500),
    )
    _apply_object_buffers(prefix, source, sink_entries, batch, capture_)
    retry = getattr(pipeline, 'retry_config', None) or {}
    errors = dict(
        retries=int(retry.get('retries', 3)),
        delay_seconds=float(retry.get('delay', 1)),
        max_delay_seconds=float(retry.get('max_delay', 30)),
    )
    capture_.notes.append(
        f'{prefix} runs from the service\'s start, with no trigger. A batch that still fails '
        f'after {errors["retries"]} retries stops the stream, so no message is dropped; its '
        'position stays where it was. A sink with dead_letter: true takes the messages that '
        'fail on their own instead.'
    )
    if len(dead_letters) > 1:
        problems.append(
            f'{prefix} has {len(dead_letters)} dead-letter sinks '
            f'({", ".join(d["name"] for d in dead_letters)}); a stream has one.'
        )
    elif dead_letters:
        errors['dead_letter'] = dead_letters[0]
    if not sink_entries and not problems:
        problems.append(f'{prefix} has only a dead-letter sink; it needs a sink for its messages.')
    return dict(
        uuid=pipeline.uuid,
        name=pipeline.name or pipeline.uuid,
        description=pipeline.description or None,
        max_concurrent_runs=1,
        variables=_json_safe(getattr(pipeline, 'variables', None) or {}),
        blocks=blocks,
        triggers=[],
        stream=dict(source=source, sinks=sink_entries, batch=batch, errors=errors),
    )


def _apply_object_buffers(prefix: str, source: Dict, sinks: List[Dict], batch: Dict,
                          capture_: Capture) -> None:
    """Object storage sinks' buffers set larger batches: one object holds one batch."""
    buffers = [(s.pop('buffer_bytes', None), s.pop('buffer_ms', None)) for s in sinks]
    sizes = [b for b, _ in buffers if b]
    waits = [w for _, w in buffers if w]
    if not sizes and not waits:
        return
    if source.get('kind') == 'http':
        capture_.notes.append(
            f'{prefix}: object storage buffer settings are not applied to a webhook stream, '
            'whose callers wait for the sinks; each request\'s messages make one object.'
        )
        return
    if sizes:
        batch['max_bytes'] = max(sizes)
        # Size decides, as in Mage's buffer: a count that is reached first does not.
        batch['max_records'] = max(batch['max_records'], 1_000_000)
    if waits:
        batch['max_wait_ms'] = max(batch['max_wait_ms'], max(waits))
    capture_.notes.append(
        f'{prefix}: batches hold up to {batch.get("max_bytes", 8 * 1024 * 1024) // 1024} KiB '
        f'or {batch["max_wait_ms"] // 1000} s of messages, from the object storage buffer '
        'settings; each batch becomes one object, written before its messages are '
        'acknowledged.'
    )


def _block_entry(pipeline, block, project: Path, repo_config, capture_: Capture,
                 problems: List[str], conditionals: Dict[str, Dict],
                 stream: bool = False) -> Optional[Dict]:
    """
    A block for the manifest, or None when it is left out (with a note or a problem).
    `stream`: a stream's transformer.
    """
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
    )
    from mage_ai.data_preparation.shared.retry import RetryConfig, resolve_retry_config

    prefix = f'Pipeline {pipeline.uuid}'
    block_type = str(getattr(block.type, 'value', block.type))
    language = str(getattr(block.language, 'value', block.language))
    where = f'{prefix}, block {block.uuid}'
    if block_type in NOTEBOOK_TYPES:
        capture_.notes.append(
            f'{where}: a {block_type} block; it is notebook only and left out.',
        )
        return None
    if block_type not in SUPPORTED_TYPES:
        problems.append(f'{where} is a {block_type} block, which services do not run yet.')
        return None
    if language not in ('python', 'rust', 'r', 'sql'):
        problems.append(
            f'{where} is written in {language}; services run Python, R, Rust and SQL '
            'blocks.',
        )
        return None
    if language == 'sql':
        reason = _sql_unsupported(block)
        if reason:
            problems.append(f'{where}: {reason}')
            return None
    if is_dynamic_block(block) or is_dynamic_block_child(block) or is_replicated_block(block):
        problems.append(f'{where} is dynamic or replicated, which services do not run yet.')
        return None
    if getattr(block, 'callback_blocks', None):
        problems.append(f'{where} has callbacks, which services do not run yet.')
        return None
    conditions = []
    for conditional in getattr(block, 'conditional_blocks', None) or []:
        entry_ = _conditional_entry(conditional, project, prefix, problems)
        if entry_ is not None:
            conditions.append(conditional.uuid)
            conditionals.setdefault(conditional.uuid, entry_)
            capture_.files.add(Path(conditional.file_path).resolve())
    if (block.configuration or {}).get('variables'):
        problems.append(
            f'{where} reads its upstream with chunk or batch settings, which services do not '
            'support yet.'
        )
        return None
    _note_unchecked_guards(pipeline, block, project, where, capture_.notes)
    path = Path(block.file_path).resolve()
    if not path.is_file():
        problems.append(f'{where}: its file {path} is missing.')
        return None
    retry = RetryConfig.load(config=resolve_retry_config(
        getattr(repo_config, 'retry_config', None),
        getattr(pipeline, 'retry_config', None),
        block.retry_config,
    ))
    entry = dict(
        uuid=block.uuid,
        type=block_type,
        language=language,
        file=str(path.relative_to(project)),
        sha256=_digest(path),
        upstream=list(block.upstream_block_uuids or []),
        retry=dict(
            retries=int(retry.retries or 0),
            delay_seconds=float(retry.delay or 0),
            max_delay_seconds=float(retry.max_delay or 0),
            exponential_backoff=bool(retry.exponential_backoff),
        ),
        configuration=_json_safe(block.configuration or {}),
    )
    if conditions:
        entry['conditions'] = conditions
    timeout = getattr(block, 'timeout', None)
    if timeout:
        entry['timeout_seconds'] = int(timeout)
    if language == 'sql':
        entry['sql'] = _sql_details(pipeline, block)
    if language == 'rust':
        crate = _prepare_rust_block(block, project, problems, where, stream=stream)
        if crate:
            entry['binary'] = f'bin/{RUST_BUNDLE}'
            entry['entry'] = crate
            capture_.rust_blocks.append((block.uuid, crate))
            if stream:
                capture_.rust_stream_crates.add(crate)
        profile = _rust_io_config(block, path, project, problems, where)
        if profile is not None:
            entry['io_config'] = profile
    capture_.files.add(path)
    return entry


def _conditional_entry(conditional, project: Path, prefix: str,
                       problems: List[str]) -> Optional[Dict]:
    """A conditional block for the manifest; services run Python conditionals."""
    where = f'{prefix}, conditional block {conditional.uuid}'
    language = str(getattr(conditional.language, 'value', conditional.language))
    if language != 'python':
        problems.append(
            f'{where} is written in {language}; services run conditional blocks written in '
            'Python. Rewrite it in Python, or remove it from the blocks it guards.'
        )
        return None
    path = Path(conditional.file_path).resolve()
    if not path.is_file():
        problems.append(f'{where}: its file {path} is missing.')
        return None
    return dict(uuid=conditional.uuid, file=str(path.relative_to(project)), sha256=_digest(path))


def _note_unchecked_guards(pipeline, block, project: Path, where: str, notes: List[str]):
    """Services run the blocks only: a data contract or monitor on a block is not checked."""
    from mage_ai.data_preparation import contracts
    from mage_ai.orchestration import monitors

    try:
        binding = contracts.binding_of(block)
    except contracts.ContractError:
        binding = None
    if binding is not None and binding.enforcement != 'off':
        notes.append(
            f'{where}: the service does not check data contract {binding.name}; check the '
            'output downstream or run the pipeline in MageML.'
        )
    for monitor in monitors.monitors_for(str(project), pipeline.uuid, block.uuid):
        notes.append(f'{where}: the service does not run monitor {monitor.name}.')


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _prepare_rust_block(block, project: Path, problems: List[str], where: str,
                        stream: bool = False) -> Optional[str]:
    from mage_ai.data_preparation.models.block.rust import build as rust_build

    try:
        prepared = rust_build.prepare(
            Path(block.file_path).read_text(), block.type, block.uuid, str(project),
            stream=stream,
        )
    except Exception as error:
        problems.append(f'{where}: {error}')
        return None
    return prepared.crate


VARIABLE_MARK = re.compile(r'__mage_var_([0-9a-f]+)__')
# Rust code that reads the io_config profile the service passes in MAGE_IO_CONFIG.
READS_IO_CONFIG = re.compile(r'Postgres::connect\(\s*\)|MAGE_IO_CONFIG')


def _variable_mark(name: str, default=None) -> str:
    reference = dict(env=name) if default is None else dict(env=name, default=default)
    return f'__mage_var_{json.dumps(reference).encode().hex()}__'


def _io_config_parts(value):
    """value with each text that reads variables replaced by {"$parts": [...]}."""
    if isinstance(value, dict):
        return {str(k): _io_config_parts(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_io_config_parts(v) for v in value]
    if not isinstance(value, str) or not VARIABLE_MARK.search(value):
        return value
    parts, position = [], 0
    for match in VARIABLE_MARK.finditer(value):
        if match.start() > position:
            parts.append(value[position:match.start()])
        parts.append(json.loads(bytes.fromhex(match.group(1))))
        position = match.end()
    if position < len(value):
        parts.append(value[position:])
    return {'$parts': parts}


def compile_io_config(text: str, profile: str) -> Optional[Dict]:
    """
    The io_config.yaml profile with env_var() and mage_secret_var() kept as references
    the service fills in at run time, so no secret is written into the service. None when
    there is no such profile. Raises ValueError for templates a service can't fill in.
    """
    import jinja2
    import yaml

    def unsupported(name):
        def function(*args, **kwargs):
            raise ValueError(
                f'io_config.yaml uses {name}(), which services can\'t fill in for Rust '
                'blocks; read the value with env_var() and set it in the service\'s '
                'environment or with MAGE_SERVICE_SECRETS.'
            )
        return function

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    try:
        rendered = environment.from_string(text).render(
            env_var=_variable_mark,
            mage_secret_var=lambda name: _variable_mark(secret_variable_name(name)),
            **_cloud_secret_marks(),
            json_value=unsupported('json_value'),
        )
    except jinja2.TemplateError as error:
        raise ValueError(f'io_config.yaml has a template services can\'t fill in: {error}')
    config = yaml.safe_load(rendered) or {}
    settings = config.get(profile) if isinstance(config, dict) else None
    if not isinstance(settings, dict):
        return None
    return {k: v for k, v in _io_config_parts(settings).items() if v is not None}


def _rust_io_config(
    block, path: Path, project: Path, problems: List[str], where: str,
) -> Optional[Dict]:
    """The profile a Rust block gets in MAGE_IO_CONFIG, as Mage passes it when it runs."""
    config_path = project / 'io_config.yaml'
    if not config_path.is_file():
        return None
    profile = (block.configuration or {}).get('data_provider_profile') or 'default'
    try:
        return compile_io_config(config_path.read_text(), profile)
    except Exception as error:
        # Only blocks that read the profile need it.
        if READS_IO_CONFIG.search(path.read_text(errors='replace')):
            problems.append(f'{where}: {error}')
        return None


def _triggers(pipeline, notes: List[str], problems: List[str]) -> List[Dict]:
    """The pipeline's triggers from triggers.yaml and the database, read only."""
    from mage_ai.data_preparation.models.triggers import (
        build_triggers,
        get_trigger_configs_by_name,
    )

    by_name: Dict[str, Dict] = {}
    try:
        configs = get_trigger_configs_by_name(pipeline.uuid)
        for trigger in build_triggers(list(configs.values()), pipeline.uuid):
            by_name[trigger.name] = dict(
                origin='triggers.yaml',
                schedule_type=str(getattr(trigger.schedule_type, 'value', trigger.schedule_type)),
                schedule_interval=trigger.schedule_interval,
                start_time=trigger.start_time,
                status=str(getattr(trigger.status, 'value', trigger.status)),
                variables=trigger.variables or {},
                settings=trigger.settings or {},
            )
    except Exception as error:
        notes.append(f'Pipeline {pipeline.uuid}: triggers.yaml could not be read: {error}')
    try:
        from mage_ai.orchestration.db import db_connection
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        db_connection.start_session()
        # A project whose server never started has a database without tables: no triggers.
        from sqlalchemy import inspect as sqlalchemy_inspect

        has_table = sqlalchemy_inspect(db_connection.session.get_bind()).has_table(
            PipelineSchedule.__tablename__,
        )
        schedules = PipelineSchedule.query.filter(
            PipelineSchedule.pipeline_uuid == pipeline.uuid,
        ).all() if has_table else []
        for schedule in schedules:
            stored = dict(
                origin='database',
                schedule_type=str(getattr(schedule.schedule_type, 'value', schedule.schedule_type)),
                schedule_interval=schedule.schedule_interval,
                start_time=schedule.start_time,
                status=str(getattr(schedule.status, 'value', schedule.status)),
                variables=schedule.variables or {},
                settings=schedule.settings or {},
            )
            existing = by_name.get(schedule.name)
            if existing and (
                existing['schedule_interval'] != stored['schedule_interval']
                or existing['status'] != stored['status']
            ):
                notes.append(
                    f'Pipeline {pipeline.uuid}, trigger {schedule.name}: triggers.yaml and the '
                    'database disagree; the database, which Mage runs, is exported.'
                )
            by_name[schedule.name] = stored
    except Exception as error:
        notes.append(
            f'Pipeline {pipeline.uuid}: the trigger database could not be read '
            f'({str(error).splitlines()[0][:300]}); only triggers.yaml is exported.'
        )

    triggers = []
    for trigger_name, trigger in sorted(by_name.items()):
        kind = trigger['schedule_type']
        if kind == 'after_run':
            settings = trigger['settings'] or {}
            triggers.append(dict(
                name=trigger_name,
                kind='after_run',
                schedule=None,
                start_time=None,
                timezone='UTC',
                active=trigger['status'] == 'active',
                variables=_json_safe(trigger['variables']),
                skip_if_previous_running=bool(settings.get('skip_if_previous_running')),
                after_pipeline=settings.get('after_pipeline'),
                after_status=(settings.get('after_status') or 'completed').lower(),
            ))
            continue
        if kind not in ('time', 'api'):
            notes.append(
                f'Pipeline {pipeline.uuid}, trigger {trigger_name}: {kind} triggers are not '
                'exported; call the API instead.'
            )
            continue
        interval = trigger['schedule_interval']
        if kind == 'time' and interval in (None, '', '@always_on'):
            notes.append(
                f'Pipeline {pipeline.uuid}, trigger {trigger_name}: schedule {interval!r} is not '
                'exported.'
            )
            continue
        start = trigger['start_time']
        if isinstance(start, datetime):
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            start = start.isoformat()
        triggers.append(dict(
            name=trigger_name,
            kind=kind,
            schedule=interval if kind == 'time' else None,
            start_time=start or None,
            timezone='UTC',
            active=trigger['status'] == 'active',
            variables=_json_safe(trigger['variables']),
            skip_if_previous_running=bool(trigger['settings'].get('skip_if_previous_running')),
        ))
    return triggers


SECRET_HINTS = ('PASS', 'SECRET', 'TOKEN', 'KEY', 'CREDENTIAL', 'PRIVATE', 'AUTH')
ENV_VAR_TEMPLATE = re.compile(r"""env_var\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]""")
SECRET_VAR_TEMPLATE = re.compile(r"""mage_secret_var\(\s*['"]([^'"]+)['"]""")
# Cloud secret functions in io_config.yaml or stream settings: aws_secret_var('<id>'),
# azure_secret_var('<name>'), gcp_secret_var('<secret>'[, '<key>']), ibm_secret_var('<id>'
# [, '<field>']). A second argument that is not text (aws_secret_var's cached) is no key.
CLOUD_SECRET_VAR_TEMPLATE = re.compile(
    r"\b(aws|azure|gcp|ibm)_secret_var\(\s*['\"]([^'\"]+)['\"]"
    r"(?:\s*,\s*['\"]([^'\"]+)['\"])?"
)


def _cloud_secret_marks() -> Dict:
    """The cloud secret functions for compiling settings: references to the variables the
    service fetches at start-up."""
    def mark(provider):
        def function(secret_id, *args):
            field = args[0] if args and isinstance(args[0], str) else None
            return _variable_mark(cloud_secret_variable_name(provider, secret_id, field))
        return function
    return {name: mark(provider) for name, provider in CLOUD_SECRET_FUNCTIONS.items()}


def _environment_reads(path: Path, project: Path) -> List[Tuple[str, bool, bool, str]]:
    """(name, required, secret, file) for each environment variable a file reads."""
    label = str(path.relative_to(project))
    text = path.read_text(errors='replace')
    reads = []
    if path.suffix in ('.yaml', '.yml'):
        reads += [(name, False, False) for name in ENV_VAR_TEMPLATE.findall(text)]
        reads += [(secret_variable_name(name), True, True)
                  for name in SECRET_VAR_TEMPLATE.findall(text)]
        reads += [(cloud_secret_variable_name(provider, secret_id, key or None), True, True)
                  for provider, secret_id, key in CLOUD_SECRET_VAR_TEMPLATE.findall(text)]
        return [(n, r, s, label) for n, r, s in reads]
    try:
        tree = ast.parse(text, str(path))
    except SyntaxError:
        return []

    def is_environ(node) -> bool:
        return (isinstance(node, ast.Attribute) and node.attr == 'environ'
                and isinstance(node.value, ast.Name) and node.value.id == 'os')

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant) \
                and isinstance(node.args[0].value, str):
            func = node.func
            name = node.args[0].value
            if isinstance(func, ast.Attribute) and (
                (func.attr == 'getenv' and isinstance(func.value, ast.Name)
                 and func.value.id == 'os')
                or (func.attr == 'get' and is_environ(func.value))
            ):
                reads.append((name, False, False))
            elif isinstance(func, ast.Name) and func.id == 'env_var':
                reads.append((name, False, False))
        elif isinstance(node, ast.Subscript) and is_environ(node.value) \
                and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
            reads.append((node.slice.value, True, False))
    return [(n, r, s, label) for n, r, s in reads]


# Code that reads io_config.yaml: Python loaders, SQL blocks, R's database and S3 helpers
# and Rust's io_config profiles.
READS_IO_CONFIG_FILE = re.compile(
    r'io_config|ConfigFileLoader|\b(db_connect|read_sql|write_table|read_s3|write_s3|'
    r's3_filesystem)\(|mage::postgres|Postgres::connect'
)


def _uses_io_config(capture_: Capture) -> bool:
    for path in capture_.files:
        if path.suffix == '.sql':
            return True
        if path.suffix in ('.py', '.r', '.R', '.rs') and READS_IO_CONFIG_FILE.search(
            path.read_text(errors='replace'),
        ):
            return True
    return False


PROFILE_LITERAL = re.compile(r"""['"]([A-Za-z0-9_][A-Za-z0-9_.\-]*)['"]""")
PROFILE_SETTING = re.compile(r'^\s*data_provider_profile:\s*[\'"]?([^\s\'"#]+)', re.M)


def _io_config_profiles(path: Path) -> Dict[str, Set[str]]:
    """{profile: the environment variables its values read} from io_config.yaml."""
    try:
        content = yaml.safe_load(path.read_text(errors='replace')) or {}
    except yaml.YAMLError:
        return {}
    def strings(value) -> Iterable[str]:
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from strings(item)
        elif isinstance(value, list):
            for item in value:
                yield from strings(item)

    profiles = {}
    for name, value in content.items() if isinstance(content, dict) else []:
        if isinstance(value, dict):
            profiles[str(name)] = {
                found for text in strings(value) for found in ENV_VAR_TEMPLATE.findall(text)
            }
    return profiles


def _profiles_used(capture_: Capture, profiles: Iterable[str]) -> Set[str]:
    """
    The io_config.yaml profiles the exported code names: as a string in a block or a module
    (ConfigFileLoader(path, 'warehouse'), mage::postgres profiles, R helpers), or as a SQL
    block's data_provider_profile in its pipeline's metadata.yaml.
    """
    profiles = set(profiles)
    named: Set[str] = set()
    for path in capture_.files:
        if path.suffix in ('.py', '.r', '.R', '.rs', '.sql'):
            named.update(PROFILE_LITERAL.findall(path.read_text(errors='replace')))
    for pipeline in capture_.manifest.get('pipelines') or []:
        metadata = capture_.project / 'pipelines' / pipeline['uuid'] / 'metadata.yaml'
        if metadata.is_file():
            text = metadata.read_text(errors='replace')
            named.update(PROFILE_SETTING.findall(text))
            if any(b['language'] == 'sql' for b in pipeline['blocks']) and \
                    'data_provider_profile' not in text:
                named.add('default')
    return named & profiles


def _environment(capture_: Capture) -> List[Dict]:
    """
    The environment variables the exported code reads. Those of io_config.yaml are
    connection settings, all of them secret: hosts, users and databases as well as
    passwords. Those of a profile the code names are required, since the blocks connect with
    them; those of other profiles are listed as optional, with a note.
    """
    variables: Dict[str, Dict] = {}
    io_config_used = _uses_io_config(capture_)

    def add(name, required, secret, label):
        if name.startswith('MAGE_SERVICE_'):
            return
        entry = variables.setdefault(name, dict(
            name=name, secret=False, required=False, used_by=[],
        ))
        entry['required'] = entry['required'] or required
        entry['secret'] = entry['secret'] or secret or any(
            hint in name.upper() for hint in SECRET_HINTS
        )
        if label not in entry['used_by']:
            entry['used_by'].append(label)

    for path in sorted(capture_.files):
        if path.suffix not in ('.py', '.yaml', '.yml'):
            continue
        if path.name == 'io_config.yaml' and path.parent == capture_.project:
            if not io_config_used:
                # Only the profiles' variables matter, and no exported code reads a profile.
                continue
            profiles = _io_config_profiles(path)
            used = _profiles_used(capture_, profiles)
            for profile in sorted(profiles):
                for name in sorted(profiles[profile]):
                    add(name, profile in used, True, f'io_config.yaml ({profile})')
            unused = sorted(p for p in profiles if p not in used and profiles[p])
            if unused:
                capture_.notes.append(
                    'io_config.yaml profiles no exported block names: '
                    f'{", ".join(unused)}; their variables are listed as optional.'
                )
            # Secrets read with mage_secret_var in io_config.yaml stay required.
            for name, required, secret, label in _environment_reads(path, capture_.project):
                if secret:
                    add(name, required, secret, label)
            continue
        for name, required, secret, label in _environment_reads(path, capture_.project):
            add(name, required, secret, label)
        if path.suffix == '.py':
            for name, required, secret, label in _client_reads(path, capture_.project):
                add(name, required, secret, label)
    # Stream connector settings: only the engine reads them, so they are not in used_by,
    # which decides what reaches block processes.
    for stream in capture_.streams:
        for connector in [stream['source'], *stream['sinks']]:
            for name, secret, required in _references(connector['config']):
                if name.startswith('MAGE_SERVICE_'):
                    continue
                entry = variables.setdefault(name, dict(
                    name=name, secret=False, required=False, used_by=[],
                ))
                entry['required'] = entry['required'] or required
                entry['secret'] = entry['secret'] or secret or any(
                    hint in name.upper() for hint in SECRET_HINTS
                )
                entry.setdefault('connectors', [])
                if connector['name'] not in entry['connectors']:
                    entry['connectors'].append(connector['name'])
    # Variables of the cloud secret functions: the service fetches them itself.
    for path in sorted(capture_.files | capture_.stream_files):
        if path.suffix not in ('.yaml', '.yml') or not path.is_file():
            continue
        text = path.read_text(errors='replace')
        for provider, secret_id, key in CLOUD_SECRET_VAR_TEMPLATE.findall(text):
            name = cloud_secret_variable_name(provider, secret_id, key or None)
            if name in variables:
                variables[name]['source'] = cloud_secret_source(provider, secret_id, key or None)
    return [variables[name] for name in sorted(variables)]


def _references(value, path: Tuple[str, ...] = ()) -> List[Tuple[str, bool, bool]]:
    """(variable, secret, required) for each environment reference in connector settings."""
    found: List[Tuple[str, bool, bool]] = []
    if isinstance(value, dict) and '$parts' in value:
        secret = (tuple(path[-2:]) in SECRET_SETTINGS
                  or (len(path) == 1 and path[0] in SECRET_KEYS))
        for part in value['$parts']:
            if isinstance(part, dict) and part.get('env'):
                found.append((part['env'], secret, 'default' not in part))
    elif isinstance(value, dict):
        for key, item in value.items():
            found.extend(_references(item, path + (str(key),)))
    elif isinstance(value, list):
        for item in value:
            found.extend(_references(item, path))
    return found


# Variables MageML's own clients read when a block does not pass the value: (module, class,
# argument that replaces the variable, [(variable, required, secret)]).
CLIENT_VARIABLES = (
    ('mage_ai.feature_stores.feast', 'FeastClient', 'url',
     [('FEAST_URL', True, False), ('FEAST_TOKEN', False, True)]),
)


def _client_reads(path: Path, project: Path) -> List[Tuple[str, bool, bool, str]]:
    """The variables of the MageML clients a file uses, such as FeastClient's FEAST_URL."""
    try:
        text = path.read_text(encoding='utf-8')
    except (OSError, UnicodeDecodeError):
        return []
    reads = []
    for module, name, argument, names in CLIENT_VARIABLES:
        if module not in text or f'{name}(' not in text:
            continue
        passed = re.search(rf'{name}\([^)]*\b{argument}\s*=', text) is not None
        label = f'{name} in {path.relative_to(project)}'
        for variable, required, secret in names:
            reads.append((variable, required and not passed, secret, label))
    return reads


def _imports(path: Path) -> Set[str]:
    try:
        tree = ast.parse(path.read_text(), str(path))
    except (SyntaxError, UnicodeDecodeError):
        return set()
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module)
    return names


def _include_shared_code(capture_: Capture, problems: List[str]) -> None:
    """Adds the project modules that the exported Python files import, transitively."""
    project = capture_.project
    seen: Set[Path] = set()
    queue = [p for p in capture_.files if p.suffix == '.py']
    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        for module in _imports(path):
            parts = module.split('.')
            if parts[0] == project.name:
                parts = parts[1:]
            if not parts or parts[0] in SKIPPED_PROJECT_DIRS:
                continue
            candidate = project.joinpath(*parts)
            found = []
            if candidate.with_suffix('.py').is_file():
                found.append(candidate.with_suffix('.py'))
            if candidate.is_dir():
                found.extend(p for p in candidate.rglob('*.py') if '__pycache__' not in p.parts)
            # Each package's __init__.py on the way, so the import resolves.
            for depth in range(1, len(parts)):
                init = project.joinpath(*parts[:depth], '__init__.py')
                if init.is_file():
                    found.append(init)
            for item in found:
                if item not in capture_.files:
                    capture_.files.add(item)
                    queue.append(item)


# A quoted string in a block's code: a candidate path of a file the block reads.
_STRING_LITERAL = re.compile(r"""(["'])((?:(?!\1)[^\n\\]){2,300})\1""")
# Code the export already follows by imports, or writes itself.
_CODE_SUFFIXES = {'.py', '.r', '.rs', '.sql'}
_MODULE_SUFFIXES = {'.py', '.r', '.rs'}


def _size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


def _human(size: int) -> str:
    for unit in ('bytes', 'KB', 'MB', 'GB'):
        if size < 1024 or unit == 'GB':
            return f'{size:.0f} {unit}' if unit == 'bytes' else f'{size:.1f} {unit}'
        size /= 1024
    return f'{size} bytes'


def _include_artifacts(capture_: Capture, includes: List[str], problems: List[str]) -> None:
    """
    Model and data files the blocks read: every existing project file or folder a block
    names in a string, such as load_artifact('models/churn.cbm'), and each --include path.
    """
    from mage_ai.pipeline_services.models import ARTIFACT_LOADERS

    project = capture_.project
    found: Dict[Path, str] = {}
    # Files a Python block or --include names: their reading package goes in the image.
    python_reads: Set[Path] = set()
    for name in includes:
        candidate = project / os.path.normpath(name)
        if not candidate.resolve().is_relative_to(project.resolve()):
            problems.append(f'--include {name}: the path is outside the project.')
        elif not candidate.exists():
            problems.append(f'--include {name}: there is no such file or folder in the project.')
        else:
            found[candidate] = 'with --include'
            python_reads.add(candidate)
    for path in sorted(capture_.files):
        if path.suffix.lower() not in _CODE_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except (OSError, UnicodeDecodeError):
            continue
        for _, literal in _STRING_LITERAL.findall(text):
            name = literal.strip()
            if name.startswith('./'):
                name = name[2:]
            parts = Path(name).parts
            if not parts or name.startswith('/') or '..' in parts:
                continue
            if parts[0] in SKIPPED_PROJECT_DIRS:
                continue
            candidate = project / name
            try:
                exists = candidate.exists()
            except OSError:
                continue
            # Modules are imported and blocks are captured; SQL files a helper reads, such as
            # read_sql('sql/orders.sql'), are data the image needs.
            if not exists or candidate.suffix.lower() in _MODULE_SUFFIXES or \
                    candidate == project:
                continue
            candidate = project / os.path.normpath(name)
            found.setdefault(candidate, f'named in {path.relative_to(project)}')
            if path.suffix == '.py':
                python_reads.add(candidate)
    included = []
    for candidate, why in sorted(found.items()):
        files = [candidate] if candidate.is_file() else [
            p for p in candidate.rglob('*') if p.is_file() and '__pycache__' not in p.parts
        ]
        capture_.files.update(files)
        module = ARTIFACT_LOADERS.get(candidate.suffix.lower(), (None, None))[1]
        if module and candidate in python_reads:
            capture_.artifact_modules.add(module)
        included.append((candidate, why))
    # A note per file, or one for them all when there are many, so they do not bury the
    # other notes.
    if len(included) > 3:
        size = sum(_size(c) for c, _ in included)
        capture_.notes.append(
            f'Included {len(included)} files the blocks name ({_human(size)}): '
            f'{", ".join(str(c.relative_to(project)) for c, _ in included)}.'
        )
    else:
        for candidate, why in included:
            capture_.notes.append(
                f'Included {candidate.relative_to(project)} ({_human(_size(candidate))}), '
                f'{why}.'
            )


def _local_install(dist) -> bool:
    """Installed from a local directory, so pip cannot fetch it by name and version."""
    try:
        direct = json.loads(dist.read_text('direct_url.json') or '{}')
    except (json.JSONDecodeError, OSError):
        return False
    return 'dir_info' in direct


# The map of installed files to their distributions, with the state of sys.path it was read
# from. Reading it takes seconds; the app previews an export each time a pipeline is chosen.
# The lock makes those previews and an export wait for one read instead of each reading it.
_owners_cache: Dict[str, Any] = {}
_owners_lock = threading.Lock()


def _environment_fingerprint() -> Tuple:
    """
    The package folders on sys.path with their modification times. Installing, upgrading or
    removing a package adds or removes a .dist-info folder, which changes its folder's time.
    Other entries are left out: an export adds the project's folder to sys.path.
    """
    entries = []
    for entry in sorted(set(sys.path)):
        if not entry.rstrip(os.sep).endswith(('site-packages', 'dist-packages')):
            continue
        try:
            entries.append((entry, os.stat(entry).st_mtime_ns))
        except OSError:
            entries.append((entry, None))
    return tuple(entries)


def _file_owners() -> Dict[str, Tuple[str, str]]:
    fingerprint = _environment_fingerprint()
    with _owners_lock:
        if _owners_cache.get('fingerprint') != fingerprint:
            _owners_cache['owners'] = _read_file_owners()
            _owners_cache['fingerprint'] = fingerprint
        return _owners_cache['owners']


def owner_key(path: str) -> str:
    """
    A file's key in the owners map: its folder resolved, its name kept. Resolving every file
    took seconds; a package installed as links to a cache (uv's symlink mode) links files,
    so the file itself stays unresolved on both sides.
    """
    folder, name = os.path.split(os.path.abspath(path))
    return os.path.join(os.path.realpath(folder), name)


def _installed_files(dist) -> List[str]:
    """
    The paths a distribution installed, relative to its folder, read from RECORD as text.
    dist.files checks each file exists, one stat per file of every package.
    """
    record = dist.read_text('RECORD')
    if record is None:
        return [str(file) for file in dist.files or []]
    return [row[0] for row in csv.reader(record.splitlines()) if row]


def _read_file_owners() -> Dict[str, Tuple[str, str]]:
    owners: Dict[str, Tuple[str, str]] = {}
    seen: Set[str] = set()
    for dist in importlib.metadata.distributions():
        # Each read of dist.version parses the distribution's METADATA again.
        metadata = dist.metadata
        name = (metadata['Name'] or '').lower().replace('_', '-')
        # The first distribution of a name on sys.path is the one Python imports. With the
        # importlib_metadata backport imported, every distribution is listed twice.
        if not name or name in seen:
            continue
        seen.add(name)
        if _local_install(dist):
            continue
        version = metadata['Version']
        try:
            base = os.path.realpath(dist.locate_file(''))
        except (OSError, ValueError):
            continue
        folders: Dict[str, str] = {}
        for text in _installed_files(dist):
            # Editable installs load a finder module at start-up; it is not a dependency.
            if not text.endswith(('.py', '.so', '.pyd', '.pyc')) or '__editable__' in text:
                continue
            folder, file_name = os.path.split(text)
            # Folders resolved once each: RECORD can climb out of site-packages with `..`.
            if folder not in folders:
                folders[folder] = os.path.realpath(os.path.join(base, folder))
            owners[os.path.join(folders[folder], file_name)] = (name, version)
    return owners


# Packages MageML's own modules import inside functions.
LAZY_IMPORTS = {
    'mage_ai.feature_stores.feast': ['requests'],
}


def _requirements(capture_: Capture) -> Dict[str, str]:
    """
    The distributions the blocks' imports load, pinned to the versions in this environment.
    A subprocess imports the modules the blocks import, never the blocks themselves.
    """
    modules: Set[str] = set()
    for path in capture_.files:
        if path.suffix == '.py':
            modules.update(_imports(path))
    project_name = capture_.project.name
    local_tops = {p.name for p in capture_.project.iterdir()} | {project_name}
    candidates = sorted(
        m for m in modules
        if m.split('.')[0] not in local_tops and m.split('.')[0] not in sys.stdlib_module_names
    )
    # Batch blocks pass tables as Arrow and the worker reads them with pandas; stream blocks
    # pass JSON messages, so a stream-only service gets them only when its blocks import them.
    batch = any(not p.get('stream') for p in capture_.manifest['pipelines'])
    if batch:
        candidates += ['pyarrow', 'pandas']
    candidates += sorted(capture_.artifact_modules)
    # MageML modules that import a package inside a function, which the import below misses.
    for module, packages in LAZY_IMPORTS.items():
        if module in modules:
            candidates += packages
    if any(b['language'] == 'sql' for p in capture_.manifest['pipelines'] for b in p['blocks']):
        # What the SQL runner imports to render queries and use PostgreSQL.
        candidates += [
            'jinja2', 'inflection', 'mage_ai.io.config',
            'mage_ai.data_preparation.shared.utils', 'mage_ai.data_preparation.templates.utils',
        ]
        providers = {
            (b.get('configuration') or {}).get('data_provider')
            for p in capture_.manifest['pipelines'] for b in p['blocks'] if b['language'] == 'sql'
        }
        if 'postgres' in providers:
            candidates += ['psycopg2', 'mage_ai.io.postgres', 'mage_ai.io.postgres_types']
        if 'mysql' in providers:
            candidates += ['mysql.connector', 'mage_ai.io.mysql']
        if 'duckdb' in providers:
            candidates += ['duckdb', 'mage_ai.io.duckdb']
        if 'clickhouse' in providers:
            candidates += ['clickhouse_connect', 'mage_ai.io.clickhouse']
    if capture_.needs_r:
        # What Mage's R runner imports to pass tables and database settings to R.
        candidates += [
            'polars', 'simplejson', 'yaml', 'jinja2', 'mage_ai.shared.parsers',
            'mage_ai.shared.processes', 'mage_ai.io.config',
            'mage_ai.data_preparation.shared.utils',
        ]
    script = textwrap.dedent('''
        import importlib, json, os, sys
        failed = []
        for name in sys.argv[1:]:
            try:
                importlib.import_module(name)
            except Exception as error:
                failed.append([name, f"{type(error).__name__}: {error}"])
        files = sorted({
            os.path.abspath(module.__file__)
            for module in list(sys.modules.values())
            if getattr(module, '__file__', None)
        })
        print(json.dumps(dict(files=files, failed=failed)))
    ''')
    result = subprocess.run(
        [sys.executable, '-c', script, *candidates],
        capture_output=True, text=True, timeout=600,
        env={**os.environ, 'MAGE_REPO_PATH': str(capture_.project)},
    )
    try:
        report = json.loads(result.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        raise ExportError([f'Resolving the blocks\' packages failed:\n{result.stderr[-2000:]}'])
    for name, error in report['failed']:
        capture_.unresolved_imports.add(name)
        capture_.notes.append(
            f'Import {name} failed in this environment ({error}); add its package to the '
            'project requirements.txt if the blocks need it.'
        )
    # The distribution that installed each loaded file. Namespace packages such as
    # `google` are shared by many distributions; matching by module name pulled in every
    # Google client.
    owners = _file_owners()
    pinned: Dict[str, str] = {}
    for path in report['files']:
        owner = owners.get(owner_key(path))
        if owner is None:
            continue
        name, version = owner
        if name not in EXCLUDED_DISTRIBUTIONS:
            pinned[name] = version
    for name in BASE_PACKAGES if batch else ():
        pinned.setdefault(name, importlib.metadata.version(name))
    # Dependencies of the pinned distributions, so pip resolves nothing differently.
    pending = list(pinned)
    while pending:
        dist = pending.pop()
        try:
            requires = importlib.metadata.requires(dist) or []
        except importlib.metadata.PackageNotFoundError:
            continue
        for requirement in requires:
            if 'extra ==' in requirement:
                continue
            marker_free = requirement.split(';')[0]
            match = re.match(r'\s*([A-Za-z0-9_.\-]+)', marker_free)
            if not match:
                continue
            dep = match.group(1).lower().replace('_', '-')
            if dep in pinned or dep in EXCLUDED_DISTRIBUTIONS:
                continue
            if ';' in requirement:
                # Platform markers (Windows, older Pythons) are evaluated by pip in the image.
                continue
            try:
                pinned[dep] = importlib.metadata.version(dep)
                pending.append(dep)
            except importlib.metadata.PackageNotFoundError:
                continue
    return dict(sorted(pinned.items()))


# ---- writing the build context -------------------------------------------------------


def _kept_on_export(name: str) -> bool:
    """
    Files of the user in an export folder: the filled-in environment, local data, and the
    git repository when the folder is kept as one, so exporting again shows as a diff.
    """
    return name in ('data', '.git') or (
        (name == '.env' or name.startswith('.env.')) and name != '.env.example'
    )


def write(capture_: Capture, out: str, force: bool = False) -> Path:
    out_dir = Path(out).resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        if not force and not (out_dir / 'service.json').exists():
            raise ExportError([
                f'{out_dir} is not empty and holds no earlier export; choose another directory '
                'or pass --force.'
            ])
        for child in out_dir.iterdir():
            if _kept_on_export(child.name):
                continue
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    project_out = out_dir / capture_.project.name
    for path in sorted(capture_.files):
        target = project_out / path.relative_to(capture_.project)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    model_requirements = _embed_models(capture_, out_dir)
    (out_dir / 'service.json').write_text(json.dumps(capture_.manifest, indent=2) + '\n')

    if capture_.needs_python:
        _copy_mage_python(out_dir / 'python' / 'mage_ai')
        lines = [f'{name}=={version}' for name, version in capture_.requirements.items()]
        lines += model_requirements
        project_requirements = capture_.project / 'requirements.txt'
        if project_requirements.is_file():
            lines.append('-r ' + f'{capture_.project.name}/requirements.txt')
        (out_dir / 'requirements.txt').write_text(
            '# The packages the exported blocks import, pinned to the versions of the Mage\n'
            '# environment that exported them.\n' + '\n'.join(lines) + '\n'
        )
    if capture_.rust_blocks:
        _copy_rust_workspace(capture_, out_dir / 'rust')
    if capture_.r_project is not None:
        (out_dir / 'r').mkdir(parents=True, exist_ok=True)
        for name in ('rproject.toml', 'rv.lock', R_SYSTEM_PACKAGES):
            if (capture_.r_project / name).is_file():
                shutil.copy2(capture_.r_project / name, out_dir / 'r' / name)
    _copy_service_source(out_dir / 'build' / 'mage_service')
    (out_dir / 'Dockerfile').write_text(_dockerfile(capture_))
    (out_dir / '.dockerignore').write_text(
        '**/__pycache__\n**/target\ndata/\n.tekton/\n.github/\n.git/\ndeploy/\n.env\n.env.*\n'
    )
    (out_dir / '.gitignore').write_text(
        '# The folder can be its own repository: export again into it and commit the diff.\n'
        '.env\n.env.*\n!.env.example\ndata/\n**/__pycache__/\n**/target/\n'
    )
    (out_dir / 'compose.yaml').write_text(_compose(capture_))
    (out_dir / 'README.md').write_text(_readme(capture_))
    (out_dir / '.env.example').write_text(_env_example(capture_))
    _write_deploy_files(capture_, out_dir)
    (out_dir / 'report.txt').write_text(report(capture_))
    return out_dir


def _file_requirements(artifacts: Path) -> List[str]:
    """
    The reading package of a model logged as a plain file, such as catboost for model.cbm:
    MLflow records no requirements for it.
    """
    from mage_ai.pipeline_services.models import ARTIFACT_LOADERS

    requirements = []
    for path in sorted(artifacts.rglob('*')) if artifacts.is_dir() else []:
        module = ARTIFACT_LOADERS.get(path.suffix.lower(), (None, None))[1]
        if not module:
            continue
        try:
            dist = importlib.metadata.packages_distributions().get(module, [module])[0]
            requirements.append(f'{dist}=={importlib.metadata.version(dist)}')
        except (importlib.metadata.PackageNotFoundError, IndexError):
            requirements.append(module)
    return list(dict.fromkeys(requirements))


def _embed_models(capture_: Capture, out_dir: Path) -> List[str]:
    """Downloads each model with its metadata; returns requirements the image adds."""
    from mage_ai.pipeline_services import model_export

    capture_.manifest['models'] = []
    if not capture_.models:
        return []
    problems = []
    extra: Dict[str, str] = {}
    for name, uri in capture_.models:
        try:
            metadata = model_export.download(name, uri, out_dir / 'models' / name)
        except Exception as error:
            problems.append(
                f'Model {name} ({uri}) could not be downloaded and embedded: '
                f'{model_export.explain(error, uri)}'
            )
            continue
        capture_.manifest['models'].append(dict(
            name=name,
            uri=uri,
            path=f'models/{name}',
            sha256=metadata['sha256'],
            version=metadata.get('version'),
            run_id=metadata.get('run_id'),
            flavors=metadata.get('flavors') or [],
            size_bytes=metadata.get('size_bytes'),
        ))
        requirements = metadata.get('requirements') or _file_requirements(
            out_dir / 'models' / name / 'artifacts')
        for requirement in requirements:
            match = re.match(r'^([A-Za-z0-9_.\-]+)', requirement)
            package = match.group(1).lower().replace('_', '-') if match else requirement
            pinned = capture_.requirements.get(package)
            if pinned and requirement != f'{package}=={pinned}' and '==' in requirement:
                capture_.notes.append(
                    f'Model {name} was logged with {requirement}; the blocks use '
                    f'{package}=={pinned}, which the image keeps.'
                )
                continue
            if package not in capture_.requirements:
                extra[package] = requirement
        capture_.notes.append(
            f"Model {name}: version {metadata.get('version') or '-'}, run "
            f"{metadata.get('run_id') or '-'}, {metadata.get('size_bytes', 0):,} bytes."
        )
    if problems:
        raise ExportError(problems)
    # MLflow loads a model with an MLmodel file (pyfunc or its framework flavor); a model
    # logged as a plain file, such as model.cbm, loads without it.
    needs_mlflow = any(
        (out_dir / 'models' / name / 'artifacts' / 'MLmodel').is_file()
        for name, _ in capture_.models
    )
    if needs_mlflow and 'mlflow' not in extra and 'mlflow-skinny' not in capture_.requirements:
        for distribution in ('mlflow-skinny', 'mlflow'):
            try:
                extra.setdefault(
                    distribution,
                    f'{distribution}=={importlib.metadata.version(distribution)}',
                )
                break
            except importlib.metadata.PackageNotFoundError:
                continue
    return sorted(extra.values())


# Deployment files, rendered with the service's name: (template, destination).
DEPLOY_FILES = (
    ('tekton/pipeline.yaml', '.tekton/pipeline.yaml'),
    ('tekton/tasks.yaml', '.tekton/tasks.yaml'),
    ('tekton/listener.yaml', '.tekton/listener.yaml'),
    ('deploy/ibm/code-engine.sh', 'deploy/ibm/code-engine.sh'),
    ('deploy/ibm/README.md', 'deploy/ibm/README.md'),
    ('deploy/kubernetes.yaml', 'deploy/kubernetes.yaml'),
    ('ci/github-build-image.yml', '.github/workflows/build-image.yml'),
)


def _environment_comment(capture_: Capture) -> str:
    """
    The variables the container needs, as comment lines for the deploy files. The template's
    line is #$environment, so the template is valid YAML before it is filled in.
    """
    lines = []
    for variable in capture_.manifest.get('environment') or []:
        flags = [f for f, on in (('required', variable['required']),
                                 ('secret', variable['secret'])) if on]
        lines.append(f"   {variable['name']}{f'  ({", ".join(flags)})' if flags else ''}")
    return '\n#'.join(lines) or '   none: the blocks read no environment variable.'


def _write_deploy_files(capture_: Capture, out_dir: Path) -> None:
    environment = _environment_comment(capture_)
    for template, destination in DEPLOY_FILES:
        text = string.Template((TEMPLATES / template).read_text()).safe_substitute(
            environment=environment,
            name=capture_.name,
            project=capture_.project.name,
        )
        target = out_dir / destination
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        if target.suffix == '.sh':
            target.chmod(0o755)


def _copy_mage_python(target: Path) -> None:
    for path in MAGE_ROOT.rglob('*'):
        relative = path.relative_to(MAGE_ROOT)
        if any(part in MAGE_EXCLUDED_DIRS for part in relative.parts):
            continue
        if any(relative.is_relative_to(excluded) for excluded in MAGE_EXCLUDED_PATHS):
            continue
        r_runner = relative.is_relative_to(R_RUNNER)
        if not path.is_file() or (path.suffix not in MAGE_DATA_SUFFIXES and not r_runner):
            continue
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)


def _copy_tree(source: Path, target: Path, skip: Iterable[str]) -> None:
    skip = set(skip)

    def ignore(directory, names):
        return [n for n in names if n in skip]

    shutil.copytree(source, target, ignore=ignore, dirs_exist_ok=True)


# The one binary of a service's Rust blocks; MAGE_BLOCK names the block it runs.
RUST_BUNDLE = 'mage-blocks'


def _copy_rust_workspace(capture_: Capture, target: Path) -> None:
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    root = ws.workspace_root(str(capture_.project))
    target.mkdir(parents=True, exist_ok=True)
    for name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'):
        if (root / name).is_file():
            shutil.copy2(root / name, target / name)
    _copy_tree(root / '.mage' / 'sdk', target / '.mage' / 'sdk', ['target'])
    for _, crate in capture_.rust_blocks:
        _copy_tree(root / '.mage' / 'blocks' / crate, target / '.mage' / 'blocks' / crate,
                   ['target'])
        _block_library(target / '.mage' / 'blocks' / crate)
    crates = list(dict.fromkeys(crate for _, crate in capture_.rust_blocks))
    _write_bundle(target, [c for c in crates if c not in capture_.rust_stream_crates],
                  [c for c in crates if c in capture_.rust_stream_crates])
    capture_.rust_locked = _sync_cargo_lock(capture_, target)


def _block_library(directory: Path) -> None:
    """
    Turns a block's crate into a library whose mage_run runs the block, for the bundle.
    The block's code stays the crate's root, as in Mage, so its items and imports mean
    the same; only the generated main becomes a public function.
    """
    from mage_ai.data_preparation.models.block.rust import source as block_source

    src = directory / 'src'
    main = (src / 'main.rs').read_text()
    generated = (src / block_source.MAIN_FILE).read_text()
    call = re.search(r'fn main\(\) \{\s*mage::main\((.*)\);\s*\}\s*$', generated, re.S)
    stream = re.search(
        r'fn main\(\) \{\s*mage::serve_stream\(vec!\[\("block", '
        r'\|\| (mage::stream_block\(.*\))\)\]\);\s*\}\s*$',
        generated, re.S,
    )
    if call is not None:
        arguments = call.group(1)
        (src / 'mage_lib.rs').write_text(
            f'{block_source.GENERATED_MARKER}\n'
            f'pub fn mage_run() -> ! {{\n    mage::main({arguments});\n}}\n\n'
            '/// The block for a chain of blocks run in one process (mage::run_chain).\n'
            f'pub fn mage_block() -> mage::ChainBlock {{\n    mage::chain_block({arguments})\n}}\n'
        )
    elif stream is not None:
        (src / 'mage_lib.rs').write_text(
            f'{block_source.GENERATED_MARKER}\n'
            '/// The stream block, for the program that serves a stream (mage::serve_stream).\n'
            f'pub fn mage_stream_block() -> mage::StreamBlock {{\n    {stream.group(1)}\n}}\n'
        )
    else:
        raise ExportError([f'{directory.name}: its generated main is not the expected one.'])
    (src / 'lib.rs').write_text(
        main.replace(f'include!("{block_source.MAIN_FILE}");', 'include!("mage_lib.rs");'),
    )
    (src / 'main.rs').unlink()
    (src / block_source.MAIN_FILE).unlink()
    manifest = directory / 'Cargo.toml'
    text = manifest.read_text()
    binary = re.compile(r'\[\[bin\]\]\nname = "[^"]+"\npath = "src/main.rs"\n')
    if not binary.search(text):
        raise ExportError([f'{directory.name}: its Cargo.toml has no block binary to replace.'])
    manifest.write_text(binary.sub('[lib]\npath = "src/lib.rs"\n', text, count=1))


def _write_bundle(workspace: Path, crates: List[str], stream_crates: List[str] = ()) -> None:
    """
    One binary for all the Rust blocks: each block's binary held a copy of Polars (about
    75 MB), and after a few minutes idle macOS and Linux load each from disk again. Stream
    transformers (`stream_crates`) run in it with MAGE_STREAM=1, as one resident program.
    """
    bundle = workspace / '.mage' / 'bundle'
    (bundle / 'src').mkdir(parents=True, exist_ok=True)
    dependencies = 'mage-block = { workspace = true }\n' + ''.join(
        f'{crate} = {{ path = "../blocks/{crate}" }}\n' for crate in [*crates, *stream_crates]
    )
    (bundle / 'Cargo.toml').write_text(
        '# Generated by mage export service: the Rust blocks of the service in one binary.\n'
        f'[package]\nname = "{RUST_BUNDLE}"\nversion = "0.0.0"\nedition = "2024"\n'
        f'publish = false\n\n[[bin]]\nname = "{RUST_BUNDLE}"\npath = "src/main.rs"\n\n'
        f'[dependencies]\n{dependencies}'
    )
    arms = ''.join(f'        "{crate}" => {crate}::mage_run(),\n' for crate in crates)
    entries = ''.join(f'            ("{crate}", {crate}::mage_block),\n' for crate in crates)
    streams = ''.join(
        f'            ("{crate}", {crate}::mage_stream_block),\n' for crate in stream_crates
    )
    (bundle / 'src' / 'main.rs').write_text(
        '// Generated by mage export service. MAGE_BLOCK names the block, MAGE_CHAIN=1 runs\n'
        '// the chain of blocks job.json lists in this process, and MAGE_STREAM=1 serves the\n'
        '// stream blocks stream.json lists; the job directory is the first argument, as for a\n'
        '// block\'s own binary.\n'
        'fn main() {\n'
        '    if std::env::var("MAGE_STREAM").is_ok_and(|v| v == "1") {\n'
        '        mage::serve_stream(vec![\n'
        f'{streams}'
        '        ]);\n'
        '    }\n'
        '    if std::env::var("MAGE_CHAIN").is_ok_and(|v| v == "1") {\n'
        '        mage::run_chain(vec![\n'
        f'{entries}'
        '        ]);\n'
        '    }\n'
        '    let block = std::env::var("MAGE_BLOCK").unwrap_or_default();\n'
        '    match block.as_str() {\n'
        f'{arms}'
        '        other => {\n'
        '            eprintln!("This service has no Rust block {other:?} (MAGE_BLOCK).");\n'
        '            std::process::exit(2);\n'
        '        }\n'
        '    }\n'
        '}\n'
    )
    manifest = workspace / 'Cargo.toml'
    text = manifest.read_text()
    members = re.search(r'members\s*=\s*\[([^\]]*)\]', text)
    if members is None:
        raise ExportError([f'{manifest} has no workspace members.'])
    if '.mage/bundle' not in members.group(1):
        listed = members.group(1).rstrip().rstrip(',')
        text = (text[:members.start(1)] + f'{listed}, ".mage/bundle"' + text[members.end(1):])
        manifest.write_text(text)


def _sync_cargo_lock(capture_: Capture, workspace: Path) -> bool:
    """
    Brings the copied Cargo.lock in line with the exported crates: Mage builds blocks
    without --locked, so a project's lock can lag behind its Cargo.toml, and the export
    leaves out the crates of other pipelines. Locked versions are kept. Returns whether
    the image can build with --locked.
    """
    from mage_ai.data_preparation.models.block.rust.build import RustBuildError, _cargo

    lock = workspace / 'Cargo.lock'
    before = lock.read_bytes() if lock.is_file() else None
    try:
        cargo = _cargo()
    except RustBuildError:
        capture_.notes.append(
            'Cargo is not installed here, so Cargo.lock was not checked; the image resolves '
            'crate versions when it builds.'
        )
        return False
    error = ''
    for offline in (['--offline'], []):
        try:
            result = subprocess.run(
                [cargo, 'metadata', '--format-version', '1', *offline],
                capture_output=True, cwd=workspace, text=True, timeout=600,
            )
        except subprocess.TimeoutExpired:
            error = 'cargo metadata timed out'
            continue
        if result.returncode == 0:
            break
        error = (result.stderr.strip().splitlines() or ['cargo metadata failed'])[-1]
    else:
        capture_.notes.append(
            f'Cargo.lock could not be updated ({error}); the image resolves crate versions '
            'when it builds.'
        )
        return False
    if before is not None and lock.read_bytes() != before:
        capture_.notes.append(
            'Cargo.lock was updated for the exported crates; locked versions were kept.'
        )
    return lock.is_file()


def _copy_service_source(target: Path) -> None:
    if not (SERVICE_SOURCE / 'Cargo.toml').is_file():
        raise ExportError([
            f'The pipeline service source is missing from {SERVICE_SOURCE}; this Mage '
            'installation cannot export services.'
        ])
    _copy_tree(SERVICE_SOURCE, target, ['target', '.git'])


def _rust_version() -> str:
    toolchain = SERVICE_SOURCE / 'rust-toolchain.toml'
    if not toolchain.exists():
        return '1.98.0'
    match = re.search(r'channel\s*=\s*"([^"]+)"', toolchain.read_text())
    return match.group(1) if match else '1.98.0'


RV_VERSION = '0.20.0'
CRAN_KEY_URL = (
    'https://keyserver.ubuntu.com/pks/lookup?op=get'
    '&search=0x95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7'
)
RV_URL = (
    'https://github.com/A2-ai/rv/releases/download/v${RV_VERSION}/'
    'rv-v${RV_VERSION}-$(uname -m)-unknown-linux-gnu.tar.gz'
)
# Loads Mage's R runner without Mage's Block (as the worker does) and installs mageml.
R_PREPARE = (
    "import importlib, importlib.util, os, sys, types; "
    "d = os.path.join(os.path.dirname(importlib.util.find_spec('mage_ai').origin), "
    "'data_preparation', 'models', 'block', 'r'); "
    "p = types.ModuleType('mage_service_r'); p.__path__ = [d]; sys.modules['mage_service_r'] = p; "
    "rt = importlib.import_module('mage_service_r.runtime'); "
    "print(rt.prepare(rt.r_config()))"
)


def _uses_sdk_feature(capture_: Capture, feature: str) -> bool:
    """Whether the project's rust/Cargo.toml turns on a feature of MageML's crate."""
    import tomllib

    path = capture_.project / 'rust' / 'Cargo.toml'
    try:
        manifest = tomllib.loads(path.read_text(encoding='utf-8'))
    except (OSError, tomllib.TOMLDecodeError):
        return False
    block = ((manifest.get('workspace') or {}).get('dependencies') or {}).get('mage-block')
    return isinstance(block, dict) and feature in (block.get('features') or [])


def _dockerfile(capture_: Capture) -> str:
    project = capture_.project.name
    rust = _rust_version()
    python_version = f'{sys.version_info.major}.{sys.version_info.minor}'
    if capture_.environment is not None:
        python_version = capture_.environment.python
    # ONNX Runtime's static library needs GCC 14's C++ library, which Debian has from trixie
    # on: the Rust programs build on trixie and run on it.
    onnx = _uses_sdk_feature(capture_, 'onnx')
    build_base = 'trixie' if onnx else 'bookworm'
    rust_blocks = ''
    copy_blocks = ''
    if capture_.rust_blocks:
        locked = '--locked ' if capture_.rust_locked else ''
        # ONNX Runtime is C++: linking it needs the C++ standard library of g++.
        cxx = (
            'RUN apt-get update && apt-get install -y --no-install-recommends g++ && \\\n'
            '    rm -rf /var/lib/apt/lists/*\n'
        ) if onnx else ''
        # Each service has its own: the blocks crate has the same path in every export.
        blocks_cache = f'mage-blocks-{capture_.name}-{build_base}-$TARGETARCH'
        rust_blocks = f'''
# The Rust blocks, compiled once here into one binary that runs any of them.
{cxx}COPY rust ./rust
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
    --mount=type=cache,id={blocks_cache},target=/build/rust/target \\
    cargo build --release {locked}--manifest-path rust/Cargo.toml -p {RUST_BUNDLE} && \\
    mkdir -p /out/bin && cp rust/target/release/{RUST_BUNDLE} /out/bin/{RUST_BUNDLE}
'''
        copy_blocks = 'COPY --from=build --chown=mage:mage /out/bin ./bin\n'
    r_install = ''
    r_prepare = ''
    if capture_.needs_r:
        system_packages = ''.join(
            f' \\\n      {name}' for name in r_system_packages(capture_.r_project)
        )
        # As in Mage's image: R 4.6 from CRAN's Debian repository and rv, which installs
        # the packages rv.lock pins (binaries where Posit has them, else from source).
        r_install = f'''
ARG RV_VERSION={RV_VERSION}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \\
    curl -fsSL \\
      '{CRAN_KEY_URL}' \\
      -o /etc/apt/trusted.gpg.d/cran_debian_key.asc && \\
    printf '%s\\n' 'Types: deb' 'URIs: https://cloud.r-project.org/bin/linux/debian/' \\
      'Suites: trixie-cran46/' 'Components:' \\
      'Signed-By: /etc/apt/trusted.gpg.d/cran_debian_key.asc' \\
      > /etc/apt/sources.list.d/cran.sources && \\
    apt-get update && \\
    apt-get install -y --no-install-recommends r-base-core r-recommended r-base-dev \\
      cmake libcurl4-openssl-dev libfontconfig1-dev libfreetype6-dev libfribidi-dev \\
      libharfbuzz-dev libicu-dev libjpeg-dev libmariadb-dev libpng-dev libpq-dev \\
      libssl-dev libtiff-dev libuv1-dev libwebp-dev libxml2-dev make xz-utils \\
      zlib1g-dev{system_packages} && \\
    curl -fsSL "{RV_URL}" \\
      | tar -xz -C /usr/local/bin rv && \\
    rm -rf /var/lib/apt/lists/*
ENV LIBARROW_MINIMAL=false \\
    MAGE_R_PROJECT_DIR=/srv/mage-service/r \\
    MAGE_R_CACHE_DIR=/srv/mage-service/r-cache \\
    MAGE_R_SYNC=check
# The R packages of the project's R environment, as rv.lock pins them.
COPY r /srv/mage-service/r
RUN --mount=type=cache,target=/root/.cache/rv cd /srv/mage-service/r && rv sync
'''
        # Mage's mageml package, installed now: the image's files are read-only at run time.
        r_prepare = f'''RUN PYTHONPATH=/srv/mage-service/python python3 -c "{R_PREPARE}" && \\
    chown -R mage:mage /srv/mage-service/r-cache
# The R library was synced and checked above; nothing changes it at run time. rv still
# reads its cache directory, which must be writable: /tmp is, also on Kubernetes.
ENV MAGE_R_SYNC=off \\
    XDG_CACHE_HOME=/tmp/mage-service-cache
'''
    if capture_.needs_python:
        base = 'trixie' if capture_.needs_r or onnx else 'bookworm'
        runtime = f'''FROM python:{python_version}-slim-{base}
ENV PYTHONDONTWRITEBYTECODE=1 \\
    PYTHONUNBUFFERED=1 \\
    PIP_NO_CACHE_DIR=1 \\
    PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements.txt /tmp/requirements.txt
COPY {project}/requirements.tx[t] /tmp/{project}/
RUN pip install --requirement /tmp/requirements.txt && rm -rf /tmp/requirements.txt /tmp/{project}
{r_install}'''
        python_env = (
            ' \\\n    PYTHONPATH=/srv/mage-service/python'
            ' \\\n    MAGE_SERVICE_PYTHON=python3'
        )
        copy_python = 'COPY --chown=mage:mage python ./python\n'
    else:
        runtime = f'FROM debian:{build_base}-slim\n'
        python_env = ''
        copy_python = ''
    engine_features = ''
    kafka_build = ''
    if capture_.engine_features:
        engine_features = ' --features ' + ','.join(
            f'mage-service/{feature}' for feature in capture_.engine_features
        )
    if capture_.needs_kafka:
        # librdkafka builds with CMake, OpenSSL from source (make, perl), and includes curl's
        # header (I-40). All of it links statically: the runtime image needs nothing more.
        kafka_build = (
            '# The Kafka connector builds librdkafka and OpenSSL from source.\n'
            'RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make \\\n'
            '      perl libcurl4-openssl-dev && \\\n'
            '    rm -rf /var/lib/apt/lists/*\n'
        )
    ibm_mq_stage = ''
    copy_ibm_mq = ''
    ibm_mq_env = ''
    if capture_.needs_ibm_mq:
        ibm_mq_stage = f'''
# IBM's redistributable MQ client, for the IBM MQ connector, which loads it at run time. IBM
# publishes it for linux/amd64 only: build with --platform linux/amd64.
FROM debian:{build_base}-slim AS ibm-mq
ARG TARGETARCH
RUN if [ "$TARGETARCH" != "amd64" ]; then \\
      echo "IBM MQ's client is for linux/amd64 only; build with --platform linux/amd64" >&2; \\
      exit 1; fi && \\
    apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \\
    curl -fsSL -o /tmp/mq.tar.gz {IBM_MQ_CLIENT_URL} && \\
    echo "{IBM_MQ_CLIENT_SHA256}  /tmp/mq.tar.gz" | sha256sum -c - && \\
    mkdir -p /opt/mqm && tar -xzf /tmp/mq.tar.gz -C /opt/mqm && \\
    cd /opt/mqm && rm -rf bin inc java lap lib samp swidtag && \\
    find msg -mindepth 1 -maxdepth 1 ! -name en_US -exec rm -rf {{}} + && \\
    rm -f lib64/*.a lib64/*.dll lib64/libimq*
'''
        copy_ibm_mq = 'COPY --from=ibm-mq /opt/mqm /opt/mqm\n'
        # The client writes its error logs under its data path; the root file system may be
        # read-only, /tmp is writable.
        ibm_mq_env = (
            ' \\\n    MAGE_SERVICE_MQ_LIBRARY=/opt/mqm/lib64/libmqm_r.so'
            ' \\\n    LD_LIBRARY_PATH=/opt/mqm/lib64'
            ' \\\n    MQ_OVERRIDE_DATA_PATH=/tmp/mage-mq'
        )
    if capture_.models:
        copy_models = 'COPY --chown=mage:mage models ./models\n'
        models_env = ' \\\n    MAGE_SERVICE_MODELS_DIR=/srv/mage-service/models'
    else:
        copy_models = ''
        models_env = ''
    return f'''# syntax=docker/dockerfile:1.7
# {capture_.name}: pipelines exported from the Mage project {project}.
# docker build -t {capture_.name} .

FROM rust:{rust}-slim-{build_base} AS build
# Cargo's target folder is cached for each Debian release and CPU: a binary from another
# release can need a newer glibc, and the amd64 and arm64 builds run at the same time.
ARG TARGETARCH
WORKDIR /build
{kafka_build}COPY build/mage_service ./mage_service
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
    --mount=type=cache,id=mage-service-{build_base}-$TARGETARCH,target=/build/mage_service/target \\
    cargo build --release --locked --manifest-path mage_service/Cargo.toml \\
      -p mage-service -p mage-console{engine_features} && \\
    mkdir -p /out && \\
    cp mage_service/target/release/mage-service mage_service/target/release/mage-console /out/
{rust_blocks}{ibm_mq_stage}
{runtime}RUN groupadd --system --gid 10001 mage && \\
    useradd --system --uid 10001 --gid mage --home-dir /srv/mage-service --no-create-home mage && \\
    mkdir -p /srv/mage-service /var/lib/mage-service && \\
    chown mage:mage /var/lib/mage-service
WORKDIR /srv/mage-service
COPY --from=build /out/mage-service /out/mage-console /usr/local/bin/
{copy_ibm_mq}{copy_python}{copy_blocks}{copy_models}COPY --chown=mage:mage {project} ./{project}
COPY --chown=mage:mage service.json ./service.json
{r_prepare}
ENV MAGE_SERVICE_DIR=/srv/mage-service \\
    MAGE_SERVICE_DATA=/var/lib/mage-service \\
    MAGE_SERVICE_HOST=0.0.0.0 \\
    MAGE_SERVICE_PORT=8080{models_env}{python_env}{ibm_mq_env}
USER mage
VOLUME /var/lib/mage-service
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \\
    CMD ["mage-service", "health"]
RUN mage-service validate
ENTRYPOINT ["mage-service"]
CMD ["serve"]
'''


def _compose(capture_: Capture) -> str:
    return f'''# docker compose up --build
services:
  {capture_.name}:
    build: .
    image: {capture_.name}:latest
    ports:
      - "127.0.0.1:${{PORT:-8080}}:8080"
    environment:
      # Every API call needs this token. compose stops when it is not set (in .env or the
      # shell): a default would be a token everyone knows.
      MAGE_SERVICE_TOKEN: ${{MAGE_SERVICE_TOKEN:?Set MAGE_SERVICE_TOKEN in .env or the shell}}
      # on: this service runs the active time triggers. Keep it off while Mage runs them.
      MAGE_SERVICE_SCHEDULES: ${{MAGE_SERVICE_SCHEDULES:-off}}
    env_file:
      - path: .env
        required: false
    volumes:
      - data:/var/lib/mage-service
    read_only: true
    tmpfs:
      - /tmp
    stop_grace_period: 75s
    restart: unless-stopped
volumes:
  data:
'''


AFTER_RUN_WORDS = {'completed': 'completes', 'failed': 'fails', 'any': 'ends'}


def describe_trigger(trigger: Dict) -> str:
    """How a trigger starts runs, for people: its schedule, the run it follows, or the API."""
    if trigger['kind'] == 'after_run':
        ending = AFTER_RUN_WORDS.get(trigger.get('after_status') or 'completed', 'completes')
        return f"after {trigger['after_pipeline']} {ending}"
    return trigger.get('schedule') or 'api'


def _readme(capture_: Capture) -> str:
    pipelines = capture_.manifest['pipelines']
    first = pipelines[0]['uuid']
    rows = '\n'.join(
        f"| `{p['uuid']}` | {len(p['blocks'])} | "
        + (', '.join(
            f"{t['name']} ({describe_trigger(t)}{'' if t['active'] else ', inactive'})"
            for t in p['triggers']
        ) or 'API only')
        + ' |'
        for p in pipelines
    )
    variables = capture_.manifest.get('environment') or []
    if variables:
        header = '| Variable | Required | Secret | Read in |\n| --- | --- | --- | --- |\n'
        environment = header + '\n'.join(
            f"| `{v['name']}` | {'yes' if v['required'] else 'no'} | "
            f"{'yes' if v['secret'] else 'no'} | {', '.join(v['used_by'])} |"
            for v in variables
        )
    else:
        environment = 'The exported code reads no environment variables.'
    models = capture_.manifest.get('models') or []
    if models:
        models_text = '| Model | MLflow URI | Version | Flavors |\n| --- | --- | --- | --- |\n' + \
            '\n'.join(
                f"| `{m['name']}` | `{m['uri']}` | {m.get('version') or '-'} | "
                f"{', '.join(m.get('flavors') or []) or '-'} |"
                for m in models
            )
    else:
        models_text = 'This service embeds no models.'
    template = string.Template((TEMPLATES / 'README.md').read_text())
    return template.safe_substitute(
        environment=environment,
        models=models_text,
        name=capture_.name,
        project=capture_.project.name,
        exported_at=capture_.manifest['service']['exported_at'],
        rows=rows,
        first=first,
    )


def _env_example(capture_: Capture) -> str:
    lines = [
        '# Copy to .env and fill in; never commit .env. compose.yaml and `docker run',
        '# --env-file .env` read it.',
        'MAGE_SERVICE_TOKEN=',
    ]
    for variable in capture_.manifest.get('environment') or []:
        notes = []
        if variable['required']:
            notes.append('required')
        if variable['secret']:
            notes.append('secret')
        notes.append('read in ' + ', '.join(variable['used_by']))
        lines.append(f"# {'; '.join(notes)}")
        lines.append(f"{variable['name']}=")
    return '\n'.join(lines) + '\n'


def report(capture_: Capture) -> str:
    lines = [f'Service {capture_.name}, from project {capture_.project.name}', '']
    for pipeline in capture_.manifest['pipelines']:
        lines.append(f"{pipeline['uuid']}: {len(pipeline['blocks'])} blocks, "
                     f"at most {pipeline['max_concurrent_runs']} run(s) at once")
        for block in pipeline['blocks']:
            retry = block['retry']['retries']
            extra = f", {retry} retries" if retry else ''
            if block.get('conditions'):
                extra += f", runs when {' and '.join(block['conditions'])} hold"
            lines.append(f"  {block['language']:<6} {block['type']:<13} {block['uuid']}{extra}")
        for trigger in pipeline['triggers']:
            how = describe_trigger(trigger) if trigger['kind'] == 'after_run' else (
                trigger.get('schedule') or ''
            )
            lines.append(f"  trigger {trigger['name']}: {trigger['kind']} "
                         f"{how}{'' if trigger['active'] else ' (inactive)'}")
        lines.append('')
    files = sorted(str(p.relative_to(capture_.project)) for p in capture_.files)
    lines.append(f'Project files ({len(files)}):')
    lines.extend(f'  {f}' for f in files)
    variables = capture_.manifest.get('environment') or []
    if variables:
        lines.append('')
        lines.append(f'Environment variables ({len(variables)}; give them to the container):')
        for v in variables:
            flags = ', '.join(
                flag for flag, on in (('required', v['required']), ('secret', v['secret'])) if on
            )
            lines.append(f"  {v['name']}{f' ({flags})' if flags else ''}")
    if capture_.requirements:
        lines.append('')
        lines.append(f'Python packages ({len(capture_.requirements)}):')
        lines.extend(f'  {n}=={v}' for n, v in capture_.requirements.items())
    if capture_.notes:
        lines.append('')
        lines.append('Notes:')
        lines.extend(f'  - {note}' for note in capture_.notes)
    return '\n'.join(lines) + '\n'


def build_commands(
    out_dir: Path,
    tag: str,
    platforms: Optional[List[str]] = None,
    push: bool = False,
) -> List[List[str]]:
    """
    The docker commands that build the image and push it when asked. This machine's platform
    builds with docker build, then docker push, as the app shows: docker's own network
    reaches a registry on localhost, which a docker-container buildx builder does not. Other
    platforms build with docker buildx; several need push, since Docker keeps one platform of
    an image locally.
    """
    docker = shutil.which('docker')
    if not docker:
        raise ExportError(['docker is not installed; build the image with the Dockerfile.'])
    platforms = [p.strip() for p in (platforms or []) if p and p.strip()]
    if not platforms:
        commands = [[docker, 'build', '-t', tag, str(out_dir)]]
        if push:
            commands.append([docker, 'push', tag])
        return commands
    if len(platforms) > 1 and not push:
        raise ExportError([
            'Several platforms need --push: Docker keeps one platform of an image locally. '
            'Push to a registry, or build one platform.'
        ])
    command = [docker, 'buildx', 'build', '--tag', tag, '--platform', ','.join(platforms)]
    command += ['--push' if push else '--load', str(out_dir)]
    return [command]


def build_image(
    out_dir: Path,
    tag: str,
    platforms: Optional[List[str]] = None,
    push: bool = False,
    on_line=None,
) -> None:
    """Builds, and pushes when asked, printing docker's output or passing each line on."""
    import collections

    for command in build_commands(out_dir, tag, platforms=platforms, push=push):
        # The last lines, to say why a build failed; every line is printed or passed on.
        tail = collections.deque(maxlen=400)
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        )
        for line in process.stdout:
            line = line.rstrip('\n')
            tail.append(line)
            if on_line is None:
                print(line, flush=True)
            else:
                on_line(line)
        code = process.wait()
        if code != 0:
            what = 'docker buildx build' if command[1] == 'buildx' else f'docker {command[1]}'
            raise ExportError([
                f'{what} failed with exit code {code}. '
                + explain_build_failure(list(tail), command, tag)
            ])


# What docker prints when a build or push fails for a common reason, and what to do.
BUILD_FAILURES = (
    (('cannot connect to the docker daemon', 'is the docker daemon running',
      'docker desktop is not running', 'error during connect'),
     'Docker is not running on this machine: start Docker Desktop, Rancher Desktop or the '
     'docker service, then build again.'),
    (('no space left on device',),
     'Docker ran out of disk space. Free some with `docker builder prune` and `docker image '
     'prune`, or give Docker\'s disk more room.'),
    (('multiple platforms feature is currently not supported',
      'multi-platform build is not supported for the docker driver'),
     'This buildx builder builds one platform at a time. Create one that builds several: '
     '`docker buildx create --use --driver docker-container`.'),
    (('exec format error',),
     'A step ran for another processor without emulation. Install QEMU for Docker: '
     '`docker run --privileged --rm tonistiigi/binfmt --install all`.'),
    # Before the push: a base image that cannot be pulled also says unauthorized.
    (('failed to resolve source metadata', 'pull access denied', 'manifest unknown'),
     'Docker could not pull a base image. Check the network, or log in if the base image is in '
     'a private registry.'),
    (('denied: requested access to the resource is denied', 'unauthorized:',
      'authentication required', 'insufficient_scope', 'access forbidden', '401 unauthorized',
      '403 forbidden'),
     'The registry refused the push. Log in to {registry} on this machine (`docker login '
     '{registry}`) with an account that may push {image}; the image name must include the '
     'registry and your namespace, such as icr.io/<namespace>/<name>:<tag>.'),
    (('connection refused', 'no such host', 'i/o timeout', 'dial tcp'),
     'Docker could not reach {registry}. Check the address and the network; a local registry '
     'must be running, such as `docker run -d -p 5000:5000 registry:2` for localhost:5000.'),
)


def explain_build_failure(lines: List[str], command: List[str], tag: str) -> str:
    """Why a docker build or push failed and what to do, from the lines it printed."""
    text = '\n'.join(lines).lower()
    first = tag.split('/')[0]
    registry = first if '/' in tag and ('.' in first or ':' in first or
                                        first == 'localhost') else 'docker.io'
    for needles, advice in BUILD_FAILURES:
        if any(needle in text for needle in needles):
            return advice.format(image=tag, registry=registry)
    pushing = command[1] == 'push' or '--push' in command
    return ('The lines above say why. ' + ('If the push failed, check that this machine is '
            f'logged in to {registry} (`docker login {registry}`).' if pushing else ''))
