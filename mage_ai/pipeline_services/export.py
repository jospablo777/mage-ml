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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from mage_ai.pipeline_services.secrets import secret_variable_name

SCHEMA_VERSION = 1
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
    requirements: Dict[str, str] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    unresolved_imports: Set[str] = field(default_factory=set)
    # (name, uri) of each ML model to embed.
    models: List[Tuple[str, str]] = field(default_factory=list)
    # Whether the copied Cargo.lock matches the exported crates, so the image builds --locked.
    rust_locked: bool = False
    # The rv environment R blocks run with (rproject.toml and rv.lock).
    r_project: Optional[Path] = None

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
    if problems:
        raise ExportError(problems)

    _include_shared_code(capture_, problems)
    for extra in ('io_config.yaml', 'requirements.txt', '__init__.py'):
        if (project_path / extra).is_file():
            capture_.files.add(project_path / extra)
    if problems:
        raise ExportError(problems)

    digest = hashlib.sha256()
    for path in sorted(capture_.files):
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
    return capture_


SQL_PROVIDERS = ('postgres',)


def _sql_unsupported(block) -> Optional[str]:
    configuration = block.configuration or {}
    provider = configuration.get('data_provider')
    if provider not in SQL_PROVIDERS:
        return (
            f'a SQL block on {provider or "no data provider"}; services run SQL blocks on '
            'PostgreSQL.'
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
    return directory.resolve()


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
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
    )
    from mage_ai.data_preparation.shared.retry import RetryConfig, resolve_retry_config

    prefix = f'Pipeline {pipeline.uuid}'
    if str(pipeline.type) not in ('python', 'PipelineType.PYTHON'):
        problems.append(
            f'{prefix} is a {pipeline.type} pipeline; services run standard batch pipelines.'
        )
        return None
    blocks = []
    for block in pipeline.blocks_by_uuid.values():
        block_type = str(getattr(block.type, 'value', block.type))
        language = str(getattr(block.language, 'value', block.language))
        where = f'{prefix}, block {block.uuid}'
        if block_type in NOTEBOOK_TYPES:
            capture_.notes.append(
                f'{where}: a {block_type} block; it is notebook only and left out.',
            )
            continue
        if block_type not in SUPPORTED_TYPES:
            problems.append(f'{where} is a {block_type} block, which services do not run yet.')
            continue
        if language not in ('python', 'rust', 'r', 'sql'):
            problems.append(
                f'{where} is written in {language}; services run Python, R, Rust and SQL '
                'blocks.',
            )
            continue
        if language == 'sql':
            reason = _sql_unsupported(block)
            if reason:
                problems.append(f'{where}: {reason}')
                continue
        if is_dynamic_block(block) or is_dynamic_block_child(block) or is_replicated_block(block):
            problems.append(f'{where} is dynamic or replicated, which services do not run yet.')
            continue
        if getattr(block, 'callback_blocks', None) or getattr(block, 'conditional_blocks', None):
            problems.append(
                f'{where} has callbacks or conditionals, which services do not run yet.',
            )
            continue
        if (block.configuration or {}).get('variables'):
            problems.append(
                f'{where} reads its upstream with chunk or batch settings, which services do not '
                'support yet.'
            )
            continue
        path = Path(block.file_path).resolve()
        if not path.is_file():
            problems.append(f'{where}: its file {path} is missing.')
            continue
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
        timeout = getattr(block, 'timeout', None)
        if timeout:
            entry['timeout_seconds'] = int(timeout)
        if language == 'sql':
            entry['sql'] = _sql_details(pipeline, block)
        if language == 'rust':
            crate = _prepare_rust_block(block, project, problems, where)
            if crate:
                entry['binary'] = f'bin/{crate}'
                capture_.rust_blocks.append((block.uuid, crate))
        capture_.files.add(path)
        blocks.append(entry)
    # Upstream blocks that were left out (notebook blocks) are not dependencies.
    kept = {b['uuid'] for b in blocks}
    for entry in blocks:
        entry['upstream'] = [u for u in entry['upstream'] if u in kept]
    concurrency = (getattr(pipeline, 'concurrency_config', None) or {})
    limit = max_runs or concurrency.get('pipeline_run_limit_all_triggers') or \
        concurrency.get('pipeline_run_limit') or 1
    return dict(
        uuid=pipeline.uuid,
        name=pipeline.name or pipeline.uuid,
        description=pipeline.description or None,
        max_concurrent_runs=int(limit),
        variables=_json_safe(getattr(pipeline, 'variables', None) or {}),
        blocks=blocks,
        triggers=_triggers(pipeline, capture_.notes, problems),
    )


def _json_safe(value):
    return json.loads(json.dumps(value, default=str))


def _prepare_rust_block(block, project: Path, problems: List[str], where: str) -> Optional[str]:
    from mage_ai.data_preparation.models.block.rust import build as rust_build

    try:
        prepared = rust_build.prepare(
            Path(block.file_path).read_text(), block.type, block.uuid, str(project),
        )
    except Exception as error:
        problems.append(f'{where}: {error}')
        return None
    return prepared.crate


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
        for schedule in PipelineSchedule.query.filter(
            PipelineSchedule.pipeline_uuid == pipeline.uuid,
        ).all():
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
            f'Pipeline {pipeline.uuid}: the trigger database could not be read ({error}); '
            'only triggers.yaml is exported.'
        )

    triggers = []
    for trigger_name, trigger in sorted(by_name.items()):
        kind = trigger['schedule_type']
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


def _environment_reads(path: Path, project: Path) -> List[Tuple[str, bool, bool, str]]:
    """(name, required, secret, file) for each environment variable a file reads."""
    label = str(path.relative_to(project))
    text = path.read_text(errors='replace')
    reads = []
    if path.suffix in ('.yaml', '.yml'):
        reads += [(name, False, False) for name in ENV_VAR_TEMPLATE.findall(text)]
        reads += [(secret_variable_name(name), True, True)
                  for name in SECRET_VAR_TEMPLATE.findall(text)]
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


def _environment(capture_: Capture) -> List[Dict]:
    variables: Dict[str, Dict] = {}
    for path in sorted(capture_.files):
        if path.suffix not in ('.py', '.yaml', '.yml'):
            continue
        for name, required, secret, label in _environment_reads(path, capture_.project):
            if name.startswith('MAGE_SERVICE_'):
                continue
            entry = variables.setdefault(name, dict(
                name=name, secret=False, required=False, used_by=[],
            ))
            entry['required'] = entry['required'] or required
            entry['secret'] = entry['secret'] or secret or any(
                hint in name.upper() for hint in SECRET_HINTS
            )
            if label not in entry['used_by']:
                entry['used_by'].append(label)
    return [variables[name] for name in sorted(variables)]


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


def _local_install(dist) -> bool:
    """Installed from a local directory, so pip cannot fetch it by name and version."""
    try:
        direct = json.loads(dist.read_text('direct_url.json') or '{}')
    except (json.JSONDecodeError, OSError):
        return False
    return 'dir_info' in direct


def _file_owners() -> Dict[str, Tuple[str, str]]:
    owners: Dict[str, Tuple[str, str]] = {}
    for dist in importlib.metadata.distributions():
        name = (dist.metadata['Name'] or '').lower().replace('_', '-')
        if not name or _local_install(dist):
            continue
        for file in dist.files or []:
            text = str(file)
            # Editable installs load a finder module at start-up; it is not a dependency.
            if not text.endswith(('.py', '.so', '.pyd', '.pyc')) or '__editable__' in text:
                continue
            try:
                owners[os.path.realpath(dist.locate_file(file))] = (name, dist.version)
            except (OSError, ValueError):
                continue
    return owners


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
    candidates += ['pyarrow', 'pandas']
    if any(b['language'] == 'sql' for p in capture_.manifest['pipelines'] for b in p['blocks']):
        # What the SQL runner imports to render queries and use PostgreSQL.
        candidates += [
            'jinja2', 'inflection', 'psycopg2', 'mage_ai.io.config', 'mage_ai.io.postgres',
            'mage_ai.io.postgres_types', 'mage_ai.data_preparation.shared.utils',
            'mage_ai.data_preparation.templates.utils',
        ]
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
            os.path.realpath(module.__file__)
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
        owner = owners.get(path)
        if owner is None:
            continue
        name, version = owner
        if name not in EXCLUDED_DISTRIBUTIONS:
            pinned[name] = version
    for name in BASE_PACKAGES:
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


def write(capture_: Capture, out: str, force: bool = False) -> Path:
    out_dir = Path(out).resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        if not force and not (out_dir / 'service.json').exists():
            raise ExportError([
                f'{out_dir} is not empty and holds no earlier export; choose another directory '
                'or pass --force.'
            ])
        for child in out_dir.iterdir():
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
        for name in ('rproject.toml', 'rv.lock'):
            shutil.copy2(capture_.r_project / name, out_dir / 'r' / name)
    _copy_service_source(out_dir / 'build' / 'mage_service')
    (out_dir / 'Dockerfile').write_text(_dockerfile(capture_))
    (out_dir / '.dockerignore').write_text(
        '**/__pycache__\n**/target\ndata/\n.tekton/\ndeploy/\n.env\n.env.*\n'
    )
    (out_dir / 'compose.yaml').write_text(_compose(capture_))
    (out_dir / 'README.md').write_text(_readme(capture_))
    (out_dir / '.env.example').write_text(_env_example(capture_))
    _write_deploy_files(capture_, out_dir)
    (out_dir / 'report.txt').write_text(report(capture_))
    return out_dir


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
            problems.append(f'Model {name} ({uri}) could not be downloaded: {error}')
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
        for requirement in metadata.get('requirements') or []:
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
    if 'mlflow' not in extra and 'mlflow-skinny' not in capture_.requirements:
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
)


def _write_deploy_files(capture_: Capture, out_dir: Path) -> None:
    for template, destination in DEPLOY_FILES:
        text = string.Template((TEMPLATES / template).read_text()).safe_substitute(
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
    capture_.rust_locked = _sync_cargo_lock(capture_, target)


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


def _dockerfile(capture_: Capture) -> str:
    project = capture_.project.name
    rust = _rust_version()
    python_version = f'{sys.version_info.major}.{sys.version_info.minor}'
    rust_blocks = ''
    copy_blocks = ''
    if capture_.rust_blocks:
        crates = ' '.join(f'-p {crate}' for _, crate in capture_.rust_blocks)
        copies = ' && '.join(
            f'cp rust/target/release/{crate} /out/bin/{crate}' for _, crate in capture_.rust_blocks
        )
        locked = '--locked ' if capture_.rust_locked else ''
        rust_blocks = f'''
# The Rust blocks, compiled once here; the service runs the binaries.
COPY rust ./rust
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
    --mount=type=cache,target=/build/rust/target \\
    cargo build --release {locked}--manifest-path rust/Cargo.toml {crates} && \\
    mkdir -p /out/bin && {copies}
'''
        copy_blocks = 'COPY --from=build --chown=mage:mage /out/bin ./bin\n'
    r_install = ''
    r_prepare = ''
    if capture_.needs_r:
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
      libssl-dev libtiff-dev libuv1-dev libwebp-dev libxml2-dev make xz-utils zlib1g-dev && \\
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
        base = 'trixie' if capture_.needs_r else 'bookworm'
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
        runtime = 'FROM debian:bookworm-slim\n'
        python_env = ''
        copy_python = ''
    if capture_.models:
        copy_models = 'COPY --chown=mage:mage models ./models\n'
        models_env = ' \\\n    MAGE_SERVICE_MODELS_DIR=/srv/mage-service/models'
    else:
        copy_models = ''
        models_env = ''
    return f'''# syntax=docker/dockerfile:1.7
# {capture_.name}: pipelines exported from the Mage project {project}.
# docker build -t {capture_.name} .

FROM rust:{rust}-slim-bookworm AS build
WORKDIR /build
COPY build/mage_service ./mage_service
RUN --mount=type=cache,target=/usr/local/cargo/registry \\
    --mount=type=cache,target=/build/mage_service/target \\
    cargo build --release --locked --manifest-path mage_service/Cargo.toml -p mage-service && \\
    mkdir -p /out && cp mage_service/target/release/mage-service /out/mage-service
{rust_blocks}
{runtime}RUN groupadd --system --gid 10001 mage && \\
    useradd --system --uid 10001 --gid mage --home-dir /srv/mage-service --no-create-home mage && \\
    mkdir -p /srv/mage-service /var/lib/mage-service && \\
    chown mage:mage /var/lib/mage-service
WORKDIR /srv/mage-service
COPY --from=build /out/mage-service /usr/local/bin/mage-service
{copy_python}{copy_blocks}{copy_models}COPY --chown=mage:mage {project} ./{project}
COPY --chown=mage:mage service.json ./service.json
{r_prepare}
ENV MAGE_SERVICE_DIR=/srv/mage-service \\
    MAGE_SERVICE_DATA=/var/lib/mage-service \\
    MAGE_SERVICE_HOST=0.0.0.0 \\
    MAGE_SERVICE_PORT=8080{models_env}{python_env}
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
      # Every API call needs this token; set your own.
      MAGE_SERVICE_TOKEN: ${{MAGE_SERVICE_TOKEN:-change-me}}
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


def _readme(capture_: Capture) -> str:
    pipelines = capture_.manifest['pipelines']
    first = pipelines[0]['uuid']
    rows = '\n'.join(
        f"| `{p['uuid']}` | {len(p['blocks'])} | "
        + (', '.join(
            f"{t['name']} ({t['schedule'] or 'api'}{'' if t['active'] else ', inactive'})"
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
            lines.append(f"  {block['language']:<6} {block['type']:<13} {block['uuid']}{extra}")
        for trigger in pipeline['triggers']:
            lines.append(f"  trigger {trigger['name']}: {trigger['kind']} "
                         f"{trigger['schedule'] or ''}{'' if trigger['active'] else ' (inactive)'}")
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


def build_image(out_dir: Path, tag: str) -> None:
    docker = shutil.which('docker')
    if not docker:
        raise ExportError(['docker is not installed; build the image with the Dockerfile.'])
    result = subprocess.run([docker, 'build', '-t', tag, str(out_dir)])
    if result.returncode != 0:
        raise ExportError([f'docker build failed with exit code {result.returncode}.'])
