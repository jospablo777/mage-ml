"""
A project's pipelines as one zip, and its import into another project: each pipeline's
folder (metadata.yaml, triggers.yaml, README, images), its blocks' files and the data
contracts they name, with a manifest of every file's SHA-256. io_config.yaml, secrets and
outputs are never in it.

An import is planned first: each file is new, the same as the project's, or different. In
"skip" mode a pipeline the project has is left as it is, and the import is refused when a
new pipeline would change a block file the project already has, which other pipelines may
run. In "replace" mode every file of the archive is written.
"""
import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Optional

FORMAT = 1
MANIFEST = 'mageml-project.json'
# Folders an archive may write to, relative to the project.
ALLOWED_ROOTS = (
    'pipelines', 'data_loaders', 'transformers', 'data_exporters', 'custom', 'sensors',
    'conditionals', 'callbacks', 'charts', 'markdowns', 'scratchpads', 'dbt', 'extensions',
    'contracts',
)
# Uncompressed size and file count an archive may have: a bomb unpacks to far more.
MAX_BYTES = 200 * 1024 * 1024
MAX_FILES = 20000
MODES = ('skip', 'replace')


class ArchiveError(Exception):
    pass


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _relative(path: str, repo_path: str) -> Optional[str]:
    try:
        relative = Path(path).resolve().relative_to(Path(repo_path).resolve())
    except ValueError:
        return None
    return relative.as_posix()


def pipeline_files(pipeline) -> List[str]:
    """Paths, relative to the project, of the files a pipeline needs."""
    from mage_ai.data_preparation import contracts

    repo_path = pipeline.repo_path
    files = set()
    for root, _dirs, names in os.walk(pipeline.dir_path):
        if '__pycache__' in root or '/.' in root.replace(os.sep, '/'):
            continue
        for name in names:
            if name.endswith('.pyc'):
                continue
            relative = _relative(os.path.join(root, name), repo_path)
            if relative:
                files.add(relative)
    blocks = list(pipeline.blocks_by_uuid.values()) + list(
        getattr(pipeline, 'conditionals_by_uuid', {}).values()
    ) + list(getattr(pipeline, 'callbacks_by_uuid', {}).values())
    for block in blocks:
        path = getattr(block, 'file_path', None)
        if path and os.path.isfile(path):
            relative = _relative(path, repo_path)
            if relative:
                files.add(relative)
        try:
            binding = contracts.binding_of(block)
        except Exception:
            binding = None
        if binding is not None:
            path = contracts.contract_path(binding.name, repo_path)
            if path.is_file():
                files.add(_relative(str(path), repo_path))
    return sorted(f for f in files if f)


def export(repo_path: str, pipeline_uuids: Optional[Iterable[str]] = None) -> bytes:
    """The zip of the pipelines (all of the project's when none are named)."""
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.server.constants import VERSION

    uuids = list(pipeline_uuids or Pipeline.get_all_pipelines(repo_path))
    entries: Dict[str, bytes] = {}
    pipelines = []
    for uuid in sorted(uuids):
        pipeline = Pipeline.get(uuid, repo_path=repo_path, check_if_exists=True)
        if pipeline is None:
            raise ArchiveError(f'The project has no pipeline {uuid}.')
        needed = pipeline_files(pipeline)
        pipelines.append(dict(uuid=pipeline.uuid, name=pipeline.name,
                              type=str(getattr(pipeline.type, 'value', pipeline.type)),
                              files=needed))
        for relative in needed:
            with open(os.path.join(repo_path, relative), 'rb') as file:
                entries[relative] = file.read()

    manifest = dict(
        format=FORMAT,
        mageml_version=VERSION,
        created_at=datetime.now(timezone.utc).isoformat(),
        pipelines=pipelines,
        files={path: _sha256(content) for path, content in sorted(entries.items())},
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MANIFEST, json.dumps(manifest, indent=2))
        for path, content in sorted(entries.items()):
            archive.writestr(path, content)
    return buffer.getvalue()


def _read(content: bytes) -> Dict:
    """The manifest and files of an archive, checked."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        raise ArchiveError('The file is not a zip.')
    with archive:
        infos = [i for i in archive.infolist() if not i.is_dir()]
        if len(infos) > MAX_FILES or sum(i.file_size for i in infos) > MAX_BYTES:
            raise ArchiveError(
                f'The archive unpacks to more than {MAX_FILES} files or '
                f'{MAX_BYTES // (1024 * 1024)} MB.'
            )
        names = [i.filename for i in infos]
        if MANIFEST not in names:
            raise ArchiveError(f'The zip has no {MANIFEST}: it is not a MageML project export.')
        manifest = json.loads(archive.read(MANIFEST))
        if manifest.get('format') != FORMAT:
            raise ArchiveError(f'Unknown archive format {manifest.get("format")}.')
        files = {}
        for name in names:
            if name == MANIFEST or name.startswith('__MACOSX/'):
                continue
            path = PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts or path.parts[0] not in ALLOWED_ROOTS:
                raise ArchiveError(f'The archive writes outside the project folders: {name}.')
            data = archive.read(name)
            expected = (manifest.get('files') or {}).get(name)
            if expected and expected != _sha256(data):
                raise ArchiveError(f'{name} does not match its checksum in the manifest.')
            files[name] = data
    return dict(manifest=manifest, files=files)


def _pipeline_of(path: str) -> Optional[str]:
    parts = PurePosixPath(path).parts
    return parts[1] if parts[0] == 'pipelines' and len(parts) > 2 else None


def plan(repo_path: str, content: bytes, mode: str = 'skip') -> Dict:
    """What an import would do, file by file, without writing."""
    if mode not in MODES:
        raise ArchiveError(f'mode must be one of {", ".join(MODES)}.')
    read = _read(content)
    existing_pipelines = {
        p['uuid'] for p in read['manifest'].get('pipelines') or []
        if os.path.isdir(os.path.join(repo_path, 'pipelines', p['uuid']))
    }
    # The pipelines of the archive that need each file.
    needed_by: Dict[str, List[str]] = {}
    for entry in read['manifest'].get('pipelines') or []:
        for path in entry.get('files') or []:
            needed_by.setdefault(path, []).append(entry['uuid'])
    imported = {
        p['uuid'] for p in read['manifest'].get('pipelines') or []
        if mode == 'replace' or p['uuid'] not in existing_pipelines
    }
    files = []
    conflicts = []
    for path, data in sorted(read['files'].items()):
        target = os.path.join(repo_path, *PurePosixPath(path).parts)
        if not os.path.exists(target):
            status = 'new'
        else:
            with open(target, 'rb') as file:
                status = 'same' if file.read() == data else 'different'
        users = needed_by.get(path) or ([_pipeline_of(path)] if _pipeline_of(path) else [])
        needed = any(uuid in imported for uuid in users) or not users
        if status == 'same' or not needed:
            action = 'keep'
        elif mode == 'skip' and status == 'different':
            # A block or contract file the project has, which its pipelines may run.
            conflicts.append(path)
            action = 'conflict'
        else:
            action = 'write'
        files.append(dict(path=path, status=status, action=action, pipelines=users))
    return dict(
        mode=mode,
        mageml_version=read['manifest'].get('mageml_version'),
        pipelines=[
            dict(p, exists=p['uuid'] in existing_pipelines,
                 action='skip' if mode == 'skip' and p['uuid'] in existing_pipelines
                 else ('replace' if p['uuid'] in existing_pipelines else 'create'))
            for p in read['manifest'].get('pipelines') or []
        ],
        files=files,
        conflicts=conflicts,
    )


def import_archive(repo_path: str, content: bytes, mode: str = 'skip') -> Dict:
    """Writes the archive's files as plan() says; refused when the plan has conflicts."""
    planned = plan(repo_path, content, mode)
    if planned['conflicts']:
        raise ArchiveError(
            'The project has different versions of these files, which other pipelines may '
            'use: ' + ', '.join(planned['conflicts']) + '. Import with mode replace to '
            'overwrite them.'
        )
    files = _read(content)['files']
    written = []
    for entry in planned['files']:
        if entry['action'] != 'write':
            continue
        target = Path(repo_path, *PurePosixPath(entry['path']).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(files[entry['path']])
        written.append(entry['path'])
    return dict(planned, written=written)
