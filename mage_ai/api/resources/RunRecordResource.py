import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path
from mage_ai.shared.processes import (
    HAS_PROCESS_GROUPS,
    stop_process_group,
    supervised_command,
)


@dataclass
class _Reproduction:
    pipeline_run_id: int
    process: subprocess.Popen
    directory: str
    started_at: float
    finished_at: Optional[float] = None
    stopped: bool = False

    @property
    def log_path(self) -> str:
        return os.path.join(self.directory, 'log.txt')

    @property
    def report_path(self) -> str:
        return os.path.join(self.directory, 'report.json')


_lock = threading.Lock()
_reproductions: Dict[int, _Reproduction] = {}


def reproduce_command(repo_path: str, pipeline_uuid: str, run_id: int, report: str) -> List[str]:
    return [
        sys.executable, '-m', 'mage_ai.cli.main', 'reproduce',
        repo_path, pipeline_uuid, str(run_id), '--yes', '--report', report,
    ]


def _run(pk):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    try:
        run = PipelineRun.get(int(pk))
    except (TypeError, ValueError):
        run = None
    if run is None:
        raise ApiError(dict(code=404, message='The pipeline run does not exist.'))
    return run


def _pipeline(run, repo_path: str):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(run.pipeline_uuid, repo_path=repo_path, check_if_exists=True)
    if pipeline is None:
        raise ApiError(dict(code=404, message='The pipeline does not exist.'))
    return pipeline


def _reproduction(reproduction: Optional[_Reproduction]) -> Optional[Dict]:
    if reproduction is None:
        return None
    code = reproduction.process.poll()
    if code is not None and reproduction.finished_at is None:
        reproduction.finished_at = time.time()
    try:
        with open(reproduction.log_path, encoding='utf-8', errors='replace') as file:
            log = file.read()
    except FileNotFoundError:
        log = ''
    report = None
    if code is not None and os.path.exists(reproduction.report_path):
        with open(reproduction.report_path, encoding='utf-8') as file:
            report = json.load(file)
    if code is None:
        status = 'running'
    elif reproduction.stopped:
        status = 'stopped'
    elif report is None:
        status = 'error'
    else:
        status = 'passed' if report.get('passed') else 'failed'
    return dict(
        finished_at=reproduction.finished_at,
        log=log,
        report=report,
        started_at=reproduction.started_at,
        status=status,
    )


class RunRecordResource(GenericResource):
    """
    The run record of a pipeline run (orchestration/run_records.py):
    GET /api/run_records/<pipeline run id>[?compare_with=<id>] returns what the run ran
    with, its outputs' digests and, with compare_with, what differs from another run;
    POST /api/run_records starts `mage reproduce` for a run, whose log and result the GET
    returns; DELETE stops it.
    """

    @classmethod
    def member(cls, pk, user, **kwargs) -> 'RunRecordResource':
        from mage_ai.orchestration import run_records

        run = _run(pk)
        pipeline = _pipeline(run, get_repo_path(user=user))
        model = dict(
            id=run.id,
            pipeline_run_id=run.id,
            pipeline_uuid=run.pipeline_uuid,
            reproduction=_reproduction(_reproductions.get(run.id)),
            outputs={
                b.block_uuid: (b.metrics or {}).get('outputs') for b in run.block_runs
            },
        )
        try:
            recorded = run_records.manifests(pipeline, run.id)
        except Exception as error:
            raise ApiError(dict(code=500, message=f'Reading the run record failed: {error}'))
        if recorded:
            latest = recorded[-1]
            environment = run_records.environment_document(
                pipeline, latest['environment']['digest'],
            )
            model.update(
                captures=len(recorded),
                captured_at=latest['captured_at'],
                code=dict(
                    digest=latest['code']['digest'],
                    files=len(latest['code']['files']),
                    truncated=latest['code']['truncated'],
                ),
                code_changed_between_starts=len(
                    {m['code']['digest'] for m in recorded}
                ) > 1,
                environment=dict(
                    latest['environment'],
                    packages=len(environment.get('packages') or {}),
                ),
                git=latest.get('git'),
                variables=latest['variables']['names'],
            )
        query = kwargs.get('query') or {}
        compare_with = query.get('compare_with')
        if isinstance(compare_with, list):
            compare_with = compare_with[0] if compare_with else None
        if compare_with:
            other = _run(compare_with)
            if other.pipeline_uuid != run.pipeline_uuid:
                raise ApiError(dict(
                    code=400, message='Runs of the same pipeline compare; this one is not.',
                ))
            try:
                model['comparison'] = run_records.compare(pipeline, other, run)
            except run_records.RunRecordError as error:
                model['comparison_error'] = str(error)
        return cls(model, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'RunRecordResource':
        repo_path = get_repo_path(user=user)
        run = _run(payload.get('pipeline_run_id'))
        pipeline = _pipeline(run, repo_path)
        with _lock:
            current = _reproductions.get(run.id)
            if current is not None and current.process.poll() is None:
                raise ApiError(dict(
                    code=409, message='A reproduction of this run is already running.',
                ))
            directory = tempfile.mkdtemp(prefix='mage_reproduce_api_')
            log = open(os.path.join(directory, 'log.txt'), 'w', encoding='utf-8')
            process = subprocess.Popen(
                supervised_command(reproduce_command(
                    repo_path, pipeline.uuid, run.id, os.path.join(directory, 'report.json'),
                )),
                env={**os.environ, 'PYTHONUNBUFFERED': '1', 'NO_COLOR': '1'},
                start_new_session=HAS_PROCESS_GROUPS,
                stderr=subprocess.STDOUT,
                stdout=log,
            )
            log.close()
            _reproductions[run.id] = _Reproduction(
                directory=directory,
                pipeline_run_id=run.id,
                process=process,
                started_at=time.time(),
            )
        return cls.member(run.id, user, **kwargs)

    async def delete(self, **kwargs):
        reproduction = _reproductions.get(self.model.get('pipeline_run_id'))
        if reproduction is not None and reproduction.process.poll() is None:
            reproduction.stopped = True
            stop_process_group(reproduction.process)
