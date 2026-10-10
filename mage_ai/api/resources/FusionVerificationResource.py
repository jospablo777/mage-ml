import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
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
class _Verification:
    pipeline_uuid: str
    process: subprocess.Popen
    directory: str
    started_at: float
    finished_at: Optional[float] = None
    exact: bool = False
    stopped: bool = False
    extra: Dict = field(default_factory=dict)

    @property
    def log_path(self) -> str:
        return os.path.join(self.directory, 'log.txt')

    @property
    def report_path(self) -> str:
        return os.path.join(self.directory, 'report.json')


_lock = threading.Lock()
_verifications: Dict[str, _Verification] = {}


def verification_command(
    repo_path: str, pipeline_uuid: str, report_path: str, variables: Dict, exact: bool,
) -> List[str]:
    command = [
        sys.executable, '-m', 'mage_ai.cli.main', 'verify-fusion',
        repo_path, pipeline_uuid, '--yes', '--report', report_path,
    ]
    if variables:
        command += ['--runtime-vars', json.dumps(variables)]
    if exact:
        command.append('--exact')
    return command


class FusionVerificationResource(GenericResource):
    """
    Verify fusion from the pipeline settings: POST /api/fusion_verifications starts
    `mage verify-fusion` for a pipeline in its own process (it replaces the job manager,
    so it cannot run inside the server), GET /api/fusion_verifications/<pipeline uuid>
    returns its log and, when it ends, the report; DELETE stops it.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'FusionVerificationResource':
        from mage_ai.data_preparation.models.pipeline import Pipeline

        pipeline_uuid = payload.get('pipeline_uuid')
        repo_path = get_repo_path(user=user)
        if not isinstance(pipeline_uuid, str) or not Pipeline.get(
            pipeline_uuid, repo_path=repo_path, check_if_exists=True,
        ):
            raise ApiError(dict(code=404, message='The pipeline does not exist.'))
        variables = payload.get('variables') or {}
        if not isinstance(variables, dict):
            raise ApiError(dict(code=400, message='variables must be an object.'))
        with _lock:
            current = _verifications.get(pipeline_uuid)
            if current is not None and current.process.poll() is None:
                raise ApiError(dict(
                    code=409, message='A verification of this pipeline is already running.',
                ))
            directory = tempfile.mkdtemp(prefix='mage_verify_fusion_')
            exact = bool(payload.get('exact'))
            command = verification_command(
                repo_path, pipeline_uuid, os.path.join(directory, 'report.json'), variables, exact,
            )
            log = open(os.path.join(directory, 'log.txt'), 'w', encoding='utf-8')
            process = subprocess.Popen(
                supervised_command(command),
                cwd=os.path.dirname(repo_path),
                env={**os.environ, 'PYTHONUNBUFFERED': '1', 'NO_COLOR': '1'},
                start_new_session=HAS_PROCESS_GROUPS,
                stderr=subprocess.STDOUT,
                stdout=log,
            )
            log.close()
            verification = _Verification(
                directory=directory,
                exact=exact,
                pipeline_uuid=pipeline_uuid,
                process=process,
                started_at=time.time(),
            )
            _verifications[pipeline_uuid] = verification
        return cls(cls._present(verification), user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'FusionVerificationResource':
        verification = _verifications.get(pk)
        if verification is None:
            return cls(dict(id=pk, pipeline_uuid=pk, status='none'), user, **kwargs)
        return cls(cls._present(verification), user, **kwargs)

    async def delete(self, **kwargs):
        verification = _verifications.get(self.model.get('pipeline_uuid'))
        if verification is not None and verification.process.poll() is None:
            verification.stopped = True
            stop_process_group(verification.process)

    @classmethod
    def _present(cls, verification: _Verification) -> Dict:
        code = verification.process.poll()
        if code is not None and verification.finished_at is None:
            verification.finished_at = time.time()
        try:
            with open(verification.log_path, encoding='utf-8', errors='replace') as file:
                log = file.read()
        except FileNotFoundError:
            log = ''
        report = None
        if code is not None and os.path.exists(verification.report_path):
            with open(verification.report_path, encoding='utf-8') as file:
                report = json.load(file)
        if code is None:
            status = 'running'
        elif verification.stopped:
            status = 'stopped'
        elif report is None:
            status = 'error'
        else:
            status = 'passed' if report.get('passed') else 'failed'
        return dict(
            exact=verification.exact,
            finished_at=verification.finished_at,
            id=verification.pipeline_uuid,
            log=log,
            pipeline_uuid=verification.pipeline_uuid,
            report=report,
            started_at=verification.started_at,
            status=status,
        )
