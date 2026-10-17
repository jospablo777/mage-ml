"""
The API of a running service, for `mage service runs` and `mage service logs`.

Without an environment the service is the local `mage service run` (its port and token in
.mage/run/<name>/); with one, it is the environment's deployment (the port from its labels,
the token in .mage/deploy/<name>/<env>/token).
"""
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional

from mage_ai.pipeline_services import deploy, local_run


class ClientError(RuntimeError):
    pass


@dataclass
class Endpoint:
    url: str
    token: str

    def get(self, path: str, params: Optional[Dict] = None, timeout: float = 30) -> bytes:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items()
                                        if v is not None})
        request = urllib.request.Request(
            f'{self.url}{path}' + (f'?{query}' if query else ''),
            headers={'Authorization': f'Bearer {self.token}'},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            raise ClientError(f'{error.code}: {_api_message(error.read())}') from None
        except OSError as error:
            raise ClientError(f'No service answers on {self.url} ({error}).') from None

    def json(self, path: str, params: Optional[Dict] = None):
        return json.loads(self.get(path, params))


def _api_message(body: bytes) -> str:
    try:
        return str(json.loads(body).get('error') or body.decode(errors='replace'))
    except (ValueError, AttributeError):
        return body.decode(errors='replace').strip()


def endpoint(repo_path: str, name: str, environment: Optional[str] = None,
             target: Optional[deploy.DockerTarget] = None) -> Endpoint:
    if environment is None:
        folder = local_run.run_dir(repo_path, name)
        port_file, token_file = folder / 'port', folder / 'token'
        if not port_file.is_file() or not token_file.is_file():
            raise ClientError(
                f'{name} has not run here: start it with `mage service run {name}`, or name '
                'a deployed environment with --env.'
            )
        port = port_file.read_text().strip()
        return Endpoint(f'http://127.0.0.1:{port}', token_file.read_text().strip())
    deployment = (target or deploy.DockerTarget()).inspect(name, environment)
    if deployment is None or deployment.port is None:
        raise ClientError(f'{name} is not deployed in {environment}.')
    token_file = deploy.token_file(repo_path, name, environment)
    if not token_file.is_file():
        raise ClientError(f'No API token for {name} in {environment} ({token_file}).')
    return Endpoint(f'http://127.0.0.1:{deployment.port}', token_file.read_text().strip())


def runs(api: Endpoint, pipeline: Optional[str] = None, status: Optional[str] = None,
         limit: int = 20) -> List[Dict]:
    return api.json('/v1/runs', dict(pipeline=pipeline, status=status, limit=limit))['runs']


def run_log(api: Endpoint, run_id: Optional[str] = None, block: Optional[str] = None,
            tail: Optional[int] = None) -> str:
    """A run's log as text; the latest run's without an id."""
    if run_id is None:
        latest = runs(api, limit=1)
        if not latest:
            raise ClientError('The service has no runs yet.')
        run_id = latest[0]['id']
    path = f'/v1/runs/{urllib.parse.quote(run_id, safe="")}/logs'
    return api.get(path, dict(format='text', block=block, tail=tail)).decode()


def runs_table(items: List[Dict]) -> str:
    rows = [('RUN', 'PIPELINE', 'STATUS', 'SOURCE', 'CREATED', 'DURATION', 'ERROR')]
    for run in items:
        duration = run.get('duration_ms')
        error = (run.get('error') or '').splitlines()
        rows.append((
            run['id'], run['pipeline'], run['status'], run.get('source') or '',
            run.get('created_at') or '',
            f'{duration / 1000:.1f} s' if duration is not None else '',
            (error[0][:80] if error else ''),
        ))
    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    return '\n'.join(
        '  '.join(str(cell).ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    )
