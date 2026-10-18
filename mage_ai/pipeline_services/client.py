"""
The API of a running service, for `mage service runs` and `mage service logs`.

Without an environment the service is the local `mage service run` (its port and token in
.mage/run/<name>/); with one, it is the environment's deployment (the port from its Docker
labels, or a kubectl port-forward; the token in .mage/deploy/<name>/<env>/token).
"""
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from mage_ai.pipeline_services import deploy, local_run


class ClientError(RuntimeError):
    pass


@dataclass
class Endpoint:
    url: str
    token: str
    # A port-forward to stop once done.
    process: Optional[object] = field(default=None, compare=False, repr=False)

    def close(self) -> None:
        if self.process is not None:
            self.process.terminate()
            self.process.wait(timeout=10)
            self.process = None

    def get(self, path: str, params: Optional[Dict] = None, timeout: float = 30,
            body: Optional[Dict] = None) -> bytes:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items()
                                        if v is not None})
        headers = {'Authorization': f'Bearer {self.token}'}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request(
            f'{self.url}{path}' + (f'?{query}' if query else ''), headers=headers,
            data=None if body is None else json.dumps(body).encode(),
            method='GET' if body is None else 'POST',
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
    target = target or deploy.DockerTarget()
    token_file = deploy.token_file(repo_path, name, environment)
    if hasattr(target, 'api_url'):
        if not token_file.is_file():
            raise ClientError(f'No API token for {name} in {environment} ({token_file}).')
        return Endpoint(target.api_url(name, environment), token_file.read_text().strip())
    if hasattr(target, 'port_forward'):
        if not token_file.is_file():
            raise ClientError(f'No API token for {name} in {environment} ({token_file}).')
        port, process = target.port_forward(name, environment)
        return Endpoint(f'http://127.0.0.1:{port}', token_file.read_text().strip(), process)
    deployment = target.inspect(name, environment)
    if deployment is None or deployment.port is None:
        raise ClientError(f'{name} is not deployed in {environment}.')
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


def call(api: Endpoint, pipeline: Optional[str] = None, variables: Optional[Dict] = None,
         wait: int = 120) -> Dict:
    """Starts a run and waits for it; the pipeline may be left out when there is one."""
    if pipeline is None:
        names = [p['id'] if isinstance(p, dict) else p
                 for p in api.json('/v1/pipelines').get('pipelines') or []]
        if len(names) != 1:
            raise ClientError(f'Name the pipeline: the service has {", ".join(names)}.')
        pipeline = names[0]
    path = f'/v1/pipelines/{urllib.parse.quote(pipeline, safe="")}/runs'
    return json.loads(api.get(path, dict(wait=wait), timeout=wait + 30,
                              body=dict(variables=variables or {})))


def result(api: Endpoint, run: Dict, block: Optional[str] = None):
    """The output of the block named, or of the last block that has one, as JSON."""
    with_outputs = [b['block'] for b in run.get('blocks') or [] if b.get('outputs')]
    if block is None:
        if not with_outputs:
            return None
        block = with_outputs[-1]
    path = f'/v1/runs/{urllib.parse.quote(run["id"], safe="")}/output'
    return block, json.loads(api.get(path, dict(block=block, format='json')))


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
