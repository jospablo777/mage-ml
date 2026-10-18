"""
The IBM Cloud Code Engine deploy adapter (B3), on the Code Engine API v2 with an IAM token
(IBM_CLOUD_API_KEY, or a trusted profile; see mage_ai.services.ibm.iam).

    mage-<service>-<env>                 the app: one instance, always on
    mage-<service>-<env>-env-<digest>    the variables and secrets, named by their content

Code Engine keeps only the latest inactive revision, so the app carries what a rollback
needs: MAGE_DEPLOY (commit, lock, values digest, reason) and MAGE_DEPLOY_PREVIOUS (the same
with the previous image and Secret). A deploy that does not become ready within the timeout
is patched back to the app as it was.

Code Engine apps have no persistent volume: the run history and the positions of sources
that keep them in the engine start empty when the instance is replaced.
"""
import json
import os
import shlex
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Dict, List, Optional, Tuple

from mage_ai.pipeline_services.deploy import (
    CONTAINER_PORT,
    DeployError,
    Deployment,
    names,
)
from mage_ai.pipeline_services.kubernetes import NAME, secret_name

API = 'https://api.{region}.codeengine.cloud.ibm.com/v2'
VISIBILITY = dict(public='local_public', private='local_private', project='local')
# Secrets kept besides the running and the previous deployment's.
SPARE_SECRETS = 3

Request = Callable[[str, str, Optional[Dict], Dict[str, str]], Tuple[int, Dict]]


def _http(method: str, url: str, body: Optional[Dict],
          headers: Dict[str, str]) -> Tuple[int, Dict]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            text = response.read()
            return response.status, (json.loads(text) if text else {})
    except urllib.error.HTTPError as error:
        text = error.read()
        try:
            return error.code, json.loads(text) if text else {}
        except ValueError:
            return error.code, dict(message=text.decode(errors='replace'))
    except OSError as error:
        raise DeployError(f'Code Engine did not answer: {error}') from None


def _token() -> str:
    from mage_ai.services.ibm.iam import IAMError, TokenManager

    environ = dict(os.environ)
    # The ibmcloud CLI's variable works too.
    if not environ.get('IBM_CLOUD_API_KEY') and environ.get('IBMCLOUD_API_KEY'):
        environ['IBM_CLOUD_API_KEY'] = environ['IBMCLOUD_API_KEY']
    try:
        return TokenManager.from_environment(environ).token()
    except IAMError as error:
        raise DeployError(f'{error} Set IBM_CLOUD_API_KEY.') from None


class CodeEngineTarget:
    def __init__(self, settings: Dict, environment: str = '', request: Optional[Request] = None,
                 token: Optional[Callable[[], str]] = None,
                 sleep: Optional[Callable[[float], None]] = None):
        missing = [key for key in ('region', 'project_id', 'image') if not settings.get(key)]
        if missing:
            raise DeployError(
                f'Set {", ".join(missing)} in environments/{environment or "<env>"}.yaml: '
                'Code Engine runs the image from a registry, in the project with that ID.'
            )
        visibility = settings.get('visibility', 'public')
        if visibility not in VISIBILITY:
            raise DeployError(f'visibility is one of {", ".join(VISIBILITY)}.')
        self.settings = settings
        self.region = settings['region']
        self.project_id = settings['project_id']
        self.visibility = VISIBILITY[visibility]
        self._request = request or _http
        self._token = token or _token
        self._sleep = sleep or time.sleep
        self._bearer: Optional[str] = None

    # API

    def call(self, method: str, path: str, body: Optional[Dict] = None,
             headers: Optional[Dict[str, str]] = None, ok=(200, 201, 202, 204)) -> Tuple[int, Dict]:
        if self._bearer is None:
            self._bearer = self._token()
        url = API.format(region=self.region) \
            + f'/projects/{urllib.parse.quote(self.project_id)}{path}'
        status, answer = self._request(method, url, body, {
            'Authorization': f'Bearer {self._bearer}', 'Accept': 'application/json',
            'Content-Type': 'application/json', **(headers or {}),
        })
        if ok and status not in ok:
            reason = answer.get('message') or answer.get('errors') or answer
            raise DeployError(f'Code Engine {method} {path} failed ({status}): {reason}')
        return status, answer

    def _app(self, name: str) -> Optional[Dict]:
        status, answer = self.call('GET', f'/apps/{name}', ok=(200, 404))
        return None if status == 404 else answer

    # Images

    def image(self, service: str, digest: str) -> str:
        return f'{self.settings["image"]}:{digest}'

    def publish(self, box, image: str, build_image: Callable) -> List[str]:
        # Code Engine runs amd64.
        build_image(box, image, platforms=self.settings.get('platforms') or ['linux/amd64'],
                    push=True)
        return []

    # Deploys

    def _environment(self, secret: str, current: Dict, previous: Optional[Dict]) -> List[Dict]:
        literals = dict(
            MAGE_SERVICE_HOST='0.0.0.0',
            MAGE_SERVICE_STRICT_ENV='on',
            MAGE_DEPLOY=json.dumps(current, sort_keys=True),
        )
        if previous:
            literals['MAGE_DEPLOY_PREVIOUS'] = json.dumps(previous, sort_keys=True)
        return [dict(type='secret_full_reference', reference=secret)] + [
            dict(type='literal', name=key, value=value) for key, value in literals.items()
        ]

    def _settings_digest(self) -> str:
        import hashlib

        keys = ('cpu', 'memory', 'image_secret', 'visibility')
        chosen = {k: str(self.settings.get(k) or '') for k in keys}
        return hashlib.sha256(json.dumps(chosen, sort_keys=True).encode()).hexdigest()[:12]

    def _spec(self, image: str, environment: List[Dict]) -> Dict:
        spec = dict(
            image_reference=image,
            image_port=CONTAINER_PORT,
            run_env_variables=environment,
            # Schedules and streams need the instance up; the run history allows one.
            scale_min_instances=1,
            scale_max_instances=1,
            managed_domain_mappings=self.visibility,
            probe_readiness=dict(type='http', path='/healthz', port=CONTAINER_PORT),
        )
        for key, field in (('cpu', 'scale_cpu_limit'), ('memory', 'scale_memory_limit'),
                           ('image_secret', 'image_secret')):
            if self.settings.get(key):
                spec[field] = str(self.settings[key])
        return spec

    def deploy(self, service: str, environment: str, image: str, env: Dict[str, str],
               annotations: Dict[str, str], timeout: float = 600) -> Deployment:
        name = names(service, environment)['current']
        if len(name) > 63 or not NAME.match(name):
            raise DeployError(f'{name} is not a Code Engine app name: lowercase letters, '
                              'digits and dashes, at most 63 characters.')
        secret = secret_name(name, env)
        status, _ = self.call('POST', '/secrets',
                              dict(name=secret, format='generic', data=env), ok=(201, 409))
        before = self._app(name)
        # The settings are part of what is recorded, so every change alters the variables
        # and makes a revision to wait for.
        current = dict(annotations, image=image, secret=secret,
                       settings=self._settings_digest())
        recorded = _recorded(before, 'MAGE_DEPLOY') if before else None
        if before is not None and recorded == current:
            return self.inspect(service, environment)
        previous = recorded
        spec = self._spec(image, self._environment(secret, current, previous))
        if before is None:
            self.call('POST', '/apps', dict(spec, name=name))
        else:
            self._patch(name, before, spec)
        if self._wait(name, before, timeout):
            self._prune_secrets(name, {secret, (previous or {}).get('secret')})
            return self.inspect(service, environment)
        failed = self._app(name) or {}
        reason = (failed.get('status_details') or {}).get('reason') or ''
        restored = False
        if before is not None:
            self._patch(name, failed or before, _restorable(before))
            restored = True
        raise DeployError(
            f'{name} did not become ready within {int(timeout)} seconds'
            + (f' ({reason})' if reason else '') + '; '
            + ('the app is patched back to the previous deployment.' if restored
               else 'it is left as it is: see `mage service logs --engine`.')
        )

    def _patch(self, name: str, app: Dict, spec: Dict) -> None:
        self.call('PATCH', f'/apps/{name}', spec, headers={
            'If-Match': app.get('entity_tag', '*'),
            'Content-Type': 'application/merge-patch+json',
        })

    def _wait(self, name: str, before: Optional[Dict], timeout: float) -> bool:
        """True when a new revision is ready; False on failure or timeout."""
        old = ((before or {}).get('status_details') or {}).get('latest_created_revision')
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            app = self._app(name) or {}
            details = app.get('status_details') or {}
            created = details.get('latest_created_revision')
            if created and created != old:
                if app.get('status') == 'ready' and details.get('latest_ready_revision') == created:
                    return True
                if app.get('status') == 'failed':
                    return False
            self._sleep(5)
        return False

    def _prune_secrets(self, app: str, keep: set) -> None:
        prefix = f'{app}-env-'
        _, answer = self.call('GET', '/secrets?limit=100')
        mine = sorted(
            (s for s in answer.get('secrets') or [] if s.get('name', '').startswith(prefix)),
            key=lambda s: s.get('created_at', ''), reverse=True,
        )
        spare = [s['name'] for s in mine if s['name'] not in keep][:SPARE_SECRETS]
        for item in mine:
            if item['name'] not in keep and item['name'] not in spare:
                self.call('DELETE', f'/secrets/{item["name"]}', ok=(200, 202, 204, 404))

    def rollback(self, service: str, environment: str, timeout: float = 600) -> Deployment:
        name = names(service, environment)['current']
        app = self._app(name)
        if app is None:
            raise DeployError(f'{service} is not deployed in {environment}.')
        previous = _recorded(app, 'MAGE_DEPLOY_PREVIOUS')
        if not previous:
            raise DeployError(f'{service} has no previous deployment in {environment}.')
        current = _recorded(app, 'MAGE_DEPLOY') or {}
        spec = self._spec(previous['image'],
                          self._environment(previous['secret'], previous, current))
        self._patch(name, app, spec)
        if not self._wait(name, app, timeout):
            raise DeployError(f'{name} did not become ready after the rollback within '
                              f'{int(timeout)} seconds.')
        return self.inspect(service, environment)

    def inspect(self, service: str, environment: str) -> Optional[Deployment]:
        app = self._app(names(service, environment)['current'])
        if app is None:
            return None
        state = dict(ready='running', deploying='updating', failed='failed',
                     warning='unavailable').get(app.get('status', ''), app.get('status', ''))
        recorded = _recorded(app, 'MAGE_DEPLOY') or {}
        return Deployment(
            service=service, environment=environment, state=state or 'unknown',
            image=app.get('image_reference', ''), commit=recorded.get('commit', ''),
            lock=recorded.get('lock', ''), reason=recorded.get('reason', ''),
            values=recorded.get('values', ''),
        )

    def current_token(self, service: str, environment: str) -> Optional[str]:
        app = self._app(names(service, environment)['current'])
        secret = (_recorded(app, 'MAGE_DEPLOY') or {}).get('secret') if app else None
        if not secret:
            return None
        status, answer = self.call('GET', f'/secrets/{secret}', ok=(200, 404))
        return (answer.get('data') or {}).get('MAGE_SERVICE_TOKEN') if status == 200 else None

    def api_url(self, service: str, environment: str) -> str:
        app = self._app(names(service, environment)['current'])
        if app is None:
            raise DeployError(f'{service} is not deployed in {environment}.')
        url = app.get('endpoint') or app.get('endpoint_internal')
        if not url:
            raise DeployError(f'{service} in {environment} has no endpoint yet.')
        return url.rstrip('/')

    def log_command(self, service: str, environment: str, tail: int = 200,
                    follow: bool = False) -> List[str]:
        """The ibmcloud CLI's app logs, logged in with the API key in a home of its own."""
        name = names(service, environment)['current']
        logs = ['ibmcloud', 'ce', 'app', 'logs', '--application', name, '--tail', str(tail),
                *(['--follow'] if follow else [])]
        script = (
            'export IBMCLOUD_HOME="$(mktemp -d)"; '
            'export IBMCLOUD_API_KEY="${IBMCLOUD_API_KEY:-$IBM_CLOUD_API_KEY}"; '
            f'ibmcloud login -r {shlex.quote(self.region)} -q >/dev/null && '
            f'ibmcloud ce project select --id {shlex.quote(self.project_id)} -q >/dev/null && '
            + ' '.join(shlex.quote(part) for part in logs)
        )
        return ['sh', '-c', script]


def _recorded(app: Optional[Dict], key: str) -> Optional[Dict]:
    for variable in (app or {}).get('run_env_variables') or []:
        if variable.get('type') == 'literal' and variable.get('name') == key:
            try:
                return json.loads(variable.get('value') or '')
            except ValueError:
                return None
    return None


def _restorable(app: Dict) -> Dict:
    """The fields of an app as it was that a patch puts back."""
    restored = dict(
        image_reference=app.get('image_reference'),
        run_env_variables=[
            {k: v[k] for k in ('type', 'name', 'value', 'reference', 'key', 'prefix')
             if v.get(k) is not None}
            for v in app.get('run_env_variables') or []
        ],
    )
    for field in ('image_secret', 'scale_cpu_limit', 'scale_memory_limit'):
        if app.get(field):
            restored[field] = app[field]
    return restored
