"""
Deploying a service to a target (ADR-0023), with the Docker adapter first (B3).

A deploy builds the service's image, starts it as the environment's container with labels
that record the commit, the lock and the image, and waits for /healthz. When the new
container does not become healthy, it is removed and the previous one comes back: the
platform holds the truth about what runs, so status and diff read the labels, and CI never
commits back. The previous container is kept, stopped, for rollback.

    mage-<service>-<env>            the running deployment
    mage-<service>-<env>-previous   the one before, stopped
    mage-<service>-<env>-data       the volume with run history and stream positions
"""
import json
import os
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

LABEL = 'ai.mageml'
# Targets whose deployments status asks for remotely.
REMOTE_TARGETS = ('kubernetes', 'code_engine')
CONTAINER_PORT = 8080
DATA_PATH = '/var/lib/mage-service'


class DeployError(RuntimeError):
    pass


@dataclass
class Deployment:
    service: str
    environment: str
    state: str
    image: str = ''
    commit: str = ''
    lock: str = ''
    port: Optional[int] = None
    # Why uncommitted changes were deployed (--allow-uncommitted).
    reason: str = ''
    # values_digest of the environment's values.
    values: str = ''
    # The environment whose image a promotion deployed (deploy --from).
    promoted_from: str = ''

    def drift(self, lock: Optional[str]) -> str:
        if self.state != 'running':
            return self.state
        if not lock or not self.lock:
            return 'running, version unknown'
        return 'running the repository version' if lock == self.lock else 'behind the repository'


Runner = Callable[[List[str]], subprocess.CompletedProcess]


def _docker(arguments: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(['docker', *arguments], capture_output=True, text=True)


def names(service: str, environment: str) -> Dict[str, str]:
    base = f'mage-{service}-{environment}'
    return dict(current=base, previous=f'{base}-previous', volume=f'{base}-data')


class DockerTarget:
    def __init__(self, runner: Runner = _docker, health: Optional[Callable] = None):
        self.runner = runner
        self.health = health or _healthy

    def _run(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess:
        result = self.runner(list(arguments))
        if check and result.returncode != 0:
            raise DeployError(
                f'docker {" ".join(arguments[:2])} failed: {(result.stderr or "").strip()}'
            )
        return result

    def _exists(self, name: str) -> bool:
        return self._run('container', 'inspect', name, check=False).returncode == 0

    def deploy(
        self,
        service: str,
        environment: str,
        image: str,
        port: int,
        env: Dict[str, str],
        labels: Dict[str, str],
        timeout: float = 120,
    ) -> Deployment:
        n = names(service, environment)
        if self._exists(n['previous']):
            self._run('rm', '-f', n['previous'])
        had_current = self._exists(n['current'])
        if had_current:
            self._run('stop', n['current'])
            self._run('rename', n['current'], n['previous'])

        all_labels = {
            f'{LABEL}.service': service,
            f'{LABEL}.environment': environment,
            f'{LABEL}.image': image,
            f'{LABEL}.port': str(port),
            **{f'{LABEL}.{k}': v for k, v in labels.items()},
        }
        descriptor, env_file = tempfile.mkstemp(prefix='mage-deploy-', suffix='.env')
        try:
            with os.fdopen(descriptor, 'w') as file:
                for key, value in env.items():
                    file.write(f'{key}={value}\n')
            arguments = [
                'run', '-d', '--name', n['current'], '--restart', 'unless-stopped',
                '-p', f'127.0.0.1:{port}:{CONTAINER_PORT}',
                '-v', f'{n["volume"]}:{DATA_PATH}', '--env-file', env_file,
            ]
            for key, value in all_labels.items():
                arguments += ['--label', f'{key}={value}']
            self._run(*arguments, image)
        except DeployError:
            self._restore(n, had_current)
            raise
        finally:
            os.unlink(env_file)

        if self.health(port, timeout):
            return self.inspect(service, environment)
        logs = self._run('logs', '--tail', '40', n['current'], check=False)
        self._run('rm', '-f', n['current'], check=False)
        restored = self._restore(n, had_current)
        raise DeployError(
            f'{n["current"]} did not become healthy within {int(timeout)} seconds; '
            + ('the previous deployment runs again.' if restored else 'it was removed.')
            + f'\n{(logs.stdout or "") + (logs.stderr or "")}'.rstrip()
        )

    def _restore(self, n: Dict[str, str], had_current: bool) -> bool:
        if not had_current or not self._exists(n['previous']):
            return False
        if self._exists(n['current']):
            self._run('rm', '-f', n['current'], check=False)
        self._run('rename', n['previous'], n['current'])
        self._run('start', n['current'])
        return True

    def rollback(self, service: str, environment: str) -> Deployment:
        n = names(service, environment)
        if not self._exists(n['previous']):
            raise DeployError(f'{service} has no previous deployment in {environment}.')
        swap = f'{n["current"]}-swap'
        if self._exists(n['current']):
            self._run('stop', n['current'])
            self._run('rename', n['current'], swap)
        self._run('rename', n['previous'], n['current'])
        self._run('start', n['current'])
        if self._exists(swap):
            self._run('rename', swap, n['previous'])
        return self.inspect(service, environment)

    def current_token(self, service: str, environment: str) -> Optional[str]:
        """The API token the deployment runs with, so a deploy from elsewhere keeps it."""
        result = self._run('container', 'inspect', names(service, environment)['current'],
                           check=False)
        if result.returncode != 0:
            return None
        for item in (json.loads(result.stdout)[0].get('Config') or {}).get('Env') or []:
            key, _, value = item.partition('=')
            if key == 'MAGE_SERVICE_TOKEN' and value:
                return value
        return None

    def log_command(self, service: str, environment: str, tail: int = 200,
                    follow: bool = False) -> List[str]:
        """The docker command that prints the engine's own log (start-up, schedules, streams)."""
        current = names(service, environment)['current']
        if not self._exists(current):
            raise DeployError(f'{service} is not deployed in {environment}.')
        return ['docker', 'logs', '--tail', str(tail), *(['--follow'] if follow else []),
                current]

    def inspect(self, service: str, environment: str) -> Optional[Deployment]:
        result = self._run('container', 'inspect', names(service, environment)['current'],
                           check=False)
        if result.returncode != 0:
            return None
        return _deployment(json.loads(result.stdout)[0])

    def status(self, service: Optional[str] = None) -> List[Deployment]:
        label = f'{LABEL}.service' + (f'={service}' if service else '')
        result = self._run('ps', '-a', '--filter', f'label={label}', '--format', '{{.ID}}')
        ids = result.stdout.split()
        if not ids:
            return []
        details = json.loads(self._run('container', 'inspect', *ids).stdout)
        found = [
            _deployment(item) for item in details
            if not item.get('Name', '').endswith('-previous')
        ]
        return sorted(found, key=lambda d: (d.service, d.environment))


def _deployment(item: Dict) -> Deployment:
    labels = (item.get('Config') or {}).get('Labels') or {}
    port = labels.get(f'{LABEL}.port')
    return Deployment(
        service=labels.get(f'{LABEL}.service', ''),
        environment=labels.get(f'{LABEL}.environment', ''),
        state=(item.get('State') or {}).get('Status', 'unknown'),
        image=labels.get(f'{LABEL}.image', ''),
        commit=labels.get(f'{LABEL}.commit', ''),
        lock=labels.get(f'{LABEL}.lock', ''),
        port=int(port) if port and port.isdigit() else None,
        reason=labels.get(f'{LABEL}.reason', ''),
        values=labels.get(f'{LABEL}.values', ''),
        promoted_from=labels.get(f'{LABEL}.promoted_from', ''),
    )


def _healthy(port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/healthz', timeout=5) as r:
                if r.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(1)
    return False


def copy_image(source: str, destination: str, push: bool, runner: Runner = _docker) -> None:
    """Tags the image another environment runs for this one, pulling and pushing as needed."""
    def run(*arguments):
        result = runner(list(arguments))
        if result.returncode != 0:
            raise DeployError(f'docker {arguments[0]} failed: {(result.stderr or "").strip()}')

    if runner(['image', 'inspect', source]).returncode != 0:
        run('pull', source)
    run('tag', source, destination)
    if push:
        run('push', destination)


def deployments(
    repo_path: str,
    service: Optional[str] = None,
    docker: Optional['DockerTarget'] = None,
    kubernetes_runner=None,
    request_timeout: str = '30s',
) -> Tuple[List[Deployment], List[str]]:
    """
    Every deployment of the project's services, or of one: Docker's from their labels, and
    each Kubernetes and Code Engine environment's, asked in parallel. Returns them and what
    could not be read.
    """
    import shutil
    from concurrent.futures import ThreadPoolExecutor

    import yaml

    from mage_ai.pipeline_services import definitions, kubernetes

    found: List[Deployment] = []
    problems: List[str] = []
    if docker is not None or shutil.which('docker'):
        try:
            found += (docker or DockerTarget()).status(service)
        except (DeployError, OSError, ValueError) as error:
            problems.append(f'Docker: {error}')
    clusters = []
    for name in [service] if service else definitions.names(repo_path):
        folder = definitions.service_path(repo_path, name) / definitions.ENVIRONMENTS_DIR
        for path in sorted(folder.glob('*.yaml')) if folder.is_dir() else []:
            try:
                settings = yaml.safe_load(path.read_text()) or {}
            except yaml.YAMLError:
                continue
            if isinstance(settings, dict) and settings.get('target') in REMOTE_TARGETS:
                clusters.append((name, path.stem, settings))

    def inspect(item):
        name, environment, settings = item
        if settings['target'] == 'code_engine':
            from mage_ai.pipeline_services import code_engine

            return code_engine.CodeEngineTarget(settings, environment=environment) \
                .inspect(name, environment)
        options = dict(environment=environment, request_timeout=request_timeout)
        if kubernetes_runner is not None:
            options['runner'] = kubernetes_runner
        return kubernetes.KubernetesTarget(settings, **options).inspect(name, environment)

    with ThreadPoolExecutor(max_workers=max(1, min(8, len(clusters)))) as pool:
        futures = [(item, pool.submit(inspect, item)) for item in clusters]
        for (name, environment, _), future in futures:
            try:
                deployment = future.result()
            except (DeployError, OSError, ValueError) as error:
                problems.append(f'{name} {environment}: {error}')
                continue
            if deployment is not None:
                found.append(deployment)
    return sorted(found, key=lambda d: (d.service, d.environment)), problems


def git_commit(repo_path: str) -> str:
    """HEAD's commit, with +dirty when the project has uncommitted changes."""
    def git(*args) -> str:
        return subprocess.run(['git', *args], cwd=repo_path, capture_output=True,
                              text=True).stdout.strip()

    commit = git('rev-parse', 'HEAD')
    if not commit:
        return ''
    return commit + ('+dirty' if git('status', '--porcelain', '--', '.') else '')


def token_file(repo_path: str, service: str, environment: str) -> Path:
    root = Path(repo_path) / '.mage' / 'deploy'
    # The tokens stay out of Git whatever the project's .gitignore says.
    ignore = root / '.gitignore'
    if not ignore.is_file():
        root.mkdir(parents=True, exist_ok=True)
        ignore.write_text('*\n')
    return root / service / environment / 'token'


def values_digest(values: Dict[str, str]) -> str:
    """
    What the environment's values were, to tell a deploy that changes nothing. It covers the
    random API token, so it says nothing about the other values.
    """
    import hashlib

    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()[:16]
