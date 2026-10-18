"""
The Kubernetes deploy adapter (B3): kubectl applies the service's objects in the
environment's context and namespace, waits for the rollout, and undoes it when it fails.

    mage-<service>-<env>                 Deployment and Service
    mage-<service>-<env>-data            the volume with run history and stream positions
    mage-<service>-<env>-env-<digest>    the variables and secrets, named by their content

The Secret's name changes with its content, so `kubectl rollout undo` brings back the
variables of the previous revision with its image; a Secret edited in place would keep the
values that broke it. The commit, the lock and the image are annotations of the pod template
for the same reason: an undo restores them, so status always describes what runs.
"""
import hashlib
import json
import re
import subprocess
from typing import Callable, Dict, List, Optional

from mage_ai.pipeline_services.deploy import (
    CONTAINER_PORT,
    DATA_PATH,
    DeployError,
    Deployment,
    names,
)

GROUP = 'ai.mageml'
# What the Deployment keeps for rollout undo; Secrets of older revisions are deleted.
REVISIONS = 10
NAME = re.compile(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')

Runner = Callable[[List[str], Optional[str]], subprocess.CompletedProcess]


def _kubectl(arguments: List[str], input: Optional[str] = None) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(['kubectl', *arguments], input=input, capture_output=True,
                              text=True)
    except FileNotFoundError:
        raise DeployError('kubectl is not installed; Kubernetes targets use it.') from None


class KubernetesTarget:
    def __init__(self, settings: Dict, runner: Runner = _kubectl, environment: str = '',
                 request_timeout: str = '30s'):
        self.context = settings.get('context')
        if not self.context:
            raise DeployError(
                f'Name the kubectl context in environments/{environment or "<env>"}.yaml '
                '(context: <name>), so a deploy never goes to whichever cluster is current.'
            )
        self.namespace = settings.get('namespace') or 'default'
        self.settings = settings
        self.runner = runner
        self.request_timeout = request_timeout
        replicas = settings.get('replicas', 1)
        if replicas != 1:
            raise DeployError(
                f'replicas is {replicas}: a service runs one replica, since its run history '
                'is a SQLite file on a ReadWriteOnce volume.'
            )

    # kubectl

    def _run(self, *arguments: str, input: Optional[str] = None,
             check: bool = True) -> subprocess.CompletedProcess:
        result = self.runner(
            ['--context', self.context, '--namespace', self.namespace,
             '--request-timeout', self.request_timeout, *arguments],
            input,
        )
        if check and result.returncode != 0:
            raise DeployError(
                f'kubectl {" ".join(arguments[:2])} failed: {(result.stderr or "").strip()}'
            )
        return result

    def _get(self, kind: str, name: str) -> Optional[Dict]:
        result = self._run('get', kind, name, '--output', 'json', check=False)
        if result.returncode != 0:
            if 'NotFound' in (result.stderr or '') or 'not found' in (result.stderr or ''):
                return None
            raise DeployError(f'kubectl get {kind} failed: {(result.stderr or "").strip()}')
        return json.loads(result.stdout)

    # Images

    def image(self, service: str, digest: str) -> str:
        repository = self.settings.get('image') or f'mage-{service}'
        return f'{repository}:{digest}'

    def publish(self, box, image: str, build_image: Callable) -> List[str]:
        """Builds the image and makes it reachable by the cluster; returns notes."""
        if self.settings.get('image'):
            build_image(box, image, platforms=self.settings.get('platforms'), push=True)
            return []
        build_image(box, image)
        cluster = self.settings.get('kind_cluster')
        if cluster:
            result = subprocess.run(['kind', 'load', 'docker-image', image, '--name', cluster],
                                    capture_output=True, text=True)
            if result.returncode != 0:
                raise DeployError(f'kind load failed: {result.stderr.strip()}')
            return []
        return [f'{image} stays in this Docker: set image: <registry>/<repository> to push it, '
                'or kind_cluster: <name> to load it into kind.']

    # Objects

    def manifests(self, service: str, environment: str, image: str, env: Dict[str, str],
                  annotations: Dict[str, str]) -> List[Dict]:
        n = names(service, environment)
        name = n['current']
        if len(name) > 63 or not NAME.match(name):
            raise DeployError(
                f'{name} is not a Kubernetes name: lowercase letters, digits and dashes, at '
                'most 63 characters.'
            )
        selector = {f'{GROUP}/service': service, f'{GROUP}/environment': environment}
        secret = secret_name(name, env)
        container = dict(
            name='service',
            image=image,
            imagePullPolicy='IfNotPresent',
            ports=[dict(containerPort=CONTAINER_PORT, name='http')],
            envFrom=[dict(secretRef=dict(name=secret))],
            env=[
                # Kafka's static membership (group.instance.id) follows the pod.
                dict(name='MAGE_SERVICE_INSTANCE_ID',
                     valueFrom=dict(fieldRef=dict(fieldPath='metadata.name'))),
                dict(name='MAGE_SERVICE_STRICT_ENV', value='on'),
            ],
            volumeMounts=[dict(name='data', mountPath=DATA_PATH),
                          dict(name='tmp', mountPath='/tmp')],
            readinessProbe=dict(httpGet=dict(path='/healthz', port='http'), periodSeconds=10),
            livenessProbe=dict(httpGet=dict(path='/healthz', port='http'), periodSeconds=30,
                               initialDelaySeconds=30),
            securityContext=dict(readOnlyRootFilesystem=True, allowPrivilegeEscalation=False,
                                 capabilities=dict(drop=['ALL'])),
        )
        if self.settings.get('resources'):
            container['resources'] = self.settings['resources']
        pod = dict(
            securityContext=dict(runAsNonRoot=True, runAsUser=10001, fsGroup=10001),
            terminationGracePeriodSeconds=75,
            containers=[container],
            volumes=[dict(name='data', persistentVolumeClaim=dict(claimName=n['volume'])),
                     dict(name='tmp', emptyDir={})],
        )
        if self.settings.get('image_pull_secret'):
            pod['imagePullSecrets'] = [dict(name=self.settings['image_pull_secret'])]
        volume = dict(accessModes=['ReadWriteOnce'],
                      resources=dict(requests=dict(storage=self.settings.get('storage', '5Gi'))))
        if self.settings.get('storage_class'):
            volume['storageClassName'] = self.settings['storage_class']
        return [
            dict(apiVersion='v1', kind='PersistentVolumeClaim',
                 metadata=dict(name=n['volume'], labels=selector), spec=volume),
            dict(apiVersion='v1', kind='Secret', type='Opaque',
                 metadata=dict(name=secret, labels={**selector, f'{GROUP}/part': 'env'}),
                 stringData=env),
            dict(apiVersion='apps/v1', kind='Deployment',
                 metadata=dict(name=name, labels=selector),
                 spec=dict(
                     replicas=1,
                     # One writer of the run history at a time.
                     strategy=dict(type='Recreate'),
                     revisionHistoryLimit=REVISIONS,
                     selector=dict(matchLabels=selector),
                     template=dict(
                         metadata=dict(labels=selector, annotations={
                             'prometheus.io/scrape': 'true',
                             'prometheus.io/port': str(CONTAINER_PORT),
                             'prometheus.io/path': '/metrics',
                             f'{GROUP}/image': image,
                             **{f'{GROUP}/{k}': v for k, v in annotations.items()},
                         }),
                         spec=pod,
                     ),
                 )),
            dict(apiVersion='v1', kind='Service',
                 metadata=dict(name=name, labels=selector),
                 spec=dict(selector=selector,
                           ports=[dict(port=80, targetPort='http', name='http')])),
        ]

    # Deploys

    def deploy(self, service: str, environment: str, image: str, env: Dict[str, str],
               annotations: Dict[str, str], timeout: float = 300) -> Deployment:
        name = names(service, environment)['current']
        objects = self.manifests(service, environment, image, env, annotations)
        before = self._get('deployment', name)
        # Secrets go on stdin, never on the command line.
        applied = self._run('apply', '--filename', '-', check=False,
                            input=json.dumps(dict(apiVersion='v1', kind='List', items=objects)))
        if applied.returncode != 0:
            error = (applied.stderr or '').strip()
            if f'namespaces "{self.namespace}" not found' in error:
                raise DeployError(
                    f'The namespace {self.namespace} does not exist in {self.context}: create '
                    f'it with kubectl create namespace {self.namespace}.'
                )
            raise DeployError(f'kubectl apply failed: {error}')
        rollout = self._run('rollout', 'status', f'deployment/{name}',
                            f'--timeout={int(timeout)}s', check=False)
        if rollout.returncode == 0:
            self._prune_secrets(service, environment)
            return self.inspect(service, environment)
        logs = self._run('logs', f'deployment/{name}', '--tail', '40', check=False)
        undone = False
        if before is not None:
            undone = self._run('rollout', 'undo', f'deployment/{name}', check=False) \
                .returncode == 0
        raise DeployError(
            f'{name} did not become ready within {int(timeout)} seconds; '
            + ('the previous revision runs again.' if undone else 'it is left as it is.')
            + f'\n{(rollout.stdout or "") + (rollout.stderr or "")}'.rstrip()
            + f'\n{(logs.stdout or "") + (logs.stderr or "")}'.rstrip()
        )

    def _prune_secrets(self, service: str, environment: str) -> None:
        listed = self._run(
            'get', 'secrets', '--selector',
            f'{GROUP}/service={service},{GROUP}/environment={environment},{GROUP}/part=env',
            '--output', 'json',
        )
        secrets = sorted(
            json.loads(listed.stdout).get('items') or [],
            key=lambda item: item['metadata'].get('creationTimestamp', ''), reverse=True,
        )
        # One per revision kept, and the running one.
        for item in secrets[REVISIONS + 1:]:
            self._run('delete', 'secret', item['metadata']['name'], check=False)

    def rollback(self, service: str, environment: str, timeout: float = 300) -> Deployment:
        name = names(service, environment)['current']
        if self._get('deployment', name) is None:
            raise DeployError(f'{service} is not deployed in {environment}.')
        undo = self._run('rollout', 'undo', f'deployment/{name}', check=False)
        if undo.returncode != 0:
            raise DeployError(f'{service} has no previous deployment in {environment}.'
                              if 'history' in (undo.stderr or '')
                              else f'kubectl rollout undo failed: {undo.stderr.strip()}')
        self._run('rollout', 'status', f'deployment/{name}', f'--timeout={int(timeout)}s')
        return self.inspect(service, environment)

    def inspect(self, service: str, environment: str) -> Optional[Deployment]:
        item = self._get('deployment', names(service, environment)['current'])
        return None if item is None else deployment_of(item, service, environment)

    def current_token(self, service: str, environment: str) -> Optional[str]:
        """The API token the deployment runs with, from its Secret."""
        import base64

        item = self._get('deployment', names(service, environment)['current'])
        if item is None:
            return None
        try:
            secret = item['spec']['template']['spec']['containers'][0]['envFrom'][0][
                'secretRef']['name']
        except (KeyError, IndexError):
            return None
        found = self._get('secret', secret)
        value = ((found or {}).get('data') or {}).get('MAGE_SERVICE_TOKEN')
        return base64.b64decode(value).decode() if value else None

    def log_command(self, service: str, environment: str, tail: int = 200,
                    follow: bool = False) -> List[str]:
        name = names(service, environment)['current']
        if self._get('deployment', name) is None:
            raise DeployError(f'{service} is not deployed in {environment}.')
        return ['kubectl', '--context', self.context, '--namespace', self.namespace, 'logs',
                f'deployment/{name}', '--tail', str(tail), *(['--follow'] if follow else [])]


    def port_forward(self, service: str, environment: str, timeout: float = 20):
        """A local port to the service's API: (port, process); terminate the process after."""
        import time

        name = names(service, environment)['current']
        if self._get('deployment', name) is None:
            raise DeployError(f'{service} is not deployed in {environment}.')
        try:
            process = subprocess.Popen(
                ['kubectl', '--context', self.context, '--namespace', self.namespace,
                 'port-forward', f'service/{name}', ':80'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
        except FileNotFoundError:
            raise DeployError('kubectl is not installed; Kubernetes targets use it.') from None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = process.stdout.readline()
            found = re.search(r'127\.0\.0\.1:(\d+)', line or '')
            if found:
                return int(found.group(1)), process
            if process.poll() is not None:
                break
        process.terminate()
        raise DeployError(f'kubectl port-forward failed: {(process.stderr.read() or "").strip()}')


def secret_name(base: str, env: Dict[str, str]) -> str:
    digest = hashlib.sha256(json.dumps(env, sort_keys=True).encode()).hexdigest()[:10]
    return f'{base}-env-{digest}'


def deployment_of(item: Dict, service: str, environment: str) -> Deployment:
    annotations = ((item.get('spec') or {}).get('template') or {}).get('metadata', {}) \
        .get('annotations') or {}
    status = item.get('status') or {}
    wanted = (item.get('spec') or {}).get('replicas', 1)
    if wanted == 0:
        state = 'stopped'
    elif status.get('updatedReplicas', 0) < wanted:
        state = 'updating'
    elif status.get('readyReplicas', 0) >= wanted:
        state = 'running'
    else:
        state = 'unavailable'
    return Deployment(
        service=service,
        environment=environment,
        state=state,
        image=annotations.get(f'{GROUP}/image', ''),
        commit=annotations.get(f'{GROUP}/commit', ''),
        lock=annotations.get(f'{GROUP}/lock', ''),
        reason=annotations.get(f'{GROUP}/reason', ''),
        values=annotations.get(f'{GROUP}/values', ''),
    )
