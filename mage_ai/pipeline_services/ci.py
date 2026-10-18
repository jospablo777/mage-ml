"""
`mage service ci`: one GitHub Actions workflow for the project's services (B3).

Pull requests check the services they change and that their locks are committed. Each rule
deploys the services that define an environment, on a push to a branch or a tag:
`mage service deploy --if-changed` builds nothing when the environment already runs the
lock with the same values, and CI never commits back. Each service's secrets come from the
GitHub secret <SERVICE>_<ENV>_ENV, written as its .env.<env> (git-ignored).
"""
import importlib.metadata
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from mage_ai.pipeline_services import definitions

WORKFLOW = Path('.github') / 'workflows' / 'mage-services.yml'


class CiError(ValueError):
    pass


@dataclass
class Rule:
    environment: str
    branch: Optional[str] = None
    tag_prefix: Optional[str] = None

    @property
    def condition(self) -> str:
        if self.branch:
            return f"github.event_name == 'push' && github.ref == 'refs/heads/{self.branch}'"
        return (f"github.event_name == 'push' && "
                f"startsWith(github.ref, 'refs/tags/{self.tag_prefix}')")

    @property
    def when(self) -> str:
        return f'on a push to {self.branch}' if self.branch else \
            f'on a tag {self.tag_prefix}*'


def parse_rule(text: str) -> Rule:
    """ENV=BRANCH or ENV=tag:PREFIX*, such as dev=main or prod=tag:v*."""
    environment, separator, when = text.partition('=')
    if not separator or not environment or not when:
        raise CiError(f'{text}: write ENV=BRANCH or ENV=tag:PREFIX*, such as dev=main or '
                      'prod=tag:v*.')
    if not re.match(r'^[a-z0-9][a-z0-9-]*$', environment):
        raise CiError(f'{environment} is not an environment name.')
    if when.startswith('tag:'):
        pattern = when[len('tag:'):]
        prefix = pattern[:-1] if pattern.endswith('*') else pattern
        if any(c in prefix for c in '*?[]!') or not re.match(r'^[A-Za-z0-9._/-]*$', prefix):
            raise CiError(f'{pattern}: a tag rule is a prefix with a final *, such as v*.')
        return Rule(environment, tag_prefix=prefix)
    if not re.match(r'^[A-Za-z0-9._/-]+$', when):
        raise CiError(f'{when} is not a branch name.')
    return Rule(environment, branch=when)


def secret_name(service: str, environment: str) -> str:
    return re.sub(r'[^A-Z0-9]', '_', f'{service}_{environment}_ENV'.upper())


def registry_host(image: Optional[str]) -> str:
    if not image:
        return ''
    host = image.split('/', 1)[0]
    return host if ('.' in host or ':' in host or host == 'localhost') and '/' in image \
        else 'docker.io'


def environment_plan(repo_path: str, environment: str) -> List[Dict]:
    """The services that define the environment, with what a CI deploy needs."""
    plan = []
    for name in definitions.names(repo_path):
        path = definitions.service_path(repo_path, name) / definitions.ENVIRONMENTS_DIR \
            / f'{environment}.yaml'
        if not path.is_file():
            continue
        settings = yaml.safe_load(path.read_text()) or {}
        plan.append(dict(
            service=name,
            environment=environment,
            target=settings.get('target', 'docker'),
            secret=secret_name(name, environment),
            registry=registry_host(settings.get('image')),
        ))
    return plan


def _git_root(repo_path: str) -> Path:
    try:
        top = subprocess.run(['git', 'rev-parse', '--show-toplevel'], cwd=repo_path,
                             capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        raise CiError('The project is not in a Git repository; the workflow goes in its '
                      '.github/workflows.') from None
    return Path(top).resolve()


def workflow(project: str, rules: List[Rule], mage_version: str) -> str:
    """The workflow; project is the project's folder relative to the repository root."""
    install = [
        '      - uses: actions/setup-python@v6',
        '        with:',
        '          python-version: "3.12"',
        '      - name: Install MageML',
        '        run: |',
        '          pip install uv',
        f'          uv pip install --system "mage-ml=={mage_version}"',
        '          if [ -f "$MAGE_PROJECT/requirements.txt" ]; then',
        '            uv pip install --system -r "$MAGE_PROJECT/requirements.txt"',
        '          fi',
    ]
    lines = [
        '# Written by `mage service ci`. Pull requests check the services they change; the',
        '# rules below deploy the services that define an environment. Edit it freely;',
        '# `mage service ci --force` writes it again.',
        '#',
        '# Settings > Environments, one per deployed environment, with these secrets:',
        '#   <SERVICE>_<ENV>_ENV   the service\'s .env.<env>: KEY=value lines with its secrets;',
        '#                         MAGE_SERVICE_TOKEN=<token> pins the API token',
        '#   KUBECONFIG            a kubeconfig with the contexts the environments name',
        '#   IBM_CLOUD_API_KEY     for Code Engine environments',
        '#   REGISTRY_USERNAME and REGISTRY_PASSWORD   to push images',
        '# Pin the actions to commit SHAs if your organization requires it.',
        'name: MageML services',
        '',
        'on:',
        '  pull_request:',
    ]
    if project != '.':
        lines.append(f"    paths: ['{project}/**', '.github/workflows/mage-services.yml']")
    branches = sorted({r.branch for r in rules if r.branch})
    tags = sorted({f'{r.tag_prefix}*' for r in rules if r.tag_prefix is not None})
    if branches or tags:
        lines.append('  push:')
        if branches:
            lines.append(f'    branches: [{", ".join(branches)}]')
        if tags:
            lines.append(f"    tags: [{', '.join(repr(t) for t in tags)}]")
    lines += [
        '',
        'permissions:',
        '  contents: read',
        '',
        'env:',
        f'  MAGE_PROJECT: {project}',
        '',
        'jobs:',
        '  check:',
        "    if: github.event_name == 'pull_request'",
        '    runs-on: ubuntu-latest',
        '    steps:',
        '      - uses: actions/checkout@v4',
        '        with:',
        '          fetch-depth: 0',
        *install,
        '      - name: Check the services the pull request changes, and their locks',
        '        env:',
        '          BASE: origin/${{ github.base_ref }}',
        '        run: mage service check --affected --locked --base "$BASE" '
        '--project "$MAGE_PROJECT"',
    ]
    for rule in rules:
        env = rule.environment
        lines += [
            '',
            f'  # {env}: {rule.when}.',
            f'  plan-{env}:',
            f'    if: {rule.condition}',
            '    runs-on: ubuntu-latest',
            '    outputs:',
            '      services: ${{ steps.plan.outputs.services }}',
            '    steps:',
            '      - uses: actions/checkout@v4',
            *install,
            '      - id: plan',
            f'        run: echo "services=$(mage service list --env {env} --json '
            '--project "$MAGE_PROJECT")" >> "$GITHUB_OUTPUT"',
            '',
            f'  deploy-{env}:',
            f'    needs: plan-{env}',
            f"    if: needs.plan-{env}.outputs.services != '[]'",
            '    runs-on: ubuntu-latest',
            f'    environment: {env}',
            '    strategy:',
            '      fail-fast: false',
            '      matrix:',
            f'        include: ${{{{ fromJSON(needs.plan-{env}.outputs.services) }}}}',
            '    # One deploy of a service to an environment at a time.',
            '    concurrency:',
            f'      group: deploy-${{{{ matrix.service }}}}-{env}',
            '      cancel-in-progress: false',
            '    env:',
            '      SERVICE: ${{ matrix.service }}',
            '    steps:',
            '      - uses: actions/checkout@v4',
            *install,
            '      - uses: docker/setup-qemu-action@v3',
            '      - uses: docker/setup-buildx-action@v3',
            "      - name: Write the service's environment",
            '        env:',
            '          SERVICE_ENV: ${{ secrets[matrix.secret] }}',
            '        run: |',
            '          if [ -n "$SERVICE_ENV" ]; then',
            '            umask 077',
            f'            printf \'%s\\n\' "$SERVICE_ENV" > '
            f'"$MAGE_PROJECT/services/$SERVICE/.env.{env}"',
            '          fi',
            '      - name: Configure kubectl',
            '        env:',
            '          KUBECONFIG_CONTENT: ${{ secrets.KUBECONFIG }}',
            '        run: |',
            '          if [ -n "$KUBECONFIG_CONTENT" ]; then',
            '            mkdir -p ~/.kube',
            '            umask 077',
            '            printf \'%s\\n\' "$KUBECONFIG_CONTENT" > ~/.kube/config',
            '          fi',
            '      - name: Log in to the registry',
            "        if: matrix.registry != ''",
            '        uses: docker/login-action@v3',
            '        with:',
            '          registry: ${{ matrix.registry }}',
            '          username: ${{ secrets.REGISTRY_USERNAME }}',
            '          password: ${{ secrets.REGISTRY_PASSWORD }}',
            '      - name: Deploy',
            '        env:',
            '          IBM_CLOUD_API_KEY: ${{ secrets.IBM_CLOUD_API_KEY }}',
            f'        run: mage service deploy "$SERVICE" --env {env} --if-changed '
            '--timeout 600 --project "$MAGE_PROJECT"',
        ]
    return '\n'.join(lines) + '\n'


def write(repo_path: str, rules: List[Rule], force: bool = False,
          mage_version: Optional[str] = None) -> Path:
    """Writes the workflow at the repository's root; raises CiError."""
    root = _git_root(repo_path)
    project = Path(repo_path).resolve().relative_to(root).as_posix() \
        if Path(repo_path).resolve() != root else '.'
    for rule in rules:
        plan = environment_plan(repo_path, rule.environment)
        if not plan:
            raise CiError(f'No service defines the environment {rule.environment} '
                          f'(services/<name>/environments/{rule.environment}.yaml).')
        local = [p['service'] for p in plan if p['target'] == 'docker']
        if local:
            raise CiError(
                f'{", ".join(local)} deploy {rule.environment} to Docker, which in CI is the '
                'runner\'s and ends with the job: set target: kubernetes or code_engine in '
                f'their environments/{rule.environment}.yaml.'
            )
    if len({r.environment for r in rules}) != len(rules):
        raise CiError('Each environment takes one rule.')
    path = root / WORKFLOW
    if path.exists() and not force:
        raise CiError(f'{path.relative_to(root)} exists; pass --force to write it again.')
    version = mage_version or importlib.metadata.version('mage-ml')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(workflow(project, rules, version))
    return path
