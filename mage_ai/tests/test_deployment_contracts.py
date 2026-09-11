"""
Contracts for how the fork is released and deployed.

Each test here maps to a defect that reached the fork: workloads that started
from the upstream image, publishing workflows that pointed at namespaces the
fork has no credentials for, and library calls that a dependency bump removes.
They exist so a revert fails in CI instead of in a cluster.
"""
import pathlib
import re
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SOURCE_ROOTS = [REPO_ROOT / 'mage_ai', REPO_ROOT / 'mage_integrations']
SKIPPED_PARTS = {'frontend', 'templates', 'frontend_dist', 'frontend_dist_base_path_template'}

UPSTREAM_IMAGE = 'mageai/mageai'
# Templates and code that decide which image a deployment runs.
DEPLOYMENT_FILES = [
    'kube/app.yaml',
    'scripts/deploy/cloudformation/ecs.yaml',
    'mage_ai/cluster_manager/kubernetes/workload_manager.py',
]


def python_sources():
    for root in SOURCE_ROOTS:
        for path in root.rglob('*.py'):
            if SKIPPED_PARTS & set(path.parts):
                continue
            if 'tests' in path.parts:
                continue
            try:
                yield path, path.read_text(encoding='utf-8')
            except (UnicodeDecodeError, OSError):
                continue


class LegacyLibraryCallTest(unittest.TestCase):
    """
    Query.get became legacy in SQLAlchemy 2.0 and distutils left the standard
    library in Python 3.12. Both resolve today and disappear on the next bump.
    """

    def test_source_calls_no_legacy_query_get(self):
        pattern = re.compile(r'\.query\.get\(')
        offenders = [
            str(path.relative_to(REPO_ROOT))
            for path, text in python_sources()
            # The Weaviate client has its own query.get, unrelated to SQLAlchemy.
            if pattern.search(text) and 'weaviate' not in path.name
        ]

        self.assertEqual(offenders, [], 'use Model.get_by_id, which calls Session.get')

    def test_source_imports_no_distutils(self):
        pattern = re.compile(r'^\s*(import distutils|from distutils)', re.MULTILINE)
        offenders = [
            str(path.relative_to(REPO_ROOT))
            for path, text in python_sources()
            if pattern.search(text)
        ]

        self.assertEqual(offenders, [], 'distutils is absent from Python 3.12')

    def test_declarative_base_comes_from_the_orm_namespace(self):
        text = (REPO_ROOT / 'mage_ai/orchestration/db/models/base.py').read_text(encoding='utf-8')

        self.assertIn('from sqlalchemy.orm import declarative_base', text)
        self.assertNotIn('from sqlalchemy.ext.declarative import', text)


class DependencyCeilingTest(unittest.TestCase):
    """
    Query.from_self was removed in SQLAlchemy 2.0 while the project accepted any
    2.x or later release. A ceiling turns the next major into a lock failure.
    """

    def test_sqlalchemy_declares_a_ceiling(self):
        text = (REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8')
        requirement = re.search(r'"sqlalchemy(?P<specifier>[^"]*)"', text)

        self.assertIsNotNone(requirement)
        self.assertIn('<', requirement.group('specifier'))


class ContainerImageTest(unittest.TestCase):
    def test_deployments_do_not_reference_the_upstream_image(self):
        for name in DEPLOYMENT_FILES:
            with self.subTest(file=name):
                text = (REPO_ROOT / name).read_text(encoding='utf-8')
                self.assertNotIn(UPSTREAM_IMAGE, text)

    def test_workload_manager_reads_the_image_from_the_environment(self):
        from mage_ai.cluster_manager.constants import MAGE_CONTAINER_IMAGE_ENV_VAR

        text = (
            REPO_ROOT / 'mage_ai/cluster_manager/kubernetes/workload_manager.py'
        ).read_text(encoding='utf-8')

        self.assertEqual(MAGE_CONTAINER_IMAGE_ENV_VAR, 'MAGE_CONTAINER_IMAGE')
        self.assertIn('os.getenv(MAGE_CONTAINER_IMAGE_ENV_VAR)', text)


class NfsClientTest(unittest.TestCase):
    """
    `nfs-common` served one caller, the Cloud Filestore mount in the startup
    script. Both were removed. Either one alone is dead weight or a broken mount.
    """

    def test_the_client_and_the_mount_stay_together(self):
        dockerfile = (REPO_ROOT / 'Dockerfile').read_text(encoding='utf-8')
        startup = (REPO_ROOT / 'scripts/run_app.sh').read_text(encoding='utf-8')

        installs_client = 'nfs-common' in dockerfile
        mounts_share = re.search(r'^\s*mount\b', startup, re.MULTILINE) is not None

        self.assertEqual(
            installs_client,
            mounts_share,
            'the startup script mounts a share without the client, or the reverse',
        )


class PublishWorkflowTest(unittest.TestCase):
    """
    Both workflows were disabled because they pushed to upstream namespaces. A
    fork with no release path cannot deploy what its CI validated.
    """

    def workflow(self, name):
        path = REPO_ROOT / '.github/workflows' / name
        return path.read_text(encoding='utf-8'), yaml.safe_load(path.read_text(encoding='utf-8'))

    def test_image_workflow_publishes_the_fork(self):
        text, workflow = self.workflow('publish_docker_image.yml')

        self.assertNotIn('if: false', text)
        self.assertNotIn(UPSTREAM_IMAGE, text)
        self.assertIn('ghcr.io', text)
        # `on` parses as the boolean True.
        self.assertIn('tags', workflow[True]['push'])

    def test_distribution_workflow_builds_every_workspace_package(self):
        text, workflow = self.workflow('publish_to_pypi.yml')

        self.assertNotIn('if: false', text)
        self.assertIn('uv build --all-packages', text)
        self.assertIn('tags', workflow[True]['push'])


class UvVersionTest(unittest.TestCase):
    """
    The image, the workflows, and the audit resolve dependencies with uv. A split
    between them resolves different versions than the ones CI validated.
    """

    def test_every_pin_matches_the_image(self):
        dockerfile = (REPO_ROOT / 'Dockerfile').read_text(encoding='utf-8')
        image_version = re.search(r'ghcr\.io/astral-sh/uv:(?P<version>[\d.]+)', dockerfile)

        self.assertIsNotNone(image_version)
        version = image_version.group('version')

        for name in ['build_and_test.yml', 'dependency_audit.yml', 'publish_to_pypi.yml']:
            with self.subTest(workflow=name):
                text = (REPO_ROOT / '.github/workflows' / name).read_text(encoding='utf-8')
                pins = set(re.findall(r'uv==(?P<version>[\d.]+)', text))
                pins.update(re.findall(r'UV_VERSION: "(?P<version>[\d.]+)"', text))
                pins.discard('${{ env.UV_VERSION }}')

                self.assertTrue(pins, 'no uv version pinned in %s' % name)
                self.assertEqual(pins, {version})


class DependencyUpdateTest(unittest.TestCase):
    def test_dependabot_covers_every_lockfile(self):
        config = yaml.safe_load((REPO_ROOT / '.github/dependabot.yml').read_text(encoding='utf-8'))
        directories = set()
        for update in config['updates']:
            directories.update(update.get('directories', []))
            if update.get('directory'):
                directories.add(update['directory'])

        ecosystems = {update['package-ecosystem'] for update in config['updates']}
        self.assertEqual(ecosystems, {'uv', 'npm', 'github-actions', 'docker'})

        for lockfile in REPO_ROOT.glob('runtimes/*/uv.lock'):
            with self.subTest(runtime=lockfile.parent.name):
                self.assertIn('/%s' % lockfile.parent.relative_to(REPO_ROOT), directories)


if __name__ == '__main__':
    unittest.main()
