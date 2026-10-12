"""
What a failed docker build or push says: the reason docker printed, turned into what to do.
"""
import os
import shutil
import tempfile
import textwrap
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from mage_ai.pipeline_services import export

BUILD = ['docker', 'build', '-t', 'shop:1', '.']
PUSH = ['docker', 'push', 'icr.io/team/shop:1']


class BuildFailureTest(TestCase):
    def explain(self, line, command=BUILD, tag='shop:1'):
        return export.explain_build_failure(['#5 [2/9] RUN uv sync', line], command, tag)

    def test_each_common_failure_says_what_to_do(self):
        cases = [
            ('Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker '
             'daemon running?', BUILD, 'shop:1', 'Docker is not running on this machine'),
            ('ERROR: failed to copy: write /var/lib/docker/tmp: no space left on device', BUILD,
             'shop:1', 'docker builder prune'),
            ('ERROR: Multi-platform build is not supported for the docker driver.', BUILD,
             'shop:1', 'docker buildx create --use --driver docker-container'),
            ('exec /bin/sh: exec format error', BUILD, 'shop:1', 'tonistiigi/binfmt'),
            ('denied: requested access to the resource is denied', PUSH, 'icr.io/team/shop:1',
             'Log in to icr.io on this machine (`docker login icr.io`)'),
            ('dial tcp 127.0.0.1:5000: connect: connection refused', PUSH, 'localhost:5000/shop:1',
             'Docker could not reach localhost:5000'),
            ('ERROR: failed to resolve source metadata for docker.io/library/python:3.12: '
             'unexpected status 401 Unauthorized', BUILD, 'shop:1', 'could not pull a base image'),
        ]
        for line, command, tag, expected in cases:
            with self.subTest(expected=expected):
                self.assertIn(expected, self.explain(line, command, tag))

    def test_an_image_without_a_registry_is_on_docker_hub(self):
        reason = self.explain('unauthorized: authentication required', PUSH, 'team/shop:1')
        self.assertIn('`docker login docker.io`', reason)

    def test_an_unknown_failure_points_to_the_output(self):
        self.assertEqual(self.explain('#9 ERROR: something new'), 'The lines above say why. ')

    def test_a_failed_build_names_the_reason_and_keeps_every_line(self):
        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, True)
        docker = folder / 'docker'
        docker.write_text(textwrap.dedent('''\
            #!/bin/sh
            echo "#1 [internal] load build definition"
            echo "Cannot connect to the Docker daemon at unix:///var/run/docker.sock." >&2
            exit 1
        '''))
        docker.chmod(0o755)
        lines = []

        with patch.dict(os.environ, {'PATH': f'{folder}:{os.environ["PATH"]}'}), \
                self.assertRaises(export.ExportError) as raised:
            export.build_image(folder, 'shop:1', on_line=lines.append)

        self.assertEqual(lines, [
            '#1 [internal] load build definition',
            'Cannot connect to the Docker daemon at unix:///var/run/docker.sock.',
        ])
        self.assertEqual(raised.exception.problems[0], (
            'docker build failed with exit code 1. Docker is not running on this machine: start '
            'Docker Desktop, Rancher Desktop or the docker service, then build again.'
        ))
