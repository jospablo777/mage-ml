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


DOCKERFILE = '''\
# syntax=docker/dockerfile:1.7
FROM rust:1.95-slim-bookworm AS build
COPY build/mage_service ./mage_service

FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \\
    PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock /tmp/service-python/
RUN pip install uv && UV_PROJECT_ENVIRONMENT=/usr/local uv sync --locked
WORKDIR /srv/mage-service
COPY --from=build /out/mage-service /out/mage-console /usr/local/bin/
COPY --chown=mage:mage python ./python
COPY --chown=mage:mage models ./models
COPY --chown=mage:mage shop ./shop
COPY --chown=mage:mage service.json ./service.json
'''

# docker history of that image, newest first: (bytes, CreatedBy).
HISTORY = [
    (0, 'CMD ["serve"]'),
    (0, 'RUN /bin/sh -c mage-service validate # buildkit'),
    (9_960, 'COPY --chown=mage:mage service.json ./service.json # buildkit'),
    (5_808, 'COPY --chown=mage:mage shop ./shop # buildkit'),
    (40_000_000, 'COPY --chown=mage:mage models ./models # buildkit'),
    (5_674_117, 'COPY --chown=mage:mage python ./python # buildkit'),
    (13_920_224, 'COPY /out/mage-service /out/mage-console /usr/local/bin/ # buildkit'),
    (0, 'WORKDIR /srv/mage-service'),
    (4_310, 'RUN /bin/sh -c groupadd --system --gid 10001 mage && useradd # buildkit'),
    (310_000_000, 'RUN /bin/sh -c pip install uv==0.9.0 && cd /srv/mage-service/workers/'
                  'feast.mlflow &&     uv pip sync requirements.txt # buildkit'),
    (2_000, 'COPY workers/feast.mlflow/ /srv/mage-service/workers/feast.mlflow/ # buildkit'),
    (120_000_000, 'RUN /bin/sh -c pip install uv==0.9.0 && cd /srv/mage-service/workers/torch '
                  '&& uv sync --locked # buildkit'),
    (531_774_297, 'RUN /bin/sh -c pip install uv==0.9.0 && cd /tmp/service-python && '
                  'UV_PROJECT_ENVIRONMENT=/usr/local uv sync --locked # buildkit'),
    (451, 'COPY pyproject.toml uv.lock /tmp/service-python/ # buildkit'),
    (0, 'ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1'),
    (51_156_106, 'RUN /bin/sh -c set -eux; apt-get install -y gcc; ln -s python3 python '
                 '# buildkit'),
    (97_248_673, "# debian.sh --arch 'arm64' out/ 'bookworm' '@1791158400'"),
]


class ImageSizeTest(TestCase):
    def test_each_layer_counts_toward_the_part_of_the_image_its_step_made(self):
        parts = export.image_parts(HISTORY, DOCKERFILE)

        self.assertEqual([(p['label'], p['bytes']) for p in parts], [
            ('Base image (python:3.12-slim-bookworm)', 51_156_106 + 97_248_673 + 4_310),
            ('Python packages', 531_774_297 + 451),
            ('Workers feast, mlflow', 310_002_000),
            ('Worker torch', 120_000_000),
            ('MageML runtime', 13_920_224 + 5_674_117),
            ('Models', 40_000_000),
            ('Project code', 5_808 + 9_960),
        ])
        self.assertEqual(sum(p['bytes'] for p in parts), sum(size for size, _ in HISTORY))

    def test_a_base_image_layer_like_a_step_of_ours_stays_in_the_base(self):
        # The base image's own steps can mention what ours do; only what comes after our
        # first step is ours.
        history = [(5, 'COPY --chown=mage:mage python ./python # buildkit'),
                   (0, 'ENV PYTHONDONTWRITEBYTECODE=1'),
                   (7, 'RUN /bin/sh -c cp /out/mage-service /usr/local/bin # buildkit')]

        self.assertEqual(
            [(p['key'], p['bytes']) for p in export.image_parts(history, DOCKERFILE)],
            [('base', 7), ('runtime', 5)],
        )

    def test_the_size_in_the_terminal_says_what_changed_since_the_build_before(self):
        size = dict(image='shop:1', bytes=700_000_000, parts=[
            dict(key='base', label='Base image', bytes=150_000_000),
            dict(key='worker:torch', label='Worker torch', bytes=550_000_000),
        ])
        previous = dict(bytes=400_000_000, parts=[
            dict(key='base', label='Base image', bytes=150_000_000),
            dict(key='python', label='Python packages', bytes=250_000_000),
        ])

        self.assertEqual(export.describe_image_size(size, previous), [
            'Image shop:1: 700 MB  (+300 MB)',
            '  Base image       150 MB',
            '  Worker torch     550 MB  (+550 MB)',
        ])
        self.assertEqual(export.describe_image_size(size)[0], 'Image shop:1: 700 MB')

    def fake_docker(self, history_lines, exit_code=0):
        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, True)
        history = folder / 'history.txt'
        history.write_text(''.join(f'{size}\t{by}\n' for size, by in history_lines))
        docker = folder / 'docker'
        docker.write_text(textwrap.dedent(f'''\
            #!/bin/sh
            [ {exit_code} -ne 0 ] && echo "Error: No such image: $7" >&2 && exit {exit_code}
            case "$1" in
              history) cat "{history}" ;;
              image) echo "sha256:abc linux/arm64" ;;
            esac
        '''))
        docker.chmod(0o755)
        (folder / 'Dockerfile').write_text(DOCKERFILE)
        return folder

    def test_a_built_image_is_measured_from_docker_history(self):
        folder = self.fake_docker(HISTORY)

        with patch.dict(os.environ, {'PATH': f'{folder}:{os.environ["PATH"]}'}):
            size = export.measure_image('shop:1', folder)

        self.assertEqual(size['bytes'], sum(s for s, _ in HISTORY))
        self.assertEqual(size['platform'], 'linux/arm64')
        self.assertEqual(size['id'], 'sha256:abc')
        self.assertEqual(len(size['parts']), 7)

    def test_an_image_docker_does_not_have_is_not_measured(self):
        # A build for several platforms pushes the image and keeps none locally.
        folder = self.fake_docker([], exit_code=1)

        with patch.dict(os.environ, {'PATH': f'{folder}:{os.environ["PATH"]}'}):
            self.assertIsNone(export.measure_image('icr.io/team/shop:1', folder))


class ImageBuildsTest(TestCase):
    def setUp(self):
        from mage_ai.pipeline_services import exports

        self.exports = exports
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        patcher = patch.object(exports, 'exports_root', return_value=self.root)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.directory = self.root / 'shop'
        self.directory.mkdir()
        (self.directory / 'service.json').write_text(
            '{"service": {"exported_at": "2026-11-18T10:00:00+00:00", "source_sha256": "f00"}}')

    def test_each_build_is_kept_newest_first_outside_the_export_folder(self):
        sizes = [dict(bytes=n, image='shop:1', parts=[]) for n in range(12)]

        with patch.object(export, 'measure_image', side_effect=sizes):
            for _ in sizes:
                self.exports.record_build('/repo', 'shop', 'shop:1', self.directory)

        builds = self.exports.image_builds('/repo', 'shop')
        self.assertEqual([b['bytes'] for b in builds], list(range(11, 1, -1)))
        self.assertEqual(builds[0]['source_sha256'], 'f00')
        self.assertEqual(builds[0]['exported_at'], '2026-11-18T10:00:00+00:00')
        # Exporting again replaces the export's folder; the builds stay.
        self.assertFalse((self.directory / '.builds').exists())
        self.assertNotIn('.builds', [e['name'] for e in self.exports.list_exports('/repo')])

    def test_a_build_docker_does_not_keep_records_nothing(self):
        with patch.object(export, 'measure_image', return_value=None):
            self.assertIsNone(
                self.exports.record_build('/repo', 'shop', 'icr.io/a/shop:1', self.directory))

        self.assertEqual(self.exports.image_builds('/repo', 'shop'), [])

    def test_a_service_never_built_has_no_builds(self):
        self.assertEqual(self.exports.image_builds('/repo', 'other'), [])
