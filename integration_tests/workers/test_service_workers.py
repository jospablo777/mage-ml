"""
An exported service whose pipeline uses two Python environments (item 84): the loader in
the project's Python worker legacy (six 1.16.0), the transformer in the image's Python
(MageML's six). The export builds the worker as a virtual environment of its own, and the
engine runs each block in its interpreter.
"""
import importlib.metadata
import json
import subprocess
import sys
import textwrap
import types
from pathlib import Path

import pytest

from integration_tests.exported_services import export_service, run_service

LOAD = '''
import sys

import six


@data_loader
def load(*args, **kwargs):
    return {'six': six.__version__, 'python': sys.executable}
'''
TRANSFORM = '''
import sys

import six


@transformer
def transform(data, *args, **kwargs):
    return dict(data, next_six=six.__version__, next_python=sys.executable)
'''


@pytest.fixture
def project(tmp_path):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.settings.repo import get_repo_path, set_repo_path
    from mage_ai.tests.base_test import remove_test_directory, set_up_test_repo

    previous = get_repo_path()
    holder = types.SimpleNamespace()
    set_up_test_repo(holder)
    repo = Path(holder.repo_path)
    worker = repo / 'python_workers' / 'legacy'
    worker.mkdir(parents=True)
    (worker / 'pyproject.toml').write_text(textwrap.dedent('''
        [project]
        name = "legacy"
        version = "0"
        requires-python = ">=3.12"
        dependencies = ["six==1.16.0"]
    '''))
    subprocess.run(['uv', 'lock', '--project', str(worker)], check=True, capture_output=True)
    pipeline = Pipeline.create('two_workers', repo_path=str(repo))
    load = Block.create('two_workers_load', 'data_loader', str(repo), language='python',
                        configuration=dict(python_worker='legacy'))
    Path(load.file_path).write_text(LOAD)
    pipeline.add_block(load)
    transform = Block.create('two_workers_transform', 'transformer', str(repo),
                             language='python')
    Path(transform.file_path).write_text(TRANSFORM)
    pipeline.add_block(transform, upstream_block_uuids=[load.uuid])
    yield str(repo)
    # The tests after it on this worker read pipelines from the repository path.
    set_repo_path(previous)
    remove_test_directory(holder._test_directory)


def test_each_block_runs_in_its_python_worker_in_the_exported_service(project, tmp_path):
    directory = export_service(project, 'two_workers', tmp_path)
    manifest = json.loads((directory / 'service.json').read_text())
    assert manifest['python_workers'] == {'legacy': 'workers/legacy/.venv/bin/python'}
    # As the image's Dockerfile does: the worker's environment from its lock, on this Python.
    subprocess.run(
        ['uv', 'venv', '.venv', '--python', sys.executable],
        cwd=directory / 'workers' / 'legacy', check=True, capture_output=True,
    )
    subprocess.run(
        ['uv', 'sync', '--locked', '--no-dev', '--no-install-project'],
        cwd=directory / 'workers' / 'legacy', check=True, capture_output=True,
    )
    run = run_service(directory, 'two_workers', tmp_path)
    [last] = [b for b in run['blocks'] if b['block'] == 'two_workers_transform']
    output = json.loads(Path(last['outputs'][0]['path']).read_text())
    assert output['six'] == '1.16.0'
    assert output['python'].endswith('workers/legacy/.venv/bin/python')
    assert output['next_six'] == importlib.metadata.version('six')
    assert not output['next_python'].endswith('workers/legacy/.venv/bin/python')
