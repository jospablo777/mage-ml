"""
The worker protocol's conformance suite (docs/development/worker-protocol.md): a worker
driven as a black box, through its process, the way MageML runs (environments.run_block,
warm_workers) and the way exported services run it. MAGE_WORKER_COMMAND runs the suite
against another implementation, such as `python /path/to/worker.py`; by default it is
MageML's Python worker.
"""
import json
import os
import shlex
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Dict, List

from mage_ai.pipeline_services.runtime import worker as python_worker

COMMAND = shlex.split(os.environ.get('MAGE_WORKER_COMMAND') or '') or [
    sys.executable, '-u', python_worker.__file__,
]


class Worker:
    """A worker process: requests on stdin, replies on its own descriptor."""

    def __init__(self, cwd: str):
        read_fd, write_fd = os.pipe()
        self.process = subprocess.Popen(
            COMMAND, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=cwd, pass_fds=(write_fd,),
            env=dict(os.environ, MAGE_WORKER_REPLY_FD=str(write_fd), MAGE_REPO_PATH=cwd,
                     PYTHONUNBUFFERED='1'),
        )
        os.close(write_fd)
        self.replies = os.fdopen(read_fd)

    def read(self) -> Dict:
        line = self.replies.readline()
        if not line:
            raise AssertionError(f'No reply: the worker exited with {self.process.wait()}.')
        return json.loads(line)

    def send(self, request: Dict) -> Dict:
        self.process.stdin.write(json.dumps(request) + '\n')
        self.process.stdin.flush()
        return self.read()

    def close(self) -> List[str]:
        self.process.stdin.close()
        self.process.wait(timeout=30)
        return [self.process.stdout.read(), self.process.stderr.read()]


class ConformanceTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())
        self.worker = Worker(str(self.directory))
        self.ready = self.worker.read()

    def tearDown(self):
        if self.worker.process.poll() is None:
            self.worker.process.kill()
            self.worker.process.wait()
        for stream in (self.worker.process.stdout, self.worker.process.stderr,
                       self.worker.process.stdin, self.worker.replies):
            stream.close()

    def block(self, name: str, body: str, block_type: str = 'transformer') -> Dict:
        path = self.directory / f'{name}.py'
        path.write_text(textwrap.dedent(body))
        return dict(uuid=name, type=block_type, file=str(path), configuration={})

    def request(self, block: Dict, inputs: List[Dict] = None, **extra) -> Dict:
        return dict(id=f'r-{block["uuid"]}', block=block, inputs=inputs or [], kwargs={},
                    output_dir=str(self.directory / 'out' / block['uuid']), **extra)

    def value(self, record: Dict):
        return python_worker.read_input(record)

    def test_it_says_it_is_ready_first(self):
        self.assertEqual(self.ready['type'], 'ready')
        self.assertEqual(self.ready['protocol'], python_worker.PROTOCOL_VERSION)
        self.assertIsInstance(self.ready['pid'], int)

    def test_a_block_answers_its_outputs_with_its_id(self):
        load = self.block('load', '''
            @data_loader
            def load(*args, **kwargs):
                return {'rows': 3}
        ''', 'data_loader')
        result = self.worker.send(self.request(load))
        self.assertEqual((result['type'], result['id'], result['ok']), ('result', 'r-load', True))
        self.assertEqual([self.value(r) for r in result['outputs']], [{'rows': 3}])
        self.assertIsInstance(result['seconds'], (int, float))
        self.assertIsInstance(result['rss_bytes'], int)

    def test_requests_are_answered_in_order_by_one_process(self):
        pid = self.block('pid', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                return os.getpid()
        ''', 'data_loader')
        first = self.worker.send(dict(self.request(pid), id='first'))
        second = self.worker.send(dict(self.request(pid), id='second'))
        self.assertEqual((first['id'], second['id']), ('first', 'second'))
        self.assertEqual(self.value(first['outputs'][0]), self.value(second['outputs'][0]))

    def test_every_kind_of_input_reaches_the_block(self):
        import numpy as np
        import pandas as pd

        records = []
        for index, value in enumerate([pd.DataFrame({'a': [1, 2]}), {'k': [1, 'x']},
                                       np.arange(3)]):
            folder = self.directory / 'in' / str(index)
            folder.mkdir(parents=True)
            records.append(python_worker.write_output(value, str(folder), 0))
        records.append(dict(kind='list', items=[records[1], records[1]]))
        records.append(dict(kind='empty'))
        describe = self.block('describe', '''
            @transformer
            def describe(frame, mapping, array, several, nothing, *args, **kwargs):
                return {'frame': frame['a'].tolist(), 'mapping': mapping,
                        'array': array.tolist(), 'several': several, 'nothing': nothing}
        ''')
        result = self.worker.send(self.request(describe, records))
        self.assertTrue(result['ok'], result)
        self.assertEqual(self.value(result['outputs'][0]), dict(
            frame=[1, 2], mapping={'k': [1, 'x']}, array=[0, 1, 2],
            several=[{'k': [1, 'x']}, {'k': [1, 'x']}], nothing=None))

    def test_a_failure_answers_its_phase_message_and_traceback_and_the_worker_goes_on(self):
        broken = self.block('broken', '''
            @transformer
            def broken(*args, **kwargs):
                raise ValueError('row 7 has no id')
        ''')
        result = self.worker.send(self.request(broken))
        self.assertFalse(result['ok'])
        self.assertEqual(result['error']['phase'], 'run')
        self.assertIn('row 7 has no id', result['error']['message'])
        self.assertIn('broken.py', result['error']['details'])
        unknown = self.worker.send(dict(id='odd', op='no-such-op'))
        self.assertEqual((unknown['id'], unknown['ok']), ('odd', False))
        fine = self.block('fine', '''
            @data_loader
            def load(*args, **kwargs):
                return 1
        ''', 'data_loader')
        self.assertTrue(self.worker.send(self.request(fine))['ok'])

    def test_a_chain_passes_values_in_memory_and_answers_the_last_outputs(self):
        # A returned list is several outputs; one value that holds a list is a dict here.
        start = self.block('start', '''
            @data_loader
            def load(*args, **kwargs):
                return {'values': [1, 2]}
        ''', 'data_loader')
        grow = self.block('grow', '''
            @transformer
            def grow(data, *args, **kwargs):
                return {'values': data['values'] + [len(data['values']) + 1]}
        ''')
        result = self.worker.send(dict(id='chain', chain=[start, grow, dict(grow, uuid='again')],
                                       inputs=[], kwargs={},
                                       output_dir=str(self.directory / 'out' / 'chain')))
        self.assertTrue(result['ok'], result)
        self.assertEqual(self.value(result['outputs'][0]), {'values': [1, 2, 3, 4]})
        self.assertEqual([b['block_uuid'] for b in result['blocks']], ['start', 'grow', 'again'])

    def test_end_markers_follow_what_the_request_printed(self):
        loud = self.block('loud', '''
            import sys

            @data_loader
            def load(*args, **kwargs):
                print('to stdout')
                print('to stderr', file=sys.stderr)
                return 1
        ''', 'data_loader')
        self.worker.send(self.request(loud, end_marker=True))
        bye = self.worker.send(dict(id='bye', op='shutdown'))
        self.assertEqual((bye['type'], bye['id']), ('bye', 'bye'))
        out, err = self.worker.close()
        self.assertEqual(self.worker.process.returncode, 0)
        marker = f'{python_worker.END_MARKER}r-loud'
        self.assertLess(out.index('to stdout'), out.index(marker))
        self.assertLess(err.index('to stderr'), err.index(marker))

    def test_a_request_does_not_change_the_next_ones_directory(self):
        move = self.block('move', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                os.chdir('/')
                return 1
        ''', 'data_loader')
        where = self.block('where', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                return os.getcwd()
        ''', 'data_loader')
        self.worker.send(self.request(move))
        result = self.worker.send(self.request(where))
        self.assertEqual(os.path.realpath(self.value(result['outputs'][0])),
                         os.path.realpath(self.directory))

    def test_it_exits_when_its_requests_end(self):
        self.worker.close()
        self.assertEqual(self.worker.process.returncode, 0)
