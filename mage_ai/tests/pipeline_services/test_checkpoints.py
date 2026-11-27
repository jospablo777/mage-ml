"""
Checkpoints inside long blocks (runtime/checkpoints.py): progress saved in one attempt of a
block run is there for the next, atomically, as JSON, and gone once the block completes.
"""
import io
import json
import os
import tempfile
import textwrap

from mage_ai.pipeline_services.runtime import worker
from mage_ai.pipeline_services.runtime.checkpoints import (
    MAX_BYTES,
    Checkpoint,
    CheckpointError,
)
from mage_ai.tests.base_test import TestCase


class CheckpointTest(TestCase):
    def test_a_saved_value_is_there_for_the_next_attempt(self):
        directory = os.path.join(tempfile.mkdtemp(), 'block')
        first = Checkpoint(directory)
        self.assertEqual(first.load('page', 0), 0)
        first.save('page', 7)
        first.save('done', ['a.csv', 'b.csv'])

        # The next attempt is another process with the same folder.
        following = Checkpoint(directory)
        self.assertEqual(following.load('page', 0), 7)
        self.assertEqual(following.load('done'), ['a.csv', 'b.csv'])
        self.assertEqual(following.keys(), ['done', 'page'])
        self.assertFalse([n for n in os.listdir(directory) if n.endswith('.tmp')])

        following.clear()
        self.assertEqual(Checkpoint(directory).load('page'), None)

    def test_values_are_json_progress_and_keys_are_file_names(self):
        checkpoint = Checkpoint(tempfile.mkdtemp())
        with self.assertRaisesRegex(CheckpointError, 'takes a JSON value'):
            checkpoint.save('frame', object())
        with self.assertRaisesRegex(CheckpointError, 'takes a JSON value'):
            checkpoint.save('nan', float('nan'))
        with self.assertRaisesRegex(CheckpointError, 'over 1 MB'):
            checkpoint.save('big', 'x' * MAX_BYTES)
        for key in ('', '../escape', '.hidden', 'a/b', 'x' * 201):
            with self.assertRaisesRegex(CheckpointError, 'A checkpoint key'):
                checkpoint.save(key, 1)

    def test_a_failed_save_keeps_the_value_saved_before(self):
        directory = tempfile.mkdtemp()
        checkpoint = Checkpoint(directory)
        checkpoint.save('page', 3)
        original = os.replace

        def killed(*args, **kwargs):
            raise KeyboardInterrupt

        os.replace = killed
        try:
            with self.assertRaises(KeyboardInterrupt):
                checkpoint.save('page', 4)
        finally:
            os.replace = original
        self.assertEqual(Checkpoint(directory).load('page'), 3)
        self.assertEqual(os.listdir(directory), ['page.json'])

    def test_without_a_folder_it_lasts_for_the_execution(self):
        checkpoint = Checkpoint(None)
        checkpoint.save('n', 1)
        self.assertEqual(checkpoint.load('n'), 1)
        self.assertEqual(Checkpoint(None).load('n'), None)


class WorkerCheckpointTest(TestCase):
    def test_a_service_block_resumes_from_its_last_checkpoint(self):
        directory = tempfile.mkdtemp()
        path = os.path.join(directory, 'pages.py')
        with open(path, 'w') as file:
            file.write(textwrap.dedent('''
                @data_loader
                def load(*args, **kwargs):
                    checkpoint = kwargs['checkpoint']
                    start = checkpoint.load('page', 0)
                    for page in range(start, 5):
                        checkpoint.save('page', page + 1)
                        if page == 2 and start == 0:
                            raise RuntimeError('the source went away')
                    return {'started_at': start}
            '''))
        block = dict(uuid='folder/pages', type='data_loader', file=path)
        root = os.path.join(directory, 'checkpoints')
        request = dict(op='run', block=block, inputs=[], checkpoint_root=root,
                       output_dir=os.path.join(directory, 'out'))
        requests = io.StringIO('\n'.join(
            json.dumps(dict(request, id=str(n))) for n in (1, 2)) + '\n')
        channel = io.StringIO()

        worker.serve(requests, channel)

        replies = [json.loads(line) for line in channel.getvalue().splitlines()][1:]
        self.assertEqual([r['ok'] for r in replies], [False, True])
        output = replies[1]['outputs'][0]
        with open(output['path']) as file:
            self.assertEqual(json.load(file), {'started_at': 3})
        # The engine's name for the block's folder.
        self.assertEqual(os.listdir(root), ['folder__pages'])
