"""
`mage partitions`: the states of a pipeline's partitions, and queuing runs of the missing
ones, in a project made with mage init.
"""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


class PartitionsCliTest(unittest.TestCase):
    def test_states_and_running_the_missing(self):
        directory = tempfile.mkdtemp()
        env = dict(os.environ, MAGE_DATA_DIR=os.path.join(directory, 'data'),
                   DISABLE_USAGE_STATISTICS='1')
        cli = [sys.executable, '-m', 'mage_ai.cli.main']
        subprocess.run(cli + ['init', 'proj'], cwd=directory, env=env, check=True,
                       capture_output=True)
        project = Path(directory) / 'proj'
        pipeline = project / 'pipelines' / 'regions'
        pipeline.mkdir(parents=True)
        (pipeline / '__init__.py').write_text('')
        (pipeline / 'metadata.yaml').write_text(textwrap.dedent('''
            blocks: []
            name: regions
            type: python
            uuid: regions
            partitions:
              key: region
              values: [eu, us]
        '''))

        def mage(*args):
            result = subprocess.run(cli + ['partitions', str(project), 'regions', *args],
                                    env=env, capture_output=True, text=True, timeout=300)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result.stdout

        self.assertIn('regions: 2 partitions of region (2 missing).', mage())
        ran = mage('--run', 'missing')
        self.assertIn('Queued 2 partition run(s); 0 already had one.', ran)
        self.assertIn('(2 queued)', ran)
        self.assertIn('  eu  queued  run ', mage('--state', 'queued'))
