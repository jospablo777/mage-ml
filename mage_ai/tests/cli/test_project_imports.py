"""
Project code is importable in mage run as in the notebook and in exported services: the
CLI put only the project's parent on the path, so `from utils.helpers import ...` in a
block failed with No module named 'utils' on the first mage run.
"""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


class ProjectImportsTest(unittest.TestCase):
    def test_a_block_imports_project_code_by_its_folder_name(self):
        directory = tempfile.mkdtemp()
        env = dict(os.environ, MAGE_DATA_DIR=os.path.join(directory, 'data'),
                   DISABLE_USAGE_STATISTICS='1')
        cli = [sys.executable, '-m', 'mage_ai.cli.main']
        subprocess.run(cli + ['init', 'proj'], cwd=directory, env=env, check=True,
                       capture_output=True)
        project = Path(directory) / 'proj'
        (project / 'utils').mkdir(exist_ok=True)
        (project / 'utils' / '__init__.py').write_text('')
        (project / 'utils' / 'helpers.py').write_text('ANSWER = 42\n')
        (project / 'data_loaders' / 'load_answer.py').write_text(textwrap.dedent('''
            import pandas as pd
            from utils.helpers import ANSWER

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'answer': [ANSWER]})
        '''))
        pipeline = project / 'pipelines' / 'answers'
        pipeline.mkdir(parents=True)
        (pipeline / '__init__.py').write_text('')
        (pipeline / 'metadata.yaml').write_text(textwrap.dedent('''
            blocks:
            - downstream_blocks: []
              language: python
              name: load_answer
              type: data_loader
              upstream_blocks: []
              uuid: load_answer
            name: answers
            type: python
            uuid: answers
        '''))
        # Run from elsewhere: the working folder is not the project.
        result = subprocess.run(cli + ['run', str(project), 'answers'], cwd='/', env=env,
                                capture_output=True, text=True, timeout=300)
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-3000:])
        self.assertIn('Pipeline run completed.', result.stdout)

    def test_an_installed_package_keeps_its_name(self):
        from mage_ai.settings.repo import add_project_to_sys_path

        before = list(sys.path)
        try:
            add_project_to_sys_path('/tmp/some_project')
            self.assertEqual(sys.path[-2:], ['/tmp', '/tmp/some_project'])
            # Already on the path: not added twice.
            add_project_to_sys_path('/tmp/some_project')
            self.assertEqual(sys.path.count('/tmp/some_project'), 1)
        finally:
            sys.path[:] = before
