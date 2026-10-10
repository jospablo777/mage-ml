"""
Every block run starts a process that imports the scheduler's job code; where processes
start with spawn (macOS, Linux from Python 3.14) each one pays the imports again. These
packages took about 3.5 s of a 4.5 s import and only some blocks need them, so they are
imported where they are used.
"""
import json
import subprocess
import sys

from mage_ai.tests.base_test import TestCase

HEAVY = ['dbt', 'sklearn', 'jsonschema', 'sqlglot', 'IPython', 'memory_profiler',
         'jupyter_client', 'aiohttp']


class ImportCostTest(TestCase):
    def test_the_job_code_does_not_import_packages_few_blocks_need(self):
        code = (
            'import json, sys\n'
            'import mage_ai.orchestration.pipeline_scheduler_original\n'
            'import mage_ai.data_preparation.executors.block_executor\n'
            f'print(json.dumps([m for m in {HEAVY!r} if m in sys.modules]))\n'
        )
        result = subprocess.run(
            [sys.executable, '-c', code], capture_output=True, text=True, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        self.assertEqual(json.loads(result.stdout.strip().splitlines()[-1]), [])
