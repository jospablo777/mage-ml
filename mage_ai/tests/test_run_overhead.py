import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'run_overhead', ROOT / 'scripts' / 'ci' / 'run_overhead.py',
)
run_overhead = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(run_overhead)


class RunOverheadTest(TestCase):
    def test_a_slower_run_past_the_limit_and_the_floor_regresses(self):
        result = run_overhead.verdict([50, 51, 52], [60, 61, 62])
        self.assertEqual((result['base_ms'], result['head_ms']), (51, 61))
        self.assertTrue(result['regressed'])

    def test_noise_within_the_limit_passes(self):
        self.assertFalse(run_overhead.verdict([50, 51, 52], [53, 54, 55])['regressed'])

    def test_a_large_share_of_a_tiny_time_is_under_the_floor(self):
        # 3 ms more on 10 ms is 30%, but under the 5 ms floor.
        self.assertFalse(run_overhead.verdict([10, 10, 10], [13, 13, 13])['regressed'])

    def test_the_tiny_pipeline_runs(self):
        # In its own process: measuring sets the repository path of the process.
        with tempfile.TemporaryDirectory() as folder:
            out = Path(folder) / 'result.json'
            subprocess.run(
                [sys.executable, SPEC.origin, 'measure', '--runs', '1', '--out', str(out)],
                check=True, cwd=folder, capture_output=True,
            )
            result = json.loads(out.read_text())
        self.assertEqual(len(result['runs_ms']), 1)
        self.assertGreater(result['median_ms'], 0)
