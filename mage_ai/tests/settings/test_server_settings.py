"""Settings read from environment variables when mage_ai.settings.server is imported."""
import os
import subprocess
import sys
import unittest


def setting(name: str, value):
    env = {k: v for k, v in os.environ.items() if k != name}
    if value is not None:
        env[name] = value
    out = subprocess.run(
        [sys.executable, '-c', f'from mage_ai.settings import server; print(server.{name})'],
        capture_output=True, text=True, env=env, timeout=120,
    )
    assert out.returncode == 0, out.stderr[-2000:]
    return out.stdout.strip().splitlines()[-1]


class HideEnvVarValuesTest(unittest.TestCase):
    def test_on_by_default_and_off_with_zero_or_false(self):
        """HIDE_ENV_VAR_VALUES=0 read as 0 or 1, which is 1: it could not be turned off."""
        self.assertEqual(setting('HIDE_ENV_VAR_VALUES', None), 'True')
        self.assertEqual(setting('HIDE_ENV_VAR_VALUES', ''), 'True')
        self.assertEqual(setting('HIDE_ENV_VAR_VALUES', '1'), 'True')
        self.assertEqual(setting('HIDE_ENV_VAR_VALUES', '0'), 'False')
        self.assertEqual(setting('HIDE_ENV_VAR_VALUES', 'false'), 'False')


if __name__ == '__main__':
    unittest.main()
