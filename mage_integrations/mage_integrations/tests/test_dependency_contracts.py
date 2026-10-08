import pathlib
import re
import unittest

import pendulum

SOURCE_ROOT = pathlib.Path(__file__).resolve().parents[1]
PENDULUM_CALL = re.compile(r'\bpendulum\.([A-Za-z_]+)\b')


class PendulumApiTest(unittest.TestCase):
    def test_source_calls_only_functions_pendulum_provides(self):
        # pendulum 3 removed utcnow(), and the Facebook Ads source called it on a path
        # no other test reaches.
        missing = []
        for path in SOURCE_ROOT.rglob('*.py'):
            if 'tests' in path.relative_to(SOURCE_ROOT).parts:
                continue
            text = path.read_text(encoding='utf-8', errors='ignore')
            for name in sorted(set(PENDULUM_CALL.findall(text))):
                if not hasattr(pendulum, name):
                    missing.append('%s: pendulum.%s' % (path.relative_to(SOURCE_ROOT), name))

        self.assertEqual(missing, [])


if __name__ == '__main__':
    unittest.main()
