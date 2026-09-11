import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ContainerStartupTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='mage-startup-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.project = self.root / 'project with spaces'
        self.project.mkdir()
        self.commands = self.root / 'commands.jsonl'
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        # The interpreter is passed through the environment because an interpreter
        # path containing spaces cannot be used in a shebang.
        recorder = (
            'import json, os, sys\n'
            'argv = sys.argv[1:]\n'
            'with open(os.environ["STARTUP_COMMANDS"], "a") as output:\n'
            '    output.write(json.dumps(argv) + "\\n")\n'
            'sys.exit(int(os.environ.get("STARTUP_EXIT_" + '
            'os.path.basename(argv[0]).upper(), "0")))\n'
        )
        for name in ('uv', 'mage'):
            executable = self.bin / name
            executable.write_text(
                '#!/bin/sh\n'
                f'exec "$STARTUP_PYTHON" -c \'{recorder}\' "$0" "$@"\n'
            )
            executable.chmod(0o755)
        self.script = Path(__file__).resolve().parents[2] / 'scripts' / 'run_app.sh'
        self.env = {
            'PATH': f'{self.bin}{os.pathsep}{os.environ["PATH"]}',
            'USER_CODE_PATH': str(self.project),
            'STARTUP_COMMANDS': str(self.commands),
            'STARTUP_PYTHON': sys.executable,
        }

    def run_startup(self, *args):
        return subprocess.run(
            ['bash', str(self.script), *args],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=10,
        )

    def read_commands(self):
        return [json.loads(line) for line in self.commands.read_text().splitlines()]

    def test_requirements_respect_runtime_constraints(self):
        (self.project / 'requirements.txt').write_text('example-package==1.0\n')
        constraints = self.root / 'runtime constraints.txt'
        constraints.write_text('example-package==1.0\n')
        self.env['MAGE_RUNTIME_CONSTRAINTS'] = str(constraints)
        result = self.run_startup()
        self.assertEqual(result.returncode, 0, result.stderr)
        install, check, start = self.read_commands()
        self.assertEqual(install[1:3], ['pip', 'install'])
        self.assertEqual(install[install.index('--constraint') + 1], str(constraints))
        self.assertEqual(
            install[install.index('--requirement') + 1],
            str(self.project / 'requirements.txt'),
        )
        self.assertEqual(check[1:3], ['pip', 'check'])
        self.assertEqual(start[1:3], ['start', str(self.project)])

    def test_failed_dependency_install_stops_startup(self):
        (self.project / 'requirements.txt').write_text('example-package==1.0\n')
        self.env['STARTUP_EXIT_UV'] = '17'
        result = self.run_startup()
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(self.read_commands()), 1)

    def test_custom_command_preserves_arguments_and_exit_status(self):
        self.env['STARTUP_EXIT_MAGE'] = '23'
        result = self.run_startup('mage', 'run', 'argument with spaces')
        self.assertEqual(result.returncode, 23)
        self.assertEqual(self.read_commands()[0][1:], ['run', 'argument with spaces'])
