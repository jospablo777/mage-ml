"""
Contracts for the third-party libraries Mage calls directly.

Mage 0.9.79 pinned these to exact versions. This fork uses floors instead, so
the resolver can move and the APIs Mage depends on need coverage.

Web server coverage lives in mage_ai/tests/server/test_server_http_smoke.py.
"""
import re
import unittest
from datetime import datetime, timedelta
from importlib.metadata import version

from packaging.version import Version

# rich renders the help in color whenever it believes it is attached to a
# terminal, and it treats GitHub Actions as one. Colored output splits an option
# name across escape sequences, so "--host" arrives as "-", an escape, "-host".
ANSI_ESCAPE = re.compile(r'\x1b\[[0-9;]*m')

# Versions below these carry known CVEs. The downstream service used to enforce
# them with override-dependencies.
SECURITY_FLOORS = {
    'aiohttp': '3.14.3',
    'click': '8.3.3',
    'cryptography': '50.0.0',
    'GitPython': '3.1.59',
    'Jinja2': '3.1.6',
    'jupyter-server': '2.20.0',
    'PyJWT': '2.13.0',
    'requests': '2.33.0',
    'setuptools': '83.0.0',
    'tornado': '6.5.8',
    'typer': '0.18',
    'Werkzeug': '3.1.6',
}


class SecurityFloorTest(unittest.TestCase):
    def test_installed_versions_meet_floors(self):
        for package, floor in sorted(SECURITY_FLOORS.items()):
            with self.subTest(package=package):
                installed = version(package)
                self.assertGreaterEqual(
                    Version(installed),
                    Version(floor),
                    '%s %s is below the %s security floor' % (package, installed, floor),
                )


class CliContractTest(unittest.TestCase):
    """typer and click back the whole CLI, including `mage start`."""

    def setUp(self):
        import typer.main
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        self.app = app
        # Registered names, not callback names. typer converts underscores to
        # dashes, so `clean_cached_variables` is exposed as clean-cached-variables.
        self.command_names = list(typer.main.get_command(app).commands)
        self.runner = CliRunner()

    def help_for(self, *arguments):
        result = self.runner.invoke(self.app, [*arguments, '--help'])

        self.assertEqual(result.exit_code, 0, result.output)

        return ANSI_ESCAPE.sub('', result.output)

    def test_root_help_succeeds(self):
        self.assertIn('Usage', self.help_for())

    def test_command_names_are_stable(self):
        # These are the public CLI surface. Renaming one breaks every caller.
        self.assertEqual(
            self.command_names,
            ['init', 'start', 'run', 'clean-cached-variables', 'clean-old-logs',
             'create-spark-cluster'],
        )

    def test_every_command_parses(self):
        # --help returns before the command body runs, so this covers argument
        # declaration and parsing without starting a server.
        for name in self.command_names:
            with self.subTest(command=name):
                self.help_for(name)

    def test_start_accepts_its_options(self):
        # The options every deployment passes.
        output = self.help_for('start')

        for option in ['--host', '--port', '--manage-instance', '--instance-type']:
            with self.subTest(option=option):
                self.assertIn(option, output)

    def test_typer_group_subclass_still_works(self):
        # mage_ai/cli/main.py subclasses TyperGroup to control command order.
        from typer.core import TyperGroup

        from mage_ai.cli.main import OrderCommands

        self.assertTrue(issubclass(OrderCommands, TyperGroup))

    def test_unknown_command_is_rejected(self):
        result = self.runner.invoke(self.app, ['not-a-real-command'])

        self.assertNotEqual(result.exit_code, 0)


class AuthenticationContractTest(unittest.TestCase):
    """PyJWT and bcrypt back sign-in and API tokens."""

    def test_jwt_token_round_trip(self):
        from mage_ai.authentication.oauth2 import decode_token, encode_token

        expires = datetime.utcnow() + timedelta(days=1)
        encoded = encode_token('some-token', expires)

        self.assertIsInstance(encoded, str)

        decoded = decode_token(encoded)
        self.assertEqual(decoded['token'], 'some-token')
        self.assertAlmostEqual(decoded['expires'], expires.timestamp(), places=3)

    def test_password_hash_round_trip(self):
        from mage_ai.authentication.passwords import (
            create_bcrypt_hash,
            generate_salt,
            verify_password,
        )

        salt = generate_salt()
        hashed = create_bcrypt_hash('correct horse battery staple', salt)

        self.assertTrue(verify_password('correct horse battery staple', hashed))
        self.assertFalse(verify_password('wrong password', hashed))


class TemplatingContractTest(unittest.TestCase):
    def test_jinja2_template_renders(self):
        # `from jinja2 import Template` is the form used in settings loading,
        # dbt caching and configuration options.
        from jinja2 import Template

        rendered = Template('{{ greeting }} {{ name }}').render(greeting='hello', name='mage')

        self.assertEqual(rendered, 'hello mage')

    def test_jinja2_callable_interpolation(self):
        # Repo settings are rendered with env_var() passed in as a callable.
        from jinja2 import Template

        rendered = Template('project: {{ env_var("MAGE_TEST_VAR") }}').render(
            env_var=lambda _name: 'value-from-env',
        )

        self.assertEqual(rendered, 'project: value-from-env')


class HttpClientContractTest(unittest.TestCase):
    def test_aiohttp_client_session_surface(self):
        # Used in project resources and usage statistics.
        import aiohttp

        self.assertTrue(hasattr(aiohttp, 'ClientSession'))
        self.assertTrue(hasattr(aiohttp.ClientSession, 'get'))
        self.assertTrue(hasattr(aiohttp.ClientSession, 'post'))
        self.assertTrue(hasattr(aiohttp, 'ClientError'))


class TornadoContractTest(unittest.TestCase):
    def test_surfaces_mage_subclasses(self):
        # Subclassed directly in mage_ai/server. Losing one breaks server import.
        import tornado.web
        import tornado.websocket

        for attr in ['Application', 'RequestHandler', 'StaticFileHandler', 'HTTPError']:
            with self.subTest(attr=attr):
                self.assertTrue(hasattr(tornado.web, attr))

        self.assertTrue(hasattr(tornado.websocket, 'WebSocketHandler'))


if __name__ == '__main__':
    unittest.main()
