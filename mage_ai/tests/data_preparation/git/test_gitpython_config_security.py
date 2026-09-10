"""
Regression and compatibility tests for the GitPython 3.1.62 upgrade.

Security context: CVE-2026-78676 (CVSS 9.3) covers GitPython <= 3.1.58, where
git-config values were written without escaping. A value holding a newline was
serialized verbatim, so the text after the newline became a live config
directive. Setting `core.hooksPath` that way gives arbitrary code execution the
next time git runs a hook.

Mage reaches this path in `mage_ai/data_preparation/git/__init__.py`:

    user.name / user.email   <- taken from the user's Git settings
    safe.directory           <- written into the *global* ~/.gitconfig
    .gitmodules sections     <- rewritten during submodule sync

These tests pin the fixed behaviour so a future downgrade or a resolver drift
back below 3.1.59 fails CI instead of shipping.
"""
import os
import subprocess
import tempfile
import unittest

import git
from git.config import GitConfigParser

# Text that turns into an executable git directive if it is written unescaped.
HOOKS_PATH_PAYLOAD = 'Josep\n[core]\n\thooksPath = /tmp/mage_should_not_run'


def _git(*args, cwd):
    # Pin identity on the command line so the test never reads or writes the
    # developer's real global config.
    subprocess.run(
        ['git', '-c', 'user.name=t', '-c', 'user.email=t@t.t', *args],
        cwd=cwd, check=True, capture_output=True,
    )


class GitPythonConfigInjectionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmp, 'config')
        with open(self.config_path, 'w') as f:
            f.write('[user]\n\temail = a@b.c\n')

    def _sections(self):
        parser = GitConfigParser(self.config_path, read_only=True)
        try:
            return parser.sections()
        finally:
            parser.release()

    def test_set_value_rejects_newline_in_value(self):
        # The direct CVE-2026-78676 path. On 3.1.41 this call succeeded and
        # wrote a live [core] section carrying hooksPath.
        writer = GitConfigParser(self.config_path, read_only=False)
        with self.assertRaises(ValueError):
            writer.set_value('user', 'name', HOOKS_PATH_PAYLOAD)
        writer.release()

        self.assertNotIn('core', self._sections())

    def test_set_value_rejects_carriage_return_and_nul(self):
        for payload in ['a\r[core]', 'a\x00[core]', 'a\r\n[core]']:
            with self.subTest(payload=payload):
                writer = GitConfigParser(self.config_path, read_only=False)
                with self.assertRaises(ValueError):
                    writer.set_value('user', 'name', payload)
                writer.release()

        self.assertNotIn('core', self._sections())

    def test_safe_directory_rejects_poisoned_repo_path(self):
        # Mage writes safe.directory into the global ~/.gitconfig. A repo path
        # carrying a newline must not be able to append directives there.
        poisoned = '/tmp/repo\n[core]\n\thooksPath = /tmp/mage_should_not_run'
        writer = GitConfigParser(self.config_path, read_only=False)
        with self.assertRaises(ValueError):
            writer.set_value('safe', 'directory', poisoned)
        writer.release()

        self.assertNotIn('core', self._sections())

    def test_rewrite_does_not_activate_dormant_multiline_value(self):
        # A quoted value spanning lines is inert until something rewrites the
        # file. An unrelated write must re-escape it rather than flatten it into
        # new directives.
        with open(self.config_path, 'w') as f:
            f.write('[user]\n\tname = "harmless\\n[core]\\n\\thooksPath = /tmp/evil"\n')

        writer = GitConfigParser(self.config_path, read_only=False)
        writer.set_value('safe', 'directory', '/some/repo').release()

        self.assertNotIn('core', self._sections())

        parser = GitConfigParser(self.config_path, read_only=True)
        try:
            # The value survives the round trip unchanged instead of being split.
            self.assertEqual(
                parser.get_value('user', 'name'),
                'harmless\n[core]\n\thooksPath = /tmp/evil',
            )
            self.assertEqual(parser.get_value('safe', 'directory'), '/some/repo')
        finally:
            parser.release()

    def test_option_name_with_injection_is_rejected(self):
        # `Git.update_config` splits a caller-supplied key on '.' and passes the
        # parts straight through as section and option.
        writer = GitConfigParser(self.config_path, read_only=False)
        with self.assertRaises(ValueError):
            writer.set_value('user', 'name\n[core]\nhooksPath', '/tmp/evil')
        writer.release()

        self.assertNotIn('core', self._sections())


class GitPythonCompatibilityTest(unittest.TestCase):
    """
    Smoke tests for the GitPython APIs Mage depends on. Section 3.2 of the
    modernization plan lists these as the surfaces to verify on every bump.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _make_repo(self):
        path = os.path.join(self.tmp, 'source')
        os.makedirs(path)
        _git('init', '-q', cwd=path)
        with open(os.path.join(path, 'README.md'), 'w') as f:
            f.write('mage\n')
        _git('add', 'README.md', cwd=path)
        _git('commit', '-qm', 'initial', cwd=path)
        return path

    def test_clone_from_local_path(self):
        source = self._make_repo()
        target = os.path.join(self.tmp, 'clone')

        repo = git.Repo.clone_from(source, target)
        try:
            self.assertTrue(os.path.exists(os.path.join(target, 'README.md')))
            self.assertIsNotNone(repo.head.commit)
        finally:
            repo.close()

    def test_config_writer_set_value_round_trip(self):
        repo = git.Repo(self._make_repo())
        try:
            # Same call shape as Git.__set_git_config.
            repo.config_writer().set_value('user', 'name', 'Josep').release()
            repo.config_writer().set_value('user', 'email', 'j@example.com').release()

            reader = repo.config_reader()
            self.assertEqual(reader.get_value('user', 'name'), 'Josep')
            self.assertEqual(reader.get_value('user', 'email'), 'j@example.com')
        finally:
            repo.close()

    def test_config_writer_remove_section(self):
        # Used by the submodule restore path.
        repo = git.Repo(self._make_repo())
        try:
            repo.config_writer().set_value('submodule "sub"', 'url', 'https://x/y').release()

            writer = repo.config_writer()
            writer.remove_section('submodule "sub"')
            writer.release()

            self.assertNotIn('submodule "sub"', repo.config_reader().sections())
        finally:
            repo.close()

    def test_gitmodules_parser_read_and_write(self):
        # Mirrors update_gitmodules: quoted subsection names must still be
        # accepted by the 3.1.62 section-name validation.
        path = os.path.join(self.tmp, '.gitmodules')
        with open(path, 'w') as f:
            f.write(
                '[submodule "libs/shared"]\n'
                '\tpath = libs/shared\n'
                '\turl = https://old/repo.git\n'
            )

        parser = GitConfigParser(path, read_only=True)
        try:
            self.assertEqual(parser.sections(), ['submodule "libs/shared"'])
            self.assertEqual(parser.get('submodule "libs/shared"', 'path'), 'libs/shared')
        finally:
            parser.release()

        writer = GitConfigParser(path, read_only=False)
        writer.set('submodule "libs/shared"', 'url', 'https://new/repo.git')
        writer.release()

        parser = GitConfigParser(path, read_only=True)
        try:
            self.assertEqual(parser.get('submodule "libs/shared"', 'url'), 'https://new/repo.git')
            self.assertEqual(parser.get('submodule "libs/shared"', 'path'), 'libs/shared')
        finally:
            parser.release()

    def test_installed_version_is_patched(self):
        # CVE-2026-78676 is fixed in 3.1.59. Guard against a resolver drift.
        major, minor, patch = (int(p) for p in git.__version__.split('.')[:3])
        self.assertEqual((major, minor), (3, 1))
        self.assertGreaterEqual(patch, 59)


if __name__ == '__main__':
    unittest.main()
