"""
The header shows an Update badge whenever the reported latest version differs from
the running one. This fork carries a local version segment and can be ahead of the
release it was forked from, so the reported version has to account for that.
"""
import unittest
from unittest.mock import patch

from mage_ai.server.constants import VERSION
from mage_ai.server.version_check import (
    fetch_published_version,
    get_latest_version,
    newer_version,
    update_check_enabled,
)


def run(coroutine):
    import asyncio

    return asyncio.run(coroutine)


class NewerVersionTests(unittest.TestCase):
    def test_a_newer_release_is_reported(self):
        self.assertEqual(newer_version('0.9.79', '0.9.80'), '0.9.80')
        self.assertEqual(newer_version('0.9.79+ml.2', '0.10.0'), '0.10.0')

    def test_an_older_release_is_ignored(self):
        self.assertEqual(newer_version('0.9.80', '0.9.79'), '0.9.80')

    def test_a_local_build_counts_as_ahead_of_its_base_release(self):
        """This is the case that kept the badge on permanently."""
        self.assertEqual(newer_version('0.9.79+ml.2', '0.9.79'), '0.9.79+ml.2')

    def test_a_later_local_build_beats_an_earlier_one(self):
        self.assertEqual(newer_version('0.9.79+ml.2', '0.9.79+ml.3'), '0.9.79+ml.3')
        self.assertEqual(newer_version('0.9.79+ml.3', '0.9.79+ml.2'), '0.9.79+ml.3')

    def test_the_same_version_is_not_an_update(self):
        self.assertEqual(newer_version('0.9.79+ml.2', '0.9.79+ml.2'), '0.9.79+ml.2')

    def test_an_unusable_candidate_is_ignored(self):
        for candidate in (None, '', 'latest', 'not-a-version'):
            with self.subTest(candidate=candidate):
                self.assertEqual(newer_version('0.9.79+ml.2', candidate), '0.9.79+ml.2')


class GetLatestVersionTests(unittest.TestCase):
    def published(self, version):
        return patch(
            'mage_ai.server.version_check.fetch_published_version',
            return_value=version,
        )

    def test_returns_the_installed_version_when_nothing_newer_is_published(self):
        with self.published('0.9.79'):
            self.assertEqual(run(get_latest_version()), VERSION)

    def test_returns_a_newer_published_version(self):
        with self.published('99.0.0'):
            self.assertEqual(run(get_latest_version()), '99.0.0')

    def test_returns_the_installed_version_when_the_lookup_fails(self):
        with self.published(None):
            self.assertEqual(run(get_latest_version()), VERSION)

    def test_the_check_can_be_turned_off(self):
        with patch('mage_ai.server.version_check.update_check_enabled', return_value=False):
            with patch('mage_ai.server.version_check.fetch_published_version') as fetch:
                self.assertEqual(run(get_latest_version()), VERSION)
                fetch.assert_not_called()

    def test_an_offline_lookup_returns_nothing_rather_than_raising(self):
        with patch(
            'mage_ai.server.version_check.aiohttp.ClientSession',
            side_effect=OSError('no network'),
        ):
            self.assertIsNone(run(fetch_published_version()))


class UpdateCheckSettingTests(unittest.TestCase):
    def test_enabled_by_default(self):
        with patch.dict('os.environ', {}, clear=False):
            import os

            os.environ.pop('MAGE_UPDATE_CHECK_ENABLED', None)
            self.assertTrue(update_check_enabled())

    def test_disabled_by_the_environment(self):
        for value in ('0', 'false', 'False', 'no'):
            with self.subTest(value=value):
                with patch.dict('os.environ', {'MAGE_UPDATE_CHECK_ENABLED': value}):
                    self.assertFalse(update_check_enabled())


if __name__ == '__main__':
    unittest.main()
