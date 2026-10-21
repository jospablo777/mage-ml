"""
The header shows an Update badge whenever the reported latest version differs from
the running one. This fork carries a local version segment and can be ahead of the
release it was forked from, so the reported version has to account for that.
"""
import unittest
from unittest.mock import patch

from mage_ai.server import version_check
from mage_ai.server.constants import VERSION
from mage_ai.server.version_check import (
    fetch_published_version,
    get_latest_version,
    newer_version,
    refresh_latest_version,
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
    def setUp(self):
        version_check._latest.update(version=None, checked=0.0, task=None)

    def published(self, version):
        return patch(
            'mage_ai.server.version_check.fetch_published_version',
            return_value=version,
        )

    def latest_after_refresh(self):
        async def check():
            await refresh_latest_version()
            return await get_latest_version()

        return run(check())

    def test_returns_the_installed_version_when_nothing_newer_is_published(self):
        with self.published('0.0.9'):
            self.assertEqual(self.latest_after_refresh(), VERSION)

    def test_returns_a_newer_published_version(self):
        with self.published('99.0.0'):
            self.assertEqual(self.latest_after_refresh(), '99.0.0')

    def test_returns_the_installed_version_when_the_lookup_fails(self):
        with self.published(None):
            self.assertEqual(self.latest_after_refresh(), VERSION)

    def test_a_failed_lookup_keeps_the_release_found_before(self):
        with self.published('99.0.0'):
            self.latest_after_refresh()
        with self.published(None):
            self.assertEqual(self.latest_after_refresh(), '99.0.0')

    def test_a_request_never_waits_for_github(self):
        # Every request that read the project waited for GitHub, up to 3 s each, and the
        # whole timeout without internet access.
        import asyncio
        import time

        async def slow(*args, **kwargs):
            await asyncio.sleep(5)
            return '99.0.0'

        async def check():
            with patch('mage_ai.server.version_check.fetch_published_version', slow):
                started = time.monotonic()
                first = await get_latest_version()
                second = await get_latest_version()
                elapsed = time.monotonic() - started
                version_check._latest['task'].cancel()
                return first, second, elapsed

        first, second, elapsed = run(check())
        self.assertEqual((first, second), (VERSION, VERSION))
        self.assertLess(elapsed, 0.5)

    def test_the_check_runs_again_only_when_due(self):
        async def check():
            await get_latest_version()
            first = version_check._latest['task']
            await first
            await get_latest_version()
            return first, version_check._latest['task']

        with self.published('99.0.0'):
            first, second = run(check())
        self.assertIs(first, second)

    def test_the_check_can_be_turned_off(self):
        with patch('mage_ai.server.version_check.update_check_enabled', return_value=False):
            with patch('mage_ai.server.version_check.fetch_published_version') as fetch:
                self.assertEqual(run(get_latest_version()), VERSION)
                fetch.assert_not_called()

    def test_the_latest_github_release_tag_is_the_published_version(self):
        from unittest.mock import AsyncMock, MagicMock

        def session_answering(status, payload):
            response = MagicMock(status=status)
            response.json = AsyncMock(return_value=payload)
            get = MagicMock()
            get.__aenter__ = AsyncMock(return_value=response)
            get.__aexit__ = AsyncMock(return_value=False)
            session = MagicMock()
            session.get = MagicMock(return_value=get)
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=False)
            return session

        target = 'mage_ai.server.version_check.client_session'
        with patch(target, return_value=session_answering(200, {'tag_name': 'v0.9.79+ml.6'})) \
                as client:
            self.assertEqual(run(fetch_published_version('owner/repo')), '0.9.79+ml.6')
        url = client.return_value.get.call_args.args[0]
        self.assertEqual(url, 'https://api.github.com/repos/owner/repo/releases/latest')
        # No release yet: GitHub answers 404.
        with patch(target, return_value=session_answering(404, {'message': 'Not Found'})):
            self.assertIsNone(run(fetch_published_version('owner/repo')))

    def test_an_offline_lookup_returns_nothing_rather_than_raising(self):
        with patch(
            'mage_ai.server.version_check.client_session',
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


class ClientSessionTests(unittest.TestCase):
    def test_sessions_use_proxy_settings_and_a_short_timeout(self):
        """Sign-in and update checks ignored HTTPS_PROXY (upstream #5748)."""
        from mage_ai.shared.http_client import client_session

        async def make():
            async with client_session() as session:
                return session.trust_env, session.timeout.total

        self.assertEqual(run(make()), (True, 30))

if __name__ == '__main__':
    unittest.main()
