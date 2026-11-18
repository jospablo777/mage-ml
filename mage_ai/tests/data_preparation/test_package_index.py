import io
import json
import urllib.error
from unittest import TestCase
from unittest.mock import patch

from mage_ai.data_preparation import package_index

SIMPLE = {
    'name': 'mlflow',
    'versions': ['2.8.1', '3.15.0', '3.16.0rc1', '3.16.0', '3.15.1'],
    'files': [
        {'filename': 'mlflow-2.8.1-py3-none-any.whl', 'yanked': False,
         'upload-time': '2023-11-09T10:00:00Z'},
        {'filename': 'mlflow-2.8.1.tar.gz', 'yanked': False, 'upload-time': '2023-11-09T09:58:00Z'},
        {'filename': 'mlflow-3.15.1-py3-none-any.whl', 'yanked': 'broken metadata'},
        {'filename': 'mlflow-3.15.1.tar.gz', 'yanked': True},
        {'filename': 'mlflow-3.16.0-py3-none-any.whl', 'yanked': False},
    ],
}


class PackageIndexTest(TestCase):
    def setUp(self):
        package_index._cache.clear()

    def answer(self, payload=None, error=None):
        def urlopen(request, timeout):
            self.requests.append((request.full_url, request.headers.get('Accept')))
            if error:
                raise error
            return io.BytesIO(json.dumps(payload).encode())

        self.requests = []
        return patch.object(package_index.urllib.request, 'urlopen', side_effect=urlopen)

    def test_releases_are_newest_first_without_yanked_ones_and_latest_is_final(self):
        with self.answer(SIMPLE):
            found = package_index.releases('MLflow')
            package_index.releases('mlflow')
        self.assertEqual(found['versions'], ['3.16.0', '3.15.0', '2.8.1'])
        self.assertEqual(found['latest'], '3.16.0')
        self.assertEqual(found['prereleases'], ['3.16.0rc1'])
        # PEP 700: each version's first upload, where the index says.
        self.assertEqual(found['uploaded'], {'2.8.1': '2023-11-09T09:58:00Z'})
        self.assertEqual(self.requests, [('https://pypi.org/simple/mlflow/',
                                          'application/vnd.pypi.simple.v1+json')],
                         'one request: the answer is kept')

    def test_the_index_uv_uses_is_read_and_named_without_credentials(self):
        index = 'https://user:token@pkgs.example.com/simple/'
        with patch.dict('os.environ', {'UV_DEFAULT_INDEX': index}), \
                self.answer(error=urllib.error.HTTPError('u', 404, 'missing', {}, None)), \
                self.assertRaises(package_index.PackageIndexError) as raised:
            package_index.releases('mlflwo')
        self.assertEqual(self.requests[0][0], 'https://user:token@pkgs.example.com/simple/mlflwo/')
        self.assertEqual(str(raised.exception), 'No package named mlflwo on pkgs.example.com.')
        self.assertTrue(raised.exception.missing)

    def test_an_unreachable_index_and_a_bad_name_say_so(self):
        with self.answer(error=urllib.error.URLError('dns error')), \
                self.assertRaisesRegex(package_index.PackageIndexError,
                                       r'The package index \(pypi.org\) could not be reached'):
            package_index.releases('mlflow')
        with self.assertRaisesRegex(package_index.PackageIndexError, 'is not a package name'):
            package_index.releases('two words')
