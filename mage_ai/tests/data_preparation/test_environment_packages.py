import json
import unittest
from unittest.mock import patch

from mage_ai.data_preparation import environment_packages as ep

RPROJECT = '''[project]
name = "models"
r_version = "4.6"
# Where packages come from.
repositories = [
    {alias = "CRAN", url = "https://cloud.r-project.org/"},
]
dependencies = [
    "arrow",
    "jsonlite",
]
'''

CARGO = '''[workspace]
resolver = "3"
members = [".mage/blocks/*"]

[workspace.dependencies]
# Every block can use these.
mage-block = { path = ".mage/sdk", features = ["postgres"] }
serde = "1"
'''


class EnvironmentPackagesTest(unittest.TestCase):
    def test_r_packages_are_added_once_with_github_ones_at_their_tag(self):
        text, added = ep.add_r_packages(
            RPROJECT, ['xgboost', 'arrow', 'tidyverse/dplyr@v1.1.4', 'xgboost'],
        )
        self.assertEqual(added, ['xgboost', 'dplyr'])
        self.assertIn('# Where packages come from.', text)
        self.assertIn('    "xgboost",\n', text)
        self.assertIn('{name = "dplyr", git = "https://github.com/tidyverse/dplyr", '
                      'tag = "v1.1.4"}', text)
        with self.assertRaisesRegex(ep.PackageError, 'not an R package'):
            ep.add_r_packages(RPROJECT, ['bad name!'])

    def test_a_snapshot_date_pins_r_versions_through_posit_package_manager(self):
        text = ep.with_r_snapshot(RPROJECT, '2026-10-12')
        self.assertLess(text.index('cran/2026-10-12'), text.index('cloud.r-project.org'))
        # Moved, not added again.
        again = ep.with_r_snapshot(text, '2026-11-01')
        self.assertEqual(again.count('packagemanager.posit.co'), 1)
        self.assertIn('cran/2026-11-01', again)
        with self.assertRaisesRegex(ep.PackageError, 'snapshot date'):
            ep.with_r_snapshot(RPROJECT, 'latest')

    def test_crates_take_a_version_features_or_the_newest_on_crates_io(self):
        text, added = ep.add_crates(
            CARGO, ['regex', 'polars@0.55[lazy, sql]', 'serde@2'], newest=lambda name: '1.11',
        )
        self.assertEqual(added, ['regex', 'polars'])
        self.assertIn('# Every block can use these.', text)
        self.assertIn('regex = "1.11"', text)
        self.assertIn('polars = {version = "0.55", features = ["lazy", "sql"]}', text)
        self.assertIn('serde = "1"', text, 'a listed crate is left as it is')
        with self.assertRaisesRegex(ep.PackageError, 'not a crate'):
            ep.add_crates(CARGO, ['no spaces allowed'], newest=lambda name: '1')

    def test_the_newest_version_comes_from_crates_io(self):
        class Response:
            def __init__(self, body):
                self.body = body

            def read(self):
                return json.dumps(self.body).encode()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch('urllib.request.urlopen',
                   return_value=Response(dict(crate=dict(max_stable_version='1.11.2')))):
            self.assertEqual(ep.newest_crate_version('regex'), '1.11')
        with patch('urllib.request.urlopen',
                   return_value=Response(dict(crate=dict(max_stable_version='0.55.2')))):
            self.assertEqual(ep.newest_crate_version('polars'), '0.55')
        with patch('urllib.request.urlopen', side_effect=OSError('offline')):
            with self.assertRaisesRegex(ep.PackageError, 'give its version, such as x@1'):
                ep.newest_crate_version('x')

    def test_a_toolchain_and_a_brought_cargo_toml(self):
        text = ep.with_toolchain('[toolchain]\nchannel = "1.98.0"\nprofile = "minimal"\n',
                                 '1.99.1')
        self.assertIn('channel = "1.99.1"', text)
        self.assertIn('profile = "minimal"', text)
        with self.assertRaisesRegex(ep.PackageError, 'not a Rust toolchain'):
            ep.with_toolchain('', 'latest')
        brought = ep.with_rust_essentials('[workspace.dependencies]\nrand = "0.9"\n')
        self.assertIn('".mage/blocks/*"', brought)
        self.assertIn('mage-block', brought)
        self.assertIn('rand = "0.9"', brought)

    def test_packages_are_written_as_people_type_them(self):
        self.assertEqual(ep.split('xgboost, ranger\nglmnet  bundle'),
                         ['xgboost', 'ranger', 'glmnet', 'bundle'])
