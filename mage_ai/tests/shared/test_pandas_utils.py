import sys
import types
import warnings
from unittest.mock import patch

from mage_ai.shared.pandas_utils import (
    get_setting_with_copy_warning,
    ignore_setting_with_copy_warning,
)
from mage_ai.tests.base_test import TestCase


class FakeSettingWithCopyWarning(Warning):
    pass


def build_module(name: str, warning_class=None) -> types.ModuleType:
    module = types.ModuleType(name)
    if warning_class is not None:
        module.SettingWithCopyWarning = warning_class
    return module


class PandasUtilsTests(TestCase):
    def test_get_setting_with_copy_warning_from_pandas_errors(self):
        """pandas 1.5.0 through 2.x expose the class from pandas.errors."""
        modules = {
            'pandas.errors': build_module('pandas.errors', FakeSettingWithCopyWarning),
        }
        with patch.dict(sys.modules, modules):
            self.assertIs(get_setting_with_copy_warning(), FakeSettingWithCopyWarning)

    def test_get_setting_with_copy_warning_from_pandas_core_common(self):
        """pandas before 1.5.0 only exposes the class from pandas.core.common."""
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module(
                'pandas.core.common', FakeSettingWithCopyWarning
            ),
        }
        with patch.dict(sys.modules, modules):
            self.assertIs(get_setting_with_copy_warning(), FakeSettingWithCopyWarning)

    def test_get_setting_with_copy_warning_removed_in_pandas_3(self):
        """pandas 3.0 dropped the class when copy-on-write became the default."""
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_get_setting_with_copy_warning_without_pandas(self):
        modules = {
            'pandas': None,
            'pandas.errors': None,
            'pandas.core.common': None,
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_get_setting_with_copy_warning_ignores_non_warning_attribute(self):
        modules = {
            'pandas.errors': build_module('pandas.errors', 'not a warning class'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_ignore_setting_with_copy_warning_installs_filter(self):
        modules = {
            'pandas.errors': build_module('pandas.errors', FakeSettingWithCopyWarning),
        }
        with patch.dict(sys.modules, modules), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.assertTrue(ignore_setting_with_copy_warning())
            warnings.warn('ignored', FakeSettingWithCopyWarning, stacklevel=2)

        self.assertEqual(caught, [])

    def test_ignore_setting_with_copy_warning_is_a_noop_in_pandas_3(self):
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertFalse(ignore_setting_with_copy_warning())

    def test_ignore_setting_with_copy_warning_with_installed_pandas(self):
        """Whatever pandas is installed, this must never raise an ImportError."""
        with warnings.catch_warnings():
            self.assertIsInstance(ignore_setting_with_copy_warning(), bool)
