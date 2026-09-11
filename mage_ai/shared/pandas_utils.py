import warnings
from typing import Optional, Type


def get_setting_with_copy_warning() -> Optional[Type[Warning]]:
    """
    Return pandas' SettingWithCopyWarning class, or None when pandas does not expose it.

    The class moved around between releases: it lived in pandas.core.common before 1.5.0,
    moved to pandas.errors in 1.5.0, and was removed in 3.0.0 when copy-on-write became
    the default. Probing for it is safer than comparing version strings, which also
    compares '1.10.0' as lower than '1.5.0'.
    """
    for module_name in ('pandas.errors', 'pandas.core.common'):
        try:
            module = __import__(module_name, fromlist=['SettingWithCopyWarning'])
            warning_class = module.SettingWithCopyWarning
        except (AttributeError, ImportError):
            continue

        if isinstance(warning_class, type) and issubclass(warning_class, Warning):
            return warning_class

    return None


def ignore_setting_with_copy_warning() -> bool:
    """
    Silence pandas' SettingWithCopyWarning when the installed pandas still raises it.

    Returns whether a filter was installed.
    """
    warning_class = get_setting_with_copy_warning()

    if warning_class is None:
        return False

    warnings.simplefilter(action='ignore', category=warning_class)

    return True
