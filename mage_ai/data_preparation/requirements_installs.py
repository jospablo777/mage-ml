"""
When MageML last installed the project's requirements.txt into its Python: after a git pull
or reset that installs requirements. With MAGE_RESTART_STREAMING_PIPELINES_ON_REQUIREMENTS_CHANGE,
streaming runs that started before it restart, so they load the new packages. An edit that
was not installed, or an install that failed, restarts nothing: the run would start again
on the old packages.
"""
import hashlib
import json
import os
from datetime import datetime
from typing import Optional

import pytz

FILE_NAME = 'requirements_installed.json'


def _path(repo_path: str) -> str:
    from mage_ai.settings.repo import get_variables_dir

    return os.path.join(get_variables_dir(repo_path=repo_path), FILE_NAME)


def record_install(repo_path: str, requirements_file: str) -> None:
    """Records a successful install of the file, now."""
    with open(requirements_file, 'rb') as file:
        digest = hashlib.sha256(file.read()).hexdigest()
    path = _path(repo_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f'{path}.tmp'
    with open(temporary, 'w') as file:
        json.dump(dict(
            installed_at=datetime.now(tz=pytz.UTC).isoformat(),
            requirements_file=requirements_file,
            sha256=digest,
        ), file)
    os.replace(temporary, path)


def installed_at(repo_path: str) -> Optional[datetime]:
    """When the project's requirements were last installed; None when MageML never did."""
    try:
        with open(_path(repo_path)) as file:
            value = json.load(file).get('installed_at')
        installed = datetime.fromisoformat(value)
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    return installed if installed.tzinfo else pytz.UTC.localize(installed)
