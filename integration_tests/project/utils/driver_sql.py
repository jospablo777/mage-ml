"""Reads the driver pipelines' SQL files, with the test's schema filled in."""
import os
from pathlib import Path

from mage_ai.settings.repo import get_repo_path


def read_sql(relative_path: str) -> str:
    text = (Path(get_repo_path()) / relative_path).read_text()
    return text.format(schema=os.environ['MAGE_TEST_SCHEMA'])
