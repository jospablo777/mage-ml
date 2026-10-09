"""
Fixtures for R blocks, which run with R 4.6 and the rv environment in
integration_tests/r_env. `make -C integration_tests r-env` installs it.
"""
import os
import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def r_project_dir():
    path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
    if not path:
        pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
    for executable in ('Rscript', 'rv'):
        if shutil.which(executable) is None:
            pytest.skip(f'R is not configured: {executable} is not on PATH')
    return Path(path).resolve()


@pytest.fixture(scope='session')
def r_library(r_project_dir):
    """The library of the R test environment, for running R directly."""
    from mage_ai.data_preparation.models.block.r import runtime

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('MAGE_R_PROJECT_DIR', str(r_project_dir))
        return runtime.library_path(runtime.r_config())


@pytest.fixture(scope='session', autouse=True)
def r_environment(r_project_dir, tmp_path_factory):
    """
    Point R blocks at the test environment, with a mageml cache per session. It is a
    session fixture so that it applies before module fixtures run R blocks.
    """
    cache = tmp_path_factory.getbasetemp() / 'mageml-cache'
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv('MAGE_R_PROJECT_DIR', str(r_project_dir))
        patch.setenv('MAGE_R_CACHE_DIR', str(cache))
        patch.setenv('MAGE_R_SYNC', 'check')
        patch.delenv('MAGE_R_TIMEOUT', raising=False)
        yield
