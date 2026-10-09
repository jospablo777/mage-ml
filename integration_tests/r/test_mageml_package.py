"""
The mageml R package: its testthat tests, its style, which lintr checks against the
tidyverse style guide, and its documentation, which roxygen2 generates.
"""
import filecmp
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from mage_ai.data_preparation.models.block.r.runtime import MAGEML_DIRECTORY


def rscript(r_library, code, cwd, extra_libraries=()):
    paths = ', '.join(f"'{path}'" for path in (*extra_libraries, r_library))
    result = subprocess.run(
        ['Rscript', '--vanilla', '-e', f'.libPaths(c({paths}), include.site = FALSE)\n{code}'],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=600,
        env={**os.environ, 'TZ': 'UTC'},
    )
    return result


@pytest.fixture
def package_copy(tmp_path):
    copy = tmp_path / 'mageml'
    shutil.copytree(MAGEML_DIRECTORY, copy)
    return copy


def test_testthat(r_library, package_copy):
    result = rscript(
        r_library,
        "testthat::test_local('.', reporter = 'summary', stop_on_failure = TRUE)",
        cwd=package_copy,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_lintr(r_library, package_copy, tmp_path):
    # lintr resolves names across the package's files when the package is installed.
    library = tmp_path / 'library'
    library.mkdir()
    result = rscript(
        r_library,
        f"""
install.packages('.', repos = NULL, type = 'source', lib = '{library}', quiet = TRUE)
lints <- lintr::lint_package()
print(lints)
quit(status = length(lints))
""",
        cwd=package_copy,
        extra_libraries=[library],
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_documentation_is_generated_from_the_source(r_library, package_copy):
    result = rscript(r_library, 'roxygen2::roxygenise()', cwd=package_copy)
    assert result.returncode == 0, result.stdout + result.stderr

    pages = [f'man/{page.name}' for page in (package_copy / 'man').iterdir()]
    for name in ['NAMESPACE', 'DESCRIPTION', *pages]:
        assert (MAGEML_DIRECTORY / name).exists(), f'{name} is not committed; run roxygen2'
        assert filecmp.cmp(package_copy / name, MAGEML_DIRECTORY / name, shallow=False), (
            f'{name} differs from what roxygen2 generates; run roxygen2::roxygenise()'
        )


def test_r_cmd_check(r_library, package_copy, tmp_path):
    built = subprocess.run(
        ['R', 'CMD', 'build', '--no-build-vignettes', str(package_copy)],
        cwd=tmp_path, capture_output=True, text=True, timeout=600,
        env={**os.environ, 'R_LIBS': str(r_library), 'R_LIBS_USER': '', 'R_LIBS_SITE': ''},
    )
    assert built.returncode == 0, built.stdout + built.stderr
    tarball = next(Path(tmp_path).glob('mageml_*.tar.gz'))

    checked = subprocess.run(
        ['R', 'CMD', 'check', '--no-manual', tarball.name],
        cwd=tmp_path, capture_output=True, text=True, timeout=900,
        env={
            **os.environ, 'R_LIBS': str(r_library), 'R_LIBS_USER': '', 'R_LIBS_SITE': '',
            'TZ': 'UTC', '_R_CHECK_FORCE_SUGGESTS_': 'false',
        },
    )

    output = checked.stdout + checked.stderr
    assert checked.returncode == 0, output
    assert 'Status: OK' in output, output[-3000:]
