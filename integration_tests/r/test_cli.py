"""
`mage r init`, `mage r status` and `mage r sync` with R 4.6 and rv.
"""
import tomllib

import pandas as pd
from typer.testing import CliRunner

from mage_ai.cli.main import app
from mage_ai.data_preparation.models.block.r import execute_r_code
from mage_ai.data_preparation.models.constants import BlockType


def text(result) -> str:
    """The output with whitespace collapsed; rich wraps long lines."""
    return ' '.join(result.output.split())


def test_init_status_and_sync(tmp_path, monkeypatch):
    monkeypatch.delenv('MAGE_R_PROJECT_DIR')
    project = tmp_path / 'project'
    project.mkdir()
    runner = CliRunner()

    # The tidyverse is the default; glue keeps the test small.
    created = runner.invoke(app, ['r', 'init', str(project), '--package', 'glue'])
    assert created.exit_code == 0, created.output
    assert 'Created the R environment' in text(created)

    with open(project / 'r' / 'rproject.toml', 'rb') as file:
        config = tomllib.load(file)['project']
    assert config['r_version'] == '4.6'
    assert config['dependencies'] == ['glue', 'arrow', 'bit64', 'jsonlite', 'tibble']
    assert [r['alias'] for r in config['repositories']] == ['PPM', 'CRAN']
    assert (project / 'r' / 'rv.lock').exists()

    status = runner.invoke(app, ['r', 'status', str(project)])
    assert status.exit_code == 0, status.output
    assert 'R blocks can run.' in text(status)

    again = runner.invoke(app, ['r', 'init', str(project)])
    assert again.exit_code == 1
    assert 'already has an rproject.toml' in text(again)

    run = execute_r_code(
        BlockType.TRANSFORMER,
        '#* @transformer\nf <- function(df_1) { df_1$g <- glue::glue("x{df_1$a}"); df_1 }',
        input_vars=[pd.DataFrame({'a': [1, 2]})],
        repo_path=str(project),
    )
    assert run.outputs[0]['g'].tolist() == ['x1', 'x2']

    synced = runner.invoke(app, ['r', 'sync', str(project)])
    assert synced.exit_code == 0, synced.output


def test_status_reports_problems(tmp_path, monkeypatch):
    monkeypatch.delenv('MAGE_R_PROJECT_DIR')
    project = tmp_path / 'project'
    (project / 'r').mkdir(parents=True)
    (project / 'r' / 'rproject.toml').write_text(
        '[project]\nname = "p"\nr_version = "4.6"\nrepositories = [\n'
        '    {alias = "PPM", url = "https://packagemanager.posit.co/cran/latest"},\n]\n'
        'dependencies = ["glue"]\n',
    )

    status = CliRunner().invoke(app, ['r', 'status', str(project)])

    assert status.exit_code == 1
    assert 'R blocks need arrow, bit64, jsonlite, tibble' in text(status)


def test_projects_without_an_environment(tmp_path, monkeypatch):
    monkeypatch.delenv('MAGE_R_PROJECT_DIR')

    status = CliRunner().invoke(app, ['r', 'status', str(tmp_path)])

    assert status.exit_code == 1
    assert 'has no R environment' in text(status)
    assert 'mage r init' in text(status)
