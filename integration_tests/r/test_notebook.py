"""
R blocks run from the notebook: the code in the editor runs with execute_with_callback,
the tests report to the notebook, and the output preview shows the frame.
"""
import json
import uuid

import pytest

R_TRANSFORMER = '''
suppressPackageStartupMessages(library(tidyverse))

#* @transformer
add_total <- function(df_1, ...) {
  cat("rows from Python:", nrow(df_1), "\\n")
  df_1 |> mutate(total = price * quantity)
}

#* @test
totals_are_positive <- function(output) {
  stopifnot("A total is negative" = all(output$total > 0))
}

#* @test
has_ten_rows <- function(output) {
  stopifnot("Expected 10 rows" = nrow(output) == 10)
}
'''

PYTHON_LOADER = '''
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'price': [1.5, 2.0, 3.25],
        'quantity': pd.array([2, 1, 4], dtype='Int64'),
    })
'''


@pytest.fixture
def notebook_pipeline(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create(f'r_notebook_{uuid.uuid4().hex[:8]}', repo_path=mage_project)
    loader = Block.create(
        f'{pipeline.uuid}_load', BlockType.DATA_LOADER, mage_project,
        language=BlockLanguage.PYTHON, pipeline=pipeline,
    )
    loader.update_content(PYTHON_LOADER)
    transformer = Block.create(
        f'{pipeline.uuid}_r', BlockType.TRANSFORMER, mage_project,
        language=BlockLanguage.R, pipeline=pipeline, upstream_block_uuids=[loader.uuid],
    )
    pipeline = Pipeline.get(pipeline.uuid, repo_path=mage_project)
    return pipeline, pipeline.get_block(loader.uuid), pipeline.get_block(transformer.uuid)


def test_a_new_r_block_starts_from_the_tidyverse_template(notebook_pipeline):
    _, _, transformer = notebook_pipeline

    assert transformer.file_path.endswith('.r')
    assert '#* @transformer' in transformer.content
    assert 'library(tidyverse)' in transformer.content


def test_an_r_block_runs_from_the_notebook(notebook_pipeline, capsys):
    from mage_ai.data_preparation.models.block.outputs import get_outputs_for_display_sync

    pipeline, loader, transformer = notebook_pipeline
    global_vars = dict(pipeline.variables or {})
    loader.execute_with_callback(from_notebook=True, global_vars=global_vars)

    result = transformer.execute_with_callback(
        custom_code=R_TRANSFORMER, from_notebook=True, global_vars=global_vars,
    )
    frame = result['output'][0]
    assert frame['total'].tolist() == [3.0, 2.0, 13.0]
    assert 'rows from Python: 3' in capsys.readouterr().out

    with pytest.raises(Exception, match='Failed to pass tests'):
        transformer.run_tests(
            custom_code=R_TRANSFORMER, from_notebook=True, global_vars=global_vars,
            update_tests=False,
        )
    reports = [
        json.loads(line.split('[__internal_test__]', 1)[1])
        for line in capsys.readouterr().out.splitlines()
        if '[__internal_test__]' in line
    ]
    assert reports[0]['message'] == 'FAIL: has_ten_rows (block: %s)' % transformer.uuid
    assert reports[0]['error'] == 'Expected 10 rows'
    assert reports[-1]['message'] == '1/2 tests passed.'

    display = get_outputs_for_display_sync(transformer, sample_count=10)
    table = next(output for output in display if output.get('type') == 'table')
    assert table['sample_data']['columns'] == ['price', 'quantity', 'total']
    assert table['sample_data']['rows'] == [[1.5, 2, 3.0], [2.0, 1, 2.0], [3.25, 4, 13.0]]
