"""
Consecutive R blocks in one R session (execute_r_chain, mageml::run_chain): the same
values as block by block, each block's tests, and failures that name their block.
"""
import textwrap

import pandas as pd
import pytest

LOAD = '''
#* @data_loader
load <- function(...) {
  tibble::tibble(id = 1:5, amount = c(2, 0.5, 3, NA, 8))
}
'''
CLEAN = '''
suppressPackageStartupMessages(library(dplyr))

#* @transformer
clean <- function(df_1, ...) {
  df_1 |> filter(!is.na(amount), amount > 1) |> mutate(double = amount * 2)
}

#* @test
no_missing <- function(output) {
  stopifnot("missing amounts remain" = !anyNA(output$amount))
}
'''
TOTAL = '''
suppressPackageStartupMessages(library(dplyr))

#* @transformer
total <- function(df_1, ...) {
  df_1 |> summarise(total = sum(double), rows = n())
}
'''


def steps(*codes, keep=False):
    names = ['load', 'clean', 'total', 'extra']
    types = ['data_loader'] + ['transformer'] * 3
    return [
        dict(block_type=types[i], code=textwrap.dedent(code), block_uuid=names[i],
             keep_output=keep)
        for i, code in enumerate(codes)
    ]


def test_a_chain_gives_what_blocks_give_one_by_one():
    from mage_ai.data_preparation.models.block.r import execution

    run = execution.execute_r_chain(steps(LOAD, CLEAN, TOTAL), [], {})
    values = []
    for step in steps(LOAD, CLEAN, TOTAL):
        values = execution.execute_r_code(
            step['block_type'], step['code'], values, {}, block_uuid=step['block_uuid'],
        ).outputs
    pd.testing.assert_frame_equal(run.outputs[0], values[0])
    assert run.outputs[0].to_dict('records') == [dict(total=26.0, rows=3)]
    assert [b['block_uuid'] for b in run.blocks] == ['load', 'clean', 'total']
    assert [(t['name'], t['passed']) for t in run.tests] == [('clean: no_missing', True)]


def test_kept_outputs_come_back():
    from mage_ai.data_preparation.models.block.r import execution

    run = execution.execute_r_chain(steps(LOAD, CLEAN, TOTAL, keep=True), [], {})
    assert list(run.kept) == [0, 1]
    assert run.kept[1][0]['double'].tolist() == [4.0, 6.0, 16.0]


def test_a_failed_test_stops_the_chain_at_its_block():
    from mage_ai.data_preparation.models.block.r import execution

    failing = CLEAN.replace('!anyNA(output$amount)', 'nrow(output) > 10')
    with pytest.raises(execution.RChainError) as caught:
        execution.execute_r_chain(steps(LOAD, failing, TOTAL), [], {})
    assert caught.value.block_uuid == 'clean'
    assert '1 of 1 tests failed' in str(caught.value)
    assert 'no_missing (missing amounts remain)' in str(caught.value)


def test_an_error_names_its_block():
    from mage_ai.data_preparation.models.block.r import execution

    broken = CLEAN.replace('amount > 1', 'missing_column > 1')
    with pytest.raises(execution.RChainError) as caught:
        execution.execute_r_chain(steps(LOAD, broken, TOTAL), [], {})
    assert caught.value.block_uuid == 'clean'
    assert 'missing_column' in str(caught.value)
    assert str(caught.value).startswith('R block clean failed')


def test_blocks_get_the_values_they_get_one_by_one():
    """Between blocks run one by one, an integer64 column that fits arrives as an integer,
    so amount * units is a double; a chain must give the next block the same value."""
    from mage_ai.data_preparation.models.block.r import execution

    load = '''
#* @data_loader
load <- function(...) {
  tibble::tibble(amount = c(2.5, 1.25), units = bit64::as.integer64(c(3, 4)))
}
'''
    multiply = '''
#* @transformer
multiply <- function(df_1, ...) {
  dplyr::mutate(df_1, revenue = amount * units)
}
'''
    chained = execution.execute_r_chain(steps(load, multiply), [], {}).outputs[0]
    values = []
    for step in steps(load, multiply):
        values = execution.execute_r_code(
            step['block_type'], step['code'], values, {}, block_uuid=step['block_uuid'],
        ).outputs
    assert chained['revenue'].tolist() == [7.5, 5.0]
    pd.testing.assert_frame_equal(chained, values[0])
