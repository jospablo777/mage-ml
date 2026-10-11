"""R conditional blocks: a `#* @condition` function decides whether its block runs."""
import pandas as pd
import pytest

from mage_ai.data_preparation.models.block.r import RBlockError, execute_r_code
from mage_ai.data_preparation.models.constants import BlockType

CONDITION = '''
#* @condition
enough_rows <- function(df_1, ...) {
  nrow(df_1) >= variable("minimum", 1)
}
'''


def decide(code, *inputs, **global_vars):
    return execute_r_code(
        BlockType.CONDITIONAL, code, input_vars=list(inputs), global_vars=global_vars,
    ).outputs


def test_a_condition_reads_the_guarded_blocks_inputs_and_variables():
    frame = pd.DataFrame({'value': [1.0, 2.0, 3.0]})
    assert decide(CONDITION, frame, minimum=3) == [True]
    assert decide(CONDITION, frame, minimum=4) == [False]


def test_condition_registers_from_code_too():
    assert decide('condition(function(...) FALSE)') == [False]


def test_a_condition_must_return_true_or_false():
    with pytest.raises(RBlockError, match='returns TRUE or FALSE'):
        decide('#* @condition\nf <- function(...) c(TRUE, FALSE)\n')
    with pytest.raises(RBlockError, match='returns TRUE or FALSE'):
        decide('#* @condition\nf <- function(...) NA\n')
