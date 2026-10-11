"""How an R block's failure reads when Rscript dies or R reports a common error."""
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mage_ai.data_preparation.models.block.r import execution
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.shared.error_hints import r_hint


def run_with(rscript):
    """Runs an R block with run_rscript replaced, and returns the error message."""
    libraries = SimpleNamespace(rv=None, mageml=Path('/nonexistent'))
    with patch.object(execution.runtime, 'r_config', return_value=SimpleNamespace()), \
            patch.object(execution.runtime, 'prepare', return_value=libraries), \
            patch.object(execution.runtime, 'run_rscript', side_effect=rscript):
        try:
            execution.execute_r_code(
                BlockType.TRANSFORMER, 'transform <- function(df) df', block_uuid='clean',
            )
        except execution.RBlockError as error:
            return str(error)
    raise AssertionError('the R block did not fail')


class RFailureTest(unittest.TestCase):
    def test_a_killed_rscript_says_it_was_killed(self):
        message = run_with(lambda *args, **kwargs: -9)
        self.assertIn('R block clean failed: Rscript was killed (SIGKILL)', message)
        self.assertIn('out of memory', message)

    def test_a_missing_package_says_how_to_add_it(self):
        def rscript(config, runner, args, env=None, cwd=None):
            with open(os.path.join(args[0], 'error.txt'), 'w') as file:
                file.write("there is no package called ‘janitor’")
            return 1

        message = run_with(rscript)
        self.assertIn("there is no package called ‘janitor’", message)
        self.assertIn('`rv add janitor`', message)

    def test_hints_match_both_quote_styles_and_nothing_else(self):
        self.assertIn('rv add data.table', r_hint("there is no package called 'data.table'"))
        self.assertIsNone(r_hint('object x not found'))
        self.assertIsNone(r_hint(''))
