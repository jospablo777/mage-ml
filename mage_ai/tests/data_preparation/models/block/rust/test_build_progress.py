from mage_ai.data_preparation.models.block.rust.build import progress_line
from mage_ai.tests.base_test import TestCase


class BuildProgressTest(TestCase):
    def test_colored_progress_and_lock_waits_are_shown_without_their_colors(self):
        """Cargo runs with CARGO_TERM_COLOR=always: a colored line never matched, so no
        build progress reached a log, nor a wait on another build's lock (I-567)."""
        self.assertEqual(
            progress_line('\x1b[1m\x1b[32m   Compiling\x1b[0m polars v0.55.2\n'),
            'Compiling polars v0.55.2',
        )
        self.assertEqual(
            progress_line('\x1b[1m\x1b[36m    Blocking\x1b[0m waiting for file lock on build '
                          'directory\n'),
            'Blocking waiting for file lock on build directory',
        )
        self.assertIsNone(progress_line('\x1b[1m\x1b[32m    Finished\x1b[0m dev profile\n'))
        self.assertIsNone(progress_line('warning: unused variable\n'))
