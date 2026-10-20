import os
import unittest
from unittest.mock import patch

from mage_ai.data_preparation.shared.utils import get_template_vars
from mage_ai.shared.jinja import compiled


class CompiledTemplateTest(unittest.TestCase):
    def test_a_text_compiles_once_and_renders_with_the_current_environment(self):
        text = "concurrency: {{ env_var('MAGE_TEST_JINJA_CONCURRENCY') }}"
        self.assertIs(compiled(text), compiled(text))
        with patch.dict(os.environ, MAGE_TEST_JINJA_CONCURRENCY='4'):
            self.assertEqual(compiled(text).render(**get_template_vars()), 'concurrency: 4')
        with patch.dict(os.environ, MAGE_TEST_JINJA_CONCURRENCY='9'):
            self.assertEqual(compiled(text).render(**get_template_vars()), 'concurrency: 9')
