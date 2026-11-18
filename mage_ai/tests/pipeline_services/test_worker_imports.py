"""
The Python worker (pipeline_services/runtime/worker.py) runs blocks in named Python
workers, whose environments pin their own packages (pandas 2 for Feast, for example).
It must not load MageML there (I-19): it imports MageML only in the SQL and R runners,
whose blocks always run in the base environment, and in execute_block for the audit
context of models.predict, which needs only the standard library.
"""
import ast
import sys
import unittest
from pathlib import Path

PIPELINE_SERVICES = Path(__file__).resolve().parents[2] / 'pipeline_services'
WORKER = PIPELINE_SERVICES / 'runtime' / 'worker.py'
MODELS = PIPELINE_SERVICES / 'models.py'
# Where the worker may import MageML, and what.
ALLOWED = {
    ('run_sql_block', 'mage_ai.pipeline_services.runtime'),
    ('execute_block', 'mage_ai.pipeline_services'),
}


def imports(path: Path):
    """(enclosing function or None, module) for each import in the file."""
    found = []

    def walk(node, function):
        for child in ast.iter_child_nodes(node):
            name = child.name if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                else function
            if isinstance(child, ast.ImportFrom):
                found.append((name, child.module or ''))
            elif isinstance(child, ast.Import):
                found.extend((name, alias.name) for alias in child.names)
            walk(child, name)

    walk(ast.parse(path.read_text()), None)
    return found


class WorkerImportsTest(unittest.TestCase):
    def test_the_worker_imports_mageml_only_where_its_blocks_run_in_the_base_environment(self):
        mageml = {(f, m) for f, m in imports(WORKER) if m.split('.')[0] == 'mage_ai'}
        self.assertEqual(mageml - ALLOWED, set())

    def test_the_worker_starts_on_the_standard_library_alone(self):
        top = [m for f, m in imports(WORKER) if f is None]
        outside = [m for m in top if m.split('.')[0] not in sys.stdlib_module_names]
        self.assertEqual(outside, [])

    def test_the_audit_context_needs_only_the_standard_library(self):
        # The worker imports models and calls set_decision_context for every block. Model
        # libraries (onnxruntime, xgboost...) load inside the functions that read each
        # format, from the block's own environment.
        found = imports(MODELS)
        top = [m for f, m in found if f is None]
        context = [m for f, m in found if f == 'set_decision_context']
        for module in top + context:
            self.assertIn(module.split('.')[0], sys.stdlib_module_names, module)
