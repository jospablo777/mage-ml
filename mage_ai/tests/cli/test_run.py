import os
from unittest import mock

from typer.testing import CliRunner

from mage_ai.cli.main import app
from mage_ai.tests.base_test import TestCase

runner = CliRunner()


class RunTests(TestCase):
    def test_run_with_no_arguments(self):
        result = runner.invoke(app, ['run'])
        assert result.exit_code == 2

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_run_with_arguments(self, mock_executor_factory, mock_pipeline):
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic'])
        mock_pipeline.get.assert_any_call(
            'load_titanic', repo_path=os.path.abspath('my_mage_project'),
        )
        mock_executor_factory.get_pipeline_executor.return_value.execute.assert_called_once()
        assert result.exit_code == 0
        assert 'Pipeline run completed.' in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_run_with_tests(self, mock_executor_factory, mock_pipeline):
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic', '--test'])
        mock_pipeline.get.assert_any_call(
            'load_titanic', repo_path=os.path.abspath('my_mage_project'),
        )
        mock_executor_factory.get_pipeline_executor.return_value.execute.assert_called_once_with(
            analyze_outputs=mock.ANY,
            global_vars=mock.ANY,
            run_sensors=mock.ANY,
            run_tests=True,
            pipeline_run_id=None,
            update_status=mock.ANY,
        )
        assert result.exit_code == 0
        assert 'Pipeline run completed.' in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_run_prints_r_environment_errors_without_a_traceback(
        self, mock_executor_factory, mock_pipeline,
    ):
        from mage_ai.data_preparation.models.block.r.runtime import REnvironmentError

        mock_executor_factory.get_pipeline_executor.return_value.execute.side_effect = (
            REnvironmentError(
                'The R library is not synced with rv.lock [dependencies] [/x]. Run `mage r sync`.'
            )
        )
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic'])
        assert result.exit_code == 1
        assert 'Run `mage r sync`.' in result.output
        # rich would drop bracketed words or fail on [/x]; the CLI escapes them.
        assert '[dependencies] [/x]' in result.output
        assert 'Traceback' not in result.output
        assert 'Pipeline run completed.' not in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_run_prints_contract_violations_without_a_traceback(
        self, mock_executor_factory, mock_pipeline,
    ):
        from mage_ai.data_preparation.contracts import ContractViolation

        mock_executor_factory.get_pipeline_executor.return_value.execute.side_effect = (
            ContractViolation(
                'Contract monthly_revenue failed for monthly_revenue_pandas:\n'
                '  region: 12 values not in [north, south, east, west]'
            )
        )
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic'])
        assert result.exit_code == 1
        assert 'Contract monthly_revenue failed' in result.output
        assert '[north, south, east, west]' in result.output
        assert 'Traceback' not in result.output
        assert 'Pipeline run completed.' not in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_a_failed_block_prints_a_plain_traceback_with_its_notes(
        self, mock_executor_factory, mock_pipeline,
    ):
        error = ModuleNotFoundError("No module named 'sklearn'", name='sklearn')
        error.add_note('Block train (transformers/train.py) failed.')
        mock_executor_factory.get_pipeline_executor.return_value.execute.side_effect = error
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic'])
        assert result.exit_code == 1
        assert 'Traceback (most recent call last)' in result.output
        assert "ModuleNotFoundError: No module named 'sklearn'" in result.output
        assert 'Block train (transformers/train.py) failed.' in result.output
        assert 'Pipeline run failed.' in result.output
        assert '╭' not in result.output
