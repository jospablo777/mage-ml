import os
import shutil
import tempfile
from unittest import mock

from typer.testing import CliRunner

from mage_ai.cli.main import app
from mage_ai.tests.base_test import TestCase

runner = CliRunner()


class RunTests(TestCase):
    def setUp(self):
        super().setUp()
        # mage run checks that the project exists.
        cwd = os.getcwd()
        directory = tempfile.mkdtemp()
        os.makedirs(os.path.join(directory, 'my_mage_project'))
        os.chdir(directory)
        self.addCleanup(os.chdir, cwd)
        self.addCleanup(shutil.rmtree, directory, True)

    def test_run_with_no_arguments(self):
        result = runner.invoke(app, ['run'])
        assert result.exit_code == 2

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_run_with_arguments(self, mock_executor_factory, mock_pipeline):
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
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
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
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
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
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
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
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
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
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


class RunInputTests(TestCase):
    def test_a_missing_project_is_named(self):
        result = runner.invoke(app, ['run', '/no/such/project', 'load_titanic'])
        assert result.exit_code == 2
        assert 'Project /no/such/project does not exist.' in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    def test_a_missing_pipeline_lists_the_projects_pipelines(self, mock_pipeline):
        mock_pipeline.get_all_pipelines.return_value = ['sales', 'churn']
        project = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, project, True)
        result = runner.invoke(app, ['run', project, 'slaes'])
        assert result.exit_code == 2
        output = ' '.join(result.output.split())
        assert 'Pipeline slaes is not in project' in output
        assert 'Its pipelines: churn, sales.' in output
        mock_pipeline.get.assert_not_called()

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_invalid_runtime_variables_stop_the_run(self, mock_executor_factory, mock_pipeline):
        mock_pipeline.get_all_pipelines.return_value = ['sales']
        project = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, project, True)
        for value, expected in (
            ('{bad json', '--runtime-vars is not valid JSON'),
            ('[1, 2]', '--runtime-vars must be a JSON object'),
        ):
            result = runner.invoke(app, ['run', project, 'sales', '--runtime-vars', value])
            assert result.exit_code == 2, result.output
            assert expected in ' '.join(result.output.split()), result.output
        mock_executor_factory.get_pipeline_executor.assert_not_called()
