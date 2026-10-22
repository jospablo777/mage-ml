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
            max_parallel=None,
            record_run=True,
            run_sensors=mock.ANY,
            run_tests=True,
            pipeline_run_id=None,
            update_status=mock.ANY,
        )
        assert result.exit_code == 0
        assert 'Pipeline run completed.' in result.output

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_tests_run_by_default_and_no_test_skips_them(
        self, mock_executor_factory, mock_pipeline,
    ):
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
        execute = mock_executor_factory.get_pipeline_executor.return_value.execute

        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic'])
        assert result.exit_code == 0, result.output
        assert execute.call_args.kwargs['run_tests'] is True

        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic', '--no-test'])
        assert result.exit_code == 0, result.output
        assert execute.call_args.kwargs['run_tests'] is False

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_no_record_leaves_the_run_unrecorded(self, mock_executor_factory, mock_pipeline):
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
        result = runner.invoke(app, ['run', 'my_mage_project', 'load_titanic', '--no-record'])
        assert result.exit_code == 0, result.output
        kwargs = mock_executor_factory.get_pipeline_executor.return_value.execute.call_args.kwargs
        assert kwargs['record_run'] is False

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_max_parallel_reaches_the_executor(self, mock_executor_factory, mock_pipeline):
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
        result = runner.invoke(
            app, ['run', 'my_mage_project', 'load_titanic', '--max-parallel', '2'],
        )
        assert result.exit_code == 0, result.output
        execute = mock_executor_factory.get_pipeline_executor.return_value.execute
        assert execute.call_args.kwargs['max_parallel'] == 2

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


class RunCleanupTests(TestCase):
    def setUp(self):
        super().setUp()
        cwd = os.getcwd()
        directory = tempfile.mkdtemp()
        os.makedirs(os.path.join(directory, 'my_mage_project'))
        os.chdir(directory)
        self.addCleanup(os.chdir, cwd)
        self.addCleanup(shutil.rmtree, directory, True)

    @mock.patch('mage_ai.data_preparation.variable_manager.VariableManager')
    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_clean_after_success_removes_the_runs_outputs(
        self, mock_executor_factory, mock_pipeline, mock_manager,
    ):
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
        mock_pipeline.get.return_value.blocks_by_uuid = {'load': None}
        mock_manager.return_value.clean_run_outputs.return_value = ['x']
        result = runner.invoke(app, [
            'run', 'my_mage_project', 'load_titanic', '--clean-after-success',
            '--execution-partition', 'cli_1',
        ])
        assert result.exit_code == 0, result.output
        mock_manager.return_value.clean_run_outputs.assert_called_once_with(
            mock.ANY, ['load'], 'cli_1',
        )
        # A failed run keeps its outputs for debugging.
        mock_manager.reset_mock()
        mock_executor_factory.get_pipeline_executor.return_value.execute.side_effect = (
            RuntimeError('failed')
        )
        result = runner.invoke(app, [
            'run', 'my_mage_project', 'load_titanic', '--clean-after-success',
        ])
        assert result.exit_code == 1
        mock_manager.return_value.clean_run_outputs.assert_not_called()

    def test_clean_cached_variables_refuses_a_duration_it_cannot_read(self):
        result = runner.invoke(
            app, ['clean-cached-variables', 'my_mage_project', '--older-than', 'soon'],
        )
        assert result.exit_code == 2, result.output
        assert 'not a duration' in result.output


class VarOptionTests(TestCase):
    def test_var_options_parse_with_their_json_types(self):
        from mage_ai.cli.utils import parse_var_options

        self.assertEqual(
            parse_var_options(['date=2026-01-31', 'n=2', 'on=true', 'ids=[1, 2]', 'eq=a=b']),
            {'date': '2026-01-31', 'n': 2, 'on': True, 'ids': [1, 2], 'eq': 'a=b'},
        )
        with self.assertRaises(ValueError):
            parse_var_options(['no_equals'])

    @mock.patch('mage_ai.data_preparation.models.pipeline.Pipeline')
    @mock.patch('mage_ai.data_preparation.executors.executor_factory.ExecutorFactory')
    def test_var_overrides_runtime_vars(self, mock_executor_factory, mock_pipeline):
        project = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, project, True)
        mock_pipeline.get_all_pipelines.return_value = ['load_titanic']
        mock_pipeline.get.return_value.variables = {}
        result = runner.invoke(app, [
            'run', project, 'load_titanic', '--runtime-vars', '{"a": 1, "b": 1}',
            '--var', 'b=2', '--var', 'c=text',
        ])
        assert result.exit_code == 0, result.output
        variables = mock_executor_factory.get_pipeline_executor.return_value.execute.call_args \
            .kwargs['global_vars']
        assert variables['a'] == 1 and variables['b'] == 2 and variables['c'] == 'text'
