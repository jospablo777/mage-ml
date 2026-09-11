"""
Data handling across block boundaries.

Every value a block returns is written to a variable and read back by the next
block. These runs cover the output shapes that appear in loader, transformer and
exporter pipelines: a cursor dictionary, several dataframes merged one to one, an
empty dataframe used as a branch signal, a fitted model, and a scored frame.

pandas 3 changed the dtypes on both sides of that boundary, so the assertions are
about what survives the round trip.
"""
import textwrap
from pathlib import Path

import pandas as pd

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase


class BlockDataHandlingTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.output = Path(self.repo_path) / f'{self.pipeline.uuid}.parquet'

    def block(self, name, kind, code, upstream=()):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python',
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def run_pipeline(self, **variables):
        run = trigger_pipeline(self.pipeline.uuid, variables=variables)
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        PipelineExecutor(
            self.pipeline, execution_partition=run.execution_partition,
        ).execute(
            pipeline_run_id=run.id,
            global_vars=run.get_variables(),
            allow_blocks_to_fail=False,
        )
        scheduler.schedule()
        run.refresh()

        failures = [
            b.block_uuid for b in run.block_runs
            if b.status.value not in ('completed', 'condition_failed')
        ]
        self.assertEqual(failures, [], f'blocks did not complete: {failures}')
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)

        return run

    def test_cursor_dictionary_passes_through_blocks(self):
        """
        A high-water-mark loader returns df.iloc[0].to_dict(). The dictionary holds a
        timestamp and an integer, is stored as JSON, and is read back as query
        parameters by the next loader.
        """
        cursor = self.block('cursor', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                frame = pd.DataFrame({
                    'last_id': pd.Series([345038], dtype='int64'),
                    'last_observed_at': pd.to_datetime(
                        ['2026-07-20 18:15:34.258586+00:00'], utc=True),
                })
                row = frame.iloc[0].to_dict()
                # pandas 3 hands back Python scalars, which the database driver binds.
                assert type(row['last_id']) is int, type(row['last_id'])
                return row
        ''')
        window = self.block('window', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(params, **kwargs):
                assert isinstance(params, dict), type(params)
                frame = pd.DataFrame({'high_id': pd.Series([345100], dtype='int64')})
                return params | frame.iloc[0].to_dict()
        ''', upstream=[cursor])
        self.block('sink', 'data_exporter', '''
            import pathlib
            import pandas as pd
            @data_exporter
            def export(params, **kwargs):
                assert sorted(params) == ['high_id', 'last_id', 'last_observed_at'], params
                assert params['high_id'] > params['last_id']
                # A dictionary holding a timestamp is stored with its element types, so
                # the value comes back as a Timestamp and not as text.
                assert isinstance(params['last_observed_at'], pd.Timestamp), (
                    type(params['last_observed_at']).__name__)
                assert params['last_observed_at'] == pd.Timestamp(
                    '2026-07-20 18:15:34.258586+00:00')
                pathlib.Path(kwargs['output_path']).write_text(str(params['high_id']))
        ''', upstream=[window])

        self.run_pipeline(output_path=str(self.output))

        self.assertEqual(self.output.read_text(), '345100')

    def test_dataframes_keep_their_dtypes_across_a_one_to_one_merge(self):
        """Four loaders feeding one transformer, the shape of a feature join."""
        loaders = [
            self.block(f'load{index}', 'data_loader', f'''
                import pandas as pd
                @data_loader
                def load(**kwargs):
                    rows = 6
                    return pd.DataFrame({{
                        'record_id': pd.Series(range(1, rows + 1), dtype='int64'),
                        'indicator_{index}': pd.Series([0, 1] * (rows // 2), dtype='int64'),
                        'observed_at': pd.to_datetime(
                            ['2026-01-0%d 00:00:00+00:00' % (i + 1) for i in range(rows)],
                            utc=True),
                    }})
            ''')
            for index in range(4)
        ]
        merge = self.block('merge', 'transformer', '''
            @transformer
            def transform(first, second, third, fourth, **kwargs):
                joined = first.copy()
                for position, frame in enumerate((second, third, fourth), start=2):
                    joined = joined.merge(
                        frame,
                        on='record_id',
                        how='left',
                        validate='one_to_one',
                        suffixes=('', f'__source_{position}'),
                    )
                assert joined['record_id'].is_unique
                return joined
        ''', upstream=loaders)
        validate = self.block('validate', 'transformer', '''
            import pandas as pd
            INDICATORS = ['indicator_0', 'indicator_1', 'indicator_2', 'indicator_3']
            @transformer
            def transform(frame, **kwargs):
                output = frame[['record_id', 'observed_at'] + INDICATORS].copy()
                output['record_id'] = pd.to_numeric(
                    output['record_id'], errors='raise').astype('int64')
                output['observed_at'] = pd.to_datetime(
                    output['observed_at'], errors='raise', utc=True)
                for column in INDICATORS:
                    output[column] = output[column].map(bool).astype('bool')
                nulls = output.isna().sum()
                assert nulls[nulls > 0].empty, nulls.to_dict()
                return output
        ''', upstream=[merge])
        self.block('sink', 'data_exporter', '''
            import pandas as pd
            @data_exporter
            def export(frame, **kwargs):
                for column in ['indicator_0', 'indicator_1', 'indicator_2', 'indicator_3']:
                    assert pd.api.types.is_bool_dtype(frame[column]), (
                        column, str(frame[column].dtype))
                assert pd.api.types.is_datetime64_any_dtype(frame['observed_at'])
                assert str(frame['observed_at'].dt.tz) == 'UTC'
                frame.to_parquet(kwargs['output_path'], index=False)
        ''', upstream=[validate])

        self.run_pipeline(output_path=str(self.output))

        stored = pd.read_parquet(self.output)
        self.assertEqual(len(stored), 6)
        self.assertEqual(str(stored['record_id'].dtype), 'int64')
        self.assertEqual(str(stored['indicator_0'].dtype), 'bool')
        self.assertEqual(str(stored['observed_at'].dt.tz), 'UTC')

    def test_text_columns_survive_the_variable_round_trip(self):
        """Text columns carry a StringDtype in pandas 3 rather than object."""
        source = self.block('source', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({
                    'record_id': pd.Series([1, 2, 3], dtype='int64'),
                    'label': pd.Series(['alpha', None, 'gamma']),
                    'score': pd.Series([0.25, 0.5, None], dtype='float64'),
                })
        ''')
        self.block('sink', 'data_exporter', '''
            import pandas as pd
            @data_exporter
            def export(frame, **kwargs):
                assert frame['label'].tolist()[0] == 'alpha'
                assert pd.isna(frame['label'].iloc[1])
                assert pd.isna(frame['score'].iloc[2])
                assert pd.api.types.is_float_dtype(frame['score'])
                frame.to_parquet(kwargs['output_path'], index=False)
        ''', upstream=[source])

        self.run_pipeline(output_path=str(self.output))

        stored = pd.read_parquet(self.output)
        self.assertEqual(stored['label'].tolist()[0], 'alpha')
        self.assertTrue(pd.isna(stored['label'].iloc[1]))

    def test_empty_dataframe_reaches_the_next_block_as_a_dataframe(self):
        """
        A loader that filters every candidate returns an empty frame. The branch that
        reports the no-op has to receive a dataframe, not None, and keep its columns.
        """
        source = self.block('source', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({
                    'record_id': pd.Series([], dtype='int64'),
                    'region': pd.Series([], dtype='str'),
                })
        ''')
        self.block('noop', 'transformer', '''
            import pandas as pd
            @transformer
            def transform(frame, **kwargs):
                assert isinstance(frame, pd.DataFrame), type(frame)
                assert frame.empty
                assert list(frame.columns) == ['record_id', 'region'], list(frame.columns)
                return dict(status='no_op', candidate_count=0)
        ''', upstream=[source])

        self.run_pipeline()

    def test_a_fitted_model_passes_to_the_scoring_block(self):
        """A model artifact is stored beside the variable rather than as JSON."""
        model = self.block('model', 'data_loader', '''
            import numpy as np
            from sklearn.linear_model import LogisticRegression
            @data_loader
            def load(**kwargs):
                estimator = LogisticRegression()
                estimator.fit(
                    np.array([[1.0, 3.0], [2.0, 4.0], [1.5, 3.5], [2.5, 4.5]]),
                    np.array([0, 1, 0, 1]),
                )
                return estimator
        ''')
        features = self.block('features', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({
                    'record_id': pd.Series([1, 2], dtype='int64'),
                    'a': pd.Series([1.0, 2.0], dtype='float64'),
                    'b': pd.Series([3.0, 4.0], dtype='float64'),
                })
        ''')
        score = self.block('score', 'transformer', '''
            import numpy as np
            import pandas as pd
            @transformer
            def transform(estimator, frame, **kwargs):
                assert hasattr(estimator, 'predict_proba'), type(estimator)
                inputs = frame.copy()
                record_ids = pd.to_numeric(inputs.pop('record_id'),
                                           errors='raise').astype('int64')
                probabilities = np.asarray(estimator.predict_proba(inputs))
                return pd.DataFrame({
                    'record_id': record_ids.to_numpy(),
                    'score': probabilities[:, 1].astype('float64'),
                    'scored_at': pd.Timestamp.now(tz='UTC'),
                })
        ''', upstream=[model, features])
        self.block('sink', 'data_exporter', '''
            import pandas as pd
            @data_exporter
            def export(frame, **kwargs):
                assert frame['score'].between(0.0, 1.0).all()
                assert frame['record_id'].is_unique
                assert str(frame['scored_at'].dt.tz) == 'UTC'
                frame.to_parquet(kwargs['output_path'], index=False)
        ''', upstream=[score])

        self.run_pipeline(output_path=str(self.output))

        stored = pd.read_parquet(self.output)
        self.assertEqual(stored['record_id'].tolist(), [1, 2])
        self.assertEqual(str(stored['score'].dtype), 'float64')

    def test_a_custom_block_dictionary_reaches_its_downstream_blocks(self):
        """A custom block resolves identity metadata once and shares it with two loaders."""
        identity = self.block('identity', 'custom', '''
            @custom
            def resolve(**kwargs):
                return dict(model_name='detector', alias='champion', version_id='7')
        ''')
        self.block('artifact', 'data_loader', '''
            @data_loader
            def load(info, **kwargs):
                assert isinstance(info, dict), type(info)
                assert info['version_id'] == '7', info
                return dict(name=info['model_name'], version=int(info['version_id']))
        ''', upstream=[identity])
        self.block('metadata', 'data_loader', '''
            import pathlib
            @data_loader
            def load(info, **kwargs):
                pathlib.Path(kwargs['output_path']).write_text(info['alias'])
                return info
        ''', upstream=[identity])

        self.run_pipeline(output_path=str(self.output))

        self.assertEqual(self.output.read_text(), 'champion')

    def test_a_conditional_reads_the_upstream_frame_to_pick_a_branch(self):
        """
        Both example flows gate on whether the previous block produced rows. The
        conditional receives the guarded block's inputs, so it sees that frame.
        """
        source = self.block('source', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'record_id': pd.Series([], dtype='int64')})
        ''')
        proceed = self.block('proceed', 'transformer', '''
            from pathlib import Path
            @transformer
            def transform(frame, **kwargs):
                Path(kwargs['output_path']).write_text('the empty branch ran')
                return frame
        ''', upstream=[source])
        has_rows = self.block('has_rows', 'conditional', '''
            @condition
            def should_run(frame, *args, **kwargs):
                return not frame.empty
        ''')
        self.pipeline.update_block(proceed, conditional_block_uuids=[has_rows.uuid])
        self.block('report', 'transformer', '''
            @transformer
            def transform(frame, **kwargs):
                assert frame.empty
                return dict(status='no_op', candidate_count=0)
        ''', upstream=[source])

        run = self.run_pipeline(output_path=str(self.output))

        statuses = {b.block_uuid.split('_')[-1]: b.status.value for b in run.block_runs}
        self.assertEqual(statuses['proceed'], 'condition_failed')
        self.assertEqual(statuses['report'], 'completed')
        self.assertFalse(self.output.exists())

    def test_output_that_cannot_be_stored_fails_at_the_producing_block(self):
        """
        A value that is neither a dataframe nor serializable used to leave an empty
        variable file behind and fail later, while reading it.
        """
        producer = self.block('producer', 'data_loader', '''
            class Handle:
                pass
            @data_loader
            def load(**kwargs):
                return Handle()
        ''')
        self.block('sink', 'data_exporter', '''
            @data_exporter
            def export(value, **kwargs):
                raise AssertionError('the downstream block should not have run')
        ''', upstream=[producer])

        run = trigger_pipeline(self.pipeline.uuid, variables={})
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        PipelineExecutor(
            self.pipeline, execution_partition=run.execution_partition,
        ).execute(
            pipeline_run_id=run.id,
            global_vars=run.get_variables(),
            allow_blocks_to_fail=True,
        )
        scheduler.schedule()
        run.refresh()

        statuses = {b.block_uuid.split('_')[-1]: b.status.value for b in run.block_runs}
        self.assertEqual(statuses.get('producer'), 'failed')
        self.assertNotEqual(statuses.get('sink'), 'completed')
