import copy
import json
import os
import subprocess
import sys
from contextlib import contextmanager, redirect_stdout
from logging import Logger
from typing import Callable, Dict, Generator, List

import pandas as pd

from mage_ai.data_cleaner.transformer_actions.utils import clean_column_name
from mage_ai.data_integrations.destinations.truncate import (
    should_truncate,
    truncate_destination_table,
)
from mage_ai.data_integrations.logger.utils import print_log_from_line
from mage_ai.data_integrations.utils.config import (
    build_config,
    get_batch_fetch_limit,
    get_catalog_by_stream,
)
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.data_integration.constants import (
    CONFIG_KEY_CLEAN_UP_INPUT_FILE,
)
from mage_ai.data_preparation.models.block.integration.records import (
    records_frame,
    records_per_call,
)
from mage_ai.data_preparation.models.constants import PYTHON_COMMAND, BlockType
from mage_ai.data_preparation.shared.stream import StreamToLogger
from mage_ai.shared.hash import merge_dict
from mage_ai.shared.pandas_utils import missing_as_none
from mage_ai.shared.security import filter_out_config_values


class IntegrationBlock(Block):
    @contextmanager
    def _redirect_streams(
        self,
        build_block_output_stdout: Callable[..., object] = None,
        from_notebook: bool = False,
        logger: Logger = None,
        logging_tags: Dict = None,
    ) -> Generator[None, None, None]:
        if build_block_output_stdout:
            stdout = build_block_output_stdout(self.uuid)
        elif logger is not None and not from_notebook:
            stdout = StreamToLogger(logger, logging_tags=logging_tags)
        else:
            stdout = sys.stdout

        try:
            with redirect_stdout(stdout) as out:
                yield out
        finally:
            if isinstance(stdout, StreamToLogger):
                stdout.flush()

    def _execute_block(
        self,
        outputs_from_input_vars,
        execution_partition: str = None,
        from_notebook: bool = False,
        global_vars: Dict = None,
        input_vars: List = None,
        logger: Logger = None,
        logging_tags: Dict = None,
        input_from_output: Dict = None,
        runtime_arguments: Dict = None,
        **kwargs,
    ) -> List:
        if logging_tags is None:
            logging_tags = dict()

        index = self.template_runtime_configuration.get('index', None)
        is_last_block_run = self.template_runtime_configuration.get('is_last_block_run', False)
        selected_streams = self.template_runtime_configuration.get('selected_streams', [])
        stream = selected_streams[0] if len(selected_streams) >= 1 else None
        destination_table = self.template_runtime_configuration.get('destination_table', stream)
        query_data = runtime_arguments or {}
        query_data = query_data.copy()

        tags = dict(block_tags=dict(
            destination_table=destination_table,
            index=index,
            stream=stream,
            type=self.type,
            uuid=self.uuid,
        ))
        updated_logging_tags = merge_dict(
            logging_tags,
            dict(tags=tags),
        )

        variables_dictionary_for_config = merge_dict(global_vars, {
            'pipeline.name': self.pipeline.name if self.pipeline else None,
            'pipeline.uuid': self.pipeline.uuid if self.pipeline else None,
        })

        if index is not None:
            source_state_file_path = self.pipeline.source_state_file_path(
                destination_table=destination_table,
                stream=stream,
            )
            destination_state_file_path = self.pipeline.destination_state_file_path(
                destination_table=destination_table,
                stream=stream,
            )
            source_output_file_path = self.pipeline.source_output_file_path(stream, index)

            stream_catalog = get_catalog_by_stream(
                self.pipeline.data_loader.file_path,
                stream,
                global_vars,
                pipeline=self.pipeline,
            ) or dict()

            replication_method = stream_catalog.get('replication_method')
            # An INCREMENTAL stream's batches of one run all read from the bookmark the
            # previous run ended with, each its own slice (_offset below). Moved after each
            # batch, the bookmark (compared with >=) made the next batch read the last row
            # of the one before again.
            if replication_method == 'LOG_BASED' or (
                replication_method == 'INCREMENTAL' and not index
            ):
                from mage_integrations.sources.utils import (
                    update_source_state_from_destination_state,
                )
                update_source_state_from_destination_state(
                    source_state_file_path,
                    destination_state_file_path,
                )

        outputs = []
        if BlockType.DATA_LOADER == self.type:
            lines_in_file = 0

            with open(source_output_file_path, 'w') as f:
                config, config_json = build_config(
                    self.pipeline.data_loader.file_path,
                    variables_dictionary_for_config,
                )
                batch_fetch_limit = get_batch_fetch_limit(config)

                if stream_catalog.get('replication_method') in ('FULL_TABLE', 'INCREMENTAL') or (
                    stream_catalog.get('replication_method') == 'LOG_BASED' and
                    not stream_catalog.get('bookmark_properties')
                ):
                    query_data['_offset'] = batch_fetch_limit * index
                if not is_last_block_run:
                    query_data['_limit'] = batch_fetch_limit

                args = [
                    PYTHON_COMMAND,
                    self.pipeline.source_file_path,
                    '--config_json',
                    config_json,
                    '--log_to_stdout',
                    '1',
                    '--settings',
                    self.pipeline.settings_file_path,
                    '--state',
                    source_state_file_path,
                    '--query_json',
                    json.dumps(query_data),
                ]

                if len(selected_streams) >= 1:
                    args += [
                        '--selected_streams_json',
                        json.dumps(selected_streams),
                    ]

                proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                for line in proc.stdout:
                    f.write(line.decode())
                    print_log_from_line(
                        line,
                        config=config,
                        logger=logger,
                        logging_tags=logging_tags,
                        tags=tags,
                    )
                    lines_in_file += 1

                outputs.append(proc)

            proc.communicate()
            if proc.returncode != 0 and proc.returncode is not None:
                cmd = proc.args if isinstance(proc.args, str) else str(proc.args)
                raise subprocess.CalledProcessError(
                    proc.returncode,
                    filter_out_config_values(cmd, config),
                )

            file_size = os.path.getsize(source_output_file_path)
            msg = f'Finished writing {file_size} bytes with {lines_in_file} lines to output '\
                f'file {source_output_file_path}.'
            if logger:
                logger.info(msg, **updated_logging_tags)
            else:
                print(msg)
        elif BlockType.TRANSFORMER == self.type:
            from mage_integrations.sources.constants import COLUMN_TYPE_NULL
            from mage_integrations.transformers.utils import (
                convert_data_type,
                infer_dtypes,
            )
            from mage_integrations.utils.logger.constants import (
                TYPE_RECORD,
                TYPE_SCHEMA,
            )

            decorated_functions = []
            test_functions = []

            results = {
                self.type: self._block_decorator(decorated_functions),
                'test': self._block_decorator(test_functions),
            }
            results.update(outputs_from_input_vars)

            exec(self.content, results)

            per_call = records_per_call(self.configuration)
            input_kwargs = merge_dict(
                global_vars,
                dict(
                    index=index,
                    query=query_data,
                    stream=stream,
                ),
            )

            # Support an 'aliases' property for columns, which keeps types mapped correctly
            # in case of transformer code using pandas.rename
            output_col_aliases = {}
            # We need to look at original stream_catalog schema props:
            for _name, col_props in stream_catalog['schema']['properties'].items():
                for alias in col_props.get('aliases', []):
                    output_col_aliases[alias] = col_props

            def __update_col_names(schema_dict: Dict, properties_key: str, new_columns: List[str]):
                properties_cols = schema_dict.get(properties_key, [])
                if properties_cols:
                    updated_properties_cols = []
                    for col in properties_cols:
                        if col not in new_columns:
                            clean_col = clean_column_name(col)
                            if clean_col in new_columns:
                                updated_properties_cols.append(clean_col)
                        else:
                            updated_properties_cols.append(col)
                    schema_dict[properties_key] = updated_properties_cols

            def __schema_of(schema_original: Dict, df: pd.DataFrame) -> Dict:
                properties_updated = {
                    k: dict(type=[COLUMN_TYPE_NULL, convert_data_type(v)])
                    for k, v in infer_dtypes(df).items()
                }
                schema_updated = copy.deepcopy(schema_original)
                properties_original = schema_updated['schema']['properties']
                schema_updated['schema']['properties'] = {
                    k: properties_original[k] if k in properties_original else v
                    for k, v in properties_updated.items()
                }
                # Separately, update schema types from alias map
                for cname in schema_updated['schema']['properties'].keys():
                    if cname in output_col_aliases:
                        schema_updated['schema']['properties'][cname] = output_col_aliases[cname]

                # Update column names in unique_constraints and key_properties
                new_columns = schema_updated['schema']['properties'].keys()
                __update_col_names(schema_updated, 'unique_constraints', new_columns)
                __update_col_names(schema_updated, 'key_properties', new_columns)
                return schema_updated

            records_read = 0
            records_transformed = 0
            schema_original = None
            schema_updated = None
            # Lines held until the transformer's first rows give the schema they follow.
            held = []
            # Record messages read since the last call, in order.
            messages = []

            transformed_file_path = f'{source_output_file_path}.transformed'
            with open(source_output_file_path, 'r') as f, \
                    open(transformed_file_path, 'w') as output:

                def __write(line: str):
                    if schema_updated is None and schema_original is not None:
                        held.append(line)
                    else:
                        output.write(line + '\n')

                def __transform():
                    nonlocal records_read, records_transformed, schema_updated
                    if not messages:
                        return
                    records_read += len(messages)
                    input_vars = [records_frame([m['record'] for m in messages])]
                    block_function = self._validate_execution(decorated_functions, input_vars)
                    if block_function is None:
                        rows = [m['record'] for m in messages]
                    else:
                        try:
                            df = self.execute_block_function(
                                block_function,
                                input_vars,
                                global_vars=input_kwargs,
                                from_notebook=from_notebook,
                            )
                        except Exception as error:
                            if per_call > 1:
                                error.add_note(
                                    f'This transformer receives up to {per_call} records of '
                                    f'stream {stream} in one DataFrame. If it handles one '
                                    'record at a time, set "Records per call" to 1 in the '
                                    "block's settings (configuration.records_per_call).",
                                )
                            raise
                        if schema_updated is None and schema_original is not None and df.shape[0]:
                            schema_updated = __schema_of(schema_original, df)
                            for line in held:
                                if json.loads(line).get('type') == TYPE_SCHEMA:
                                    line = json.dumps(schema_updated)
                                output.write(line + '\n')
                            held.clear()
                        rows = missing_as_none(df).to_dict('records')

                    # The other keys of a record message (stream, time_extracted) come from
                    # the first message of the call.
                    template = {k: v for k, v in messages[0].items() if k != 'record'}
                    messages.clear()
                    for row in rows:
                        __write(json.dumps(merge_dict(template, dict(record=row))))
                    records_transformed += len(rows)

                    msg = f'{records_read} records read, {records_transformed} records ' \
                        'written by the transformer...'
                    if logger:
                        logger.info(msg, **updated_logging_tags)
                    else:
                        print(msg)

                for line in f:
                    line = line.strip() if line else ''
                    if len(line) == 0:
                        continue

                    try:
                        data = json.loads(line)
                    except json.decoder.JSONDecodeError:
                        data = None
                    line_type = data.get('type') if isinstance(data, dict) else None

                    if TYPE_RECORD == line_type:
                        messages.append(data)
                        if len(messages) >= per_call:
                            __transform()
                        continue

                    # Records before a STATE or SCHEMA message are written before it.
                    __transform()
                    if TYPE_SCHEMA == line_type:
                        if schema_original is None:
                            schema_original = data
                        if schema_updated is not None:
                            line = json.dumps(schema_updated)
                    __write(line)

                __transform()
                # No rows came out: the stream keeps the schema it had.
                for line in held:
                    output.write(line + '\n')

            os.replace(transformed_file_path, source_output_file_path)

            msg = f'Transformed {records_read} total records for stream {stream} into ' \
                f'{records_transformed} records, {per_call} records per call.'
            file_size = os.path.getsize(source_output_file_path)
            msg2 = f'Finished writing {file_size} bytes to output file {source_output_file_path}.'
            if logger:
                logger.info(msg, **updated_logging_tags)
                logger.info(msg2, **updated_logging_tags)
            else:
                print(msg)
                print(msg2)

            self.test_functions = test_functions
        elif BlockType.DATA_EXPORTER == self.type:
            override = {}
            if destination_table:
                override['table'] = destination_table

            file_size = os.path.getsize(source_output_file_path)
            msg = f'Reading {file_size} bytes from {source_output_file_path} as input file.'
            if logger:
                logger.info(msg, **updated_logging_tags)
            else:
                print(msg)

            config, config_json = build_config(
                self.pipeline.data_exporter.file_path,
                variables_dictionary_for_config,
                override=override,
            )

            destination_args = [
                PYTHON_COMMAND,
                self.pipeline.destination_file_path,
                '--config_json',
                config_json,
                '--log_to_stdout',
                '1',
                '--settings',
                self.pipeline.data_exporter.file_path,
                '--state',
                self.pipeline.destination_state_file_path(
                    destination_table=destination_table,
                    stream=stream,
                ),
            ]

            # A stream's batches run one after another here, so the first one empties the
            # table for the whole sync.
            if index == 0 and should_truncate(config, stream_catalog.get('replication_method')):
                truncate_destination_table(
                    destination_args,
                    config,
                    logger=logger,
                    logging_tags=logging_tags,
                    tags=tags,
                )

            proc = subprocess.Popen(
                destination_args + ['--input_file_path', source_output_file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            for line in proc.stdout:
                print_log_from_line(
                    line,
                    config=config,
                    logger=logger,
                    logging_tags=logging_tags,
                    tags=tags,
                )

            proc.communicate()
            if proc.returncode != 0 and proc.returncode is not None:
                cmd = proc.args if isinstance(proc.args, str) else str(proc.args)
                raise subprocess.CalledProcessError(
                    proc.returncode,
                    filter_out_config_values(cmd, config),
                )

            # Automatically clean up the input file to save space
            if config.get(CONFIG_KEY_CLEAN_UP_INPUT_FILE):
                if os.path.exists(source_output_file_path):
                    os.remove(source_output_file_path)

            outputs.append(proc)

        return outputs


class SourceBlock(IntegrationBlock):
    pass


class DestinationBlock(IntegrationBlock):
    def to_dict(
        self,
        include_content: bool = False,
        include_outputs: bool = False,
        include_block_pipelines: bool = False,
        sample_count: int = None,
        check_if_file_exists: bool = False,
        destination_table: str = None,
        state_stream: str = None,
    ):
        data = {}
        if state_stream and destination_table:
            from mage_ai.data_preparation.models.pipelines.integration_pipeline import (
                IntegrationPipeline,
            )
            integration_pipeline = IntegrationPipeline(
                self.pipeline.uuid, repo_path=self.repo_path
            )
            destination_state_file_path = integration_pipeline.destination_state_file_path(
                destination_table=destination_table,
                stream=state_stream,
            )
            if os.path.isfile(destination_state_file_path):
                with open(destination_state_file_path, 'r') as f:
                    text = f.read()
                    d = json.loads(text) if text else {}
                    bookmark_values = d.get('bookmarks', {}).get(state_stream)
                    data['bookmarks'] = bookmark_values

        return merge_dict(
            super().to_dict(
                include_content=include_content,
                include_outputs=include_outputs,
                include_block_pipelines=include_block_pipelines,
                sample_count=sample_count,
                check_if_file_exists=check_if_file_exists,
            ),
            data,
        )

    async def to_dict_async(
        self,
        include_content: bool = False,
        include_outputs: bool = False,
        include_block_pipelines: bool = False,
        sample_count: int = None,
        check_if_file_exists: bool = False,
        destination_table: str = None,
        state_stream: str = None,
        **kwargs,
    ) -> Dict:
        return self.to_dict(
            include_content=include_content,
            include_outputs=include_outputs,
            include_block_pipelines=include_block_pipelines,
            sample_count=sample_count,
            check_if_file_exists=check_if_file_exists,
            destination_table=destination_table,
            state_stream=state_stream,
        )

    def update(self, data, update_state=False, **kwargs):
        if update_state:
            from mage_ai.data_preparation.models.pipelines.integration_pipeline import (
                IntegrationPipeline,
            )
            from mage_integrations.destinations.utils import (
                update_destination_state_bookmarks,
            )

            integration_pipeline = IntegrationPipeline(
                self.pipeline.uuid, repo_path=self.repo_path
            )
            tap_stream_id = data.get('tap_stream_id')
            destination_table = data.get('destination_table')
            bookmark_values = data.get('bookmark_values', {})
            if tap_stream_id and destination_table:
                destination_state_file_path = integration_pipeline.destination_state_file_path(
                    destination_table=destination_table,
                    stream=tap_stream_id,
                )
                update_destination_state_bookmarks(
                    destination_state_file_path,
                    tap_stream_id,
                    bookmark_values=bookmark_values
                )

        return super().update(data, **kwargs)

    def output_variables(self, execution_partition: str = None) -> List[str]:
        return []


class TransformerBlock(IntegrationBlock):
    pass
