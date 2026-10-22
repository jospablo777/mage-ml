import asyncio
import io
import logging
import logging.handlers
import os
import re
import traceback
from typing import Callable, Dict, List

from mage_ai.data_preparation.logging import LoggingConfig
from mage_ai.data_preparation.models.constants import LOGS_SUBDIR
from mage_ai.data_preparation.models.file import File
from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_config
from mage_ai.data_preparation.storage.local_storage import LocalStorage
from mage_ai.settings.repo import get_repo_path
from mage_ai.settings.server import LOGS_DIR_PATH
from mage_ai.shared.array import find
from mage_ai.shared.dates import str_to_timedelta, utc_now

MAX_LOG_FILE_SIZE = 20 * 1024 * 1024
# Segments a log keeps when it rotates: 200 GB at 20 MB each. It was 10, and output past
# 220 MB was deleted; printed text is never dropped (log_windows.py reads every segment).
LOG_FILE_SEGMENTS = 10_000


def pipeline_logs_dir(pipeline_uuid: str, repo_path: str = None) -> str:
    """The folder of a pipeline's local logs, as LoggerManager writes them, without the
    logger and log file a LoggerManager opens."""
    repo_config = get_repo_config(repo_path=repo_path or get_repo_path())
    logs_dir = LOGS_DIR_PATH or repo_config.variables_dir
    return os.path.join(logs_dir, 'pipelines', pipeline_uuid, LOGS_SUBDIR)


class LoggerManager:
    def __init__(
        self,
        repo_path: str = None,
        logs_dir: str = None,
        pipeline_uuid: str = None,
        block_uuid: str = None,
        filename: str = None,
        partition: str = None,
        repo_config: RepoConfig = None,
        subpartition: str = None,
    ):
        """
        Initialize the LoggerManager with provided configuration.

        Args:
            repo_path (str, optional): Root directory path of the repository.
            logs_dir (str, optional): Directory path where logs will be stored.
            pipeline_uuid (str, optional): Unique identifier for the pipeline.
            block_uuid (str, optional): Unique identifier for the block.
            partition (str, optional): Partition identifier.
            repo_config (RepoConfig, optional): Repository configuration.
            subpartition (str, optional): Subpartition identifier.
        """
        self.repo_path = repo_path or get_repo_path()
        self.logs_dir = logs_dir
        self.pipeline_uuid = pipeline_uuid
        self.block_uuid = block_uuid
        self.filename = filename
        self.partition = partition
        self.subpartition = subpartition

        # Load the repository configuration for logging
        self.repo_config = repo_config or get_repo_config()
        logging_config = self.repo_config.logging_config if self.repo_config else dict()
        self.logging_config = LoggingConfig.load(config=logging_config)

        # Create logger instance based on provided identifiers
        if self.pipeline_uuid:
            logger_name_parts = [self.pipeline_uuid]
        else:
            logger_name_parts = ['all_pipelines']
        if self.partition is not None:
            logger_name_parts.append(self.partition)
        if self.block_uuid is not None:
            logger_name_parts.append(self.block_uuid)
        self.logger_name = '/'.join(logger_name_parts)

        # Set the log level based on the configuration
        self.log_level = logging.getLevelName(self.logging_config.level)

        # Create log formatter
        self.formatter = logging.Formatter(
            '%(asctime)s %(message)s',
            '%Y-%m-%dT%H:%M:%S',
        )

        self.stream = None
        # The handler this instance attached, which only it removes.
        self._handler = None
        self._logger = None

        self.storage = LocalStorage()

    @property
    def logger(self) -> logging.Logger:
        """
        The logger, with its handler attached on first use. Reading a log never logs: it
        created the log's folder and an empty log file, and when the reader was collected it
        took the file handler off the logger a running block wrote with.
        """
        if self._logger is not None:
            return self._logger
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.log_level)

        if not logger.handlers:
            if self.logging_config.destination_config:
                # If there is a destination configuration, use a stream handler
                handler = self.create_stream_handler()
            else:
                # If no destination configuration, use a rotating file handler
                log_filepath = self.get_log_filepath(create_dir=True)
                handler = logging.handlers.RotatingFileHandler(
                    log_filepath,
                    backupCount=LOG_FILE_SEGMENTS,
                    maxBytes=MAX_LOG_FILE_SIZE,
                )

            handler.setLevel(self.log_level)
            handler.setFormatter(self.formatter)
            logger.addHandler(handler)
            self._handler = handler
        elif self.logging_config.destination_config:
            # If a stream handler already exists, get the stream object
            stream_handler = find(
                lambda hr: hr.__class__ == logging.StreamHandler, logger.handlers
            )
            if stream_handler:
                self.stream = stream_handler.stream

        self._logger = logger
        return logger

    def __del__(self):
        """
        Close the log file if this instance attached a file handler.
        """
        handler = getattr(self, '_handler', None)
        if handler is None or not isinstance(
            handler, (logging.FileHandler, logging.handlers.RotatingFileHandler),
        ):
            return
        try:
            self._logger.removeHandler(handler)
            handler.close()
        except Exception:
            traceback.print_exc()

    def create_stream_handler(self):
        """
        Create a stream handler to output logs to a stream (e.g., in-memory buffer).
        Used when a destination configuration is present.

        Returns:
            logging.StreamHandler: The stream handler.
        """
        self.stream = io.StringIO()
        return logging.StreamHandler(self.stream)

    def create_log_filepath_dir(self, path):
        """
        Create the directory path for log files, if it doesn't exist.

        Args:
            path (str): The path to create the directory.
        """
        os.makedirs(path, exist_ok=True)

    def delete_old_logs(self):
        """
        Delete old log files based on log retention_period
        """
        log_retention_period = self.logging_config.retention_period
        if not log_retention_period:
            return
        min_partition = (utc_now() -
                         str_to_timedelta(log_retention_period)).strftime(
                            format='%Y%m%dT%H%M%S')

        from mage_ai.data_preparation.models.constants import PipelineType
        from mage_ai.data_preparation.models.pipeline import Pipeline
        if self.pipeline_uuid is None:
            pipeline_uuids = Pipeline.get_all_pipelines(self.repo_path)
        else:
            pipeline_uuids = [self.pipeline_uuid]

        for pipeline_uuid in pipeline_uuids:
            print(f'Removing old logs from pipeline {pipeline_uuid}')
            # Skip cleaning up logs for streaming pipelines
            skip_clean_up_logs = False
            try:
                p = Pipeline.get(pipeline_uuid)
                if p.type == PipelineType.STREAMING:
                    print(f'Pipeline {pipeline_uuid} is a Streaming pipeline. '
                          'Skip cleaning up logs.')
                    skip_clean_up_logs = True
            except Exception as e:
                print(f'Fail to load pipeline {pipeline_uuid} due to error {e}. '
                      'Skip cleaning up logs.')
                skip_clean_up_logs = True

            if skip_clean_up_logs:
                continue

            pipeline_log_path = self.get_log_filepath_prefix(pipeline_uuid=pipeline_uuid)
            dirs = self.storage.listdir(pipeline_log_path)
            for dirname in dirs:
                if dirname.isdigit():
                    pipeline_schedule_vpath = os.path.join(pipeline_log_path, dirname)
                    execution_partitions = self.storage.listdir(
                        pipeline_schedule_vpath,
                    )
                    for partition in execution_partitions:
                        if partition <= min_partition:
                            pipeline_partition_vpath = os.path.join(
                                pipeline_schedule_vpath,
                                partition,
                            )
                            print(f'Removing folder {pipeline_partition_vpath}')
                            self.storage.remove_dir(pipeline_partition_vpath)

    def get_log_filepath_prefix(
        self,
        pipeline_uuid: str = None,
    ):
        """
        Get the prefix of the log file path.

        Returns:
            str: The log file path prefix based on pipeline_uuid, logs_dir, partition, and
                 subpartition.
        """
        logs_dir = self.logs_dir or LOGS_DIR_PATH or self.repo_config.variables_dir

        return os.path.join(
            logs_dir,
            'pipelines',
            pipeline_uuid or self.pipeline_uuid or 'all_pipelines',
            LOGS_SUBDIR,
            self.partition or '',
            self.subpartition or '',
        )

    def get_log_filepath(self, create_dir: bool = False):
        """
        Get the full log file path for the current pipeline or block.

        Args:
            create_dir (bool, optional): If True, create the log file directory if it doesn't exist.

        Returns:
            str: The full log file path.
        Raises:
            Exception: If pipeline_uuid is None.
        """
        prefix = self.get_log_filepath_prefix()

        if create_dir:
            self.create_log_filepath_dir(prefix)

        if self.filename is not None:
            log_filepath = os.path.join(prefix, self.filename)
        elif self.block_uuid is None:
            log_filepath = os.path.join(prefix, 'pipeline.log')
        else:
            log_filepath = os.path.join(prefix, f'{self.block_uuid}.log')
        return log_filepath

    def get_logs(self):
        """
        The log as a log item: its last lines over all its rotated segments, with first_line
        and total_lines for reading the lines before (log_windows.py).
        """
        from mage_ai.data_preparation.logging import log_windows

        return log_windows.window(self.get_log_filepath())

    async def get_logs_async(self):
        """As get_logs, in a thread."""
        from mage_ai.data_preparation.logging import log_windows

        return await asyncio.to_thread(log_windows.window, self.get_log_filepath())

    async def get_logs_in_subpartition_async(self, filter_func: Callable = None) -> List[Dict]:
        """
        Get logs asynchronously from multiple log files within the subpartition.

        Args:
            filter_func (Callable, optional): A filter function to apply on each log file.

        Returns:
            List[Dict]: A list of dictionaries containing the logs, including their content.
        """
        from mage_ai.data_preparation.logging import log_windows

        files = []

        base_path = self.get_log_filepath_prefix()
        if os.path.exists(base_path):
            for filename in os.listdir(base_path):
                full_path = f'{base_path}/{filename}'
                if not os.path.isfile(full_path):
                    continue
                # A rotated segment (name.log.3) is read with its log.
                if re.search(r'\.log\.[0-9]+$', filename):
                    continue

                file = File.from_path(full_path)
                should_add = True

                if filter_func:
                    should_add = filter_func(file)

                if should_add:
                    files.append(file)

        return await asyncio.gather(*[
            asyncio.to_thread(log_windows.window, os.path.join(file.dir_path, file.filename))
            for file in files
        ])

    def upload_logs(self, key: str, logs: str) -> None:
        """
        Upload logs to the configured destination.
        (Placeholder, this function is not implemented yet.)

        Args:
            key (str): key to upload to logs to in the destination
            logs (str): logs to upload
        """
        pass

    def output_logs_to_destination(self) -> None:
        """
        Fetch existing logs from the destination if they exist and
        upload logs from the stream to the configured destination.
        """
        if self.stream:
            existing_logs = self.get_logs().get('content')
            new_logs = self.stream.getvalue()
            if existing_logs:
                new_logs = existing_logs + '\n' + new_logs

            key = self.get_log_filepath()
            self.upload_logs(key, new_logs)
