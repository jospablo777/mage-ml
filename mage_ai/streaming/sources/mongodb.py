from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Union

from bson.timestamp import Timestamp
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.typings import _Pipeline

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sources.base import BaseSource


@dataclass
class MongoDBConfig(BaseConfig):
    connection_str: str
    database: str
    # Without a collection, the source watches every collection of the database.
    collection: str = None
    batch_size: Optional[int] = 10
    pipeline: Optional[_Pipeline] = None
    # Seconds since the epoch, or a BSON Timestamp, to start the change stream at.
    operation_time: Optional[Union[int, float, Timestamp]] = None
    start_after: Optional[Mapping[str, Any]] = None
    full_document: Optional[str] = None


class MongoSource(BaseSource):
    """
    Reads a change stream and hands the transformer one change at a time. After each
    change the transformer handles, the stream's resume token is saved in the pipeline's
    streaming checkpoint, and a restarted source resumes after it.

    Every error, from the transformer too, was printed and the source returned, so the
    pipeline stopped as if it had finished. Each start watched from that moment, so
    changes made while the pipeline was stopped were lost. operation_time was passed to
    watch under a name it does not take, and without a collection the source watched
    nothing.
    """
    config_class = MongoDBConfig

    def init_client(self):
        self.client = MongoClient(self.config.connection_str)

    def read(self, handler: Callable):
        pass

    def build_watch_args(self) -> Dict:
        watch_args = {}
        if self.config.batch_size is not None:
            watch_args['batch_size'] = self.config.batch_size
        if self.config.pipeline is not None:
            watch_args['pipeline'] = self.config.pipeline
        if self.config.full_document is not None:
            watch_args['full_document'] = self.config.full_document

        resume_token = self._resume_token()
        if resume_token is not None:
            watch_args['resume_after'] = resume_token
        elif self.config.start_after is not None:
            watch_args['start_after'] = self.config.start_after
        elif self.config.operation_time is not None:
            operation_time = self.config.operation_time
            if not isinstance(operation_time, Timestamp):
                operation_time = Timestamp(int(operation_time), 1)
            watch_args['start_at_operation_time'] = operation_time
        return watch_args

    def _namespace(self) -> Dict:
        return dict(database=self.config.database, collection=self.config.collection)

    def _resume_token(self) -> Optional[Mapping]:
        """
        The token of the last change handled on the watched database and collection. A
        token of another namespace, saved before the config changed, is not used.
        """
        checkpoint = self.checkpoint or {}
        if checkpoint.get('resume_token') is None:
            return None
        if {k: checkpoint.get(k) for k in ('database', 'collection')} != self._namespace():
            self._print('The saved resume token is for another database or collection.')
            return None
        return checkpoint['resume_token']

    def batch_read(self, handler: Callable):
        self._print('Start getting message for MongoDB streaming.')
        database = self.client.get_database(self.config.database)
        target = database[self.config.collection] if self.config.collection else database

        watch_args = self.build_watch_args()
        try:
            stream = target.watch(**watch_args)
        except OperationFailure as error:
            if 'resume_after' in watch_args:
                raise RuntimeError(
                    'The change stream cannot resume after the last handled change, which '
                    'is no longer in the oplog. Delete the streaming checkpoint at '
                    f'{self.checkpoint_path} to start at the current time, or set '
                    f'start_after or operation_time. {error}'
                ) from error
            raise

        with stream:
            for change in stream:
                self._print(f'Received a new message: {change}.')
                handler([change])
                self.checkpoint = dict(resume_token=stream.resume_token, **self._namespace())
                self.update_checkpoint()

    def destroy(self):
        client = getattr(self, 'client', None)
        if client is not None:
            client.close()
