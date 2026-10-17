import time
from dataclasses import dataclass
from typing import Dict, List

from elasticsearch import Elasticsearch, helpers

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sinks.base import BaseSink


@dataclass
class ElasticSearchConfig(BaseConfig):
    host: str
    index_name: str
    verify_certs: bool = True
    api_key: str = None
    ca_cert: str = None
    _id: str = None
    _op_type: str = None
    _source: str = None


class ElasticSearchSink(BaseSink):
    config_class = ElasticSearchConfig

    def init_client(self):
        # Initialize elasticsearch client
        self.client = Elasticsearch(
                hosts=[self.config.host],
                api_key=self.config.api_key,
                ca_certs=self.config.ca_cert,
                verify_certs=self.config.verify_certs,
        )

    def test_connection(self):
        return True

    def write(self, message: Dict):
        self._print(f'Ingest data {message}, time={time.time()}')
        if self.config._id is not None:
            self.client.index(
                index=self.config.index_name,
                body=message,
                id=message[self.config._id],
                refresh=True
            )
        else:
            self.client.index(
                index=self.config.index_name,
                body=message,
                refresh=True
            )

    def batch_write(self, messages: List[Dict]):
        self._print(f'Batch ingest data {messages}, time={time.time()}')
        docs = []
        for msg in messages:
            doc = {'_index': self.config.index_name}
            # The bulk helper reads _id; `id` was stored as a field of the document, so
            # replays wrote duplicates instead of replacing documents.
            if self.config._id is not None:
                doc['_id'] = msg[self.config._id]
            op_type = self.config._op_type or 'index'
            if self.config._op_type is not None:
                doc['_op_type'] = op_type
            if op_type == 'update':
                # A partial update sends the fields under doc.
                doc[self.config._source or 'doc'] = msg
            elif self.config._source not in (None, '_source'):
                doc[self.config._source] = msg
            else:
                # The message is the document. It was wrapped as {"_doc": message}.
                doc['_source'] = msg
            docs.append(doc)

        helpers.bulk(self.client, docs)
