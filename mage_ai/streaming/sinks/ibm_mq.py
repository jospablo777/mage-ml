"""
IBM MQ sink for streaming pipelines: each batch is put under syncpoint, persistent, and
committed once every message is on the queue; a failure backs the batch out.
"""
from typing import Dict, List

import simplejson

from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.sinks.base import BaseSink
from mage_ai.streaming.sources.ibm_mq import IbmMqConfig, connect, import_ibmmq


class IbmMqSink(BaseSink):
    config_class = IbmMqConfig

    def init_client(self):
        self.ibmmq = import_ibmmq()
        self._print(f'Connecting to {self.config.label()}.')
        options = self.ibmmq.CMQC.MQOO_OUTPUT | self.ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING
        self.qm, self.queue = connect(self.config, options)
        self._print('Connected.')

    def test_connection(self):
        pass

    def write(self, message: Dict):
        self.batch_write([message])

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        ibmmq = self.ibmmq
        try:
            for message in messages:
                if isinstance(message, dict) and 'data' in message and 'metadata' in message:
                    message = message['data']
                if isinstance(message, (bytes, bytearray)):
                    body = bytes(message)
                elif isinstance(message, str):
                    body = message.encode()
                else:
                    body = simplejson.dumps(
                        message, default=encode_complex, ignore_nan=True,
                    ).encode()
                md = ibmmq.MD()
                md.Format = ibmmq.CMQC.MQFMT_STRING
                md.CodedCharSetId = 1208
                md.Persistence = ibmmq.CMQC.MQPER_PERSISTENT
                pmo = ibmmq.PMO()
                pmo.Options = ibmmq.CMQC.MQPMO_SYNCPOINT | ibmmq.CMQC.MQPMO_FAIL_IF_QUIESCING
                self.queue.put(body, md, pmo)
        except BaseException:
            self.qm.backout()
            raise
        self.qm.commit()
        self._print(f'Put {len(messages)} messages on {self.config.queue}.')

    def destroy(self):
        qm = getattr(self, 'qm', None)
        if qm is None:
            return
        try:
            qm.backout()
        finally:
            try:
                self.queue.close()
            finally:
                qm.disconnect()
                self.qm = None
