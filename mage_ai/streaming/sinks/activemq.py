import threading
import uuid
from dataclasses import dataclass
from typing import Dict, List

import simplejson
import stomp
from stomp.exception import ConnectFailedException

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.sinks.base import BaseSink


@dataclass
class ActiveMQConfig(BaseConfig):
    connection_host: str
    connection_port: int
    queue_name: str
    username: str = 'admin'
    password: str = 'admin'
    # Seconds to wait for the broker to confirm a batch.
    receipt_timeout: float = 30


class ReceiptListener(stomp.ConnectionListener):
    def __init__(self):
        self.receipts = {}
        self.errors = []

    def on_receipt(self, frame):
        event = self.receipts.get(frame.headers.get('receipt-id'))
        if event is not None:
            event.set()

    def on_error(self, frame):
        self.errors.append(frame.body or frame.headers.get('message'))
        for event in self.receipts.values():
            event.set()


class ActiveMQSink(BaseSink):
    """
    Sends each message as persistent JSON and waits for the broker's receipt of the
    batch's last message, which STOMP sends after the frames before it. Messages were
    sent without a receipt, so a message the broker rejected was lost; they were not
    persistent, so a broker restart lost them; dates and decimals failed json.dumps.
    """
    config_class = ActiveMQConfig

    def init_client(self):
        self._print(f'Starting to initialize producer for queue {self.config.queue_name}')
        try:
            conn = stomp.Connection11([
                (self.config.connection_host, int(self.config.connection_port)),
            ])
            self._print('Connecting to broker')
            self.listener = ReceiptListener()
            conn.set_listener('mage', self.listener)
            conn.connect(self.config.username, self.config.password, wait=True)
        except ConnectFailedException:
            self._print('Connection Error! Please check broker connection')
            raise
        self.connection = conn
        self._print('Connected to broker')

    def write(self, message: Dict):
        self.batch_write([message])

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        self._print(
            f'Batch ingest {len(messages)} messages. Sample: {messages[0]}'
        )
        receipt = str(uuid.uuid4())
        received = threading.Event()
        self.listener.receipts[receipt] = received
        try:
            for index, message in enumerate(messages):
                if isinstance(message, dict):
                    data = message.get('data', message)
                    metadata = message.get('metadata', None)
                else:
                    data = message
                    metadata = None
                headers = {str(k): str(v) for k, v in (metadata or {}).items()}
                headers['persistent'] = 'true'
                if index == len(messages) - 1:
                    headers['receipt'] = receipt
                self.connection.send(
                    destination=f'/queue/{self.config.queue_name}',
                    body=simplejson.dumps(
                        data, default=encode_complex, ignore_nan=True,
                    ).encode('utf-8'),
                    content_type='application/json',
                    headers=headers,
                )
            if not received.wait(self.config.receipt_timeout):
                raise TimeoutError('ActiveMQ did not confirm the messages.')
            if self.listener.errors:
                raise RuntimeError(f'ActiveMQ rejected a message: {self.listener.errors[-1]}')
        finally:
            self.listener.receipts.pop(receipt, None)

    def destroy(self):
        connection = getattr(self, 'connection', None)
        if connection is not None and connection.is_connected():
            try:
                connection.disconnect()
            except Exception:
                pass
