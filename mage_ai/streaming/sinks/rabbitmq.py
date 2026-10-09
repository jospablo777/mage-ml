from dataclasses import dataclass
from typing import Dict, List

import pika
import simplejson

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.sinks.base import BaseSink
from mage_ai.streaming.sources.rabbitmq import connection_label, connection_parameters


@dataclass
class RabbitMQConfig(BaseConfig):
    connection_host: str
    connection_port: int
    queue_name: str
    username: str = 'guest'
    password: str = 'guest'
    url_protocol: str = 'amqp'
    amqp_url_virtual_host: str = r'%2f'


class RabbitMQSink(BaseSink):
    """
    Publishes each message to the queue and waits for the broker to confirm it. Without
    confirms, a message that no queue took or that the broker rejected was lost without
    an error. Messages are persistent, so a durable queue keeps them across a broker
    restart.
    """
    config_class = RabbitMQConfig

    def init_client(self):
        self._print('Start initializing producer.')
        self._print(f'Starting to initialize producer for queue {self.config.queue_name}')
        self._print(f'Trying to connect on {connection_label(self.config)}')
        try:
            self.connection = pika.BlockingConnection(connection_parameters(self.config))
        except Exception:
            self._print('Connection Error! Please check RabbitMQ connection')
            raise
        self.main_channel = self.connection.channel()
        # basic_publish raises UnroutableError or NackError.
        self.main_channel.confirm_delivery()
        self._print('Connected on broker. Finish initializing producer.')

    def write(self, message: Dict):
        pass

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        self._print(
            f'Batch ingest {len(messages)} messages. Sample: {messages[0]}'
        )
        for message in messages:
            if isinstance(message, dict):
                data = message.get('data', message)
                metadata = message.get('metadata', None)
            else:
                data = message
                metadata = None
            message_properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=pika.DeliveryMode.Persistent,
                headers=metadata,
            )
            self.main_channel.basic_publish(
                exchange='',
                routing_key=self.config.queue_name,
                # Dates, UUIDs and other values json cannot write raised.
                body=simplejson.dumps(
                    data, default=encode_complex, ignore_nan=True,
                ).encode('utf-8'),
                properties=message_properties,
                mandatory=True,
            )

    def destroy(self):
        connection = getattr(self, 'connection', None)
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                pass
