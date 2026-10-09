from collections import namedtuple
from dataclasses import dataclass
from typing import Callable, Dict
from urllib.parse import quote

import pika
from pika.exceptions import AMQPConnectionError

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sources.base import BaseSource

# The message handed to the transformer, for better usage on transformer blocks.
Payload = namedtuple('Payload', ['method', 'properties', 'body'])


@dataclass
class ConsumeConfig:
    auto_ack: bool = False
    exclusive: bool = False
    inactivity_timeout: float = None


@dataclass
class RabbitMQConfig(BaseConfig):
    connection_host: str
    connection_port: int
    queue_name: str
    configure_consume: bool = False
    username: str = 'guest'
    password: str = 'guest'
    amqp_url_virtual_host: str = r'%2f'
    url_protocol: str = 'amqp'
    consume_config: ConsumeConfig = None
    # How many messages the broker sends before the source acks them. Without a limit it
    # sent the whole queue, which the source held in memory.
    prefetch_count: int = 100
    # With batch_size above 1, the transformer gets a list of up to batch_size messages,
    # or of the messages that arrived within batch_timeout seconds. One message at a time
    # made each sink write one row per call.
    batch_size: int = 1
    batch_timeout: float = 1.0

    @classmethod
    def parse_config(self, config: Dict) -> Dict:
        consume_config = config.get('consume_config')
        if consume_config is not None and type(consume_config) is dict:
            config['consume_config'] = ConsumeConfig(**consume_config)
        return config


def connection_parameters(config) -> pika.URLParameters:
    """
    The parameters of a RabbitMQ connection. The user name and password were written
    into the URL unquoted, so a password with @, / or : failed to connect.
    """
    return pika.URLParameters(
        f'{config.url_protocol}://{quote(config.username, safe="")}:'
        f'{quote(config.password, safe="")}@{config.connection_host}:'
        f'{config.connection_port}/{config.amqp_url_virtual_host}'
    )


def connection_label(config) -> str:
    """The connection URL without the credentials, which were printed."""
    return (
        f'{config.url_protocol}://{config.connection_host}:{config.connection_port}/'
        f'{config.amqp_url_virtual_host}'
    )


class SettlingChannel:
    """
    The channel handed to the transformer, which can ack, nack or reject its message. It
    records those calls, so the source does not ack the message again: a second ack of
    a delivery closes the channel.
    """

    def __init__(self, channel):
        self._channel = channel
        self._settled = set()
        self._settled_up_to = 0

    def _record(self, delivery_tag: int, multiple: bool) -> None:
        if multiple:
            self._settled_up_to = max(self._settled_up_to, delivery_tag)
        else:
            self._settled.add(delivery_tag)

    def is_settled(self, delivery_tag: int) -> bool:
        return delivery_tag in self._settled or delivery_tag <= self._settled_up_to

    def basic_ack(self, delivery_tag=0, multiple=False):
        self._record(delivery_tag, multiple)
        return self._channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        self._record(delivery_tag, multiple)
        return self._channel.basic_nack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue,
        )

    def basic_reject(self, delivery_tag=0, requeue=True):
        self._record(delivery_tag, False)
        return self._channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    def __getattr__(self, name):
        return getattr(self._channel, name)


class RabbitMQSource(BaseSource):
    """
    Hands each message to the transformer as a Payload, or a list of them with
    batch_size, with the channel in the channel keyword argument. A message is acked once
    the transformer returns, unless the transformer acked, nacked or rejected it; when
    the transformer raises, the message stays unacked and goes back to the queue when the
    connection closes. Messages were never acked unless the transformer did it, so a
    restarted pipeline read them again.
    """
    config_class = RabbitMQConfig

    def init_client(self):
        queue_name = self.config.queue_name

        self._print(f'Starting to initialize consumer for queue {queue_name}')
        self._print(f'Trying to connect on {connection_label(self.config)}')
        try:
            self.create_connection = pika.BlockingConnection(
                connection_parameters(self.config),
            )
        except AMQPConnectionError:
            self._print('Connection Error, please check broker connection')
            raise
        self._print('Connected on broker')

        self.channel = self.create_connection.channel()
        if self.config.prefetch_count:
            self.channel.basic_qos(prefetch_count=self.config.prefetch_count)

    def read(self, handler: Callable):
        pass

    def batch_read(self, handler: Callable):
        self._print('Start consuming messages.')

        consume_config = ConsumeConfig()
        if self.config.configure_consume and self.config.consume_config is not None:
            consume_config = self.config.consume_config
        batch_size = max(int(self.config.batch_size or 1), 1)
        inactivity_timeout = consume_config.inactivity_timeout
        if batch_size > 1:
            inactivity_timeout = self.config.batch_timeout

        channel = SettlingChannel(self.channel)
        batch = []

        def handle(messages):
            if batch_size > 1:
                self._print(f'Received {len(messages)} messages')
                handler(messages, channel=channel)
            else:
                self.__print_message(messages[0])
                handler(messages[0], channel=channel)
            if consume_config.auto_ack:
                return
            for message in messages:
                if not channel.is_settled(message.method.delivery_tag):
                    self.channel.basic_ack(delivery_tag=message.method.delivery_tag)

        for method, properties, body in self.channel.consume(
            self.config.queue_name,
            auto_ack=consume_config.auto_ack,
            exclusive=consume_config.exclusive,
            inactivity_timeout=inactivity_timeout,
        ):
            if method is not None:
                batch.append(Payload(method, properties, body))
            # An inactivity timeout gives None values. It reached the transformer as a
            # message; it ends a partial batch.
            if batch and (len(batch) >= batch_size or method is None):
                messages, batch = batch, []
                handle(messages)

    def destroy(self):
        connection = getattr(self, 'create_connection', None)
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                pass

    def __print_message(self, method):
        self._print(f'Received message {method}')
