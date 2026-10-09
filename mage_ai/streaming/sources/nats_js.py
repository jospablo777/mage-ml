import asyncio
import json
import ssl
import threading
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple, Union

import nats
from nats.errors import NoServersError, TimeoutError
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.enum import StrEnum
from mage_ai.streaming.constants import DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS
from mage_ai.streaming.sources.base import BaseSource, SourceConsumeMethod

Credentials = Union[str, Tuple[str, str]]


@dataclass
class SSLConfig:
    cafile: str = None
    certfile: str = None
    keyfile: str = None
    check_hostname: bool = False


class ConsumerType(StrEnum):
    PULL = "PULL"
    PUSH = "PUSH"


@dataclass
class NATSConfig(BaseConfig):
    server_url: str
    stream_name: str
    subject: str = None
    nkeys_seed_str: Optional[str] = None
    user_credentials: Optional[Credentials] = None
    use_tls: bool = False
    ssl_config: Optional[SSLConfig] = None
    consumer_name: Optional[str] = None
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout: int = DEFAULT_TIMEOUT_MS / 1000  # Convert to seconds
    consumer_type: ConsumerType = ConsumerType.PULL
    use_queue_group: bool = True
    # Seconds JetStream waits for an ack before it delivers a message again.
    ack_wait: float = None

    @classmethod
    def parse_config(self, config: Dict = None) -> Dict:
        ssl_config = config.get('ssl_config')
        if ssl_config and type(ssl_config) is dict:
            config['ssl_config'] = SSLConfig(**ssl_config)
        return config


def decode(data: bytes):
    """A message's JSON value, or its text when it is not JSON."""
    try:
        return json.loads(data)
    except ValueError:
        return data.decode('utf-8', errors='replace')


class NATSSource(BaseSource):
    """
    Reads a JetStream stream with a durable pull consumer, which hands batches to the
    transformer, or a push consumer, which hands one message at a time. A message is acked
    after the transformer returns; when the transformer raises, its messages are nacked,
    so JetStream delivers them again.

    Messages were acked before the transformer ran, so a failed transformer lost them. A
    message that was not JSON failed every fetch. The push consumer stopped the first time
    no message came within the timeout. A failed connection was printed and the source
    failed later with an AttributeError.
    """
    config_class = NATSConfig

    def __init__(self, config, **kwargs):
        self.nc = None
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.start_loop, daemon=True)
        self.thread.start()
        try:
            super().__init__(config)
        except Exception:
            self.destroy()
            raise
        self.set_consumer_type()

    def set_consumer_type(self):
        if self.config.consumer_type == ConsumerType.PUSH:
            self.consume_method = SourceConsumeMethod.READ
        else:
            self.consume_method = SourceConsumeMethod.BATCH_READ

    def start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop_loop(self):
        loop, thread = getattr(self, 'loop', None), getattr(self, 'thread', None)
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(5)

    def _run(self, coroutine, timeout: float = None):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout)

    async def ainit_client(self):
        connect_opts = {
            'servers': [self.config.server_url],
            'error_cb': self.error_cb,
            'reconnected_cb': self.reconnected_cb,
            'disconnected_cb': self.disconnected_cb,
            'closed_cb': self.closed_cb,
            # Fail once instead of retrying the first connection forever.
            'allow_reconnect': True,
            'max_reconnect_attempts': 3,
        }

        if self.config.use_tls and self.config.ssl_config:
            ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
            if self.config.ssl_config.cafile:
                ssl_ctx.load_verify_locations(self.config.ssl_config.cafile)
            if self.config.ssl_config.certfile and self.config.ssl_config.keyfile:
                ssl_ctx.load_cert_chain(
                    certfile=self.config.ssl_config.certfile,
                    keyfile=self.config.ssl_config.keyfile
                )
            connect_opts['tls'] = ssl_ctx

        if self.config.nkeys_seed_str:
            connect_opts['nkeys_seed_str'] = self.config.nkeys_seed_str
        if self.config.user_credentials:
            connect_opts['user_credentials'] = self.config.user_credentials

        self.nc = await nats.connect(**connect_opts)
        self._print(f'Connected to NATS server at {self.nc.connected_url.netloc}')
        self.js = self.nc.jetstream()

        try:
            await self.js.stream_info(self.config.stream_name)
        except NotFoundError:
            await self.js.add_stream(
                name=self.config.stream_name,
                subjects=[self.config.subject],
            )

        consumer_name = self.config.consumer_name or self.config.stream_name
        consumer_config = None
        if self.config.ack_wait:
            consumer_config = ConsumerConfig(ack_wait=self.config.ack_wait)
        if self.config.consumer_type == ConsumerType.PULL:
            self.sub = await self.js.pull_subscribe(
                self.config.subject,
                durable=consumer_name,
                config=consumer_config,
            )
            return

        subs_options = {
            'stream': self.config.stream_name,
            'subject': self.config.subject,
            'durable': consumer_name,
            'manual_ack': True,
            'config': consumer_config,
        }
        # nats-py requires the value of 'queue' to be the same as 'durable'
        if self.config.use_queue_group:
            subs_options['queue'] = consumer_name
        self.sub = await self.js.subscribe(**subs_options)

    async def disconnected_cb(self):
        self._print('Got disconnected!')

    async def reconnected_cb(self):
        self._print(f'Got reconnected to {self.nc.connected_url.netloc}')

    async def error_cb(self, e):
        self._print(f'There was an error: {e}')

    async def closed_cb(self):
        self._print('Connection is closed')

    async def aclose_client(self):
        nc = getattr(self, 'nc', None)
        if nc is not None and not nc.is_closed:
            await nc.close()

    def init_client(self):
        try:
            self._run(self.ainit_client())
        except NoServersError as error:
            raise ConnectionError(
                f'Could not connect to NATS server at {self.config.server_url}: {error}',
            ) from error

    def close_client(self):
        loop = getattr(self, 'loop', None)
        if loop is not None and loop.is_running():
            try:
                self._run(self.aclose_client(), timeout=5)
            except Exception as error:
                self._print(f'Error closing the connection: {error}')

    def destroy(self):
        self.close_client()
        self.stop_loop()

    async def _settle(self, messages, ack: bool):
        for msg in messages:
            if ack:
                await msg.ack()
            else:
                await msg.nak()

    def _handle(self, handler: Callable, value, messages) -> None:
        try:
            handler(value)
        except BaseException:
            self._run(self._settle(messages, ack=False))
            raise
        self._run(self._settle(messages, ack=True))

    def batch_read(self, handler: Callable):
        try:
            while True:
                messages = self.fetch_messages()
                if not messages:
                    continue
                self._print(f'Fetched {len(messages)} messages')
                self._handle(handler, [decode(msg.data) for msg in messages], messages)
        finally:
            self.destroy()

    def read(self, handler: Callable):
        try:
            while True:
                msg = self.fetch_message()
                if msg is None:
                    continue
                self._handle(handler, decode(msg.data), [msg])
        finally:
            self.destroy()

    async def afetch_message(self):
        try:
            return await self.sub.next_msg(self.config.timeout)
        except TimeoutError:
            return None

    def fetch_message(self):
        return self._run(self.afetch_message())

    async def afetch_messages(self):
        try:
            return await self.sub.fetch(self.config.batch_size, timeout=self.config.timeout)
        except TimeoutError:
            return []

    def fetch_messages(self):
        return self._run(self.afetch_messages())
