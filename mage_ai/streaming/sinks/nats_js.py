import asyncio
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

import nats
import simplejson

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.sinks.base import BaseSink
from mage_ai.streaming.sources.nats_js import Credentials


@dataclass
class NATSConfig(BaseConfig):
    server_url: str
    subject: str
    # The stream that takes the subject. It is created when it does not exist.
    stream_name: Optional[str] = None
    nkeys_seed_str: Optional[str] = None
    user_credentials: Optional[Credentials] = None
    # Seconds to wait for JetStream to acknowledge each message.
    timeout: float = 5


class NATSSink(BaseSink):
    """
    Publishes each message to a JetStream subject as JSON and waits for JetStream to
    store it, so a message no stream takes raises. Messages with the format
    {"data": {...}, "metadata": {...}} publish their data, with the metadata as headers.
    """
    config_class = NATSConfig

    def init_client(self):
        self.nc = None
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self._print(f'Connecting to NATS server at {self.config.server_url}')
        self._run(self._connect())
        self._print('Connected to NATS server.')

    def _run(self, coroutine, timeout: float = None):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout)

    async def _connect(self):
        options = dict(servers=[self.config.server_url], max_reconnect_attempts=3)
        if self.config.nkeys_seed_str:
            options['nkeys_seed_str'] = self.config.nkeys_seed_str
        if self.config.user_credentials:
            options['user_credentials'] = self.config.user_credentials
        self.nc = await nats.connect(**options)
        self.js = self.nc.jetstream()
        if self.config.stream_name:
            from nats.js.errors import NotFoundError

            try:
                await self.js.stream_info(self.config.stream_name)
            except NotFoundError:
                await self.js.add_stream(
                    name=self.config.stream_name,
                    subjects=[self.config.subject],
                )

    async def _publish(self, messages: List):
        for message in messages:
            headers = None
            data = message
            if self._is_message_format_v2(message):
                data = message.get('data')
                metadata = message.get('metadata') or {}
                headers = {str(k): str(v) for k, v in metadata.items()}
            await self.js.publish(
                self.config.subject,
                simplejson.dumps(
                    data, default=encode_complex, ignore_nan=True,
                ).encode('utf-8'),
                stream=self.config.stream_name,
                timeout=self.config.timeout,
                headers=headers,
            )

    def write(self, message: Dict):
        self.batch_write([message])

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        self._print(f'Batch ingest {len(messages)} messages. Sample: {messages[0]}')
        self._run(self._publish(messages))

    def destroy(self):
        loop = getattr(self, 'loop', None)
        if loop is None or not loop.is_running():
            return
        if getattr(self, 'nc', None) is not None and not self.nc.is_closed:
            try:
                self._run(self.nc.close(), timeout=5)
            except Exception as error:
                self._print(f'Error closing the connection: {error}')
        loop.call_soon_threadsafe(loop.stop)
        self.thread.join(5)
