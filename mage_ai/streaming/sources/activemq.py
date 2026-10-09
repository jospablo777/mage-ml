import queue
import time
import uuid
from dataclasses import dataclass
from typing import Callable

import stomp
from stomp.exception import ConnectFailedException

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sources.base import BaseSource


@dataclass
class ActiveMQConfig(BaseConfig):
    connection_host: str
    connection_port: int
    queue_name: str
    configure_consume: bool = False
    username: str = 'admin'
    password: str = 'admin'
    # The transformer gets a list of up to batch_size message bodies, or of those that
    # arrived within batch_timeout seconds.
    batch_size: int = 1
    batch_timeout: float = 1.0


class ActiveMQMsgListener(stomp.ConnectionListener):
    """Queues the frames STOMP delivers, for batch_read to handle on its own thread."""

    def __init__(self, frames: queue.Queue):
        self.frames = frames
        self.disconnected = False

    def on_error(self, frame):
        print('Received an error "%s"' % frame.body)

    def on_message(self, frame):
        self.frames.put(frame)

    def on_disconnected(self):
        self.disconnected = True


class ActiveMQSource(BaseSource):
    """
    Hands the transformer a list of message bodies. A message is acked after the
    transformer returns; when the transformer raises, the error stops the pipeline and
    its messages go back to the queue when the connection closes. When the connection
    drops, batch_read raises.

    The transformer ran on the STOMP receiver thread, which swallowed its errors, and the
    subscription acked each message on delivery, so a failed transformer lost its
    messages while the source slept forever, also after the connection dropped.
    """
    config_class = ActiveMQConfig

    def init_client(self):
        self._print(f'Starting to initialize consumer for queue {self.config.queue_name}')
        try:
            conn = stomp.Connection11([
                (self.config.connection_host, int(self.config.connection_port)),
            ])
            self._print('Connecting to broker')
            conn.connect(self.config.username, self.config.password, wait=True)
        except ConnectFailedException:
            self._print('Connection Error! Please check broker connection')
            raise
        self.connection = conn
        self._print('Connected to broker')

    def read(self, handler: Callable):
        pass

    def batch_read(self, handler: Callable):
        self._print('Start consuming messages.')
        frames = queue.Queue()
        listener = ActiveMQMsgListener(frames)
        subscription = str(uuid.uuid4())
        self.connection.set_listener('mage', listener)
        self.connection.subscribe(
            destination=f'/queue/{self.config.queue_name}',
            id=subscription,
            ack='client-individual',
        )
        batch_size = max(int(self.config.batch_size or 1), 1)

        while True:
            batch = []
            deadline = None
            while len(batch) < batch_size:
                if listener.disconnected or not self.connection.is_connected():
                    raise ConnectionError('The connection to ActiveMQ was lost.')
                timeout = 1.0
                if batch:
                    timeout = deadline - time.monotonic()
                    if timeout <= 0:
                        break
                try:
                    frame = frames.get(timeout=timeout)
                except queue.Empty:
                    continue
                batch.append(frame)
                if deadline is None:
                    deadline = time.monotonic() + self.config.batch_timeout

            self._print(f'Received {len(batch)} messages')
            # When the handler raises, its messages stay unacked: ActiveMQ delivers them
            # again once the connection closes. A NACK would send them to the dead
            # letter queue.
            handler([frame.body for frame in batch])
            for frame in batch:
                self.connection.ack(frame.headers['message-id'], subscription)

    def destroy(self):
        connection = getattr(self, 'connection', None)
        if connection is not None and connection.is_connected():
            try:
                connection.disconnect()
            except Exception:
                pass
