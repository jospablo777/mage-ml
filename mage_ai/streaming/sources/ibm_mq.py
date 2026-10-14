"""
IBM MQ source for streaming pipelines, on IBM's `ibmmq` package (the `ibm-mq` extra), which
needs the IBM MQ client installed.

Messages are read under syncpoint and committed after the pipeline handled their batch; when
the pipeline fails, the batch is backed out and the queue manager delivers it again. A normal
disconnect commits the open unit of work, so the source backs out before every disconnect.
The same settings run in exported services, on the engine's Rust connector.
"""
import json
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sources.base import BaseSource

REASON_NO_MESSAGE = 2033


@dataclass
class IbmMqConfig(BaseConfig):
    channel: str
    queue: str
    queue_manager: Optional[str] = None
    connection_name: Optional[str] = None
    host: Optional[str] = None
    port: int = 1414
    user: Optional[str] = None
    password: Optional[str] = None
    application_name: str = 'mage-stream'
    cipher_spec: Optional[str] = None
    key_repository: Optional[str] = None
    certificate_label: Optional[str] = None
    # Services read with this many connections in parallel; here one reads.
    connections: int = 1
    batch_size: int = 100
    batch_timeout: float = 1.0

    def address(self) -> str:
        if self.connection_name:
            return self.connection_name
        if not self.host:
            raise ValueError('IBM MQ settings need connection_name (host(port)) or host.')
        return f'{self.host}({self.port})'

    def label(self) -> str:
        return f'queue {self.queue} on {self.address()} ({self.channel})'


def import_ibmmq():
    try:
        import ibmmq
    except ImportError as error:
        raise ImportError(
            'IBM MQ needs the ibmmq package and the IBM MQ client: install mage-ml[ibm-mq], '
            'and the IBM MQ client (on macOS the MQ toolkit) with its libraries on the '
            f'library path. ({error})'
        ) from error
    return ibmmq


def connect(config: IbmMqConfig, open_options: int):
    """A connection and its open queue."""
    ibmmq = import_ibmmq()
    cd = ibmmq.CD()
    cd.ChannelName = config.channel.encode()
    cd.ConnectionName = config.address().encode()
    cd.ChannelType = ibmmq.CMQXC.MQCHT_CLNTCONN
    cd.TransportType = ibmmq.CMQXC.MQXPT_TCP
    sco = None
    if config.cipher_spec or config.key_repository:
        if not (config.cipher_spec and config.key_repository):
            raise ValueError('IBM MQ TLS needs both cipher_spec and key_repository.')
        cd.SSLCipherSpec = config.cipher_spec.encode()
        sco = ibmmq.SCO()
        sco.KeyRepository = config.key_repository.encode()
        if config.certificate_label:
            sco.CertificateLabel = config.certificate_label.encode()
    csp = None
    if config.user:
        csp = ibmmq.CSP()
        csp.CSPUserId = config.user.encode()
        csp.CSPPassword = (config.password or '').encode()
    qm = ibmmq.QueueManager(None)
    options = dict(cd=cd)
    if csp is not None:
        options['csp'] = csp
    if sco is not None:
        options['sco'] = sco
    qm.connect_with_options(config.queue_manager or '', **options)
    queue = ibmmq.Queue(qm, config.queue.encode(), open_options)
    return qm, queue


def decode(data: bytes):
    """A JSON value, or the text when the message is not JSON; binary data is refused."""
    try:
        text = data.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError(
            'An IBM MQ message is neither JSON nor UTF-8 text; it stays on the queue.'
        ) from None
    try:
        return json.loads(text)
    except ValueError:
        return text


class IbmMqSource(BaseSource):
    config_class = IbmMqConfig

    def init_client(self):
        self.ibmmq = import_ibmmq()
        self._print(f'Connecting to {self.config.label()}.')
        options = (self.ibmmq.CMQC.MQOO_INPUT_AS_Q_DEF
                   | self.ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING)
        self.qm, self.queue = connect(self.config, options)
        self._print('Connected.')

    def test_connection(self):
        pass

    def read(self, handler: Callable):
        self.batch_read(lambda messages: [handler(m) for m in messages])

    def _get(self, wait_ms: int) -> Optional[bytes]:
        ibmmq = self.ibmmq
        md = ibmmq.MD()
        gmo = ibmmq.GMO()
        gmo.Options = (ibmmq.CMQC.MQGMO_SYNCPOINT | ibmmq.CMQC.MQGMO_FAIL_IF_QUIESCING
                       | ibmmq.CMQC.MQGMO_CONVERT
                       | (ibmmq.CMQC.MQGMO_WAIT if wait_ms > 0 else ibmmq.CMQC.MQGMO_NO_WAIT))
        gmo.WaitInterval = max(wait_ms, 0)
        md.CodedCharSetId = 1208
        try:
            return self.queue.get(None, md, gmo)
        except ibmmq.MQMIError as error:
            if error.reason == REASON_NO_MESSAGE:
                return None
            raise

    def _read_batch(self) -> List[bytes]:
        started = time.monotonic()
        timeout = float(self.config.batch_timeout)
        messages = []
        while len(messages) < self.config.batch_size:
            remaining = timeout - (time.monotonic() - started)
            wait = timeout if not messages else max(remaining, 0)
            data = self._get(int(wait * 1000))
            if data is None:
                break
            messages.append(data)
        return messages

    def batch_read(self, handler: Callable):
        self._print('Start consuming messages.')
        while True:
            raw = self._read_batch()
            if not raw:
                continue
            try:
                handler([decode(data) for data in raw])
            except BaseException:
                self.qm.backout()
                raise
            self.qm.commit()

    def destroy(self):
        qm = getattr(self, 'qm', None)
        if qm is None:
            return
        try:
            # A normal disconnect commits the unit of work.
            qm.backout()
        finally:
            try:
                self.queue.close()
            finally:
                qm.disconnect()
                self.qm = None
