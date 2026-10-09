import time
from dataclasses import dataclass
from typing import Dict, List

import simplejson
from kafka import KafkaProducer

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.enum import StrEnum
from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.constants import DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS
from mage_ai.streaming.sinks.base import BaseSink

# Seconds a send waits for Kafka to acknowledge it.
SEND_TIMEOUT_SECONDS = 60


class SecurityProtocol(StrEnum):
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'
    SSL = 'SSL'


@dataclass
class SASLConfig:
    mechanism: str = 'PLAIN'
    username: str = None
    password: str = None
    # OAuth configuration for OAUTHBEARER mechanism
    oauth_token_url: str = None
    oauth_client_id: str = None
    oauth_client_secret: str = None
    oauth_scope: str = None


@dataclass
class SSLConfig:
    cafile: str = None
    certfile: str = None
    keyfile: str = None
    password: str = None
    check_hostname: bool = False


@dataclass
class KafkaConfig(BaseConfig):
    bootstrap_server: str
    topic: str
    # None detects the broker's protocol version. 0.10.2, the default before, made every
    # request time out against Kafka 4, which removed protocol versions older than 2.1.
    api_version: str = None
    security_protocol: SecurityProtocol = None
    ssl_config: SSLConfig = None
    sasl_config: SASLConfig = None
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_ms: int = DEFAULT_TIMEOUT_MS

    @classmethod
    def parse_config(self, config: Dict) -> Dict:
        ssl_config = config.get('ssl_config')
        sasl_config = config.get('sasl_config')
        if ssl_config and type(ssl_config) is dict:
            config['ssl_config'] = SSLConfig(**ssl_config)
        if sasl_config and type(sasl_config) is dict:
            config['sasl_config'] = SASLConfig(**sasl_config)
        return config


class KafkaSink(BaseSink):
    config_class = KafkaConfig

    def init_client(self):
        self._print('Start initializing producer.')
        # Initialize kafka producer
        if self.config.batch_size > 0:
            batch_size = self.config.batch_size
        else:
            batch_size = DEFAULT_BATCH_SIZE
        if self.config.timeout_ms > 0:
            timeout_ms = self.config.timeout_ms
        else:
            timeout_ms = DEFAULT_TIMEOUT_MS
        kwargs = dict(
            bootstrap_servers=self.config.bootstrap_server,
            api_version=self.config.api_version,
            # Dates, decimals and other values json cannot write raised in send.
            value_serializer=lambda x: simplejson.dumps(
                x, default=encode_complex, ignore_nan=True,
            ).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            batch_size=batch_size,
            linger_ms=timeout_ms,
        )
        if self.config.security_protocol == SecurityProtocol.SSL:
            kwargs['security_protocol'] = SecurityProtocol.SSL
            kwargs['ssl_cafile'] = self.config.ssl_config.cafile
            kwargs['ssl_certfile'] = self.config.ssl_config.certfile
            kwargs['ssl_keyfile'] = self.config.ssl_config.keyfile
            kwargs['ssl_password'] = self.config.ssl_config.password
            kwargs['ssl_check_hostname'] = self.config.ssl_config.check_hostname
        elif self.config.security_protocol == SecurityProtocol.SASL_SSL:
            if not self.config.sasl_config:
                raise Exception('sasl_config is required when security_protocol is SASL_SSL')
            kwargs['security_protocol'] = SecurityProtocol.SASL_SSL
            kwargs['sasl_mechanism'] = self.config.sasl_config.mechanism

            # Handle OAUTHBEARER mechanism
            if self.config.sasl_config.mechanism == 'OAUTHBEARER':
                from mage_ai.streaming.sources.kafka_oauth import (
                    create_oauth_token_provider,
                )

                token_provider = create_oauth_token_provider(self.config.sasl_config)
                kwargs['sasl_oauth_token_provider'] = token_provider
            else:
                # Handle PLAIN, SCRAM mechanisms
                kwargs['sasl_plain_username'] = self.config.sasl_config.username
                kwargs['sasl_plain_password'] = self.config.sasl_config.password

            if self.config.ssl_config is not None and self.config.ssl_config.cafile:
                kwargs['ssl_cafile'] = self.config.ssl_config.cafile

        self.producer = KafkaProducer(**kwargs)
        self._print('Finish initializing producer.')

    def _send(self, message: Dict):
        if isinstance(message, dict):
            data = message.get('data', message)
            metadata = message.get('metadata', {})
        else:
            data = message
            metadata = {}

        return self.producer.send(
            topic=metadata.get('dest_topic', self.config.topic),
            value=data,
            key=metadata.get('key'),
            timestamp_ms=metadata.get('time'),
        )

    def write(self, message: Dict):
        """Send a message and wait until Kafka has it; a failed send raises."""
        self._send(message).get(timeout=SEND_TIMEOUT_SECONDS)

    def batch_write(self, messages: List[Dict]):
        """
        Send messages and wait until Kafka has every one; a failed send raises. Messages
        stayed in the producer's buffer, and the source committed their offsets once
        this returned, so a crash lost them; failed sends went unnoticed.
        """
        if not messages:
            return
        self._print(
            f'Batch ingest {len(messages)} messages, time={time.time()}. Sample: {messages[0]}'
        )
        futures = [self._send(message) for message in messages]
        self.producer.flush(timeout=SEND_TIMEOUT_SECONDS)
        for future in futures:
            future.get(timeout=SEND_TIMEOUT_SECONDS)
