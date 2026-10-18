import importlib
import json
import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List

from kafka import KafkaConsumer, TopicPartition

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.enum import StrEnum
from mage_ai.streaming.constants import DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS
from mage_ai.streaming.sources.base import BaseSource
from mage_ai.streaming.sources.shared import SerDeConfig, SerializationMethod


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
    consumer_group: str
    # None detects the broker's protocol version. 0.10.2, the default before, made every
    # request time out against Kafka 4, which removed protocol versions older than 2.1.
    api_version: str = None
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    auto_offset_reset: str = 'latest'
    include_metadata: bool = False
    security_protocol: SecurityProtocol = None
    ssl_config: SSLConfig = None
    sasl_config: SASLConfig = None
    serde_config: SerDeConfig = None
    topic: str = None
    topics: List = field(default_factory=list)
    offset: Dict = None
    max_partition_fetch_bytes: int = 1048576

    @classmethod
    def parse_config(self, config: Dict) -> Dict:
        from mage_ai.services.ibm.event_streams import expand

        # IBM Event Streams' service credentials stand for the brokers and sign-in.
        config = expand(config)
        ssl_config = config.get('ssl_config')
        sasl_config = config.get('sasl_config')
        serde_config = config.get('serde_config')
        if ssl_config and type(ssl_config) is dict:
            config['ssl_config'] = SSLConfig(**ssl_config)
        if sasl_config and type(sasl_config) is dict:
            config['sasl_config'] = SASLConfig(**sasl_config)
        if serde_config and type(serde_config) is dict:
            config['serde_config'] = SerDeConfig(**serde_config)
        return config


def consumer_settings(config: KafkaConfig, print_message=print) -> Dict:
    """
    kafka-python consumer settings from a Kafka source's config: servers, group, offsets and
    security. Sample capture uses them too, with the group removed.
    """
    consumer_kwargs = dict(
        group_id=config.consumer_group,
        bootstrap_servers=config.bootstrap_server,
        api_version=config.api_version,
        auto_offset_reset=config.auto_offset_reset,
        max_partition_fetch_bytes=config.max_partition_fetch_bytes,
        enable_auto_commit=False,
    )
    if config.security_protocol == SecurityProtocol.SSL:
        if not config.ssl_config:
            raise Exception('ssl_config is required when security_protocol is SSL')
        consumer_kwargs['security_protocol'] = SecurityProtocol.SSL
        consumer_kwargs['ssl_cafile'] = config.ssl_config.cafile
        consumer_kwargs['ssl_certfile'] = config.ssl_config.certfile
        consumer_kwargs['ssl_keyfile'] = config.ssl_config.keyfile
        consumer_kwargs['ssl_password'] = config.ssl_config.password
        consumer_kwargs[
            'ssl_check_hostname'
        ] = config.ssl_config.check_hostname
    elif config.security_protocol == SecurityProtocol.SASL_SSL:
        if not config.sasl_config:
            raise Exception('sasl_config is required when security_protocol is SASL_SSL')
        consumer_kwargs['security_protocol'] = SecurityProtocol.SASL_SSL
        consumer_kwargs['sasl_mechanism'] = config.sasl_config.mechanism

        # Handle OAUTHBEARER mechanism
        if config.sasl_config.mechanism == 'OAUTHBEARER':
            from mage_ai.streaming.sources.kafka_oauth import (
                create_oauth_token_provider,
            )

            token_provider = create_oauth_token_provider(config.sasl_config)
            consumer_kwargs['sasl_oauth_token_provider'] = token_provider
        else:
            # Handle PLAIN, SCRAM mechanisms
            consumer_kwargs['sasl_plain_username'] = config.sasl_config.username
            consumer_kwargs['sasl_plain_password'] = config.sasl_config.password

        if config.ssl_config:
            consumer_kwargs[
                'ssl_check_hostname'] = config.ssl_config.check_hostname
            if config.ssl_config.cafile:
                consumer_kwargs['ssl_cafile'] = config.ssl_config.cafile
            if config.ssl_config.certfile:
                consumer_kwargs['ssl_certfile'] = config.ssl_config.certfile
            if config.ssl_config.keyfile:
                consumer_kwargs['ssl_keyfile'] = config.ssl_config.keyfile
            if config.ssl_config.password:
                consumer_kwargs['ssl_password'] = config.ssl_config.password

    elif config.security_protocol == SecurityProtocol.SASL_PLAINTEXT:
        if not config.sasl_config:
            raise Exception('sasl_config is required when security_protocol is SASL_PLAINTEXT')
        consumer_kwargs['security_protocol'] = SecurityProtocol.SASL_PLAINTEXT
        consumer_kwargs['sasl_mechanism'] = config.sasl_config.mechanism

        # Handle OAUTHBEARER mechanism
        if config.sasl_config.mechanism == 'OAUTHBEARER':
            print_message(
                'WARNING: Using OAUTHBEARER with SASL_PLAINTEXT is not recommended. '
                'OAuth tokens should be used with SASL_SSL for secure transmission.'
            )
            from mage_ai.streaming.sources.kafka_oauth import (
                create_oauth_token_provider,
            )

            token_provider = create_oauth_token_provider(config.sasl_config)
            consumer_kwargs['sasl_oauth_token_provider'] = token_provider
        else:
            # Handle PLAIN, SCRAM mechanisms
            consumer_kwargs['sasl_plain_username'] = config.sasl_config.username
            consumer_kwargs['sasl_plain_password'] = config.sasl_config.password
    return consumer_kwargs


class KafkaSource(BaseSource):
    config_class = KafkaConfig

    def init_client(self):
        if not self.config.topic and not self.config.topics:
            raise Exception('Please specify topic or topics in the Kafka config.')

        if self.config.offset and self.config.offset.get("partitions") is None:
            raise Exception('Please specify topic partitions')

        self._print('Start initializing consumer.')
        consumer_kwargs = consumer_settings(self.config, self._print)
        if self.config.topic:
            topics = [self.config.topic]
        else:
            topics = self.config.topics

        self.consumer = KafkaConsumer(*topics, **consumer_kwargs)
        self._print('Finish initializing consumer.')

        self.schema_class = None
        if self.config.serde_config is not None:
            if (
                self.config.serde_config.serialization_method
                == SerializationMethod.PROTOBUF
            ):
                schema_classpath = self.config.serde_config.schema_classpath
                if schema_classpath is None:
                    return
                self._print(f'Loading message schema from {schema_classpath}')
                parts = schema_classpath.split('.')
                if len(parts) >= 2:
                    class_name = parts[-1]
                    libpath = '.'.join(parts[:-1])
                    self.schema_class = getattr(
                        importlib.import_module(libpath),
                        class_name,
                    )
            elif (
                self.config.serde_config.serialization_method
                == SerializationMethod.AVRO
            ):
                from confluent_avro import AvroKeyValueSerde, SchemaRegistry
                from confluent_avro.schema_registry import HTTPBasicAuth

                self.registry_client = SchemaRegistry(
                    self.config.serde_config.schema_registry_url,
                    HTTPBasicAuth(
                        self.config.serde_config.schema_registry_username,
                        self.config.serde_config.schema_registry_password,
                    ),
                    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},
                )
                self.avro_serde = AvroKeyValueSerde(
                    self.registry_client, self.config.topic
                )

    def _decode_key(self, key):
        """Decode message key based on serialization method.

        When using RAW_VALUE, keep key as raw bytes.
        Otherwise, attempt UTF-8 decode with fallback to raw bytes on error.
        """
        if not key:
            return None

        # Keep as raw bytes for RAW_VALUE serialization
        if (self.config.serde_config and
                self.config.serde_config.serialization_method == SerializationMethod.RAW_VALUE):
            return key

        # Attempt UTF-8 decode, fallback to raw bytes on error
        try:
            return key.decode('utf-8')
        except UnicodeDecodeError:
            return key

    def _convert_message(self, message):
        if self.config.include_metadata:
            message = {
                'data': self.__deserialize_message(message.value),
                'metadata': {
                    'key': self._decode_key(message.key),
                    'partition': message.partition,
                    'offset': message.offset,
                    'time': int(message.timestamp),
                    'topic': message.topic,
                },
            }
        else:
            message = self.__deserialize_message(message.value)
        return message

    def _handle_offsets(self):
        self._print('Configuring consumer offset')
        self.consumer.unsubscribe()
        partitions = [TopicPartition(partition['topic'], partition['partition']) for partition
                      in self.config.offset.get('partitions')]
        self._print(f'{partitions}')
        self.consumer.assign(partitions=partitions)

        offset_type = self.config.offset.get('type')
        offset_value = self.config.offset.get('value')

        if offset_type == 'int':
            for partition in partitions:
                self.consumer.seek(partition=partition,
                                   offset=offset_value)
        elif offset_type == 'timestamp':
            timestamps = {partition: offset_value for partition in partitions}
            # Retrieve each partition offset for given time
            offset_times = self.consumer.offsets_for_times(timestamps)

            for partition in partitions:
                self.consumer.seek(partition=partition,
                                   offset=offset_times[partition].offset)
        elif offset_type == 'beginning':
            self.consumer.seek_to_beginning()

        elif offset_type == 'end':
            self.consumer.seek_to_end()

    def read(self, handler: Callable):
        if self.config.offset:
            self._handle_offsets()
        self._print('Start consuming single messages.')
        for message in self.consumer:
            self.__print_message(message)
            message = self._convert_message(message)
            handler(message)
            # Auto-commit is off and offsets were never committed here, so a restarted
            # consumer group skipped or read again what came while it was down.
            self.consumer.commit()

    async def read_async(self, handler: Callable):
        if self.config.offset:
            self._handle_offsets()
        self._print('Start consuming messages asynchronously.')
        for message in self.consumer:
            self.__print_message(message)
            message = self._convert_message(message)
            await handler(message)
            self.consumer.commit()

    def batch_read(self, handler: Callable):
        if self.config.offset:
            self._handle_offsets()
        self._print('Start consuming messages in batches.')
        if self.config.batch_size > 0:
            batch_size = self.config.batch_size
        else:
            batch_size = DEFAULT_BATCH_SIZE
        if self.config.timeout_ms > 0:
            timeout_ms = self.config.timeout_ms
        else:
            timeout_ms = DEFAULT_TIMEOUT_MS

        while True:
            # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
            msg_pack = self.consumer.poll(
                max_records=batch_size,
                timeout_ms=timeout_ms,
            )

            message_values = []
            msg_printed = False
            for _tp, messages in msg_pack.items():
                self._print(
                    f'Received {len(messages)} messages from topic="{_tp.topic}" '
                    + f'partition={_tp.partition} at time={time.time()}'
                )
                for message in messages:
                    if not msg_printed:
                        self.__print_message(message)
                        msg_printed = True

                    message = self._convert_message(message)
                    message_values.append(message)
            if len(message_values) > 0:
                handler(message_values)
            self.consumer.commit()

    def test_connection(self):
        self.consumer._client.check_version(timeout=5)
        self._print('Test connection successfully.')

    def describe_setup(self) -> List[str]:
        """
        Each topic's partitions, retained messages and retention, and where the consumer group
        stands: behind by how many messages, or with no committed offsets and what its first
        run would read. Read with a consumer outside the group, which joins nothing.
        """
        from kafka import KafkaAdminClient

        settings = consumer_settings(self.config, self._print)
        group = settings.pop('group_id')
        probe = KafkaConsumer(**settings)
        admin_settings = {k: v for k, v in settings.items() if k not in (
            'auto_offset_reset', 'max_partition_fetch_bytes', 'enable_auto_commit')}
        admin = KafkaAdminClient(**admin_settings)
        lines = []
        try:
            try:
                committed = {tp: meta.offset for tp, meta in
                             _group_offsets(admin, group).items()
                             if meta.offset is not None and meta.offset >= 0}
            except Exception as error:  # noqa: BLE001 - reported, the rest still helps
                committed = None
                lines.append(f'Consumer group {group}: its offsets cannot be read ({error}).')
            for topic in self.config.topics or [self.config.topic]:
                partitions = probe.partitions_for_topic(topic)
                if not partitions:
                    lines.append(f'Topic {topic}: missing, or this user cannot see it.')
                    continue
                tps = [TopicPartition(topic, p) for p in sorted(partitions)]
                ends = probe.end_offsets(tps)
                starts = probe.beginning_offsets(tps)
                retained = sum(ends[tp] - starts[tp] for tp in tps)
                lines.append(f'Topic {topic}: {len(tps)} partitions, {retained:,} messages '
                             f'retained{_retention(admin, topic)}.')
                if committed is None:
                    continue
                mine = {tp: committed[tp] for tp in tps if tp in committed}
                if mine:
                    behind = sum(max(0, ends[tp] - mine[tp]) for tp in mine)
                    lines.append(
                        f'Consumer group {group}: committed on {len(mine)} of {len(tps)} '
                        f'partitions, {behind:,} messages behind.')
                elif self.config.auto_offset_reset == 'earliest':
                    lines.append(
                        f'Consumer group {group} has no committed offsets on {topic}: with '
                        f'auto_offset_reset earliest, its first run reads all {retained:,} '
                        'retained messages.')
                else:
                    lines.append(
                        f'Consumer group {group} has no committed offsets on {topic}: with '
                        f'auto_offset_reset {self.config.auto_offset_reset}, its first run '
                        f'starts at the end and skips the {retained:,} retained messages.')
        finally:
            probe.close()
            admin.close()
        return lines

    def __deserialize_message(self, message):
        if self.config.serde_config is None:
            return self.__deserialize_json(message)
        if (
            self.config.serde_config.serialization_method
            == SerializationMethod.PROTOBUF
            and self.schema_class is not None
        ):
            from google.protobuf.json_format import MessageToDict

            obj = self.schema_class()
            obj.ParseFromString(message)
            return MessageToDict(obj)
        elif self.config.serde_config.serialization_method == SerializationMethod.AVRO:
            return self.avro_serde.value.deserialize(message)
        elif (
            self.config.serde_config.serialization_method
            == SerializationMethod.RAW_VALUE
        ):
            return message
        else:
            return json.loads(message.decode('utf-8'))

    def __deserialize_json(self, message):
        return json.loads(message.decode('utf-8'))

    def __print_message(self, message):
        self._print(
            f'Receive message {message.partition}:{message.offset}: '
            f'key={message.key}, value={message.value}, time={time.time()}'
        )


def _group_offsets(admin, group: str, attempts: int = 10):
    """
    The group's committed offsets. A broker answers NotCoordinator while it elects the group's
    coordinator, as a fresh one does for a new group: such errors are tried again.
    """
    for attempt in range(attempts):
        try:
            return admin.list_consumer_group_offsets(group)
        except Exception as error:  # noqa: BLE001 - kafka-python marks the retriable ones
            if not getattr(error, 'retriable', False) or attempt == attempts - 1:
                raise
            time.sleep(0.5)


def _retention(admin, topic: str) -> str:
    """", kept 7 days" from the topic's retention.ms; nothing when it cannot be read."""
    from kafka.admin import ConfigResource, ConfigResourceType

    try:
        response = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])
        entries = response[0].to_object()['resources'][0]['config_entries']
        value = next(e['config_value'] for e in entries if e['config_names'] == 'retention.ms')
        milliseconds = int(value)
    except Exception:  # noqa: BLE001 - retention is a detail of the report
        return ''
    if milliseconds < 0:
        return ', kept forever'
    hours = milliseconds / 3_600_000
    return f', kept {hours / 24:g} days' if hours >= 24 else f', kept {hours:g} hours'
