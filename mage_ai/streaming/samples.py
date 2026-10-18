"""
Sample messages of a streaming pipeline: files of JSON lines next to the pipeline, in
pipelines/<pipeline>/samples/<name>.jsonl, captured from the source or written by hand.

A capture reads the source without joining its consumer group and without committing: it
never moves the position a running stream reads from. Each message is stored as the blocks
receive it: JSON values as JSON, other text as a string.
"""
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

SAMPLES_DIR = 'samples'
NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$')
# A sample is for looking at and running blocks on; bigger ones belong in tests.
MAX_MESSAGES = 10_000


class SampleError(Exception):
    pass


def samples_dir(repo_path: str, pipeline_uuid: str) -> Path:
    return Path(repo_path) / 'pipelines' / pipeline_uuid / SAMPLES_DIR


def _path(repo_path: str, pipeline_uuid: str, name: str) -> Path:
    if not NAME.match(name or '') or name.endswith('.jsonl'):
        raise SampleError(
            f'{name!r} is not a sample name: use letters, digits, dots, dashes and '
            'underscores, starting with a letter or digit.'
        )
    return samples_dir(repo_path, pipeline_uuid) / f'{name}.jsonl'


def list_samples(repo_path: str, pipeline_uuid: str) -> List[Dict]:
    """The pipeline's samples, newest first: name, message count, size and time."""
    folder = samples_dir(repo_path, pipeline_uuid)
    if not folder.is_dir():
        return []
    found = []
    for path in folder.glob('*.jsonl'):
        stat = path.stat()
        with path.open('rb') as file:
            count = sum(1 for line in file if line.strip())
        found.append(dict(
            name=path.stem,
            message_count=count,
            size_bytes=stat.st_size,
            updated_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(stat.st_mtime)),
        ))
    return sorted(found, key=lambda s: s['updated_at'], reverse=True)


def read_sample(repo_path: str, pipeline_uuid: str, name: str) -> List[Any]:
    path = _path(repo_path, pipeline_uuid, name)
    if not path.is_file():
        raise SampleError(f'The pipeline has no sample {name}.')
    messages = []
    for number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), start=1):
        if not line.strip():
            continue
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError as error:
            raise SampleError(f'Sample {name}, line {number}: {error.msg}.')
    return messages


def _check_finite(value: Any, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise SampleError(f'{path} is {value}; JSON has no NaN or infinity.')
    if isinstance(value, dict):
        for key, item in value.items():
            _check_finite(item, f'{path}.{key}')
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _check_finite(item, f'{path}[{index}]')


def write_sample(repo_path: str, pipeline_uuid: str, name: str, messages: List[Any]) -> Dict:
    """Writes the messages as a sample, replacing one of the same name."""
    if not isinstance(messages, list):
        raise SampleError('A sample is a list of messages.')
    if len(messages) > MAX_MESSAGES:
        raise SampleError(f'A sample holds at most {MAX_MESSAGES:,} messages.')
    for index, message in enumerate(messages):
        _check_finite(message, f'Message {index + 1}')
    path = _path(repo_path, pipeline_uuid, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.jsonl.tmp')
    temporary.write_text(
        ''.join(json.dumps(m, ensure_ascii=False) + '\n' for m in messages), encoding='utf-8',
    )
    os.replace(temporary, path)
    return next(s for s in list_samples(repo_path, pipeline_uuid) if s['name'] == name)


def delete_sample(repo_path: str, pipeline_uuid: str, name: str) -> None:
    path = _path(repo_path, pipeline_uuid, name)
    if not path.is_file():
        raise SampleError(f'The pipeline has no sample {name}.')
    path.unlink()


def ignore_in_git(repo_path: str, pipeline_uuid: str) -> None:
    """Keeps the pipeline's samples out of Git: they can hold real data."""
    folder = samples_dir(repo_path, pipeline_uuid)
    folder.mkdir(parents=True, exist_ok=True)
    ignore = folder / '.gitignore'
    if not ignore.exists():
        ignore.write_text('# Samples captured from the source can hold real data.\n*.jsonl\n')


def _source_block(pipeline):
    sources = [b for b in pipeline.blocks_by_uuid.values() if str(b.type) in (
        'data_loader', 'BlockType.DATA_LOADER')]
    if len(sources) != 1:
        raise SampleError('The stream needs one source block to capture a sample.')
    return sources[0]


def message_value(raw: Optional[bytes]) -> Any:
    """A message's value as the blocks receive it: JSON when it is JSON, else text."""
    if raw is None:
        return None
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        raise SampleError(
            'A message is neither JSON nor UTF-8 text; samples hold what Python blocks '
            'receive, which is JSON or text.'
        )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


# Field names that hold secrets or personal data, redacted on capture whatever the source
# says: the name or its last part (access_token, user_password, api_key). token_count and
# primary_key are kept.
SECRET_NAMES = (
    'password', 'passwd', 'pwd', 'secret', 'token', 'api_key', 'apikey', 'authorization',
    'credential', 'credentials', 'private_key', 'secret_key', 'access_key', 'ssn',
    'card_number', 'cvv', 'cvc', 'iban',
)


def secret_name(key: str) -> bool:
    # accessToken and access-token read as access_token.
    name = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', str(key))
    name = re.sub(r'[-.\s]+', '_', name).lower()
    return any(name == n or name.endswith('_' + n) for n in SECRET_NAMES)


REDACTED = '<redacted>'


def redact(messages: List[Any], secret_fields: Optional[List[str]] = None):
    """
    The messages with secret fields replaced by <redacted>: those the source lists in
    `secret_fields` (dotted paths into nested objects, or a field name anywhere) and those
    whose names say they are secret. Returns them and the count per redacted path.
    """
    listed = {str(f) for f in secret_fields or []}
    counts: Dict[str, int] = {}

    def walk(value, path: str):
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                child = f'{path}.{key}' if path else str(key)
                if child in listed or str(key) in listed or secret_name(key):
                    result[key] = REDACTED
                    counts[child] = counts.get(child, 0) + 1
                else:
                    result[key] = walk(item, child)
            return result
        if isinstance(value, list):
            return [walk(item, path) for item in value]
        return value

    return [walk(m, '') for m in messages], counts


def capture_sample(
    repo_path: str,
    pipeline_uuid: str,
    name: str,
    count: int = 100,
    start: str = 'latest',
    timeout_seconds: float = 15,
    variables: Optional[Dict] = None,
) -> Dict:
    """
    Reads up to `count` messages from the stream's source and writes them as a sample:
    the most recent ones (`start='latest'`) or the first ones kept (`'earliest'`). Returns
    the sample's summary with `partitions`, the messages per partition read.
    """
    from mage_ai.data_preparation.models.block.streaming import streaming_config
    from mage_ai.data_preparation.models.pipeline import Pipeline

    if not 1 <= int(count) <= MAX_MESSAGES:
        raise SampleError(f'Capture 1 to {MAX_MESSAGES:,} messages.')
    if start not in ('latest', 'earliest'):
        raise SampleError("start is 'latest' or 'earliest'.")
    _path(repo_path, pipeline_uuid, name)
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    source = _source_block(pipeline)
    language = str(getattr(source.language, 'value', source.language))
    if language != 'yaml':
        raise SampleError(
            'Samples are captured from sources set up in YAML; write or paste a sample for a '
            'Python source.'
        )
    config = streaming_config(source.content or '', variables or pipeline.variables or {})
    connector = (config or {}).get('connector_type')
    if connector == 'kafka':
        messages, partitions = _capture_kafka(config, int(count), start,
                                              float(timeout_seconds))
    elif connector in ('nats', 'nats_js'):
        messages, partitions = _capture_nats(config, int(count), start,
                                             float(timeout_seconds))
    else:
        raise SampleError(
            f'Capturing reads Kafka and NATS JetStream sources; {connector or "this"} sources '
            'take messages away from other consumers when read, so write or paste a sample.'
        )
    messages, redacted = redact(messages, config.get('secret_fields'))
    summary = write_sample(repo_path, pipeline_uuid, name, messages)
    summary['partitions'] = partitions
    summary['redacted'] = redacted
    return summary


def _shares(count: int, available: Dict[Any, int]) -> Dict[Any, int]:
    """
    How many of the newest messages to read from each partition: as even as the partitions
    allow, a partition with fewer messages leaving the rest to the others.
    """
    shares: Dict[Any, int] = {}
    remaining = count
    ordered = sorted(available, key=lambda p: available[p])
    for index, partition in enumerate(ordered):
        share = min(available[partition], math.ceil(remaining / (len(ordered) - index)))
        shares[partition] = max(share, 0)
        remaining -= shares[partition]
    return shares


def _capture_nats(config: Dict, count: int, start: str, timeout_seconds: float):
    """
    The newest (or first) messages of a JetStream stream, read by an ephemeral consumer that
    acknowledges nothing, so no durable consumer moves. Returns them and {subject: count}.
    """
    import asyncio

    return asyncio.run(_capture_nats_async(config, count, start, timeout_seconds))


async def _capture_nats_async(config: Dict, count: int, start: str, timeout_seconds: float):
    import nats
    from nats.errors import TimeoutError as NatsTimeout
    from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

    from mage_ai.streaming.sources.nats_js import NATSConfig, connect_options

    settings = {k: v for k, v in config.items() if k not in ('connector_type', 'secret_fields')}
    nats_config = NATSConfig.load(config=settings)
    connection = await nats.connect(**connect_options(nats_config))
    try:
        js = connection.jetstream()
        state = (await js.stream_info(nats_config.stream_name)).state
        if not state.messages:
            return [], {}
        first = state.first_seq
        begin = max(first, state.last_seq - count + 1) if start == 'latest' else first
        subscription = await js.pull_subscribe(
            nats_config.subject or '>', stream=nats_config.stream_name,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE, opt_start_seq=begin,
                ack_policy=AckPolicy.NONE, inactive_threshold=60,
            ),
        )
        messages, subjects = [], {}
        try:
            deadline = time.monotonic() + timeout_seconds
            while len(messages) < count and time.monotonic() < deadline:
                try:
                    batch = await subscription.fetch(min(256, count - len(messages)),
                                                     timeout=1)
                except NatsTimeout:
                    break
                for message in batch:
                    messages.append(message_value(message.data))
                    subjects[message.subject] = subjects.get(message.subject, 0) + 1
        finally:
            await subscription.unsubscribe()
        return messages, subjects
    finally:
        await connection.close()


def _capture_kafka(config: Dict, count: int, start: str, timeout_seconds: float):
    from kafka import KafkaConsumer, TopicPartition

    from mage_ai.streaming.sources.kafka import KafkaConfig, consumer_settings

    settings = dict(config)
    settings.pop('connector_type', None)
    settings.pop('secret_fields', None)
    kafka_config = KafkaConfig.load(config=settings)
    consumer_kwargs = consumer_settings(kafka_config, lambda message: None)
    # No group: nothing is committed and the stream's group is not touched.
    consumer_kwargs['group_id'] = None
    consumer_kwargs['enable_auto_commit'] = False
    consumer_kwargs['consumer_timeout_ms'] = 1000
    topics = [kafka_config.topic] if kafka_config.topic else list(kafka_config.topics or [])
    if not topics:
        raise SampleError('The source names no topic.')
    consumer = KafkaConsumer(**consumer_kwargs)
    try:
        assigned = []
        for topic in topics:
            found = consumer.partitions_for_topic(topic)
            if not found:
                raise SampleError(f'Topic {topic} does not exist or has no partitions.')
            assigned += [TopicPartition(topic, p) for p in sorted(found)]
        consumer.assign(assigned)
        ends = consumer.end_offsets(assigned)
        beginnings = consumer.beginning_offsets(assigned)
        shares = _shares(count, {p: ends[p] - beginnings[p] for p in assigned})
        for partition in assigned:
            if start == 'latest':
                consumer.seek(partition, ends[partition] - shares[partition])
            else:
                consumer.seek(partition, beginnings[partition])
        messages = []
        read: Dict[str, int] = {}
        deadline = time.monotonic() + timeout_seconds
        while len(messages) < count and time.monotonic() < deadline:
            batches = consumer.poll(timeout_ms=500, max_records=count - len(messages))
            if not batches and start == 'latest' and all(
                consumer.position(p) >= ends[p] for p in assigned
            ):
                break
            for partition, records in batches.items():
                for record in records:
                    messages.append(message_value(record.value))
                    key = f'{partition.topic}[{partition.partition}]'
                    read[key] = read.get(key, 0) + 1
        return messages[:count], read
    finally:
        consumer.close(autocommit=False)
