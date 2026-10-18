"""
A stream's dead letters in the editor (C3): the messages a block failed on, read from the
dead-letter sink with their errors, opened as a sample to reproduce the failure with Run on
sample, or replayed to the source once the code is fixed.

The engine writes each dead letter as the original message with two headers: `mage-stream`
(the pipeline) and `mage-error` (the block and its error). Reading works for Kafka dead-letter
sinks; it reads without a consumer group, so nothing is committed. Replay writes to the
source's topic, a production write: it needs the topic's name, typed by the user.
"""
from typing import Dict, List, Optional, Tuple

MAX_LETTERS = 1000


class DeadLetterError(ValueError):
    pass


def _yaml_config(block, variables: Optional[Dict]) -> Dict:
    from mage_ai.data_preparation.models.block.streaming import streaming_config

    config = streaming_config(block.content or '', variables or {})
    return config if isinstance(config, dict) else {}


def _blocks(pipeline, variables: Optional[Dict] = None) -> Tuple[Dict, Dict]:
    """The dead-letter sink's settings and the source's."""
    sink = source = None
    for block in pipeline.blocks_by_uuid.values():
        if str(getattr(block.language, 'value', block.language)) != 'yaml':
            continue
        if str(block.type) == 'data_exporter':
            config = _yaml_config(block, variables or pipeline.variables)
            if config.get('dead_letter'):
                sink = config
        elif str(block.type) == 'data_loader':
            source = _yaml_config(block, variables or pipeline.variables)
    if sink is None:
        raise DeadLetterError(
            'The stream has no dead-letter sink: add a data exporter with dead_letter: true, '
            'and messages a block fails on go there instead of stopping the stream.'
        )
    if sink.get('connector_type') != 'kafka':
        raise DeadLetterError(
            f'Dead letters are read from Kafka dead-letter sinks for now; this one is '
            f'{sink.get("connector_type")}.'
        )
    return sink, source or {}


def _client_settings(config: Dict) -> Dict:
    """kafka-python settings for a Kafka sink's or source's servers and sign-in."""
    from mage_ai.streaming.sources.kafka import KafkaConfig, consumer_settings

    settings = {k: v for k, v in config.items()
                if k not in ('connector_type', 'dead_letter', 'secret_fields')}
    settings.setdefault('consumer_group', 'unused')
    return consumer_settings(KafkaConfig.load(config=settings), lambda message: None)


def _consumer(config: Dict):
    from kafka import KafkaConsumer

    kwargs = _client_settings(config)
    # No group: nothing is committed.
    kwargs.update(group_id=None, enable_auto_commit=False, consumer_timeout_ms=1000)
    return KafkaConsumer(**kwargs)


def _producer(config: Dict):
    from kafka import KafkaProducer

    consumer_only = ('group_id', 'auto_offset_reset', 'max_partition_fetch_bytes',
                     'enable_auto_commit')
    return KafkaProducer(**{k: v for k, v in _client_settings(config).items()
                            if k not in consumer_only and v is not None})


def _header(headers, key: str) -> Optional[str]:
    for name, value in headers or []:
        if name == key:
            return value.decode('utf-8', errors='replace') if value is not None else None
    return None


def _read(sink: Dict, limit: int) -> Tuple[List[Dict], int]:
    """The newest dead letters, newest first, with their raw bytes; and how many there are."""
    import time

    from kafka import TopicPartition

    topic = sink.get('topic')
    consumer = _consumer(sink)
    letters = []
    try:
        partitions = sorted(consumer.partitions_for_topic(topic) or [])
        if not partitions:
            return [], 0
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        ends = consumer.end_offsets(tps)
        starts = consumer.beginning_offsets(tps)
        total = sum(ends[tp] - starts[tp] for tp in tps)
        for tp in tps:
            consumer.seek(tp, max(starts[tp], ends[tp] - limit))
        deadline = time.monotonic() + 15
        remaining = {tp for tp in tps if ends[tp] > starts[tp]}
        while remaining and time.monotonic() < deadline:
            for tp, records in consumer.poll(timeout_ms=500).items():
                for record in records:
                    if record.offset < ends[tp]:
                        letters.append(dict(
                            partition=record.partition, offset=record.offset,
                            timestamp=record.timestamp,
                            error=_header(record.headers, 'mage-error'),
                            stream=_header(record.headers, 'mage-stream'),
                            raw=record.value,
                        ))
            remaining = {tp for tp in remaining if consumer.position(tp) < ends[tp]}
    finally:
        consumer.close(autocommit=False)
    letters.sort(key=lambda d: (d['timestamp'] or 0, d['offset']), reverse=True)
    return letters[:limit], total


def read_dead_letters(repo_path: str, pipeline_uuid: str, limit: int = 200) -> Dict:
    """The newest dead letters: each message's value (secret fields redacted), error,
    partition, offset and time."""
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming.samples import message_value, redact

    if not 1 <= int(limit) <= MAX_LETTERS:
        raise DeadLetterError(f'Read 1 to {MAX_LETTERS:,} dead letters.')
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    sink, source = _blocks(pipeline)
    letters, total = _read(sink, int(limit))
    values, redacted = redact([message_value(d.pop('raw')) for d in letters],
                              source.get('secret_fields'))
    for letter, value in zip(letters, values):
        letter['value'] = value
    return dict(topic=sink.get('topic'), source_topic=source.get('topic'), letters=letters,
                total=total, redacted=redacted)


def open_as_sample(repo_path: str, pipeline_uuid: str, name: str, limit: int = 200) -> Dict:
    """The newest dead letters as a sample (oldest first, secret fields redacted), so Run on
    sample shows the failure again."""
    from mage_ai.streaming.samples import write_sample

    found = read_dead_letters(repo_path, pipeline_uuid, limit)
    if not found['letters']:
        raise DeadLetterError('There are no dead letters.')
    summary = write_sample(repo_path, pipeline_uuid, name,
                           [d['value'] for d in reversed(found['letters'])])
    summary['redacted'] = found.get('redacted') or {}
    return summary


def replay(repo_path: str, pipeline_uuid: str, confirm_topic: str,
           positions: Optional[List[Tuple[int, int]]] = None, limit: int = 200) -> Dict:
    """
    Writes dead letters back to the source's topic as they were received, oldest first: the
    newest `limit`, or those at `positions` ([partition, offset]). `confirm_topic` must name
    the source topic.
    """
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    sink, source = _blocks(pipeline)
    if source.get('connector_type') != 'kafka' or not source.get('topic'):
        raise DeadLetterError('Replay writes to a Kafka source with one topic.')
    if confirm_topic != source['topic']:
        raise DeadLetterError(f'Type the source topic, {source["topic"]}, to replay to it.')
    letters, _ = _read(sink, int(limit))
    wanted = {tuple(p) for p in positions or []}
    chosen = [d for d in letters if not wanted or (d['partition'], d['offset']) in wanted]
    producer = _producer(source)
    try:
        for letter in reversed(chosen):
            producer.send(source['topic'], letter['raw'])
        producer.flush()
    finally:
        producer.close()
    return dict(replayed=len(chosen), topic=source['topic'])
