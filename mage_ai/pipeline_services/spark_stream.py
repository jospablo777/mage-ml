"""
A streaming pipeline on Spark Structured Streaming, over Spark Connect (ADR-0047).

The plan comes from the export's capture, as a service's manifest does. `differences` says
what Spark cannot run as the engine does; a pipeline with any is refused with the list.
It runs a Kafka source, Python stream transformers, a window block, Kafka sinks and a Kafka
dead-letter sink:

    spark.readStream.format('kafka')       the source's topic and start
      .mapInArrow(blocks)                  the blocks, by the engine's own worker code
      .withWatermark().groupBy(window)     a window block last, over event time
      .writeStream.format('kafka')         one sink; several, or dead letters: foreachBatch

A window runs as Spark's own aggregation over the same event time, with the engine's
aggregates and result fields; it closes on one watermark for the query, where the engine
keeps one per partition, and `notes` say so.

The blocks run in mapInArrow through runtime/worker.py's run_stream_batch, which the
function carries with the blocks' sources and loads once per Python worker process, so a
block behaves as it does on the engine. Spark's Python workers need the client's Python
minor version and pyarrow.

Positions live in Spark's checkpoint, never in the consumer group: the first run starts
where the source's auto_offset_reset says. max_records is maxOffsetsPerTrigger and
max_wait_ms the trigger interval.

The plan is JSON its runner reads (runtime/spark_runner.py, which needs only pyspark-client
and pyarrow): `mage spark-stream` runs it from here, and `export` writes it with the runner,
the worker code and a Dockerfile, a container that runs the stream without MageML.
"""
import json
import re
import sys
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Mapping

from mage_ai.pipeline_services.runtime import spark_runner

RUNTIME = Path(__file__).parent / 'runtime'
SECURITY_SETTINGS = {'security_protocol', 'sasl_config'}
SOURCE_SETTINGS = {'connector_type', 'bootstrap_server', 'topic', 'topics', 'consumer_group',
                   'auto_offset_reset', 'batch_size', 'timeout_ms'} | SECURITY_SETTINGS
SINK_SETTINGS = {'connector_type', 'bootstrap_server', 'topic'} | SECURITY_SETTINGS
BLOCK_SETTINGS_ENGINE_ONLY = ('state_key', 'checkpoint')
# Fields Spark reads by get_json_object's dotted path.
FIELD_PATH = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$')
SparkStreamError = spark_runner.SparkRunError


def differences(pipeline: Dict[str, Any]) -> List[str]:
    """What Spark cannot run as the engine does, one sentence each; empty when it can."""
    stream = pipeline.get('stream') or {}
    found = []
    source = stream.get('source') or {}
    if source.get('kind') != 'kafka':
        found.append(f"The source is {source.get('kind')}; Spark runs Kafka sources for now.")
    else:
        extra = sorted(set(source.get('config') or {}) - SOURCE_SETTINGS)
        if extra:
            found.append(f"The source's settings {', '.join(extra)} have no Spark equivalent "
                         'yet.')
    dead_letter = (stream.get('errors') or {}).get('dead_letter')
    for sink in (stream.get('sinks') or []) + ([dead_letter] if dead_letter else []):
        if sink.get('kind') != 'kafka':
            found.append(f"Sink {sink.get('name')} is {sink.get('kind')}; Spark writes Kafka "
                         'for now.')
        else:
            extra = sorted(set(sink.get('config') or {}) - SINK_SETTINGS)
            if extra:
                found.append(f"Sink {sink.get('name')}'s settings {', '.join(extra)} have no "
                             'Spark equivalent yet.')
    if stream.get('delivery'):
        found.append(f"Delivery {stream['delivery']} runs on the engine only.")
    blocks = pipeline.get('blocks') or []
    windows = [b for b in blocks if b.get('language') == 'window']
    if windows:
        found.extend(_window_differences(stream, blocks, windows))
    elif stream.get('time'):
        found.append('Event time and late messages without a window run on the engine only '
                     'for now.')
    for block in blocks:
        name = block.get('uuid')
        if block.get('language') == 'window':
            continue
        if block.get('language') != 'python' or block.get('type') != 'transformer':
            # SQL and lookup blocks, and Rust and R blocks.
            found.append(f"Block {name} is a {block.get('language')} {block.get('type')}; "
                         'Spark runs Python stream transformers for now.')
            continue
        configured = [k for k in BLOCK_SETTINGS_ENGINE_ONLY
                      if (block.get('configuration') or {}).get(k)]
        if configured:
            found.append(f"Block {name} uses {', '.join(configured)}, which runs on the engine "
                         'only for now.')
    return found


def _window_differences(stream: Dict[str, Any], blocks: List[Dict[str, Any]],
                        windows: List[Dict[str, Any]]) -> List[str]:
    """What a window needs on Spark: the last block, every sink after it, late messages
    passed, and fields Spark can read."""
    found = []
    if len(windows) > 1:
        found.append(f"Blocks {', '.join(w['uuid'] for w in windows)} are windows; Spark runs "
                     'one window block per stream for now.')
        return found
    window = windows[0]
    name = window['uuid']
    # Without event_time the engine's window reads the source's timestamp and passes late
    # messages.
    time = stream.get('time') or {}
    if time and (time.get('on_late') or 'dead_letter') != 'pass':
        found.append('Late messages go to the dead-letter sink (on_late: dead_letter), which '
                     'runs on the engine only; with on_late: pass Spark drops and counts them, '
                     'as the engine does.')
    if (stream.get('errors') or {}).get('dead_letter'):
        found.append(f'A dead-letter sink next to window {name} runs on the engine only for '
                     'now.')
    after = [b['uuid'] for b in blocks if name in (b.get('upstream') or [])]
    if after:
        found.append(f"Block {', '.join(after)} follows window {name}; Spark runs a window as "
                     'the last block for now.')
    for sink in stream.get('sinks') or []:
        if (sink.get('after') or name) != name:
            found.append(f"Sink {sink.get('name')} follows {sink['after']}, not window {name}; "
                         'on Spark every sink follows the window for now.')
    upstream = list(window.get('upstream') or [])
    if len(upstream) > 1:
        found.append(f'Window {name} reads {len(upstream)} blocks; Spark reads one for now.')
    if upstream and not time.get('field'):
        found.append(f'With blocks before window {name}, Spark reads event time from a field '
                     '(event_time field:); the source\'s timestamp does not reach it.')
    spec = window.get('window') or {}
    fields = [time.get('field'), spec.get('key')] + [
        next(iter(function.values())) for _, function in spec.get('aggregates') or []
        if isinstance(function, dict)
    ]
    for field in fields:
        if field is not None and not FIELD_PATH.match(str(field)):
            found.append(f'Field {field}: Spark reads fields by plain dotted names (letters, '
                         'digits and _) for now.')
    return found


def plan(repo_path: str, pipeline_uuid: str) -> Dict[str, Any]:
    """
    The pipeline as a Spark plan, from the export's capture; refused with what differs. Its
    settings keep their variable references, filled in when it starts.
    """
    from mage_ai.pipeline_services import export

    captured = export.capture(repo_path, [pipeline_uuid])
    pipeline = captured.manifest['pipelines'][0]
    stream = pipeline.get('stream')
    if not stream:
        raise SparkStreamError(f'{pipeline_uuid} is not a streaming pipeline.')
    found = differences(pipeline)
    if found:
        raise SparkStreamError(
            f'{pipeline_uuid} cannot run on Spark as it runs on the engine:\n- '
            + '\n- '.join(found)
        )
    dead_letter = (stream.get('errors') or {}).get('dead_letter')
    blocks = pipeline.get('blocks') or []
    window = next((b for b in blocks if b.get('language') == 'window'), None)
    chain = [
        dict(
            uuid=block['uuid'],
            code=(Path(repo_path) / block['file']).read_text(),
            upstream=list(block.get('upstream') or []),
            configuration=block.get('configuration') or {},
        )
        for block in blocks if block is not window
    ]
    if window is not None:
        chain_end = dict(uuid=window['uuid'])
    else:
        chain_end = chain[-1] if chain else None
    return dict(
        pipeline_uuid=pipeline_uuid,
        source=dict(config=stream['source'].get('config') or {},
                    batch=stream.get('batch') or {}),
        sinks=[
            dict(name=sink['name'], config=sink.get('config') or {},
                 after=sink.get('after') or (chain_end['uuid'] if chain_end else ''))
            for sink in stream['sinks']
        ],
        dead_letter=dict(name=dead_letter['name'], config=dead_letter.get('config') or {})
        if dead_letter else None,
        chain=chain,
        # The window block, its upstream block's messages in; and the event time it reads.
        window=dict(uuid=window['uuid'], upstream=list(window.get('upstream') or []),
                    spec=window['window']) if window is not None else None,
        time=dict(stream.get('time') or {}) if window is not None else None,
        variables=dict(pipeline.get('variables') or {}),
    )


def notes(plan: Dict[str, Any]) -> List[str]:
    start = 'earliest' if plan['source']['config'].get('auto_offset_reset') == 'earliest' \
        else 'latest'
    found = [
        'Positions are kept in the checkpoint; the consumer group\'s committed offsets are not '
        f'read. A first run starts at the {start} offsets.',
    ]
    window = plan.get('window')
    if window:
        found += [
            f"Window {window['uuid']} closes on one watermark for the query (its latest event "
            'time less lateness), where the engine keeps one per partition: with partitions '
            'far apart in time, Spark drops as late events the engine would still count.',
            'The watermark moves at the end of each batch, where the engine moves it with '
            'each message: an event late by less than a batch still counts, and a window\'s '
            'result is written one batch later. Its Kafka timestamp is when it is written, not '
            'the window\'s end.',
            'Late and untimed events are dropped, as with on_late: pass; Spark counts the late '
            'ones in its progress (numRowsDroppedByWatermark).',
        ]
        if any(isinstance(f, dict) and 'distinct' in f
               for _, f in window['spec'].get('aggregates') or []):
            found.append('Distinct counts compare values by their text: 1 and "1" count once, '
                         'where the engine counts them apart.')
    return found


def check(plan: Dict[str, Any], environ: Mapping[str, str]) -> None:
    """Whether the plan's settings resolve with environ; raises with the first that does not."""
    spark_runner.source_options(plan, environ)
    for sink in plan['sinks'] + ([plan['dead_letter']] if plan.get('dead_letter') else []):
        spark_runner.sink_options(sink, environ)


def _version(package: str, fallback: str) -> str:
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError:
        return fallback


DOCKERFILE = """\
# The streaming pipeline {pipeline} on Spark Structured Streaming (MageML spark-stream).
# The runner starts the query on a Spark Connect server and watches it; Spark runs the blocks.
FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM python:{python}-slim
COPY --from=uv /uv /usr/local/bin/uv
RUN uv pip install --system --no-cache pyspark-client=={pyspark} pyarrow=={pyarrow}
WORKDIR /stream
COPY plan.json worker.py spark_runner.py ./
USER 10001
CMD ["python", "spark_runner.py"]
"""

README = """\
# {pipeline} on Spark

The streaming pipeline {pipeline}, exported to run on Spark Structured Streaming over Spark
Connect. The container starts the query and watches it; SIGTERM stops it, and the next
start continues from the checkpoint.

```bash
docker build -t {image} .
docker run --rm \\
  -e MAGE_SPARK_REMOTE=sc://spark.example.com:15002 \\
  -e MAGE_SPARK_CHECKPOINT=s3a://lake/checkpoints/{pipeline} \\
{variables}  {image}
```

The Spark server needs Python {python} with pyarrow on its workers (with several sinks or
dead letters, pyspark-client's libraries too), the packages the blocks import, and the
Kafka connector (org.apache.spark:spark-sql-kafka-0-10_2.13).
"""


def _variables(plan: Dict[str, Any]) -> List[str]:
    found = []

    def walk(value):
        if isinstance(value, dict) and '$parts' in value:
            found.extend(p['env'] for p in value['$parts'] if isinstance(p, dict) and
                         p.get('env'))
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(plan['source']['config'])
    for sink in plan['sinks'] + ([plan['dead_letter']] if plan.get('dead_letter') else []):
        walk(sink['config'])
    return sorted(set(found))


def export(repo_path: str, pipeline_uuid: str, output: str) -> Path:
    """A folder that runs the pipeline on Spark without MageML: the plan, the runner, the
    worker code and a Dockerfile."""
    stream = plan(repo_path, pipeline_uuid)
    folder = Path(output)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'plan.json').write_text(json.dumps(stream, indent=2, sort_keys=True) + '\n')
    for name in ('worker.py', 'spark_runner.py'):
        (folder / name).write_text((RUNTIME / name).read_text())
    python = f'{sys.version_info[0]}.{sys.version_info[1]}'
    (folder / 'Dockerfile').write_text(DOCKERFILE.format(
        pipeline=pipeline_uuid, python=python,
        pyspark=_version('pyspark-client', '4.3.0'), pyarrow=_version('pyarrow', '26.0.0'),
    ))
    variables = ''.join(f'  -e {name} \\\n' for name in _variables(stream))
    (folder / 'README.md').write_text(README.format(
        pipeline=pipeline_uuid, image=pipeline_uuid.replace('_', '-'), python=python,
        variables=variables,
    ))
    return folder
