"""
Development runs never move production's positions (SAFETY.md section 3).

A stream run from the editor, or a local `mage service run`, reads with a position of its
own: Kafka's consumer group becomes `<group>-dev-<user>`, a PostgreSQL replication slot
`<slot>_dev_<user>` and a NATS durable consumer `<name>-dev-<user>`. Queues have no position
to fork: what a run acknowledges leaves the queue, so the run says so.
"""
import getpass
import re
from typing import Dict, List, Optional, Tuple

# Sources whose acknowledged messages leave a shared queue.
QUEUES = {
    'rabbitmq': 'queue_name',
    'amazon_sqs': 'queue_name',
    'activemq': 'queue_name',
    'google_cloud_pubsub': 'subscription_id',
    'ibm_mq': 'queue',
}


def user_slug(user: Optional[str] = None) -> str:
    try:
        name = user or getpass.getuser()
    except (KeyError, OSError):
        name = 'local'
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-') or 'local'


def development_slot(slot: str, user: Optional[str] = None) -> str:
    """The replication slot a development run reads with (names allow [a-z0-9_], 63)."""
    suffix = '_dev_' + user_slug(user).replace('-', '_')
    return slot if slot.endswith(suffix) else (slot + suffix)[:63]


def for_development(config: Dict, user: Optional[str] = None) -> Tuple[Dict, List[str]]:
    """The source settings for a development run, and what changed, to show the user."""
    if not isinstance(config, dict):
        return config, []
    slug = user_slug(user)
    config = dict(config)
    kind = str(config.get('connector_type') or '').lower()
    notes = []
    if kind == 'kafka' and config.get('consumer_group'):
        group = str(config['consumer_group'])
        if not group.endswith(f'-dev-{slug}'):
            config['consumer_group'] = f'{group}-dev-{slug}'
        notes.append(f'Development run: Kafka consumer group {config["consumer_group"]}, so '
                     f'the group {group} keeps its position.')
    elif kind == 'postgres' and config.get('replication_slot'):
        slot = str(config['replication_slot'])
        config['replication_slot'] = development_slot(slot, user)
        notes.append(f'Development run: replication slot {config["replication_slot"]}, so '
                     f'the slot {slot} keeps its position. Drop it when you are done: it '
                     'holds WAL while it exists.')
    elif kind == 'nats':
        name = str(config.get('consumer_name') or config.get('stream_name') or '')
        if name and not name.endswith(f'-dev-{slug}'):
            config['consumer_name'] = f'{name}-dev-{slug}'
            notes.append(f'Development run: NATS consumer {config["consumer_name"]}, so the '
                         f'consumer {name} keeps its position.')
    elif kind == 'azure_event_hub':
        notes.append('Development run: Event Hubs consumer groups must exist before they are '
                     f'used, so this run reads with {config.get("consumer_group") or "$Default"};'
                     ' create a development consumer group and name it here to keep '
                     'production\'s checkpoints.')
    elif kind in QUEUES:
        where = config.get(QUEUES[kind]) or 'the configured queue'
        notes.append(f'Development run: this source reads {where}; messages the run '
                     'acknowledges leave it for every consumer. Point it at a development '
                     'queue to keep production\'s messages.')
    return config, notes


# The kinds of an exported service's connectors, as source connector types.
MANIFEST_KINDS = dict(sqs='amazon_sqs', pubsub='google_cloud_pubsub')


def for_development_manifest(manifest: Dict, user: Optional[str] = None) -> List[str]:
    """Rewrites the stream sources of an exported service's manifest in place; the notes."""
    notes = []
    for pipeline in manifest.get('pipelines') or []:
        source = (pipeline.get('stream') or {}).get('source')
        if not source:
            continue
        kind = MANIFEST_KINDS.get(source.get('kind'), source.get('kind'))
        config, changed = for_development(
            dict(source.get('config') or {}, connector_type=kind), user,
        )
        config.pop('connector_type', None)
        source['config'] = config
        notes += [f'{pipeline.get("uuid")}: {note}' for note in changed]
    return notes


# Sinks a development run never writes: they write nowhere, or only a failure reaches them.
SILENT_SINKS = {'dummy'}
WHERE_KEYS = ('topic', 'table', 'queue_name', 'queue', 'subject', 'bucket', 'index_name',
              'collection', 'measurement', 'stream_name', 'url')


def sinks_written(pipeline) -> List[Tuple[str, str]]:
    """(block uuid, what it writes to) for each sink a run of the streaming pipeline writes."""
    import yaml

    found = []
    for block in pipeline.blocks_by_uuid.values():
        if block.type != 'data_exporter':
            continue
        if str(block.language) != 'yaml':
            found.append((block.uuid, 'a Python sink'))
            continue
        try:
            config = yaml.safe_load(block.content or '') or {}
        except yaml.YAMLError:
            config = {}
        if not isinstance(config, dict):
            continue
        kind = str(config.get('connector_type') or 'unknown')
        if kind in SILENT_SINKS or config.get('dead_letter'):
            continue
        where = next((str(config[k]) for k in WHERE_KEYS if config.get(k)), '')
        found.append((block.uuid, f'{kind} {where}'.strip()))
    return found
