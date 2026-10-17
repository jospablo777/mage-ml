import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Dict, List

import simplejson
from influxdb_client import InfluxDBClient, WritePrecision

from mage_ai.shared.config import BaseConfig
from mage_ai.shared.parsers import encode_complex
from mage_ai.streaming.constants import DEFAULT_BATCH_SIZE, DEFAULT_TIMEOUT_MS
from mage_ai.streaming.sinks.base import BaseSink


def get_timestamp_ms():
    return int(time.time() * 1e3)


@dataclass
class InfluxDbConfig(BaseConfig):
    url: str
    token: str
    org: str
    bucket: str = 'data'
    measurement: str = 'default'
    batch_size: int = DEFAULT_BATCH_SIZE
    timeout_ms: int = DEFAULT_TIMEOUT_MS


class InfluxDbSink(BaseSink):
    """
    Writes each batch to InfluxDB in one request and returns once InfluxDB accepted it, or
    raises. The writer batched in the background, so batch_write returned before writing:
    the source acked messages that were not written, and rejected writes were only logged.

    Each message is a point: its fields are the message's values (lists and objects as JSON
    text), with metadata's measurement, tags and timestamp (milliseconds). Messages without a
    timestamp get the batch's time in nanoseconds plus their position: stamped with the same
    millisecond and tags, they overwrote each other.
    """
    config_class = InfluxDbConfig

    def init_client(self):
        from influxdb_client.client.write_api import SYNCHRONOUS

        self._print('Start initializing writer.')
        self.client = InfluxDBClient(
            url=self.config.url,
            token=self.config.token,
            org=self.config.org,
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self._print('Finish initializing writer.')

    def _records(self, message, now_ns: int) -> List[Dict]:
        if isinstance(message, dict):
            data = message.get('data', message)
            metadata = message.get('metadata', {}) or {}
        else:
            data = message
            metadata = {}

        if isinstance(data, dict):
            values = [_fields(data)]
        elif isinstance(data, Iterable) and not isinstance(data, (str, bytes)):
            values = [{'_value': _field(value)} for value in data]
        else:  # data is scalar
            values = [{'_value': _field(data)}]

        # 'time' is the documented key; 'timestamp' was the only one read.
        timestamp = metadata.get('time', metadata.get('timestamp'))
        records = []
        for index, fields in enumerate(values):
            records.append({
                'measurement': metadata.get('measurement', self.config.measurement),
                # Given timestamps are milliseconds; the others are distinct nanoseconds.
                'time': int(timestamp) * 1_000_000 if timestamp is not None else now_ns + index,
                'tags': metadata.get('tags', {}),
                'fields': fields,
            })
        return records

    def write(self, message: Dict):
        self.batch_write([message])

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        self._print(
            f'Batch ingest {len(messages)} messages, time={time.time()}. Sample: {messages[0]}'
        )
        now_ns = time.time_ns()
        records = []
        for message in messages:
            records += self._records(message, now_ns + len(records))
        self.write_api.write(
            bucket=self.config.bucket,
            record=records,
            write_precision=WritePrecision.NS,
        )


def _field(value):
    if isinstance(value, (dict, list, tuple)):
        return simplejson.dumps(value, default=encode_complex)
    return value


def _fields(data: Dict) -> Dict:
    return {str(key): _field(value) for key, value in data.items()}
