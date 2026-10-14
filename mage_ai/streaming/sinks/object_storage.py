"""
Object storage sink: each batch becomes one object, Parquet or JSON lines, on S3 and
S3-compatible stores, Google Cloud Storage, Azure (abfs://), or local disk, through pyarrow's
file systems and the clouds' usual credentials. The same settings run in exported services on
the engine's Rust sink.

Parquet keeps values exact: top-level integers past 64 bits become decimals (or text past 38
digits); a nested one stops the write, since Parquet would lose its digits, so write jsonl.
"""
import io
import json
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional
from urllib.parse import urlparse

from mage_ai.shared.config import BaseConfig
from mage_ai.streaming.sinks.base import BaseSink

INT64 = (-(2**63), 2**63 - 1)


@dataclass
class ObjectStorageConfig(BaseConfig):
    url: str
    format: str = 'parquet'
    date_partition_format: Optional[str] = None
    endpoint_url: Optional[str] = None
    region: Optional[str] = None
    buffer_size_mb: Optional[float] = None
    buffer_timeout_seconds: Optional[float] = None


def _nested_oversized(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return not INT64[0] <= value <= INT64[1]
    if isinstance(value, dict):
        return any(_nested_oversized(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_nested_oversized(v) for v in value)
    return False


def parquet_table(rows: List[Dict]):
    """An Arrow table of the rows, with integers past 64 bits kept exact."""
    import pyarrow as pa

    wide: Dict[str, int] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(
                f'row {index} is not a JSON object; Parquet takes objects, jsonl takes any value'
            )
        for name, value in row.items():
            if isinstance(value, (dict, list)) and _nested_oversized(value):
                raise ValueError(
                    f'row {index}: {name} holds an integer past 64 bits inside it; Parquet '
                    'would lose its digits, so write jsonl'
                )
            if _nested_oversized(value):
                wide[name] = max(wide.get(name, 0), len(str(abs(value))))
    if wide:
        rows = [
            {k: (None if v is None else (Decimal(v) if wide[k] <= 38 else str(v)))
             if k in wide else v for k, v in row.items()}
            for row in rows
        ]
    table = pa.Table.from_pylist(rows)
    for name, digits in wide.items():
        if digits <= 38:
            index = table.schema.get_field_index(name)
            table = table.set_column(index, name, table.column(name).cast(pa.decimal128(38, 0)))
    return table


class ObjectStorageSink(BaseSink):
    config_class = ObjectStorageConfig

    def init_client(self):
        import pyarrow.fs as fs

        if self.config.format not in ('parquet', 'jsonl'):
            raise ValueError('Object storage format is parquet or jsonl.')
        url = self.config.url
        if urlparse(url).scheme == 's3' and (self.config.endpoint_url or self.config.region):
            parsed = urlparse(url)
            options = {}
            if self.config.endpoint_url:
                options['endpoint_override'] = self.config.endpoint_url
                options['scheme'] = urlparse(self.config.endpoint_url).scheme or 'https'
            if self.config.region:
                options['region'] = self.config.region
            self.filesystem = fs.S3FileSystem(**options)
            self.base = f'{parsed.netloc}{parsed.path}'.rstrip('/')
        else:
            self.filesystem, self.base = fs.FileSystem.from_uri(url)
            self.base = self.base.rstrip('/')
        self.stream = 'stream'
        self._print(f'Writing objects to {url}.')

    def write(self, message: Dict):
        self.batch_write([message])

    def _path(self) -> str:
        parts = [self.base]
        if self.config.date_partition_format:
            parts.append(time.strftime(self.config.date_partition_format, time.gmtime()))
        stamp = time.strftime('%Y%m%dT%H%M%S', time.gmtime())
        name = f'{self.stream}-{stamp}-{uuid.uuid4().hex[:12]}.'
        parts.append(name + ('parquet' if self.config.format == 'parquet' else 'jsonl'))
        # The base keeps its leading / (local paths); the rest are joined below it.
        rest = '/'.join(p.strip('/') for p in parts[1:] if p.strip('/'))
        return f'{parts[0]}/{rest}' if parts[0] else rest

    def batch_write(self, messages: List[Dict]):
        if not messages:
            return
        rows = [m.get('data', m) if isinstance(m, dict) and 'metadata' in m else m
                for m in messages]
        path = self._path()
        if self.config.format == 'parquet':
            import pyarrow.parquet as pq

            buffer = io.BytesIO()
            pq.write_table(parquet_table(rows), buffer, compression='zstd')
            body = buffer.getvalue()
        else:
            body = ''.join(
                (row if isinstance(row, str) else json.dumps(row, ensure_ascii=False, default=str))
                + '\n' for row in rows
            ).encode('utf-8')
        parent = path.rsplit('/', 1)[0]
        if parent:
            self.filesystem.create_dir(parent, recursive=True)
        with self.filesystem.open_output_stream(path) as stream:
            stream.write(body)
        self._print(f'Wrote {len(rows)} messages to {path}.')
