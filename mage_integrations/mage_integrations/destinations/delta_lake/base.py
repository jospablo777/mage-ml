import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from mage_integrations.destinations.base import Destination as BaseDestination
from mage_integrations.destinations.constants import (
    COLUMN_TYPE_BOOLEAN,
    COLUMN_TYPE_INTEGER,
    COLUMN_TYPE_NULL,
    COLUMN_TYPE_NUMBER,
    KEY_RECORD,
)
from mage_integrations.destinations.delta_lake.constants import (
    MODE_APPEND,
    MODE_OVERWRITE,
)
from mage_integrations.destinations.utils import update_record_with_internal_columns
from mage_integrations.utils.array import find


def arrow_type(properties: Dict) -> pa.DataType:
    """
    The Arrow type of a column from its JSON schema. Arrays and objects are stored as
    JSON text, and date-time strings as text, as before.
    """
    types = list(properties.get('type') or [])
    for any_of in properties.get('anyOf', []):
        types += any_of.get('type', [])
    column_type = find(lambda t: t != COLUMN_TYPE_NULL, types)
    if column_type == COLUMN_TYPE_INTEGER:
        return pa.int64()
    if column_type == COLUMN_TYPE_NUMBER:
        return pa.float64()
    if column_type == COLUMN_TYPE_BOOLEAN:
        return pa.bool_()
    return pa.string()


def _as_text(value: Any) -> Optional[str]:
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def records_to_table(records: List[Dict], properties: Dict) -> pa.Table:
    """
    An Arrow table of records with the stream schema's types.

    The writer used to build a pandas frame, which turned integers with nulls into
    floats, then turned every column holding a null into text with '' for null. As the
    table schema was overwritten on each batch, a column's type changed with the batch.
    """
    fields = [pa.field(name, arrow_type(settings or {})) for name, settings in properties.items()]
    columns = {}
    for field in fields:
        values = [record.get(field.name) for record in records]
        if pa.types.is_string(field.type):
            values = [_as_text(value) for value in values]
        columns[field.name] = pa.array(values, type=field.type)
    return pa.table(columns, schema=pa.schema(fields))


def overwrite_predicate(table: pa.Table, partition_keys: List[str]) -> str:
    """A predicate that matches the partitions present in table."""
    def literal(value: Any) -> str:
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, (int, float)):
            return repr(value)
        return "'" + str(value).replace("'", "''") + "'"

    conditions = []
    for row in table.select(partition_keys).group_by(partition_keys).aggregate([]).to_pylist():
        parts = [
            f'"{key}" IS NULL' if row[key] is None else f'"{key}" = {literal(row[key])}'
            for key in partition_keys
        ]
        conditions.append('(' + ' AND '.join(parts) + ')')
    return ' OR '.join(conditions)


class DeltaLake(BaseDestination):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Streams and partitions written in this sync. In overwrite mode only the first
        # write to each replaces data; later batches append. Each batch used to replace
        # the table, so only the last batch of a sync was kept.
        self._overwritten: Dict[str, Set[Tuple]] = {}

    @property
    def mode(self):
        return self.config.get('mode', MODE_APPEND)

    @property
    def table_name(self):
        return self.config['table']

    def build_client(self):
        raise Exception('Subclasses must implement the build_client method.')

    def get_table_for_stream(self, stream: str) -> Optional[DeltaTable]:
        storage_options = self.build_storage_options()
        table_uri = self.build_table_uri(stream)
        if DeltaTable.is_deltatable(table_uri, storage_options=storage_options):
            return DeltaTable(table_uri, storage_options=storage_options)
        return None

    def export_batch_data(self, record_data: List[Dict], stream: str, tags: Dict = None) -> None:
        storage_options = self.build_storage_options()
        friendly_table_name = self.config['table']
        table_uri = self.build_table_uri(stream)

        tags = dict(
            records=len(record_data),
            stream=stream,
            table_name=friendly_table_name,
            table_uri=table_uri,
        )

        self.logger.info('Export data started.', tags=tags)

        self.logger.info('Checking if delta logs exist...', tags=tags)
        if self.check_and_create_delta_log(stream):
            self.logger.info('Existing delta logs exist.', tags=tags)
        else:
            self.logger.info('No delta logs exist.', tags=tags)

        for r in record_data:
            r['record'] = update_record_with_internal_columns(r['record'])

        table = records_to_table(
            [d[KEY_RECORD] for d in record_data],
            self.schemas[stream]['properties'],
        )
        partition_keys = (self.partition_keys or {}).get(stream) or []
        exists = self.get_table_for_stream(stream) is not None

        self.logger.info('Inserting records for batch 0 started.', tags=tags)
        for mode, rows, predicate in self.__writes(stream, table, partition_keys):
            write_deltalake(
                table_uri,
                rows,
                mode=mode,
                partition_by=partition_keys or None,
                predicate=predicate,
                schema_mode='merge' if exists and mode == MODE_APPEND else (
                    'overwrite' if mode == MODE_OVERWRITE and not predicate else None
                ),
                storage_options=storage_options,
            )
            exists = True
        self.logger.info('Inserting records for batch 0 completed.', tags=tags)

        self.__after_write_for_batch(stream, 0, tags=tags)

        tags.update(records_inserted=table.num_rows)

        self.logger.info('Export data completed.', tags=tags)

    def __writes(
        self,
        stream: str,
        table: pa.Table,
        partition_keys: List[str],
    ) -> List[Tuple[str, pa.Table, Optional[str]]]:
        if self.mode != MODE_OVERWRITE:
            return [(MODE_APPEND, table, None)]

        written = self._overwritten.setdefault(stream, set())
        if not partition_keys:
            if written:
                return [(MODE_APPEND, table, None)]
            written.add(())
            return [(MODE_OVERWRITE, table, None)]

        # Replace only the partitions in the batch, once per sync.
        keys = list(zip(*[table.column(k).to_pylist() for k in partition_keys]))
        new_mask = pa.array([key not in written for key in keys])
        new_rows = table.filter(new_mask)
        seen_rows = table.filter(pc.invert(new_mask))
        writes = []
        if new_rows.num_rows:
            writes.append((
                MODE_OVERWRITE,
                new_rows,
                overwrite_predicate(new_rows, partition_keys),
            ))
            written.update(keys)
        if seen_rows.num_rows:
            writes.append((MODE_APPEND, seen_rows, None))
        return writes

    def after_write_for_batch(self, stream, index, **kwargs) -> None:
        pass

    def build_storage_options(self) -> Dict:
        raise Exception('Subclasses must implement the build_storage_options method.')

    def build_table_uri(self, stream: str) -> str:
        raise Exception('Subclasses must implement the build_table_uri method.')

    def check_and_create_delta_log(self, stream: str) -> bool:
        raise Exception('Subclasses must implement the check_and_create_delta_log method.')

    def __after_write_for_batch(self, stream, index, **kwargs) -> None:
        tags = kwargs.get('tags', {})

        self.logger.info(f'Handle after write callback for batch {index} started.', tags=tags)
        self.after_write_for_batch(stream, index, **kwargs)
        self.logger.info(f'Handle after write callback for batch {index} completed.', tags=tags)


def main(destination_class):
    destination = destination_class(
        argument_parser=argparse.ArgumentParser(),
        batch_processing=True,
    )
    destination.process(sys.stdin.buffer)
