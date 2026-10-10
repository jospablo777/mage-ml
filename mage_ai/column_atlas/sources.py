"""
Mage block outputs as ColumnAtlas sources. A source names a block output: the pipeline, the
block, the output variable and, for an output of a pipeline run, the block run. Browsers send
these names; paths never leave the server. Each request resolves the source again, after the
API policy has checked the user's access to the pipeline.
"""
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from mage_ai.column_atlas.errors import AtlasInvalidRequest, AtlasUnavailable
from mage_ai.data_preparation.models.variables.constants import VariableType

# Block uuids can name files in folders (a/b); no segment may climb out with "..".
_NAME = re.compile(r'^[\w\-.:/ ]{1,255}$')
_VARIABLE = re.compile(r'^output_\d{1,6}$')
DATAFRAME_FILE = 'data.parquet'

# Outputs stored as one Parquet file. GeoDataFrames are GeoParquet; their geometry shows
# as WKT.
EXPLORABLE_TYPES = (
    VariableType.DATAFRAME,
    VariableType.POLARS_DATAFRAME,
    VariableType.GEO_DATAFRAME,
)


@dataclass(frozen=True)
class Source:
    pipeline_uuid: str
    block_uuid: str
    variable_uuid: str
    block_run_id: Optional[int] = None

    @property
    def key(self) -> str:
        return '|'.join([
            self.pipeline_uuid, self.block_uuid, self.variable_uuid, str(self.block_run_id or ''),
        ])


@dataclass(frozen=True)
class ResolvedSource:
    path: str
    # Changes whenever the output is written again: block outputs are replaced, not edited.
    generation: str
    label: str


def _name(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _NAME.match(value) or '..' in value.split('/'):
        raise AtlasInvalidRequest(f'Invalid {field}')
    return value


def parse_source(payload: Any) -> Source:
    if not isinstance(payload, dict):
        raise AtlasInvalidRequest('The request needs a source')
    variable_uuid = payload.get('variable_uuid') or 'output_0'
    if not isinstance(variable_uuid, str) or not _VARIABLE.match(variable_uuid):
        raise AtlasInvalidRequest('Only block outputs (output_N) can be explored')
    block_run_id = payload.get('block_run_id')
    if block_run_id is not None:
        if isinstance(block_run_id, bool):
            raise AtlasInvalidRequest('Invalid block run')
        try:
            block_run_id = int(block_run_id)
        except (TypeError, ValueError):
            raise AtlasInvalidRequest('Invalid block run')
        if block_run_id < 1:
            raise AtlasInvalidRequest('Invalid block run')
    return Source(
        pipeline_uuid=_name(payload.get('pipeline_uuid'), 'pipeline'),
        block_uuid=_name(payload.get('block_uuid'), 'block'),
        variable_uuid=variable_uuid,
        block_run_id=block_run_id,
    )


def _partition(source: Source) -> Optional[str]:
    if source.block_run_id is None:
        return None
    from mage_ai.orchestration.db.models.schedules import BlockRun

    block_run = BlockRun.get_by_id(source.block_run_id)
    # The block run must belong to the pipeline the user was authorized for.
    if (
        block_run is None
        or block_run.block_uuid != source.block_uuid
        or block_run.pipeline_run is None
        or block_run.pipeline_run.pipeline_uuid != source.pipeline_uuid
    ):
        raise AtlasUnavailable('The block run does not exist')
    return block_run.pipeline_run.execution_partition


def resolve(source: Source, repo_path: str) -> ResolvedSource:
    """The output's Parquet file, if it is a stored pandas or Polars DataFrame on disk."""
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.storage.local_storage import LocalStorage

    try:
        pipeline = Pipeline.get(source.pipeline_uuid, repo_path=repo_path, check_if_exists=True)
    except Exception:
        pipeline = None
    if pipeline is None or pipeline.get_block(source.block_uuid) is None:
        raise AtlasUnavailable('The block does not exist')
    partition = _partition(source)
    variable = pipeline.variable_manager.get_variable_object(
        source.pipeline_uuid,
        source.block_uuid,
        source.variable_uuid,
        partition=partition,
    )
    if variable.variable_type not in EXPLORABLE_TYPES:
        raise AtlasUnavailable('The output is not a stored DataFrame')
    if not isinstance(variable.storage, LocalStorage):
        raise AtlasUnavailable('Outputs in remote storage are not supported yet')

    root = os.path.realpath(pipeline.variable_manager.variables_dir)
    path = os.path.realpath(os.path.join(variable.variable_path, DATAFRAME_FILE))
    if os.path.commonpath([root, path]) != root:
        raise AtlasUnavailable('The output is outside the variables directory')
    try:
        stat = os.stat(path)
    except OSError:
        raise AtlasUnavailable('The output has not been stored; run the block')
    return ResolvedSource(
        path=path,
        generation=f'{stat.st_ino}-{stat.st_size}-{stat.st_mtime_ns}',
        label=f'{source.block_uuid} / {source.variable_uuid}',
    )


def source_dict(source: Source) -> Dict[str, Any]:
    return dict(
        block_run_id=source.block_run_id,
        block_uuid=source.block_uuid,
        pipeline_uuid=source.pipeline_uuid,
        variable_uuid=source.variable_uuid,
    )
