from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.api.resources.PipelineCompilationResource import _pipeline


class PipelinePartitionResource(GenericResource):
    """
    A pipeline's partitions (orchestration/partition_state.py, ADR-0039): GET
    /api/pipeline_partitions/<pipeline uuid> returns each partition's state and the count of
    each state; POST /api/pipeline_partitions with pipeline_uuid, action (run or
    accept_code) and select (missing, stale, failed, or a list of partitions) acts on them
    and returns the states after.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelinePartitionResource':
        from mage_ai.orchestration import partition_state
        from mage_ai.orchestration.partition_runs import run_selected
        from mage_ai.orchestration.partitions import PartitionError

        pipeline = _pipeline(payload.get('pipeline_uuid'), user)
        action = payload.get('action')
        try:
            if action == 'run':
                result = run_selected(pipeline, payload.get('select'))
            elif action == 'accept_code':
                values = partition_state.select(pipeline, payload.get('select'))
                result = dict(accepted=partition_state.accept_code(pipeline, values))
            else:
                raise PartitionError('action is run or accept_code.')
            states = partition_state.states(pipeline)
        except PartitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(dict(states, pipeline_uuid=pipeline.uuid, result=result), user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelinePartitionResource':
        from mage_ai.orchestration import partition_state
        from mage_ai.orchestration.partitions import PartitionError

        pipeline = _pipeline(pk, user)
        try:
            states = partition_state.states(pipeline)
        except PartitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(dict(states, pipeline_uuid=pipeline.uuid), user, **kwargs)
