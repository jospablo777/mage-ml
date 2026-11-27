from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelinePartitionPresenter(BasePresenter):
    default_attributes = [
        'counts',
        'key',
        'kind',
        'partitions',
        'pipeline_uuid',
        'result',
        'total',
    ]
