from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineCompilationPresenter(BasePresenter):
    default_attributes = [
        'automatic',
        'current',
        'done',
        'finished_at',
        'id',
        'last_compiled',
        'pipeline_uuid',
        'started_at',
        'state',
        'summary',
        'total',
        'units',
    ]
