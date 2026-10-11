from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineServicePresenter(BasePresenter):
    default_attributes = [
        'description',
        'exported_at',
        'include',
        'max_runs',
        'name',
        'pipelines',
    ]
