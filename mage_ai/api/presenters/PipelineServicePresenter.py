from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineServicePresenter(BasePresenter):
    default_attributes = [
        'description',
        'exported_at',
        'image_builds',
        'include',
        'max_runs',
        'model_registry',
        'models',
        'name',
        'path',
        'pipelines',
        'platforms',
    ]
