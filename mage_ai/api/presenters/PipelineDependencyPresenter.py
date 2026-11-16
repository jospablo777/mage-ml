from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineDependencyPresenter(BasePresenter):
    default_attributes = [
        'edges',
        'id',
        'nodes',
        'pipelines_without_dependencies',
    ]
