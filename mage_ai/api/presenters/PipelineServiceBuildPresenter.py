from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineServiceBuildPresenter(BasePresenter):
    default_attributes = [
        'buildx',
        'command',
        'docker',
        'finished_at',
        'image',
        'lines',
        'name',
        'problems',
        'push',
        'reason',
        'started_at',
        'state',
    ]
