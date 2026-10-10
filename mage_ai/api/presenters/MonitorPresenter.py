from mage_ai.api.presenters.BasePresenter import BasePresenter


class MonitorPresenter(BasePresenter):
    default_attributes = [
        'baseline',
        'block_uuid',
        'error',
        'history',
        'id',
        'latest',
        'name',
        'pipeline_uuid',
        'runs',
    ]
