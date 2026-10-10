from mage_ai.api.presenters.BasePresenter import BasePresenter


class ModelReleasePresenter(BasePresenter):
    default_attributes = [
        'alias',
        'approval',
        'champion',
        'id',
        'log',
        'model',
    ]
