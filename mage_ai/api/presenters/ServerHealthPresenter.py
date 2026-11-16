from mage_ai.api.presenters.BasePresenter import BasePresenter


class ServerHealthPresenter(BasePresenter):
    default_attributes = [
        'history',
        'id',
        'machine',
        'processes',
        'sample_seconds',
        'storage',
    ]
