from mage_ai.api.presenters.BasePresenter import BasePresenter


class ColumnAtlasQueryPresenter(BasePresenter):
    default_attributes = [
        'action',
        'result',
        'source',
    ]
