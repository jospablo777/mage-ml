from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineServiceModelPresenter(BasePresenter):
    default_attributes = [
        'models',
        'name',
        'promoted',
        'promotions',
    ]
