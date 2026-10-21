from mage_ai.api.presenters.BasePresenter import BasePresenter


class BlockTemplatePresenter(BasePresenter):
    default_attributes = [
        'block_type',
        'configuration',
        'data_source',
        'defaults',
        'description',
        'groups',
        'language',
        'name',
        'path',
        'template_type',
        'template_variables',
    ]
