from mage_ai.api.presenters.BasePresenter import BasePresenter


class DataContractPresenter(BasePresenter):
    default_attributes = [
        'columns',
        'content',
        'description',
        'error',
        'file_path',
        'id',
        'name',
        'owner',
        'report',
        'version',
    ]
