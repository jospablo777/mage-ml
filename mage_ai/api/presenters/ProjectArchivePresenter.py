from mage_ai.api.presenters.BasePresenter import BasePresenter


class ProjectArchivePresenter(BasePresenter):
    default_attributes = [
        'content',
        'file_name',
        'id',
        'plan',
        'size',
        'written',
    ]
