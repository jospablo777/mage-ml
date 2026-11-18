from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonPackagePresenter(BasePresenter):
    default_attributes = [
        'id',
        'latest',
        'name',
        'prereleases',
        'versions',
    ]
