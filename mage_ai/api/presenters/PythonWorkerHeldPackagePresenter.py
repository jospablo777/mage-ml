from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerHeldPackagePresenter(BasePresenter):
    default_attributes = [
        'checked_at',
        'id',
        'name',
        'packages',
        'reason',
        'state',
    ]
