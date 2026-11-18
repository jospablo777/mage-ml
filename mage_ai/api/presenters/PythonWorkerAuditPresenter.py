from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerAuditPresenter(BasePresenter):
    default_attributes = [
        'advisories',
        'checked_at',
        'fixable',
        'id',
        'name',
        'packages',
        'reason',
        'state',
    ]
