from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerUpdatePresenter(BasePresenter):
    default_attributes = [
        'changes',
        'held',
        'id',
        'job',
        'message',
        'name',
    ]
