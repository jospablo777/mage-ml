from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerCombinationPresenter(BasePresenter):
    default_attributes = [
        'changes',
        'combined_size',
        'id',
        'names',
        'reason',
        'saved_bytes',
        'sizes',
        'state',
        'workers',
    ]
