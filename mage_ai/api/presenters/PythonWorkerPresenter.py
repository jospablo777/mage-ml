from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerPresenter(BasePresenter):
    default_attributes = [
        'combine',
        'definition',
        'dependencies',
        'description',
        'digest',
        'job',
        'label',
        'locked',
        'name',
        'packages',
        'prepared',
        'python',
    ]
