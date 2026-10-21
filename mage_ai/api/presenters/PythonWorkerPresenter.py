from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerPresenter(BasePresenter):
    default_attributes = [
        'definition',
        'dependencies',
        'digest',
        'job',
        'locked',
        'name',
        'packages',
        'prepared',
        'python',
    ]
