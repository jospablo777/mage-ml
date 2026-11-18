from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerPresenter(BasePresenter):
    default_attributes = [
        'changes',
        'combine',
        'definition',
        'dependencies',
        'description',
        'digest',
        'job',
        'label',
        'locked',
        'name',
        'notes',
        'packages',
        'prepared',
        'pyproject',
        'python',
        'security',
        'size_bytes',
        'used_by',
    ]
