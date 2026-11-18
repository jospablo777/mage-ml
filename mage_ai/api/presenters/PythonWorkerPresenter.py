from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerPresenter(BasePresenter):
    default_attributes = [
        'changes',
        'combine',
        'confirm',
        'definition',
        'dependencies',
        'description',
        'digest',
        'held_back',
        'job',
        'label',
        'locked',
        'memory_limit',
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
