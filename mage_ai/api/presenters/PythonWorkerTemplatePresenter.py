from mage_ai.api.presenters.BasePresenter import BasePresenter


class PythonWorkerTemplatePresenter(BasePresenter):
    default_attributes = [
        'dependencies',
        'description',
        'id',
        'label',
    ]
