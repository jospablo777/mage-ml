from mage_ai.api.presenters.BasePresenter import BasePresenter


class LanguageSetupPresenter(BasePresenter):
    default_attributes = [
        'commands',
        'guide',
        'install',
        'language',
        'name',
        'problems',
        'ready',
    ]
