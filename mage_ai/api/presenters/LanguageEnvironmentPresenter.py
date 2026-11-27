from mage_ai.api.presenters.BasePresenter import BasePresenter


class LanguageEnvironmentPresenter(BasePresenter):
    default_attributes = [
        'block_count',
        'blocks',
        'commands',
        'definition',
        'environment',
        'exists',
        'id',
        'label',
        'language',
        'locked_count',
        'manifest',
        'manifest_text',
        'package_count',
        'packages',
        'pipelines',
        'problems',
        'state',
        'tools',
        'version',
    ]
