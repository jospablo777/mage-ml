from typing import Optional

from mage_ai.settings import get_settings_value
from mage_ai.settings.keys import GHE_HOSTNAME
from mage_ai.shared.enum import StrEnum


class ProviderName(StrEnum):
    ACTIVE_DIRECTORY = 'active_directory'
    AZURE_DEVOPS = 'azure_devops'
    BITBUCKET = 'bitbucket'
    GITHUB = 'github'
    GITLAB = 'gitlab'
    GHE = 'ghe'
    GOOGLE = 'google'
    OKTA = 'okta'
    OIDC_GENERIC = 'oidc_generic'


VALID_OAUTH_PROVIDERS = [e.value for e in ProviderName]

DEFAULT_GITHUB_HOSTNAME = 'https://github.com'

# Github and GHE don't need to be added to this list because they are handled
# separately for now.
GIT_OAUTH_PROVIDERS = [
    ProviderName.AZURE_DEVOPS,
    ProviderName.BITBUCKET,
    ProviderName.GITLAB,
]


def get_ghe_hostname() -> Optional[str]:
    ghe_hostname = get_settings_value(GHE_HOSTNAME)
    if ghe_hostname and not ghe_hostname.startswith('http'):
        ghe_hostname = f'https://{ghe_hostname}'

    return ghe_hostname
