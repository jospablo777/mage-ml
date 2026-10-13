import os
import threading


class SecretsManager:
    def __init__(self):
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        key_vault_url = os.getenv('AZURE_KEY_VAULT_URL')
        if not key_vault_url:
            raise Exception('Please provide valid AZURE_KEY_VAULT_URL in environment variable.')
        cred = DefaultAzureCredential()
        self.client = SecretClient(vault_url=key_vault_url, credential=cred)


# Made on first use, so a project without a key vault does not try to make one each time it
# renders a config file, and azure_secret_var says what is missing when it is used.
_secrets_manager = None
_lock = threading.Lock()


def _manager() -> SecretsManager:
    global _secrets_manager
    with _lock:
        if _secrets_manager is None:
            _secrets_manager = SecretsManager()
        return _secrets_manager


def get_secret(secret_name: str) -> str:
    return _manager().client.get_secret(secret_name).value
