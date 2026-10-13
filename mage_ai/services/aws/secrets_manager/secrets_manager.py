import threading

from mage_ai.services.aws import get_aws_boto3_client


class SecretsManager:
    def __init__(self):
        from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

        client = get_aws_boto3_client('secretsmanager')

        cache_config = SecretCacheConfig()
        self.cache = SecretCache(config=cache_config, client=client)


# Made on first use: making the client off AWS asks the instance metadata address for a
# region and waits 2 seconds, which every process that renders a config file paid.
_secrets_manager = None
_lock = threading.Lock()


def _manager() -> SecretsManager:
    global _secrets_manager
    with _lock:
        if _secrets_manager is None:
            _secrets_manager = SecretsManager()
        return _secrets_manager


def get_secret(secret_id: str, cached=True) -> str:
    if cached:
        return _manager().cache.get_secret_string(secret_id)
    else:
        return get_secret_force(secret_id)


def get_secret_force(secret_id: str) -> str:
    client = get_aws_boto3_client('secretsmanager')

    return client.get_secret_value(
        SecretId=secret_id,
    ).get('SecretString')
