from dataclasses import dataclass, fields
from typing import Dict, Optional

from mage_ai.shared.config import BaseConfig


@dataclass
class RetryConfig(BaseConfig):
    retries: int = 0
    delay: int = 5
    max_delay: int = 60
    exponential_backoff: bool = True

    def __post_init__(self):
        # A null in a config file means the default; retries=None failed the block
        # with a TypeError instead of running it.
        for field in fields(self):
            if getattr(self, field.name) is None:
                setattr(self, field.name, field.default)


def resolve_retry_config(*configs: Optional[Dict]) -> Dict:
    """
    The retry config of a block run: the project's, the pipeline's and the block's,
    in that order. A blank or null value inherits the value before it, as the docs say.
    The pipeline's config was left out and a null replaced the project's value.
    """
    resolved = {}
    for config in configs:
        for key, value in (config or {}).items():
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            resolved[key] = value
    return resolved
