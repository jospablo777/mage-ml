from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(changes: List[Dict], *args, **kwargs) -> List[Dict]:
    """Changed rows of PostgreSQL, with _mage_operation and the other change fields."""
    return [
        {k: v for k, v in change.items() if k != '_mage_commit_time'}
        for change in changes
    ]
