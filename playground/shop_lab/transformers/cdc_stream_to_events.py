from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def to_events(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    One event per changed row: the operation, the row's columns and where in the log it
    happened. A transaction's changes arrive whole, in commit order.
    """
    return [
        dict(op=m['_mage_operation'], table=f"{m['_mage_schema']}.{m['_mage_table']}",
             id=m['id'], status=m.get('status'), amount=m.get('amount'),
             lsn=m['_mage_lsn'], committed_at=m['_mage_commit_time'])
        for m in messages
    ]
