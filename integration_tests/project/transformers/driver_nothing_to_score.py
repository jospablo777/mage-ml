if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(pending, *args, **kwargs):
    return {'status': 'no_op', 'reason': 'no_pending_drivers'}
