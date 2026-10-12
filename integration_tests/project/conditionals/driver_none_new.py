if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(window, *args, **kwargs) -> bool:
    return window['high_id'] <= window['last_id']
