if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(frame, *args, **kwargs) -> bool:
    return len(frame) == 0
