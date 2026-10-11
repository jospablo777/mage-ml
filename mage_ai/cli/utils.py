import json
from typing import Any, Dict, Union


def parse_runtime_variables(variables_str: str) -> Dict[str, Any]:
    """
    Returns a dictionary of variable to parsed values.

    Args:
        variables_str (str): The variables json string

    Returns:
        Dict[str, Any]: The parsed variables dictionary
    """
    if not variables_str:
        return dict()

    # Invalid JSON used to print a traceback and run without the variables.
    try:
        vars_parsed = json.loads(variables_str)
    except json.JSONDecodeError as error:
        raise ValueError(
            f'--runtime-vars is not valid JSON ({error.msg} at character {error.pos}): '
            f'{variables_str}. Pass an object, such as \'{{"date": "2026-01-31"}}\'.'
        ) from error
    if not isinstance(vars_parsed, dict):
        raise ValueError(
            f'--runtime-vars must be a JSON object of variable names and values, not '
            f'{type(vars_parsed).__name__}: {variables_str}',
        )
    return vars_parsed


def get_value(value: Union[str, None]) -> Any:
    """
    Returns parsed value.

    If the string is a valid JSON object, deserializes it to a Python object.
    Otherwise, passes the original value.
    """
    if value is not None:
        try:
            return json.loads(value)
        except json.decoder.JSONDecodeError:
            pass
    return value
