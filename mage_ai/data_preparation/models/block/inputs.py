"""
How a block function receives its upstream blocks' outputs.

By default they arrive by position, in the order of the block's upstream_blocks: reordering
that list in metadata.yaml swaps two inputs of the same type without an error. A block can
name its inputs instead, with keyword-only parameters named after its upstream blocks:

    @transformer
    def join(*, load_orders, load_customers, **kwargs):
        ...

Each such parameter gets the output of the upstream block of that name (its uuid, the last
part of a uuid with folders, or the uuid with other characters as underscores). Upstream
outputs not named still arrive by position, in order. A required keyword-only parameter
that names no upstream block takes the run's variable of that name, or fails with the
names it could have used.
"""
import re
from inspect import Parameter, signature
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


class InputBindingError(TypeError):
    """A block function asks for an input that is neither an upstream block nor a variable."""


def _names(uuid: str) -> List[str]:
    last = uuid.split('/')[-1]
    return list(dict.fromkeys([uuid, last, re.sub(r'\W', '_', last), re.sub(r'\W', '_', uuid)]))


def bind_inputs(
    block_function: Callable,
    input_vars: Sequence[Any],
    upstream_uuids: Optional[Sequence[str]],
    global_vars: Optional[Dict] = None,
    block_uuid: Optional[str] = None,
) -> Tuple[List[Any], Dict[str, Any]]:
    """The positional and keyword arguments to call the block function with."""
    global_vars = global_vars or {}
    try:
        parameters = signature(block_function).parameters.values()
    except (TypeError, ValueError):
        return list(input_vars), dict(global_vars)
    has_kwargs = any(p.kind == Parameter.VAR_KEYWORD for p in parameters)
    keyword_only = [p for p in parameters if p.kind == Parameter.KEYWORD_ONLY]
    variables = dict(global_vars) if has_kwargs else {}
    if not keyword_only:
        return list(input_vars), variables

    by_name = {}
    if upstream_uuids is not None and len(upstream_uuids) == len(input_vars):
        for index, uuid in enumerate(upstream_uuids):
            for name in _names(uuid):
                by_name.setdefault(name, index)

    named: Dict[str, Any] = {}
    bound = set()
    for parameter in keyword_only:
        index = by_name.get(parameter.name)
        if index is not None and index not in bound:
            named[parameter.name] = input_vars[index]
            bound.add(index)
        elif parameter.name in global_vars:
            named[parameter.name] = global_vars[parameter.name]
        elif parameter.default is Parameter.empty:
            upstream = ', '.join(upstream_uuids or []) or 'none'
            raise InputBindingError(
                f'Block {block_uuid or block_function.__name__} asks for the input '
                f'`{parameter.name}`, which is neither an upstream block nor a variable of '
                f'the run. Its upstream blocks: {upstream}.'
            )
    args = [value for index, value in enumerate(input_vars) if index not in bound]
    for name in named:
        variables.pop(name, None)
    return args, {**variables, **named}
