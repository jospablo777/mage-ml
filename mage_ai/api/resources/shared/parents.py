from typing import Any, Dict

import inflection

from mage_ai.api.errors import ApiError


def parent_of(kwargs: Dict, resource: str, parent: str) -> Any:
    """
    The parent a nested route loaded (/api/{parent}/{id}/{resource}); without one, a 400 that
    names the route, where the resource code failed on None before.
    """
    model = kwargs.get('parent_model')
    if model is None or getattr(model, 'uuid', None) is None:
        raise ApiError(dict(
            code=400,
            message=f'{inflection.humanize(resource)} belong to a '
                    f'{inflection.humanize(inflection.singularize(parent)).lower()}: '
                    f'/api/{parent}/<uuid>/{resource}.',
            type='parent_required',
        ))
    return model
