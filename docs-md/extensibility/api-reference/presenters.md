# API presenters

*This will determine what attributes of the resource are returned to the client in the response.*

Define your presenter by creating a file `mage_ai/api/presenters/SwordPresenter.py`:

```python
from mage_ai.api.presenters.BasePresenter import BasePresenter

class SwordPresenter(BasePresenter):
    default_attributes = [
        'id',
        'name',
        'strength',
    ]
```

By default, whenever a sword is being returned in an API response, it will return a sword object in
JSON format with the `id`, `name`, and `strength` keys.

## Register different formats for different actions
If you want the resource to return different data, you can register unique formats and the
attributes that format should return:

```python
from mage_ai.api.presenters.BasePresenter import BasePresenter
from mage_ai.api.operations import constants as operations

class SwordPresenter(BasePresenter):
    default_attributes = [
        'id',
        'name',
        'strength',
    ]

SwordPresenter.register_formats([
    operations.LIST,
], [
    'id',
])
```

This will only return the `id` attribute when making an API call to collections endpoint
`/swords` to retrieve a list of sword resources.

## Register custom formats
You may want to define a custom format that can return specific attributes when making an API
call by including the `_format` URL parameter. For example, you can present specific fields by
calling the `GET /swords?_format=all_attributes` endpoint.

```python
from mage_ai.api.presenters.BasePresenter import BasePresenter
from mage_ai.api.operations import constants as operations

class SwordPresenter(BasePresenter):
    default_attributes = [
        'id',
        'name',
        'strength',
    ]

SwordPresenter.register_formats([
    'all_attributes',
], [
    'created_at',
    'deleted_at',
    'id',
    'name',
    'strength',
    'team',
    'user',
])
```

Note: be sure to permit reading these attributes in the `SwordPolicy`.
