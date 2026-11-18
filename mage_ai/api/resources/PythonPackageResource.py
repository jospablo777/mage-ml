from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource


class PythonPackageResource(GenericResource):
    """
    GET /api/python_packages/<name>: the package's releases on the server's package index,
    newest first, and the latest final one, which a requirement without a version gets
    (data_preparation/package_index.py). For the Workers page's version picker.
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PythonPackageResource':
        import asyncio

        from mage_ai.data_preparation import package_index

        try:
            found = await asyncio.to_thread(package_index.releases, pk)
        except package_index.PackageIndexError as error:
            raise ApiError(dict(code=404 if error.missing else 400, message=str(error)))
        return cls(dict(found, id=found['name']), user, **kwargs)
