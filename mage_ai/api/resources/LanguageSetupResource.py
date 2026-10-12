import asyncio

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class LanguageSetupResource(GenericResource):
    """
    GET /api/language_setups/r or /rust: whether blocks of that language can run here, and
    if not, what is missing, the commands that fix it and the setup guide.
    """

    @classmethod
    async def member(cls, pk, user, **kwargs):
        from mage_ai.data_preparation.models.block.language_setup import (
            LANGUAGES,
            language_setup,
        )

        if pk not in LANGUAGES:
            raise ApiError(dict(
                code=404, message=f'Setup is checked for {", ".join(LANGUAGES)}, not {pk}.',
            ))
        repo_path = get_repo_path(user=user)
        # Checking R runs Rscript, so it runs off the event loop.
        result = await asyncio.to_thread(language_setup, pk, repo_path)
        return cls(result, user, **kwargs)
