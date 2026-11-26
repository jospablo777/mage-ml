import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class LanguageEnvironmentResource(GenericResource):
    """
    The project's R and Rust environments, one per language (language_environments.py):
    GET /api/language_environments lists both from their files; GET
    /api/language_environments/<r|rust> also asks the tools and says what stops blocks;
    PUT with manifest_text saves rproject.toml or Cargo.toml once it resolves in a copy, with
    sync (R) installs the packages, and with init (Rust) creates the environment.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import language_environments

        rows = await asyncio.to_thread(language_environments.summary, get_repo_path(user=user))
        return cls.build_result_set([dict(row, id=row['language']) for row in rows], user,
                                    **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'LanguageEnvironmentResource':
        from mage_ai.data_preparation import language_environments

        try:
            row = await asyncio.to_thread(
                language_environments.detail, get_repo_path(user=user), pk,
            )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(dict(row, id=row['language']), user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        from mage_ai.data_preparation import language_environments

        repo_path = get_repo_path(user=self.current_user)
        language = self.model['language']
        try:
            if payload.get('init'):
                if language != 'rust':
                    raise language_environments.LanguageEnvironmentError(
                        'Create the R environment with `mage r init` in a terminal: it '
                        'installs the tidyverse and what R blocks need, which takes minutes.',
                    )
                row = await asyncio.to_thread(language_environments.init_rust, repo_path)
            elif payload.get('sync'):
                if language != 'r':
                    raise language_environments.LanguageEnvironmentError(
                        'Rust needs no sync: each Rust block builds with the crates when it '
                        'runs, or `mage rust build` builds them all.',
                    )
                row = await asyncio.to_thread(language_environments.sync_r, repo_path)
            elif 'manifest_text' in payload:
                row = await asyncio.to_thread(
                    language_environments.save_manifest, repo_path, language,
                    payload.get('manifest_text'),
                )
            else:
                raise language_environments.LanguageEnvironmentError(
                    'Send manifest_text to change the file, or sync to install R packages.',
                )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = dict(row, id=row['language'])
