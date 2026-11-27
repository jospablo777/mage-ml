import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class LanguageEnvironmentResource(GenericResource):
    """
    The project's R and Rust environments (language_environments.py, ADR-0040): each
    language's default and its named ones, by id (`r`, `rust`, `r.<name>`, `rust.<name>`).
    GET /api/language_environments lists them from their files; GET .../<id> also asks the
    tools and says what stops blocks; POST with language, name, start_from (default or
    minimal) and, for R, r_version creates a named one; PUT with manifest_text saves
    rproject.toml or Cargo.toml once it resolves in a copy, with sync (R) installs the
    packages, and with init (Rust) creates the default environment; DELETE removes a named
    one no block uses.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import language_environments

        rows = await asyncio.to_thread(language_environments.summary, get_repo_path(user=user))
        return cls.build_result_set(rows, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'LanguageEnvironmentResource':
        from mage_ai.data_preparation import language_environments

        try:
            row = await asyncio.to_thread(
                language_environments.detail, get_repo_path(user=user), pk,
            )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(row, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'LanguageEnvironmentResource':
        from mage_ai.data_preparation import language_environments

        try:
            row = await asyncio.to_thread(
                language_environments.create,
                get_repo_path(user=user),
                payload.get('language'),
                payload.get('name'),
                start_from=payload.get('start_from') or 'default',
                r_version=payload.get('r_version') or None,
            )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(row, user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        from mage_ai.data_preparation import language_environments

        repo_path = get_repo_path(user=self.current_user)
        language = self.model['language']
        environment = self.model.get('environment') or language_environments.DEFAULT
        try:
            if payload.get('init'):
                if language != 'rust':
                    raise language_environments.LanguageEnvironmentError(
                        'Create the default R environment with `mage r init` in a terminal: '
                        'it installs the tidyverse and what R blocks need, which takes '
                        'minutes. A named R environment is created from this page.',
                    )
                row = await asyncio.to_thread(
                    language_environments.init_rust, repo_path, environment,
                )
            elif payload.get('sync'):
                if language != 'r':
                    raise language_environments.LanguageEnvironmentError(
                        'Rust needs no sync: each Rust block builds with the crates when it '
                        'runs, or `mage rust build` builds them all.',
                    )
                row = await asyncio.to_thread(language_environments.sync_r, repo_path,
                                              environment)
            elif 'manifest_text' in payload:
                row = await asyncio.to_thread(
                    language_environments.save_manifest, repo_path, self.model['id'],
                    payload.get('manifest_text'),
                )
            else:
                raise language_environments.LanguageEnvironmentError(
                    'Send manifest_text to change the file, or sync to install R packages.',
                )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = row

    async def delete(self, **kwargs):
        from mage_ai.data_preparation import language_environments

        try:
            await asyncio.to_thread(
                language_environments.delete, get_repo_path(user=self.current_user),
                self.model['id'],
            )
        except language_environments.LanguageEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
