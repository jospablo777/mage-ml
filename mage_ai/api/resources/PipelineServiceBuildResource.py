from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PipelineServiceBuildResource(GenericResource):
    """
    Build an exported service's image on the server, for one platform or several, and push
    it to a registry the server's docker is logged in to. GET /api/pipeline_service_builds
    says whether the server can build; POST starts a build; GET
    /api/pipeline_service_builds/<service> gives its state and every line docker printed.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.pipeline_services.exports import docker_status

        return cls.build_result_set([dict(name='server', **docker_status())], user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelineServiceBuildResource':
        from mage_ai.pipeline_services import export
        from mage_ai.pipeline_services.exports import start_build

        image = payload.get('image')
        platforms = payload.get('platforms') or []
        if not isinstance(image, str) or not image.strip() or ' ' in image.strip():
            raise ApiError(dict(code=400, message='Name the image, such as icr.io/team/app:1.0.'))
        if not isinstance(platforms, list) or not all(isinstance(p, str) for p in platforms):
            raise ApiError(dict(code=400, message='platforms must be a list, such as linux/amd64.'))
        try:
            job = start_build(
                get_repo_path(user=user), payload.get('name'), image.strip(),
                platforms=platforms, push=bool(payload.get('push')),
            )
        except (FileNotFoundError, ValueError) as error:
            raise ApiError(dict(code=404, message=str(error)))
        except export.ExportError as error:
            raise ApiError(dict(code=400, message='\n'.join(error.problems)))
        return cls(job, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineServiceBuildResource':
        from mage_ai.pipeline_services.exports import build_job

        job = build_job(get_repo_path(user=user), pk)
        if not job:
            raise ApiError(dict(
                code=404, message=f'No build of {pk} ran since the server started.',
            ))
        return cls(job, user, **kwargs)
