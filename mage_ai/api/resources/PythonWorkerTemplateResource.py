from mage_ai.api.resources.GenericResource import GenericResource


class PythonWorkerTemplateResource(GenericResource):
    """GET /api/python_worker_templates: the starting points for a new Python worker
    (environments.WORKER_TEMPLATES), each with its name, label, description and packages."""

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import environments

        templates = [
            dict(id=name, dependencies=list(template['dependencies']),
                 description=template['description'], label=template['label'])
            for name, template in environments.WORKER_TEMPLATES.items()
        ]
        return cls.build_result_set(templates, user, **kwargs)
