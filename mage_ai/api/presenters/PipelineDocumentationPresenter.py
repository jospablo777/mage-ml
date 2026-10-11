from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineDocumentationPresenter(BasePresenter):
    default_attributes = [
        'exists',
        'format',
        'id',
        'image_path',
        'path',
        'pipeline_uuid',
        'quarto_available',
        'source',
        'template',
        'updated_at',
    ]
