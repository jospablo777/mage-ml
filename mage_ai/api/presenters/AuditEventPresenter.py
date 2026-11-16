from mage_ai.api.presenters.BasePresenter import BasePresenter


class AuditEventPresenter(BasePresenter):
    default_attributes = [
        'action',
        'changes',
        'count',
        'created_at',
        'error',
        'id',
        'last_at',
        'outcome',
        'parent',
        'parent_id',
        'object_type',
        'object_id',
        'source_ip',
        'user_id',
        'user_name',
    ]
