from sqlalchemy import JSON, Column, DateTime, Integer, String, Text
from sqlalchemy.schema import Index

from mage_ai.orchestration.db.models.base import BaseModel


class AuditEvent(BaseModel):
    """
    A write through the API (create, update, delete): who, what, when, and how it ended
    (orchestration/audit_log.py records them). Repeated writes of the same object by the
    same user within a minute, such as the editor's saves, are one event with a count.
    """

    user_id = Column(Integer, nullable=True)
    user_name = Column(String(255), nullable=True)
    action = Column(String(32))
    object_type = Column(String(255))
    object_id = Column(String(255), nullable=True)
    parent = Column(String(255), nullable=True)
    parent_id = Column(String(255), nullable=True)
    # succeeded, refused (403) or failed.
    outcome = Column(String(32))
    error = Column(Text, nullable=True)
    # Attribute names to what was written: short values as they are, sensitive ones hidden,
    # long ones as their length and SHA-256.
    changes = Column(JSON, nullable=True)
    source_ip = Column(String(255), nullable=True)
    count = Column(Integer, default=1)
    last_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index(None, 'created_at'),
        Index(None, 'object_type', 'object_id'),
        Index(None, 'user_id'),
    )
