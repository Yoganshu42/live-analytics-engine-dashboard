from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint, func

from db.base import Base


class ManualUpdateMarker(Base):
    __tablename__ = "manual_update_markers"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False, index=True)
    dataset_type = Column(String, nullable=False, index=True)
    job_key = Column(String, nullable=False, default="", index=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("source", "dataset_type", "job_key", name="uq_manual_update_marker_tag"),
    )
