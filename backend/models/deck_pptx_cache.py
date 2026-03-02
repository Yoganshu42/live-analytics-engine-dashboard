from sqlalchemy import Boolean, Column, DateTime, Integer, LargeBinary, String, UniqueConstraint, func

from db.base import Base


class DeckPptxCache(Base):
    __tablename__ = "deck_pptx_cache"

    id = Column(Integer, primary_key=True, index=True)
    cache_key = Column(String, nullable=False, index=True)
    partners_key = Column(String, nullable=False, default="", index=True)
    dataset_type = Column(String, nullable=False, default="sales", index=True)
    job_key = Column(String, nullable=False, default="", index=True)
    from_date = Column(String, nullable=False, default="")
    to_date = Column(String, nullable=False, default="")
    include_tables = Column(Boolean, nullable=False, default=True)
    week_window = Column(Integer, nullable=False, default=4)
    data_fingerprint = Column(String, nullable=False, default="", index=True)
    filename = Column(String, nullable=False, default="partner_deck_sales.pptx")
    size_bytes = Column(Integer, nullable=False, default=0)
    pptx_blob = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("cache_key", name="uq_deck_pptx_cache_key"),
    )
