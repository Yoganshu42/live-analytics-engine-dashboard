from sqlalchemy import Boolean, Column, DateTime, Integer, JSON, String, UniqueConstraint, func

from db.base import Base


class PrecomputedGraph(Base):
    __tablename__ = "precomputed_graphs"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False, index=True)
    dataset_type = Column(String, nullable=False, index=True)
    job_key = Column(String, nullable=False, default="", index=True)
    dimension = Column(String, nullable=False, index=True)
    metric = Column(String, nullable=False, index=True)
    bucket = Column(String, nullable=False, default="")
    from_date = Column(String, nullable=False, default="")
    to_date = Column(String, nullable=False, default="")
    rows = Column(JSON, nullable=False, default=list)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "source",
            "dataset_type",
            "job_key",
            "dimension",
            "metric",
            "bucket",
            "from_date",
            "to_date",
            name="uq_precomputed_graph_tag",
        ),
    )


class PrecomputedSummary(Base):
    __tablename__ = "precomputed_summaries"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False, index=True)
    dataset_type = Column(String, nullable=False, index=True)
    job_key = Column(String, nullable=False, default="", index=True)
    from_date = Column(String, nullable=False, default="")
    to_date = Column(String, nullable=False, default="")
    summary = Column(JSON, nullable=False, default=dict)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "source",
            "dataset_type",
            "job_key",
            "from_date",
            "to_date",
            name="uq_precomputed_summary_tag",
        ),
    )


class PrecomputedInsight(Base):
    __tablename__ = "precomputed_insights"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False, index=True)
    dataset_type = Column(String, nullable=False, index=True)
    job_key = Column(String, nullable=False, default="", index=True)
    dimension = Column(String, nullable=False, index=True)
    metric = Column(String, nullable=False, index=True)
    bucket = Column(String, nullable=False, default="")
    compare_mode = Column(Boolean, nullable=False, default=False)
    from_date = Column(String, nullable=False, default="")
    to_date = Column(String, nullable=False, default="")
    insights = Column(JSON, nullable=False, default=list)
    model = Column(String, nullable=False, default="rule-based")
    message = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "source",
            "dataset_type",
            "job_key",
            "dimension",
            "metric",
            "bucket",
            "compare_mode",
            "from_date",
            "to_date",
            name="uq_precomputed_insight_tag",
        ),
    )
