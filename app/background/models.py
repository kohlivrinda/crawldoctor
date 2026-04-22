"""Models for background job infrastructure."""
from datetime import datetime
from sqlalchemy import Column, String, DateTime, func
from app.database import Base


class BackgroundJobState(Base):
    """Tracks watermark per registered sweep job."""

    __tablename__ = "background_job_state"

    job_name = Column(String(100), primary_key=True)
    last_processed_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
