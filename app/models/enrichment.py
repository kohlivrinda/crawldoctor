"""IP enrichment model."""
from sqlalchemy import Column, String, Boolean, DateTime, Integer, Text
from app.database import Base


class IpEnrichment(Base):
    __tablename__ = "ip_enrichment"

    ip = Column(String(45), primary_key=True)

    # Identity
    company_domain = Column(String(255), nullable=True, index=True)
    company_name = Column(String(500), nullable=True)
    company_type = Column(String(100), nullable=True)
    country = Column(String(2), nullable=True, index=True)

    # Network flags — null when provider plan doesn't include security data
    is_datacenter = Column(Boolean, nullable=True)
    is_vpn = Column(Boolean, nullable=True)
    is_proxy = Column(Boolean, nullable=True)
    is_tor = Column(Boolean, nullable=True)

    # Metadata
    source = Column(String(50), nullable=True)
    enriched_at = Column(DateTime(timezone=True), nullable=True)
    ttl_expires_at = Column(DateTime(timezone=True), nullable=True, index=True)
    status = Column(String(20), nullable=False, default='pending', index=True)
    error_code = Column(String(50), nullable=True)
    error_message = Column(Text, nullable=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    first_seen_at = Column(DateTime(timezone=True), nullable=True)
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
