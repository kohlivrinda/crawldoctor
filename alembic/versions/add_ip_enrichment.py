"""add_ip_enrichment_table

Revision ID: add_ip_enrichment
Revises: fix_asn_type
Create Date: 2026-05-04

"""
from alembic import op
import sqlalchemy as sa

revision = 'add_ip_enrichment'
down_revision = 'fix_asn_type'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'ip_enrichment',
        sa.Column('ip', sa.String(45), primary_key=True),

        # Identity
        sa.Column('company_domain', sa.String(255), nullable=True),
        sa.Column('company_name', sa.String(500), nullable=True),
        sa.Column('company_type', sa.String(100), nullable=True),
        sa.Column('country', sa.String(2), nullable=True),

        # Network flags (requires security plan; null on free)
        sa.Column('is_datacenter', sa.Boolean, nullable=True),
        sa.Column('is_vpn', sa.Boolean, nullable=True),
        sa.Column('is_proxy', sa.Boolean, nullable=True),
        sa.Column('is_tor', sa.Boolean, nullable=True),

        # Metadata
        sa.Column('source', sa.String(50), nullable=True),
        sa.Column('enriched_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('ttl_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('status', sa.String(20), nullable=False, server_default='pending'),
        sa.Column('error_code', sa.String(50), nullable=True),
        sa.Column('error_message', sa.Text, nullable=True),
        sa.Column('attempt_count', sa.Integer, nullable=False, server_default='0'),
        sa.Column('first_seen_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index('ix_ip_enrichment_status', 'ip_enrichment', ['status'])
    op.create_index('ix_ip_enrichment_ttl_expires_at', 'ip_enrichment', ['ttl_expires_at'])
    op.create_index('ix_ip_enrichment_company_domain', 'ip_enrichment', ['company_domain'])
    op.create_index('ix_ip_enrichment_country', 'ip_enrichment', ['country'])


def downgrade() -> None:
    op.drop_table('ip_enrichment')
