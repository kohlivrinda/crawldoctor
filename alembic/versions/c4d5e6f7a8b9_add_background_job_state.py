"""add background_job_state table

Revision ID: c4d5e6f7a8b9
Revises: b3f1a9c7d820
Create Date: 2026-03-31

"""
from alembic import op
import sqlalchemy as sa


revision = 'c4d5e6f7a8b9'
down_revision = 'b3f1a9c7d820'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'background_job_state',
        sa.Column('job_name', sa.String(100), primary_key=True),
        sa.Column('last_processed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table('background_job_state')
