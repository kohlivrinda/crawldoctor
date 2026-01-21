"""add_summary_tables

Revision ID: 47da9be542cf
Revises: 9f3c0f9a2b1e
Create Date: 2026-01-21 03:41:40.367676

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '47da9be542cf'
down_revision = '9f3c0f9a2b1e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # lead_summaries
    op.create_table(
        'lead_summaries',
        sa.Column('client_id', sa.String(length=64), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=True),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('captured_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('captured_page', sa.String(length=2000), nullable=True),
        sa.Column('captured_path', sa.String(length=1000), nullable=True),
        sa.Column('form_data_shared', sa.Text(), nullable=True),
        sa.Column('captured_data', sa.Text(), nullable=True),
        sa.Column('source', sa.String(length=100), nullable=True),
        sa.Column('medium', sa.String(length=100), nullable=True),
        sa.Column('campaign', sa.String(length=100), nullable=True),
        sa.Column('first_referrer', sa.String(length=2000), nullable=True),
        sa.Column('first_referrer_domain', sa.String(length=200), nullable=True),
        sa.Column('first_seen', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('client_id')
    )
    op.create_index(op.f('ix_lead_summaries_client_id'), 'lead_summaries', ['client_id'], unique=False)
    op.create_index(op.f('ix_lead_summaries_email'), 'lead_summaries', ['email'], unique=False)
    op.create_index(op.f('ix_lead_summaries_name'), 'lead_summaries', ['name'], unique=False)
    op.create_index(op.f('ix_lead_summaries_source'), 'lead_summaries', ['source'], unique=False)
    op.create_index(op.f('ix_lead_summaries_captured_at'), 'lead_summaries', ['captured_at'], unique=False)

    # journey_summaries
    op.create_table(
        'journey_summaries',
        sa.Column('client_id', sa.String(length=64), nullable=False),
        sa.Column('first_seen', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_seen', sa.DateTime(timezone=True), nullable=True),
        sa.Column('visit_count', sa.Integer(), nullable=True),
        sa.Column('entry_page', sa.String(length=2000), nullable=True),
        sa.Column('exit_page', sa.String(length=2000), nullable=True),
        sa.Column('path_sequence', sa.Text(), nullable=True),
        sa.Column('email', sa.String(length=255), nullable=True),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('has_captured_data', sa.Integer(), nullable=True),
        sa.Column('source', sa.String(length=100), nullable=True),
        sa.Column('medium', sa.String(length=100), nullable=True),
        sa.Column('campaign', sa.String(length=100), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('client_id')
    )
    op.create_index(op.f('ix_journey_summaries_client_id'), 'journey_summaries', ['client_id'], unique=False)
    op.create_index(op.f('ix_journey_summaries_email'), 'journey_summaries', ['email'], unique=False)
    op.create_index(op.f('ix_journey_summaries_last_seen'), 'journey_summaries', ['last_seen'], unique=False)


def downgrade() -> None:
    op.drop_table('journey_summaries')
    op.drop_table('lead_summaries')
