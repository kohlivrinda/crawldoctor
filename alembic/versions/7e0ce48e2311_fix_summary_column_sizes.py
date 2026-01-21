"""fix_summary_column_sizes

Revision ID: 7e0ce48e2311
Revises: 47da9be542cf
Create Date: 2026-01-21 04:08:30.116281

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7e0ce48e2311'
down_revision = '47da9be542cf'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column('lead_summaries', 'captured_page', type_=sa.Text())
    op.alter_column('lead_summaries', 'captured_path', type_=sa.Text())
    op.alter_column('lead_summaries', 'first_referrer', type_=sa.Text())
    op.alter_column('journey_summaries', 'entry_page', type_=sa.Text())
    op.alter_column('journey_summaries', 'exit_page', type_=sa.Text())


def downgrade() -> None:
    op.alter_column('journey_summaries', 'exit_page', type_=sa.String(2000))
    op.alter_column('journey_summaries', 'entry_page', type_=sa.String(2000))
    op.alter_column('lead_summaries', 'first_referrer', type_=sa.String(2000))
    op.alter_column('lead_summaries', 'captured_path', type_=sa.String(1000))
    op.alter_column('lead_summaries', 'captured_page', type_=sa.String(2000))
