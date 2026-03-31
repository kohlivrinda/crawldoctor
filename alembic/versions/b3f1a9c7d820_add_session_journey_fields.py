"""add_session_journey_fields

Add entry_referrer, entry_referrer_domain, is_external_entry to visit_sessions.
Create session_id_migration_log audit table for backfill traceability.
Add composite index on (client_id, last_visit DESC) for cross-domain session lookups.

Revision ID: b3f1a9c7d820
Revises: a1b2c3d4e5f6
Create Date: 2026-03-30 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3f1a9c7d820'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- visit_sessions: new columns for journey entry context --
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    vs_columns = [col['name'] for col in inspector.get_columns('visit_sessions')]

    if 'entry_referrer' not in vs_columns:
        op.add_column('visit_sessions', sa.Column('entry_referrer', sa.String(length=2000), nullable=True))
    if 'entry_referrer_domain' not in vs_columns:
        op.add_column('visit_sessions', sa.Column('entry_referrer_domain', sa.String(length=200), nullable=True))
    if 'is_external_entry' not in vs_columns:
        op.add_column('visit_sessions', sa.Column('is_external_entry', sa.Boolean(), nullable=True, server_default=sa.text('true')))

    # -- composite index for _resolve_existing_session lookups --
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_visit_sessions_client_id_last_visit "
            "ON visit_sessions (client_id, last_visit DESC)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "ix_visit_sessions_entry_referrer_domain "
            "ON visit_sessions (entry_referrer_domain)"
        )

    # -- audit table for session ID migration during backfill --
    # Only create if it doesn't exist (safe for re-runs).
    tables = inspector.get_table_names()
    if 'session_id_migration_log' not in tables:
        op.create_table(
            'session_id_migration_log',
            sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column('old_session_id', sa.String(length=64), nullable=False, index=True),
            sa.Column('new_session_id', sa.String(length=64), nullable=False, index=True),
            sa.Column('client_id', sa.String(length=64), nullable=True),
            sa.Column('visit_count_moved', sa.Integer(), nullable=True),
            sa.Column('event_count_moved', sa.Integer(), nullable=True),
            sa.Column('migrated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        )


def downgrade() -> None:
    op.drop_table('session_id_migration_log')

    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_visit_sessions_entry_referrer_domain")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_visit_sessions_client_id_last_visit")

    op.drop_column('visit_sessions', 'is_external_entry')
    op.drop_column('visit_sessions', 'entry_referrer_domain')
    op.drop_column('visit_sessions', 'entry_referrer')
