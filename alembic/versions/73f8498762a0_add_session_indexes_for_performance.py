"""Create base tables and add session indexes for performance

Revision ID: 73f8498762a0
Revises:
Create Date: 2025-10-06 11:07:49.296124

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '73f8498762a0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- users --
    op.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id                      SERIAL PRIMARY KEY,
            username                VARCHAR(50) NOT NULL,
            email                   VARCHAR(100) NOT NULL,
            hashed_password         VARCHAR(255) NOT NULL,
            full_name               VARCHAR(100),
            is_active               BOOLEAN DEFAULT TRUE,
            is_superuser            BOOLEAN DEFAULT FALSE,
            last_login              TIMESTAMPTZ,
            created_at              TIMESTAMPTZ DEFAULT now(),
            updated_at              TIMESTAMPTZ DEFAULT now(),
            api_key                 VARCHAR(64),
            api_key_created_at      TIMESTAMPTZ,
            timezone                VARCHAR(50) DEFAULT 'UTC',
            notification_preferences TEXT
        )
    """)
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username ON users (username)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_email ON users (email)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_api_key ON users (api_key)")

    # -- funnel_configs --
    op.execute("""
        CREATE TABLE IF NOT EXISTS funnel_configs (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL UNIQUE REFERENCES users (id),
            config      JSON NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now(),
            updated_at  TIMESTAMPTZ DEFAULT now()
        )
    """)

    # -- visit_sessions --
    # Note: asn is INTEGER here; fix_asn_type migration later changes it to VARCHAR(50).
    op.execute("""
        CREATE TABLE IF NOT EXISTS visit_sessions (
            id                          VARCHAR(64) PRIMARY KEY,
            ip_address                  VARCHAR(45) NOT NULL,
            user_agent                  VARCHAR(1000) NOT NULL,
            crawler_type                VARCHAR(100),
            first_visit                 TIMESTAMPTZ DEFAULT now(),
            last_visit                  TIMESTAMPTZ DEFAULT now(),
            visit_count                 INTEGER,
            country                     VARCHAR(2),
            country_name                VARCHAR(100),
            city                        VARCHAR(100),
            latitude                    DOUBLE PRECISION,
            longitude                   DOUBLE PRECISION,
            timezone                    VARCHAR(50),
            isp                         VARCHAR(200),
            organization                VARCHAR(200),
            asn                         INTEGER
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_id ON visit_sessions (id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_ip_address ON visit_sessions (ip_address)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_crawler_type ON visit_sessions (crawler_type)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_first_visit ON visit_sessions (first_visit)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_last_visit ON visit_sessions (last_visit)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_sessions_country ON visit_sessions (country)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_sessions_crawler_first_visit ON visit_sessions (crawler_type, first_visit)")

    # -- visits --
    op.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            id                          BIGSERIAL PRIMARY KEY,
            session_id                  VARCHAR(64) NOT NULL REFERENCES visit_sessions (id),
            timestamp                   TIMESTAMPTZ DEFAULT now(),
            ip_address                  VARCHAR(45) NOT NULL,
            user_agent                  VARCHAR(1000) NOT NULL,
            page_url                    VARCHAR(2000),
            referrer                    VARCHAR(2000),
            page_title                  VARCHAR(500),
            page_domain                 VARCHAR(200),
            crawler_type                VARCHAR(100),
            crawler_confidence          DOUBLE PRECISION DEFAULT 1.0,
            is_bot                      BOOLEAN DEFAULT TRUE,
            request_method              VARCHAR(10) DEFAULT 'GET',
            request_headers             JSON,
            response_status             INTEGER,
            response_size               INTEGER,
            response_time_ms            DOUBLE PRECISION,
            country                     VARCHAR(2),
            city                        VARCHAR(100),
            tracking_id                 VARCHAR(100),
            campaign                    VARCHAR(100),
            source                      VARCHAR(100),
            medium                      VARCHAR(100),
            content_type                VARCHAR(100),
            content_language            VARCHAR(10),
            content_encoding            VARCHAR(50),
            protocol                    VARCHAR(10),
            port                        INTEGER,
            path                        VARCHAR(1000),
            query_params                JSON
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_session_id ON visits (session_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_timestamp ON visits (timestamp)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_ip_address ON visits (ip_address)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_page_url ON visits (page_url)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_page_domain ON visits (page_domain)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_crawler_type ON visits (crawler_type)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_is_bot ON visits (is_bot)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_country ON visits (country)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visits_tracking_id ON visits (tracking_id)")

    # -- visit_events --
    # Note: page_domain, referrer_domain, tracking_id, source, medium, campaign
    # are NOT included here — they are added by add_event_attribution_fields migration.
    op.execute("""
        CREATE TABLE IF NOT EXISTS visit_events (
            id                          BIGSERIAL PRIMARY KEY,
            session_id                  VARCHAR(64) NOT NULL REFERENCES visit_sessions (id),
            visit_id                    BIGINT REFERENCES visits (id),
            timestamp                   TIMESTAMPTZ DEFAULT now(),
            event_type                  VARCHAR(50),
            page_url                    VARCHAR(2000),
            referrer                    VARCHAR(2000),
            path                        VARCHAR(1000),
            event_data                  JSON
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_events_session_id ON visit_events (session_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_events_visit_id ON visit_events (visit_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_events_timestamp ON visit_events (timestamp)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_events_event_type ON visit_events (event_type)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_visit_events_page_url ON visit_events (page_url)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS visit_events CASCADE")
    op.execute("DROP TABLE IF EXISTS visits CASCADE")
    op.execute("DROP TABLE IF EXISTS visit_sessions CASCADE")
    op.execute("DROP TABLE IF EXISTS funnel_configs CASCADE")
    op.execute("DROP TABLE IF EXISTS users CASCADE")
