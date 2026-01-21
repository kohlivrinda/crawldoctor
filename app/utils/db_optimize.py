
from sqlalchemy import text
from app.database import engine

def optimize_database():
    """Apply performance optimizations and composite indexes to the tracking database."""
    print("🚀 Starting database optimization...")

    indexes = [
        # Visits optimization
        "CREATE INDEX IF NOT EXISTS idx_visits_bot_ts ON visits (is_bot, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_visits_client_ts ON visits (client_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_visits_session_ts ON visits (session_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_visits_domain_ts ON visits (page_domain, timestamp)",

        # Sessions optimization
        "CREATE INDEX IF NOT EXISTS idx_sessions_last_visit ON visit_sessions (last_visit DESC)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_client_id ON visit_sessions (client_id)",

        # Events optimization
        "CREATE INDEX IF NOT EXISTS idx_events_type_ts ON visit_events (event_type, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_events_client_id ON visit_events (client_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_visit_id ON visit_events (visit_id)"
    ]

    with engine.begin() as conn:
        for idx_sql in indexes:
            try:
                print(f"Applying: {idx_sql.split(' ON ')[0]}...")
                conn.execute(text(idx_sql))
            except Exception as e:
                print(f"⚠️  Could not apply index: {str(e)}")

    print("✅ Database optimization complete!")

if __name__ == "__main__":
    optimize_database()
