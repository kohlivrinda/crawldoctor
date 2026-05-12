"""Database configuration and session management."""
from datetime import date, timedelta
from sqlalchemy import create_engine, event, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Generator
import structlog
from fastapi import HTTPException

from app.config import settings

logger = structlog.get_logger()

# Database engine configuration with optimized settings for larger VM
engine = create_engine(
    settings.database_url,
    poolclass=QueuePool,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=settings.database_pool_timeout,
    echo=settings.debug,
    connect_args={
        "connect_timeout": 30,
    },
    execution_options={
        "isolation_level": "READ COMMITTED"
    }
)



# Set statement_timeout after connect rather than as a startup parameter,
# since Neon's connection pooler rejects it in the startup packet.
@event.listens_for(engine, "connect")
def _set_statement_timeout(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute(f"SET statement_timeout = {settings.database_statement_timeout}")
    cursor.close()

# Session configuration
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

# Metadata for migrations
metadata = MetaData()


def get_db() -> Generator[Session, None, None]:
    """Get database session dependency."""
    db = SessionLocal()
    try:
        yield db
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Database session error",
            error=str(e) or repr(e),
            error_type=type(e).__name__,
            error_args=str(e.args) if hasattr(e, 'args') else None,
        )
        db.rollback()
        raise
    finally:
        db.close()


async def init_db():
    """Initialize database tables."""
    try:
        Base.metadata.create_all(bind=engine)
        ensure_event_partitions()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error("Failed to create database tables", error=str(e))
        raise


def ensure_event_partitions() -> None:
    """Ensure visit_events is partitioned and recent partitions exist.

    WARNING: This runs at app startup and only creates partitions for
    event_partition_days_ahead into the future. If the app is not restarted
    before those days elapse, inserts for future dates will fall into the
    default partition. Consider moving partition maintenance to pg_cron
    (e.g. ``SELECT cron.schedule('create_partitions', '0 0 * * *', ...)``).
    """
    try:
        with engine.begin() as conn:
            # Check if visit_events table exists and its type
            relkind = conn.execute(
                text("SELECT relkind FROM pg_class WHERE relname = 'visit_events'")
            ).scalar()

            # If table doesn't exist yet, it will be created by SQLAlchemy as a regular table
            # Partitioning would need to be done via migration, not at runtime
            if not relkind:
                logger.info("visit_events table doesn't exist yet, will be created by SQLAlchemy")
                return
            
            # If it's not partitioned (relkind = 'r' for regular table)
            if relkind == 'r':
                logger.info("visit_events is a regular table, not partitioned (this is fine for now)")
                # Partitioning an existing table with data requires a migration
                # We'll skip it to avoid errors
                return
            
            # If it's already partitioned (relkind = 'p'), create partitions
            if relkind == 'p':
                logger.info("visit_events is partitioned, ensuring partitions exist")
                
                # Create default partition if it doesn't exist
                try:
                    conn.execute(text("CREATE TABLE IF NOT EXISTS visit_events_default PARTITION OF visit_events DEFAULT"))
                except Exception as exc:
                    logger.debug("Default partition already exists or failed", error=str(exc))

                # Create daily partitions
                today = date.today()
                start = today - timedelta(days=settings.event_partition_days_back)
                end = today + timedelta(days=settings.event_partition_days_ahead)

                current = start
                while current <= end:
                    next_day = current + timedelta(days=1)
                    partition_name = f"visit_events_{current.strftime('%Y%m%d')}"
                    try:
                        conn.execute(
                            text(
                                f"CREATE TABLE IF NOT EXISTS {partition_name} "
                                f"PARTITION OF visit_events FOR VALUES FROM ('{current}') TO ('{next_day}')"
                            )
                        )
                    except Exception as exc:
                        # Partition might already exist, that's fine
                        pass
                    current = next_day
                
                logger.info("Event partitions ensured successfully")
    except Exception as exc:
        logger.warning("Could not ensure event partitions (this is non-critical)", error=str(exc))


async def close_db():
    """Close database connections."""
    try:
        engine.dispose()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error("Error closing database connections", error=str(e))
