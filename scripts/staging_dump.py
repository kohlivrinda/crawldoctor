#!/usr/bin/env python3
"""Create a partial DB dump (last N days) from prod and load it into a staging DB.

Usage:
    # Dump last 15 days from prod to a SQL file
    python scripts/staging_dump.py dump --source-url "postgresql://..." --days 15 --out staging_data.sql

    # Load the dump into a Neon staging DB
    python scripts/staging_dump.py load --target-url "postgresql://...@ep-xxx.neon.tech/crawldoctor" --file staging_data.sql

    # One-shot: dump from prod and load into staging
    python scripts/staging_dump.py sync --source-url "postgresql://..." --target-url "postgresql://...@neon.tech/..." --days 15

Environment variables (alternative to CLI flags):
    CRAWLDOCTOR_DATABASE_URL          - prod DB (used as --source-url default)
    CRAWLDOCTOR_STAGING_DATABASE_URL  - staging DB (used as --target-url default)
"""

import argparse
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone, timedelta

# All tables in dependency order (FK targets before FK sources).
TABLES = [
    "users",
    "funnel_configs",
    "visit_sessions",
    "visits",
    "visit_events",
    "journey_summaries",
    "lead_summaries",
    "journey_form_fills",
]

# Small config/auth tables — dump in full (no date filter).
FULL_DUMP_TABLES = {"users", "funnel_configs"}

# Large tracking tables — filter by timestamp column.
DATE_FILTERED_TABLES = {
    "visit_sessions": "last_visit",
    "visits":         "timestamp",
    "visit_events":   "timestamp",
}

# Summary tables — filter by client_id overlap with recent visits.
SUMMARY_TABLES = {"journey_summaries", "lead_summaries", "journey_form_fills"}


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess, printing the command for visibility."""
    printable = " ".join(cmd[:3]) + " ..."
    print(f"  -> {printable}")
    return subprocess.run(cmd, check=True, **kwargs)


def _psql(url: str, sql: str, capture: bool = False) -> str | None:
    """Execute SQL against a Postgres URL via psql."""
    result = subprocess.run(
        ["psql", url, "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=capture,
        text=True,
    )
    if result.returncode != 0:
        if capture:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"psql command failed: {sql[:80]}...")
    return result.stdout if capture else None


def dump(source_url: str, days: int, out_path: str) -> None:
    """Dump the last `days` of data from source into a SQL file."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00")
    print(f"Dumping last {days} days (since {cutoff}) to {out_path}")

    with open(out_path, "w") as f:
        # Header
        f.write(f"-- CrawlDoctor staging dump — last {days} days (since {cutoff})\n")
        f.write("-- Generated at {}\n".format(datetime.now(timezone.utc).isoformat()))
        f.write("BEGIN;\n\n")
        # Drop FK constraints during load — partial dumps will have dangling references
        # (e.g. events pointing to visits older than the dump window).
        # session_replication_role requires superuser (not available on Neon),
        # so we drop and re-create the constraints instead.
        f.write("ALTER TABLE visit_events DROP CONSTRAINT IF EXISTS visit_events_session_id_fkey;\n")
        f.write("ALTER TABLE visit_events DROP CONSTRAINT IF EXISTS visit_events_visit_id_fkey;\n")
        f.write("ALTER TABLE visits DROP CONSTRAINT IF EXISTS visits_session_id_fkey;\n\n")

    for table in TABLES:
        print(f"  Dumping {table}...")

        if table in FULL_DUMP_TABLES:
            where_clause = ""
        elif table in DATE_FILTERED_TABLES:
            ts_col = DATE_FILTERED_TABLES[table]
            where_clause = f"WHERE {ts_col} >= '{cutoff}'"
        elif table in SUMMARY_TABLES:
            where_clause = (
                f"WHERE client_id IN ("
                f"SELECT DISTINCT client_id FROM visits WHERE timestamp >= '{cutoff}' AND client_id IS NOT NULL"
                f")"
            )
        else:
            where_clause = ""

        copy_sql = f"\\copy (SELECT * FROM {table} {where_clause}) TO STDOUT WITH (FORMAT csv, HEADER true)"

        # Use psql to run the COPY and capture CSV output
        result = subprocess.run(
            ["psql", source_url, "-c", copy_sql],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"    Warning: failed to dump {table}: {result.stderr.strip()}")
            continue

        csv_data = result.stdout
        if not csv_data.strip():
            print(f"    (empty — skipping)")
            continue

        # Parse the CSV header to get column names
        lines = csv_data.strip().split("\n")
        header = lines[0]
        data_lines = lines[1:]
        print(f"    {len(data_lines)} rows")

        with open(out_path, "a") as f:
            # Create a temp table approach: use COPY FROM STDIN with CSV
            f.write(f"-- {table}: {len(data_lines)} rows\n")
            f.write(f"\\copy {table} ({header}) FROM STDIN WITH (FORMAT csv, HEADER false);\n")
            for line in data_lines:
                f.write(line + "\n")
            f.write("\\.\n\n")

    with open(out_path, "a") as f:
        # Re-add FK constraints (allow invalid refs from partial dump)
        f.write("ALTER TABLE visits ADD CONSTRAINT visits_session_id_fkey FOREIGN KEY (session_id) REFERENCES visit_sessions(id) NOT VALID;\n")
        f.write("ALTER TABLE visit_events ADD CONSTRAINT visit_events_session_id_fkey FOREIGN KEY (session_id) REFERENCES visit_sessions(id) NOT VALID;\n")
        f.write("ALTER TABLE visit_events ADD CONSTRAINT visit_events_visit_id_fkey FOREIGN KEY (visit_id) REFERENCES visits(id) NOT VALID;\n\n")
        f.write("COMMIT;\n")

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"Done. Dump file: {out_path} ({size_mb:.1f} MB)")


def load(target_url: str, file_path: str) -> None:
    """Load a dump file into the target (staging) database.

    This:
    1. Runs alembic upgrade head (creates tables from scratch on a fresh DB)
    2. Truncates the target tables (staging is throwaway)
    3. Loads the dump
    """
    print(f"Loading {file_path} into staging DB")

    # Run migrations — on a fresh DB this creates all tables from the initial
    # migration onward; on an existing DB it applies any pending migrations.
    print("  Running alembic migrations...")
    env = os.environ.copy()
    env["CRAWLDOCTOR_DATABASE_URL"] = target_url
    try:
        _run(["alembic", "upgrade", "head"], env=env)
    except subprocess.CalledProcessError:
        print("  Warning: alembic upgrade failed — schema may already be current")

    # Truncate in reverse dependency order
    print("  Truncating staging tables...")
    truncate_sql = "TRUNCATE {} CASCADE;".format(", ".join(reversed(TABLES)))
    try:
        _psql(target_url, truncate_sql)
    except RuntimeError:
        # Tables might not exist yet on first run
        for table in reversed(TABLES):
            try:
                _psql(target_url, f"TRUNCATE {table} CASCADE;")
            except RuntimeError:
                pass

    # Load the dump via psql
    print("  Loading data...")
    _run(["psql", target_url, "-v", "ON_ERROR_STOP=1", "-f", file_path])

    # Quick row counts
    print("  Row counts:")
    for table in TABLES:
        try:
            out = _psql(target_url, f"SELECT count(*) FROM {table};", capture=True)
            count = out.strip().split("\n")[-2].strip() if out else "?"
            print(f"    {table}: {count}")
        except RuntimeError:
            print(f"    {table}: (not found)")

    print("Done. Staging DB is ready.")


def sync(source_url: str, target_url: str, days: int) -> None:
    """Dump from prod and load into staging in one step."""
    with tempfile.NamedTemporaryFile(suffix=".sql", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        dump(source_url, days, tmp_path)
        load(target_url, tmp_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Create a partial DB dump for staging (Neon).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- dump --
    p_dump = sub.add_parser("dump", help="Dump last N days from prod to a SQL file")
    p_dump.add_argument("--source-url", default=os.getenv("CRAWLDOCTOR_DATABASE_URL"),
                        help="Prod database URL (default: $CRAWLDOCTOR_DATABASE_URL)")
    p_dump.add_argument("--days", type=int, default=15, help="Number of days to include (default: 15)")
    p_dump.add_argument("--out", default="staging_data.sql", help="Output file path")

    # -- load --
    p_load = sub.add_parser("load", help="Load a dump file into staging DB")
    p_load.add_argument("--target-url", default=os.getenv("CRAWLDOCTOR_STAGING_DATABASE_URL"),
                        help="Staging database URL (default: $CRAWLDOCTOR_STAGING_DATABASE_URL)")
    p_load.add_argument("--file", required=True, help="Dump file to load")

    # -- sync --
    p_sync = sub.add_parser("sync", help="Dump from prod and load into staging in one shot")
    p_sync.add_argument("--source-url", default=os.getenv("CRAWLDOCTOR_DATABASE_URL"),
                        help="Prod database URL")
    p_sync.add_argument("--target-url", default=os.getenv("CRAWLDOCTOR_STAGING_DATABASE_URL"),
                        help="Staging database URL")
    p_sync.add_argument("--days", type=int, default=15, help="Number of days to include")

    args = parser.parse_args()

    if args.command == "dump":
        if not args.source_url:
            parser.error("--source-url required (or set CRAWLDOCTOR_DATABASE_URL)")
        dump(args.source_url, args.days, args.out)

    elif args.command == "load":
        if not args.target_url:
            parser.error("--target-url required (or set CRAWLDOCTOR_STAGING_DATABASE_URL)")
        load(args.target_url, args.file)

    elif args.command == "sync":
        if not args.source_url:
            parser.error("--source-url required (or set CRAWLDOCTOR_DATABASE_URL)")
        if not args.target_url:
            parser.error("--target-url required (or set CRAWLDOCTOR_STAGING_DATABASE_URL)")
        sync(args.source_url, args.target_url, args.days)


if __name__ == "__main__":
    main()
