"""Backfill service to reconstruct journey-based sessions from historical data.

Reads visits and visit_events (>= 2026-02-01), groups them into journeys
using the same internal/external entry logic as the live tracker, then
rewrites session_id values and rebuilds visit_sessions rows.

Usage:
    from app.services.session_backfill import SessionBackfillService
    svc = SessionBackfillService()
    result = svc.backfill(db, dry_run=True)   # preview
    result = svc.backfill(db, dry_run=False)  # apply
"""

import hashlib
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from sqlalchemy import text, func
from sqlalchemy.orm import Session
import structlog

from app.models.visit import Visit, VisitSession, VisitEvent
from app.utils.domains import is_internal_domain

logger = structlog.get_logger()

# Records before this date are left untouched.
BACKFILL_START = datetime(2026, 2, 1, tzinfo=timezone.utc)

# When two events from different cid families are within this window and
# the transition looks internal, they may be stitched into one journey.
CROSS_FAMILY_MAX_GAP = timedelta(minutes=5)


class SessionBackfillService:
    """Reconstruct journey-based session IDs from historical visit data."""

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def backfill(
        self,
        db: Session,
        dry_run: bool = True,
        batch_size: int = 500,
    ) -> Dict:
        """Run the full backfill pipeline.

        Args:
            db: SQLAlchemy session.
            dry_run: If True, compute new sessions but do NOT write changes.
            batch_size: Commit interval for writes.

        Returns a summary dict.
        """
        logger.info("Session backfill starting", dry_run=dry_run)
        try:
            db.execute(text("SET statement_timeout = 600000"))  # 10 min
            db.execute(text("SET work_mem = '256MB'"))
        except Exception:
            pass

        # Step 1+2: Load and sort records by user identity + time
        user_records = self._load_records(db)
        logger.info("Loaded users for backfill", user_count=len(user_records))

        # Step 3-4: Assign journey groups and detect session boundaries
        migration_plan = self._compute_migration_plan(user_records)

        stats = {
            "users_processed": len(user_records),
            "sessions_before": migration_plan["sessions_before"],
            "sessions_after": migration_plan["sessions_after"],
            "visits_remapped": migration_plan["visits_remapped"],
            "audit_rows": len(migration_plan["audit_log"]),
            "dry_run": dry_run,
        }
        logger.info("Backfill plan computed", **stats)

        if dry_run:
            return stats

        # Step 5-6: Apply changes
        self._apply_migration(db, migration_plan, batch_size)
        stats["applied"] = True
        logger.info("Session backfill complete", **stats)
        return stats

    # ------------------------------------------------------------------
    # Step 1+2: Load records grouped by user identity, sorted by time
    # ------------------------------------------------------------------

    # How wide each time-range page is when loading records.  Keeps
    # individual queries small so the remote Postgres doesn't OOM.
    _LOAD_WINDOW = timedelta(days=7)

    def _load_records(self, db: Session) -> Dict[str, List[dict]]:
        """Load visits + events since BACKFILL_START, grouped by best user key.

        Returns {user_key: [record, ...]} sorted ascending by timestamp.
        Each record is a lightweight dict (not the ORM object) to keep
        memory manageable on large tables.

        The query is paginated in weekly windows so that no single query
        needs to sort / stream the entire table.
        """
        user_records: Dict[str, List[dict]] = defaultdict(list)

        now = datetime.now(tz=timezone.utc)
        window_start = BACKFILL_START
        window_num = 0

        while window_start < now:
            window_end = min(window_start + self._LOAD_WINDOW, now)
            window_num += 1
            logger.info(
                "Loading visit window",
                window=window_num,
                start=window_start.isoformat(),
                end=window_end.isoformat(),
            )

            visits = (
                db.query(
                    Visit.id,
                    Visit.session_id,
                    Visit.client_id,
                    Visit.ip_address,
                    Visit.user_agent,
                    Visit.timestamp,
                    Visit.page_url,
                    Visit.page_domain,
                    Visit.referrer,
                    Visit.source,
                    Visit.medium,
                    Visit.campaign,
                )
                .filter(Visit.timestamp >= window_start)
                .filter(Visit.timestamp < window_end)
                .order_by(Visit.timestamp.asc())
                .yield_per(500)
            )

            count = 0
            for row in visits:
                rec = {
                    "type": "visit",
                    "id": row.id,
                    "old_session_id": row.session_id,
                    "client_id": row.client_id,
                    "ip_address": row.ip_address,
                    "user_agent": (row.user_agent or "")[:500],
                    "timestamp": row.timestamp,
                    "page_url": row.page_url,
                    "page_domain": row.page_domain,
                    "referrer": row.referrer,
                    "referrer_domain": self._extract_domain(row.referrer),
                    "source": row.source,
                    "medium": row.medium,
                    "campaign": row.campaign,
                }
                key = self._user_key(rec)
                user_records[key].append(rec)
                count += 1

            logger.info("Window loaded", window=window_num, records=count)
            window_start = window_end

        # Sort each user's visits by timestamp
        for key in user_records:
            user_records[key].sort(key=lambda r: r["timestamp"] or datetime.min.replace(tzinfo=timezone.utc))

        return dict(user_records)

    # ------------------------------------------------------------------
    # Step 3+4: Compute new session assignments
    # ------------------------------------------------------------------

    def _compute_migration_plan(self, user_records: Dict[str, List[dict]]) -> Dict:
        """Walk each user's timeline and assign journey-based session IDs.

        Returns a plan dict containing:
          - visit_updates: {visit_id: new_session_id}
          - event_updates: {event_id: new_session_id}
          - new_sessions: {session_id: session_attrs}
          - audit_log: [(old_session_id, new_session_id, client_id, ...)]
          - aggregate stats
        """
        visit_updates: Dict[int, str] = {}
        event_updates: Dict[int, str] = {}
        new_sessions: Dict[str, dict] = {}
        audit_log: List[dict] = []

        old_session_ids: set = set()
        new_session_ids: set = set()

        for user_key, records in user_records.items():
            journey_seq = 0
            current_session_id: Optional[str] = None
            current_session_meta: Optional[dict] = None

            for rec in records:
                old_session_ids.add(rec["old_session_id"])

                is_new_journey = self._should_start_new_journey(rec, current_session_meta)

                if is_new_journey or current_session_id is None:
                    # Start a new journey
                    current_session_id = self._make_session_id(user_key, journey_seq)
                    current_session_meta = {
                        "session_id": current_session_id,
                        "user_key": user_key,
                        "client_id": rec.get("client_id"),
                        "ip_address": rec.get("ip_address"),
                        "user_agent": rec.get("user_agent"),
                        "first_visit": rec["timestamp"],
                        "last_visit": rec["timestamp"],
                        "entry_referrer": rec.get("referrer"),
                        "entry_referrer_domain": rec.get("referrer_domain"),
                        "is_external_entry": is_new_journey,
                        "visit_count": 0,
                    }
                    new_sessions[current_session_id] = current_session_meta
                    journey_seq += 1

                # Update running session metadata — carry forward any
                # fields that were missing when the journey started
                # (e.g. first record was an event without ip/ua).
                current_session_meta["last_visit"] = rec["timestamp"]
                for field in ("client_id", "ip_address", "user_agent"):
                    if rec.get(field) and not current_session_meta.get(field):
                        current_session_meta[field] = rec[field]

                new_session_ids.add(current_session_id)

                # Only record an update if the session_id actually changes
                if rec["old_session_id"] != current_session_id:
                    visit_updates[rec["id"]] = current_session_id
                current_session_meta["visit_count"] += 1

        # Build audit log: one entry per old→new session_id mapping
        old_new_pairs: Dict[Tuple[str, str], dict] = defaultdict(lambda: {"visits": 0, "client_id": None})
        for user_key, records in user_records.items():
            journey_seq = 0
            current_session_id = None
            current_session_meta = None

            for rec in records:
                is_new = self._should_start_new_journey(rec, current_session_meta)
                if is_new or current_session_id is None:
                    current_session_id = self._make_session_id(user_key, journey_seq)
                    current_session_meta = {
                        "entry_referrer_domain": rec.get("referrer_domain"),
                        "last_visit": rec["timestamp"],
                    }
                    journey_seq += 1
                else:
                    current_session_meta["last_visit"] = rec["timestamp"]

                old_sid = rec["old_session_id"]
                if old_sid != current_session_id:
                    pair = old_new_pairs[(old_sid, current_session_id)]
                    pair["client_id"] = rec.get("client_id")
                    pair["visits"] += 1

        audit_log = [
            {
                "old_session_id": old_sid,
                "new_session_id": new_sid,
                "client_id": info["client_id"],
                "visit_count_moved": info["visits"],
                "event_count_moved": 0,
            }
            for (old_sid, new_sid), info in old_new_pairs.items()
        ]

        return {
            "visit_updates": visit_updates,
            "new_sessions": new_sessions,
            "audit_log": audit_log,
            "sessions_before": len(old_session_ids),
            "sessions_after": len(new_session_ids),
            "visits_remapped": len(visit_updates),
        }

    # Event types that can represent a new page entry.
    # Everything else (click, scroll, form_input, heartbeat, visibility, etc.)
    # is always a continuation of an existing page — never a session boundary.
    _ENTRY_EVENT_TYPES = {"page_view", "navigation", "navigate"}

    def _should_start_new_journey(self, rec: dict, current_meta: Optional[dict]) -> bool:
        """Decide whether this historical record starts a new journey.

        Conservative for backfill: only entry-type events (page_view,
        navigation) with clear external signals start a new session.
        All other event types are always continuations.
        """
        if current_meta is None:
            return True  # very first record for this user

        event_type = rec.get("event_type")
        rec_type = rec.get("type")  # "visit" or "event"

        # Visit records (from the visits table) are always page entries
        is_entry_event = rec_type == "visit" or event_type in self._ENTRY_EVENT_TYPES

        # Non-entry events (clicks, scrolls, heartbeats, form_input, etc.)
        # are always continuations — they cannot start a new journey
        if not is_entry_event:
            return False

        referrer_domain = rec.get("referrer_domain")
        source = rec.get("source")
        medium = rec.get("medium")
        campaign = rec.get("campaign")

        # Referrer domain is the strongest signal
        if referrer_domain:
            if is_internal_domain(referrer_domain):
                return False
            return True  # external referrer → new journey

        # No referrer — check source/medium/campaign fallbacks
        if source:
            src_lower = source.lower()
            if is_internal_domain(src_lower):
                return False
            return True

        if medium or campaign:
            return True

        # Zero signal on a page entry — for backfill, default to CONTINUE.
        # Historical data has many page_views with no referrer (direct visits,
        # new tabs, bookmarks). Splitting on every one of these would create
        # more fragmentation, not less. The backfill errs on the side of
        # continuity; the live tracker can be stricter.
        return False

    # ------------------------------------------------------------------
    # Step 5+6: Apply the migration
    # ------------------------------------------------------------------

    def _apply_migration(self, db: Session, plan: Dict, batch_size: int) -> None:
        """Write new session rows, remap visit/event session_ids, log audit."""
        visit_updates = plan["visit_updates"]
        new_sessions = plan["new_sessions"]
        audit_log = plan["audit_log"]

        # 6a. Bulk-insert new session rows via raw SQL (avoids per-row ORM overhead).
        # First, find which session IDs already exist so we can skip them.
        all_new_sids = list(new_sessions.keys())
        logger.info("Creating new session rows", count=len(all_new_sids))

        existing_sids: set = set()
        for i in range(0, len(all_new_sids), 5000):
            chunk = all_new_sids[i:i+5000]
            rows = db.execute(
                text("SELECT id FROM visit_sessions WHERE id = ANY(:ids)"),
                {"ids": chunk},
            ).fetchall()
            existing_sids.update(r[0] for r in rows)
        logger.info("Existing sessions found", count=len(existing_sids))

        # Build flat list of rows to insert
        to_insert = []
        for sid, meta in new_sessions.items():
            if sid in existing_sids:
                continue
            to_insert.append((
                sid,
                meta.get("ip_address") or "unknown",
                (meta.get("user_agent") or "unknown")[:500],
                meta.get("client_id"),
                meta.get("first_visit"),
                meta.get("last_visit"),
                meta.get("visit_count", 0),
                (meta.get("entry_referrer") or "")[:2000] or None,
                meta.get("entry_referrer_domain"),
                meta.get("is_external_entry", True),
            ))

        created = 0
        skipped = 0
        for i in range(0, len(to_insert), batch_size):
            chunk = to_insert[i:i+batch_size]
            params = {}
            values_parts = []
            for j, row in enumerate(chunk):
                keys = [f"id{j}", f"ip{j}", f"ua{j}", f"cid{j}", f"fv{j}", f"lv{j}",
                        f"vc{j}", f"er{j}", f"erd{j}", f"ie{j}"]
                for k, val in zip(keys, row):
                    params[k] = val
                values_parts.append(
                    f"(:{keys[0]}, :{keys[1]}, :{keys[2]}, :{keys[3]}, :{keys[4]}, "
                    f":{keys[5]}, :{keys[6]}, :{keys[7]}, :{keys[8]}, :{keys[9]})"
                )
            sql = (
                "INSERT INTO visit_sessions "
                "(id, ip_address, user_agent, client_id, first_visit, last_visit, "
                "visit_count, entry_referrer, entry_referrer_domain, is_external_entry) "
                f"VALUES {','.join(values_parts)} "
                "ON CONFLICT (id) DO NOTHING"
            )
            try:
                db.execute(text(sql), params)
                db.commit()
                created += len(chunk)
            except Exception as e:
                db.rollback()
                skipped += len(chunk)
                if skipped <= 2500:
                    logger.warning("Batch insert failed", error=str(e)[:200])

            if (i // batch_size) % 20 == 0:
                logger.info("Sessions insert progress", done=min(i + batch_size, len(to_insert)), total=len(to_insert))
        logger.info("Session rows created", created=created, skipped=skipped)

        # 5a. Remap visits — VALUES-join update (constant time per batch,
        # unlike the old CASE approach which degraded as the transaction grew).
        logger.info("Remapping visits", count=len(visit_updates))
        visit_items = list(visit_updates.items())
        for i in range(0, len(visit_items), batch_size):
            chunk = visit_items[i:i+batch_size]
            params = {}
            values_parts = []
            for j, (vid, new_sid) in enumerate(chunk):
                params[f"v{j}"] = vid
                params[f"s{j}"] = new_sid
                values_parts.append(f"(:v{j}, :s{j})")
            sql = (
                f"UPDATE visits SET session_id = _map.new_sid "
                f"FROM (VALUES {','.join(values_parts)}) AS _map(vid, new_sid) "
                f"WHERE visits.id = _map.vid"
            )
            db.execute(text(sql), params)
            db.commit()
            if (i // batch_size) % 20 == 0:
                logger.info("Visits remapped progress", done=min(i + batch_size, len(visit_items)), total=len(visit_items))
        logger.info("Visits remapped done")

        # 5b. Remap events — same VALUES-join approach
        old_to_new_session: Dict[str, str] = {}
        for entry in audit_log:
            old_to_new_session[entry["old_session_id"]] = entry["new_session_id"]

        logger.info("Remapping events by session_id", mappings=len(old_to_new_session))
        mapping_items = list(old_to_new_session.items())
        for i in range(0, len(mapping_items), batch_size):
            chunk = mapping_items[i:i+batch_size]
            params = {}
            values_parts = []
            for j, (old_sid, new_sid) in enumerate(chunk):
                params[f"o{j}"] = old_sid
                params[f"n{j}"] = new_sid
                values_parts.append(f"(:o{j}, :n{j})")
            sql = (
                f"UPDATE visit_events SET session_id = _map.new_sid "
                f"FROM (VALUES {','.join(values_parts)}) AS _map(old_sid, new_sid) "
                f"WHERE visit_events.session_id = _map.old_sid"
            )
            db.execute(text(sql), params)
            db.commit()
            if (i // batch_size) % 20 == 0:
                logger.info("Events remapped progress", done=min(i + batch_size, len(mapping_items)), total=len(mapping_items))
        logger.info("Events remapped done")

        # 6b. Write audit log
        logger.info("Writing audit log", rows=len(audit_log))
        for i in range(0, len(audit_log), batch_size):
            chunk = audit_log[i:i+batch_size]
            params = {}
            values_parts = []
            for j, entry in enumerate(chunk):
                params[f"o{j}"] = entry["old_session_id"]
                params[f"n{j}"] = entry["new_session_id"]
                params[f"c{j}"] = entry.get("client_id")
                params[f"vc{j}"] = entry.get("visit_count_moved", 0)
                params[f"ec{j}"] = entry.get("event_count_moved", 0)
                values_parts.append(f"(:o{j}, :n{j}, :c{j}, :vc{j}, :ec{j})")
            sql = (
                "INSERT INTO session_id_migration_log "
                "(old_session_id, new_session_id, client_id, visit_count_moved, event_count_moved) "
                f"VALUES {','.join(values_parts)}"
            )
            db.execute(text(sql), params)
            db.commit()
            if (i // batch_size) % 20 == 0:
                logger.info("Audit log progress", done=min(i + batch_size, len(audit_log)), total=len(audit_log))
        logger.info("Audit log done")

        # 7. Delete orphaned session rows — both remapped old sessions and
        #    any sessions with no visits/events (e.g. from dedup race conditions)
        old_sids_to_delete = [
            entry["old_session_id"]
            for entry in audit_log
            if entry["old_session_id"] != entry["new_session_id"]
        ]
        logger.info("Cleaning up remapped session rows", count=len(old_sids_to_delete))
        deleted_remapped = 0
        for i in range(0, len(old_sids_to_delete), batch_size):
            chunk = old_sids_to_delete[i:i+batch_size]
            params = {f"s{j}": sid for j, sid in enumerate(chunk)}
            placeholders = ",".join(f":s{j}" for j in range(len(chunk)))
            sql = (
                f"DELETE FROM visit_sessions WHERE id IN ({placeholders}) "
                f"AND NOT EXISTS (SELECT 1 FROM visits WHERE session_id = visit_sessions.id) "
                f"AND NOT EXISTS (SELECT 1 FROM visit_events WHERE session_id = visit_sessions.id)"
            )
            result = db.execute(text(sql), params)
            deleted_remapped += result.rowcount
            db.commit()
        logger.info("Remapped session rows deleted", deleted=deleted_remapped,
                     skipped=len(old_sids_to_delete) - deleted_remapped)

        # 7b. Clean up any other orphaned sessions (visit_count=0, no references)
        result = db.execute(text(
            "DELETE FROM visit_sessions "
            "WHERE visit_count = 0 "
            "AND NOT EXISTS (SELECT 1 FROM visits WHERE session_id = visit_sessions.id) "
            "AND NOT EXISTS (SELECT 1 FROM visit_events WHERE session_id = visit_sessions.id)"
        ))
        deleted_orphans = result.rowcount
        db.commit()
        logger.info("Orphaned session rows deleted", deleted=deleted_orphans)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _user_key(rec: dict) -> str:
        """Best-effort user identity key for grouping.

        Prefers client_id; falls back to ip+ua hash.
        """
        cid = rec.get("client_id")
        if cid:
            return f"cid:{cid}"
        ip = rec.get("ip_address", "")
        ua = (rec.get("user_agent") or "")[:500]
        return f"ipua:{ip}:{ua}"

    @staticmethod
    def _make_session_id(user_key: str, journey_seq: int) -> str:
        """Deterministic session ID from user key + journey sequence.

        Same scheme as TrackingService._generate_session_id.
        """
        data = f"{user_key}:journey:{journey_seq}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]

    @staticmethod
    def _extract_domain(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        try:
            return urlparse(url).netloc or None
        except Exception:
            return None
