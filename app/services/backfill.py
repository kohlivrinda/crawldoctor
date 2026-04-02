"""Backfill service for journey and lead summaries (form-fill based, any path)."""
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
import structlog
import json
import re

from app.models.summary import LeadSummary, JourneySummary, JourneyFormFill
from app.models.visit import Visit, VisitEvent
from app.services.analytics import is_real_form_submit

logger = structlog.get_logger()


class BackfillService:
    """Backfill journey_summaries and journey_form_fills from real form submissions (any path)."""

    def backfill_all(self, db: Session, days: int = 90):
        """Rebuild journey_summaries and journey_form_fills for users with at least one real form fill."""
        logger.info("Starting form-fill journey backfill", days=days)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        try:
            db.execute(text("SET statement_timeout = 300000"))
        except Exception:
            pass

        # 1. All form_submit events in range
        form_events = (
            db.query(VisitEvent)
            .filter(
                VisitEvent.event_type == "form_submit",
                VisitEvent.timestamp >= cutoff,
                VisitEvent.client_id.isnot(None),
                VisitEvent.client_id != "",
            )
            .order_by(VisitEvent.timestamp.asc())
            .all()
        )

        # 2. Keep only real form submits (not RUM/analytics junk)
        real_events = [e for e in form_events if is_real_form_submit(e.event_data)]
        client_ids = list({e.client_id for e in real_events if e.client_id})
        logger.info("Form-fill backfill: real form submits", total_events=len(real_events), unique_clients=len(client_ids))

        # 3. Clear pre-computed data only for clients we're about to rebuild
        if client_ids:
            db.query(JourneyFormFill).filter(JourneyFormFill.client_id.in_(client_ids)).delete(synchronize_session=False)
            db.query(JourneySummary).filter(JourneySummary.client_id.in_(client_ids)).delete(synchronize_session=False)
            db.query(LeadSummary).filter(LeadSummary.client_id.in_(client_ids)).delete(synchronize_session=False)
            db.flush()

        journeys_added = 0
        leads_added = 0
        form_fills_added = 0

        for client_id in client_ids:
            try:
                visits = (
                    db.query(Visit)
                    .filter(Visit.client_id == client_id, Visit.timestamp >= cutoff)
                    .order_by(Visit.timestamp.asc())
                    .all()
                )
                client_form_events = [e for e in real_events if e.client_id == client_id]

                if not visits and not client_form_events:
                    continue

                # First/last touch from visits and form events
                all_ts = [v.timestamp for v in visits] + [e.timestamp for e in client_form_events]
                first_seen = min(all_ts)
                last_seen = max(all_ts)
                first_visit = visits[0] if visits else None
                last_visit = visits[-1] if visits else None

                # Path sequence: chronological unique paths from visits
                path_list = []
                last_path = None
                for v in visits:
                    p = (v.path or "").strip()
                    if p != last_path:
                        path_list.append(p or "(page)")
                        last_path = p
                path_sequence = " → ".join(path_list) if path_list else None

                # JourneySummary
                journey = JourneySummary(
                    client_id=client_id,
                    first_seen=first_seen,
                    last_seen=last_seen,
                    visit_count=len(visits),
                    entry_page=first_visit.page_url if first_visit else (client_form_events[0].page_url if client_form_events else None),
                    exit_page=last_visit.page_url if last_visit else (client_form_events[-1].page_url if client_form_events else None),
                    path_sequence=path_sequence,
                    email=None,
                    name=None,
                    has_captured_data=1,
                    form_fill_count=len(client_form_events),
                    source=first_visit.source if first_visit else None,
                    medium=first_visit.medium if first_visit else None,
                    campaign=first_visit.campaign if first_visit else None,
                )
                db.add(journey)
                journeys_added += 1

                # First form fill for email/name (list display)
                first_ev = client_form_events[0]
                ed = first_ev.event_data or {}
                form_vals = ed.get("form_values") or ed.get("values") or {}
                email, name = self._extract_profile_from_values(form_vals)
                if email or name:
                    journey.email = email
                    journey.name = name

                # JourneyFormFill: one row per real form submit (multiple preserved)
                for ev in client_form_events:
                    ed = ev.event_data or {}
                    form_vals = ed.get("form_values") or ed.get("values") or {}
                    filled = ed.get("filled_fields")
                    if filled is None and isinstance(form_vals, dict):
                        filled = len(form_vals)
                    jff = JourneyFormFill(
                        client_id=client_id,
                        visit_event_id=ev.id,
                        timestamp=ev.timestamp,
                        page_url=ev.page_url,
                        path=ev.path,
                        form_values=form_vals if isinstance(form_vals, dict) else None,
                        filled_fields=filled,
                        form_id=ed.get("id"),
                        form_action=ed.get("action"),
                    )
                    db.add(jff)
                    form_fills_added += 1

                # LeadSummary: first capture only (backwards compat)
                if client_form_events:
                    first_ev = client_form_events[0]
                    ed = first_ev.event_data or {}
                    form_vals = ed.get("form_values") or ed.get("values") or {}
                    form_data_shared = " | ".join([f"{k}: {v}" for k, v in (form_vals or {}).items() if v])
                    lead = LeadSummary(
                        client_id=client_id,
                        email=email,
                        name=name,
                        captured_at=first_ev.timestamp,
                        captured_page=first_ev.page_url,
                        captured_path=first_ev.path,
                        form_data_shared=form_data_shared or None,
                        captured_data=json.dumps(form_vals)[:5000] if form_vals else None,
                        source=journey.source,
                        medium=journey.medium,
                        campaign=journey.campaign,
                        first_referrer=first_visit.referrer if first_visit else None,
                        first_referrer_domain=self._extract_domain(first_visit.referrer) if first_visit and first_visit.referrer else None,
                        first_seen=first_seen,
                        last_seen=last_seen,
                    )
                    db.add(lead)
                    leads_added += 1

            except Exception as e:
                logger.warning("Backfill skip client", client_id=client_id, error=str(e))
                continue

        db.commit()
        logger.info(
            "Backfill completed (form-fill journeys)",
            journeys=journeys_added,
            leads=leads_added,
            form_fills=form_fills_added,
        )
        return {"journeys": journeys_added, "leads": leads_added, "form_fills": form_fills_added}

    def _extract_domain(self, url: str):
        if not url:
            return None
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc or None
        except Exception:
            return None

    def _extract_profile_from_values(self, form_vals: dict):
        """Extract email and name from form_values dict."""
        if not form_vals or not isinstance(form_vals, dict):
            return None, None
        email = None
        name = None
        for k, v in form_vals.items():
            if v is None or str(v).strip() == "":
                continue
            kl = str(k).lower()
            if not email and ("email" in kl or "mail" in kl) and "@" in str(v):
                email = str(v).strip()
            if not name and ("name" in kl or "user" in kl or "full" in kl):
                name = str(v).strip()
        return email, name

    def _extract_profile(self, data_str: str):
        """Legacy: extract email/name from aggregated string (for LeadSummary form_data_shared)."""
        if not data_str or data_str == "No info shared":
            return None, None
        email = None
        name = None
        try:
            if data_str.strip().startswith("{") or data_str.strip().startswith("["):
                clean_json = data_str.split(" | ")[0].split(": ", 1)[-1] if ": {" in data_str else data_str
                parsed = json.loads(clean_json)
                if isinstance(parsed, dict):
                    vals = parsed.get("form_values") or parsed.get("values") or parsed
                    return self._extract_profile_from_values(vals if isinstance(vals, dict) else {})
        except Exception:
            pass
        if not email:
            m = re.search(r'["\']?email["\']?\s*[:=]\s*["\']?([^"\'\,\|\s]+@[^"\'\,\|\s]+)["\']?', data_str, re.I)
            if m:
                email = m.group(1)
        if not name:
            m = re.search(r'["\']?name["\']?\s*[:=]\s*["\']?([^"\'|]+)["\']?', data_str, re.I)
            if m:
                val = m.group(1).strip()
                if val.lower() not in ("null", "undefined", "unknown", "", "[object object]"):
                    name = val
        return email, name
