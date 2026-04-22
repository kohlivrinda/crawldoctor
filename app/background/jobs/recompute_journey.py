"""Recompute journey/lead summaries for a single client_id."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
import structlog

from app.background.registry import registry
from app.models.summary import JourneyFormFill, JourneySummary, LeadSummary
from app.models.visit import Visit, VisitEvent
from app.services.analytics import is_real_form_submit

logger = structlog.get_logger()


def _extract_profile_from_values(form_vals: Optional[Dict]) -> tuple:
    """Extract email and name from a form_values dict."""
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


def _extract_domain(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc or None
    except Exception:
        return None


@registry.job("recompute_journey", sweep_interval_minutes=5)
class RecomputeJourney:
    """Full recompute of summary tables for one client_id.

    Sweep: finds client_ids with form_submit events since the watermark.
    Handle: rebuilds JourneySummary + JourneyFormFill + LeadSummary for that client.
    """

    def sweep(self, db: Session, since: datetime) -> List[Dict[str, Any]]:
        """Return payloads for client_ids with new real form_submit events since `since`."""
        events = (
            db.query(VisitEvent.client_id)
            .filter(
                VisitEvent.event_type == "form_submit",
                VisitEvent.timestamp > since,
                VisitEvent.client_id.isnot(None),
                VisitEvent.client_id != "",
            )
            .distinct()
            .all()
        )

        # Filter to real form submits — need event_data to check
        real_client_ids = set()
        for (client_id,) in events:
            # Check if this client has any real form submit since watermark
            form_events = (
                db.query(VisitEvent)
                .filter(
                    VisitEvent.client_id == client_id,
                    VisitEvent.event_type == "form_submit",
                    VisitEvent.timestamp > since,
                )
                .all()
            )
            for ev in form_events:
                if is_real_form_submit(ev.event_data):
                    real_client_ids.add(client_id)
                    break

        return [{"client_id": cid} for cid in real_client_ids]

    def handle(self, db: Session, payload: Dict[str, Any]) -> None:
        """Full recompute of summaries for one client_id."""
        client_id = payload["client_id"]

        # Fetch all visits for this client
        visits = (
            db.query(Visit)
            .filter(Visit.client_id == client_id)
            .order_by(Visit.timestamp.asc())
            .all()
        )

        # Fetch all real form_submit events for this client (no time bound — full recompute)
        form_events = (
            db.query(VisitEvent)
            .filter(
                VisitEvent.client_id == client_id,
                VisitEvent.event_type == "form_submit",
                VisitEvent.client_id.isnot(None),
            )
            .order_by(VisitEvent.timestamp.asc())
            .all()
        )
        real_events = [e for e in form_events if is_real_form_submit(e.event_data)]

        if not visits and not real_events:
            # Nothing to summarize — clean up any stale rows
            db.query(JourneyFormFill).filter(JourneyFormFill.client_id == client_id).delete()
            db.query(JourneySummary).filter(JourneySummary.client_id == client_id).delete()
            db.query(LeadSummary).filter(LeadSummary.client_id == client_id).delete()
            db.commit()
            return

        # If there are no real form events, this client shouldn't have summaries
        if not real_events:
            db.query(JourneyFormFill).filter(JourneyFormFill.client_id == client_id).delete()
            db.query(JourneySummary).filter(JourneySummary.client_id == client_id).delete()
            db.query(LeadSummary).filter(LeadSummary.client_id == client_id).delete()
            db.commit()
            return

        # --- Timestamps ---
        all_ts = [v.timestamp for v in visits] + [e.timestamp for e in real_events]
        first_seen = min(all_ts)
        last_seen = max(all_ts)
        first_visit = visits[0] if visits else None
        last_visit = visits[-1] if visits else None

        # --- Path sequence ---
        path_list = []
        last_path = None
        for v in visits:
            p = (v.path or "").strip()
            if p != last_path:
                path_list.append(p or "(page)")
                last_path = p
        path_sequence = " \u2192 ".join(path_list) if path_list else None

        # --- Email / name from first form fill ---
        first_ev = real_events[0]
        ed = first_ev.event_data or {}
        form_vals = ed.get("form_values") or ed.get("values") or {}
        email, name = _extract_profile_from_values(form_vals)

        # --- Delete old JourneyFormFill rows for this client ---
        db.query(JourneyFormFill).filter(JourneyFormFill.client_id == client_id).delete()

        # --- Upsert JourneySummary ---
        journey = db.query(JourneySummary).filter(JourneySummary.client_id == client_id).first()
        if journey:
            journey.first_seen = first_seen
            journey.last_seen = last_seen
            journey.visit_count = len(visits)
            journey.entry_page = first_visit.page_url if first_visit else (real_events[0].page_url if real_events else None)
            journey.exit_page = last_visit.page_url if last_visit else (real_events[-1].page_url if real_events else None)
            journey.path_sequence = path_sequence
            journey.email = email
            journey.name = name
            journey.has_captured_data = 1
            journey.form_fill_count = len(real_events)
            journey.source = first_visit.source if first_visit else None
            journey.medium = first_visit.medium if first_visit else None
            journey.campaign = first_visit.campaign if first_visit else None
        else:
            journey = JourneySummary(
                client_id=client_id,
                first_seen=first_seen,
                last_seen=last_seen,
                visit_count=len(visits),
                entry_page=first_visit.page_url if first_visit else (real_events[0].page_url if real_events else None),
                exit_page=last_visit.page_url if last_visit else (real_events[-1].page_url if real_events else None),
                path_sequence=path_sequence,
                email=email,
                name=name,
                has_captured_data=1,
                form_fill_count=len(real_events),
                source=first_visit.source if first_visit else None,
                medium=first_visit.medium if first_visit else None,
                campaign=first_visit.campaign if first_visit else None,
            )
            db.add(journey)

        # --- Insert JourneyFormFill rows ---
        for ev in real_events:
            ed = ev.event_data or {}
            fv = ed.get("form_values") or ed.get("values") or {}
            filled = ed.get("filled_fields")
            if filled is None and isinstance(fv, dict):
                filled = len(fv)
            db.add(JourneyFormFill(
                client_id=client_id,
                visit_event_id=ev.id,
                timestamp=ev.timestamp,
                page_url=ev.page_url,
                path=ev.path,
                form_values=fv if isinstance(fv, dict) else None,
                filled_fields=filled,
                form_id=ed.get("id"),
                form_action=ed.get("action"),
            ))

        # --- Upsert LeadSummary (first form fill, backwards compat) ---
        lead = db.query(LeadSummary).filter(LeadSummary.client_id == client_id).first()
        first_form_vals = (first_ev.event_data or {}).get("form_values") or (first_ev.event_data or {}).get("values") or {}
        form_data_shared = " | ".join(
            [f"{k}: {v}" for k, v in (first_form_vals or {}).items() if v]
        )

        if lead:
            lead.email = email
            lead.name = name
            lead.captured_at = first_ev.timestamp
            lead.captured_page = first_ev.page_url
            lead.captured_path = first_ev.path
            lead.form_data_shared = form_data_shared or None
            lead.captured_data = json.dumps(first_form_vals)[:5000] if first_form_vals else None
            lead.source = journey.source
            lead.medium = journey.medium
            lead.campaign = journey.campaign
            lead.first_referrer = first_visit.referrer if first_visit else None
            lead.first_referrer_domain = _extract_domain(first_visit.referrer) if first_visit and first_visit.referrer else None
            lead.first_seen = first_seen
            lead.last_seen = last_seen
        else:
            lead = LeadSummary(
                client_id=client_id,
                email=email,
                name=name,
                captured_at=first_ev.timestamp,
                captured_page=first_ev.page_url,
                captured_path=first_ev.path,
                form_data_shared=form_data_shared or None,
                captured_data=json.dumps(first_form_vals)[:5000] if first_form_vals else None,
                source=journey.source,
                medium=journey.medium,
                campaign=journey.campaign,
                first_referrer=first_visit.referrer if first_visit else None,
                first_referrer_domain=_extract_domain(first_visit.referrer) if first_visit and first_visit.referrer else None,
                first_seen=first_seen,
                last_seen=last_seen,
            )
            db.add(lead)

        db.commit()
        logger.info(
            "Recomputed journey",
            client_id=client_id,
            visits=len(visits),
            form_fills=len(real_events),
        )
