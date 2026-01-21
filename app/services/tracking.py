"""Simplified tracking service for logging ALL visits with automatic categorization."""
import hashlib
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs
from sqlalchemy.orm import Session
from sqlalchemy import func
import structlog

from app.models.visit import Visit, VisitSession, VisitEvent
from app.models.summary import LeadSummary, JourneySummary
from app.services.crawler_detection import CrawlerDetectionService, CrawlerDetectionResult
from app.services.event_batcher import event_batcher
from app.services.geo import GeoLocationService

logger = structlog.get_logger()


class TrackingService:
    """Simplified service for tracking ALL visits with automatic categorization."""
    
    def __init__(self):
        self.crawler_detector = CrawlerDetectionService()
        self.geo_service = GeoLocationService()
        
    def _generate_session_id(
        self,
        ip_address: str,
        user_agent: str,
        page_domain: Optional[str] = None,
        client_id: Optional[str] = None
    ) -> str:
        """Generate a privacy-friendly session ID.

        Priority:
        - If client_id is provided (from first-party storage), prefer it
        - Include page_domain to avoid cross-site collisions
        - Rotate daily to bound session lifetime
        """
        day_bucket = datetime.now(timezone.utc).date().isoformat()
        if client_id:
            session_data = f"cid:{client_id}:d:{page_domain or ''}:day:{day_bucket}"
        else:
            session_data = f"ipua:{ip_address}:{user_agent}:d:{page_domain or ''}:day:{day_bucket}"
        return hashlib.sha256(session_data.encode()).hexdigest()[:32]
    
    def _extract_page_info(self, url: str) -> Dict[str, Any]:
        """Extract information from page URL."""
        if not url:
            return {}
        
        try:
            parsed = urlparse(url)
            return {
                "domain": parsed.netloc,
                "protocol": parsed.scheme,
                "port": parsed.port,
                "path": parsed.path,
                "query_params": dict(parse_qs(parsed.query))
            }
        except Exception:
            return {}

    def _extract_utm(self, page_info: Dict[str, Any], referrer: Optional[str]) -> Dict[str, Optional[str]]:
        """Extract UTM/source information from URL or referrer."""
        utm = {"source": None, "medium": None, "campaign": None}
        try:
            q = page_info.get("query_params") or {}
            if isinstance(q, dict):
                # parse_qs returns lists
                utm["source"] = (q.get("utm_source") or q.get("source") or q.get("ref") or [None])[0]
                utm["medium"] = (q.get("utm_medium") or [None])[0]
                utm["campaign"] = (q.get("utm_campaign") or q.get("campaign") or [None])[0]
        except Exception:
            pass
        # Derive source from referrer domain if utm_source missing
        if not utm["source"] and referrer:
            try:
                r = urlparse(referrer)
                utm["source"] = r.netloc
            except Exception:
                pass
        return utm
    
    def _categorize_visitor(self, user_agent: str) -> Dict[str, Any]:
        """Categorize visitor based on user agent - LOG ALL REQUESTS."""
        detection_result = self.crawler_detector.detect_crawler(user_agent)
        
        # Enhanced categorization
        category = "unknown"
        if "gpt" in user_agent.lower() or "openai" in user_agent.lower():
            category = "chatgpt"
        elif "claude" in user_agent.lower() or "anthropic" in user_agent.lower():
            category = "claude"
        elif "perplexity" in user_agent.lower():
            category = "perplexity"
        elif "google" in user_agent.lower() and "ai" in user_agent.lower():
            category = "google_ai"
        elif "bot" in user_agent.lower():
            category = "bot"
        elif "mobile" in user_agent.lower():
            category = "mobile_human"
        elif "mozilla" in user_agent.lower() or "chrome" in user_agent.lower() or "safari" in user_agent.lower():
            category = "desktop_human"
        else:
            category = "other"
        
        return {
            "category": category,
            "is_crawler": detection_result.is_crawler,
            "crawler_name": detection_result.crawler_name,
            "confidence": detection_result.confidence_score,
            "detection_method": detection_result.detection_method
        }
    
    async def track_visit(
        self,
        db: Session,
        ip_address: str,
        user_agent: str,
        page_url: Optional[str] = None,
        referrer: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        tracking_id: Optional[str] = None,
        custom_data: Optional[Dict[str, Any]] = None,
        client_id: Optional[str] = None,
        client_side_data: Optional[Dict[str, Any]] = None,
    ) -> Visit:
        """Track ANY visit with automatic categorization."""
        
        # Extract page information
        page_info = self._extract_page_info(page_url or "")

        # Generate session ID (domain-aware; prefer CID when provided)
        session_id = self._generate_session_id(ip_address, user_agent, page_info.get("domain"), client_id)
        
        # Get or create session
        session = db.query(VisitSession).filter(VisitSession.id == session_id).first()
        if not session:
            session = VisitSession(
                id=session_id,
                ip_address=ip_address,
                user_agent=user_agent[:500],
                client_id=client_id,  # Store client_id for unified tracking
                first_visit=datetime.now(timezone.utc),
                last_visit=datetime.now(timezone.utc),
                visit_count=0
            )
            # Add client-side data to session if provided
            if client_side_data:
                session.client_side_timezone = client_side_data.get('timezone')
                session.client_side_language = client_side_data.get('language')
                session.client_side_screen_resolution = client_side_data.get('screen_resolution')
                session.client_side_viewport_size = client_side_data.get('viewport_size')
                session.client_side_device_memory = client_side_data.get('device_memory')
                session.client_side_connection_type = client_side_data.get('connection_type')
            db.add(session)
        else:
            # Update client_id if provided and not already set
            if client_id and not session.client_id:
                session.client_id = client_id
            # Update client-side data if provided and not already set
            if client_side_data:
                if not session.client_side_timezone:
                    session.client_side_timezone = client_side_data.get('timezone')
                if not session.client_side_language:
                    session.client_side_language = client_side_data.get('language')
                if not session.client_side_screen_resolution:
                    session.client_side_screen_resolution = client_side_data.get('screen_resolution')
                if not session.client_side_viewport_size:
                    session.client_side_viewport_size = client_side_data.get('viewport_size')
                if not session.client_side_device_memory:
                    session.client_side_device_memory = client_side_data.get('device_memory')
                if not session.client_side_connection_type:
                    session.client_side_connection_type = client_side_data.get('connection_type')
        
        # Update session (only update last_visit here; increment visit_count only on new visit)
        session.last_visit = datetime.now(timezone.utc)
        db.add(session)
        
        # Categorize visitor
        visitor_info = self._categorize_visitor(user_agent)
        
        # Extract UTM/source information
        utm_info = self._extract_utm(page_info, referrer)
        
        # Get geographic information
        # Prioritize humans in geo budget; defer bots
        geo_info = await self.geo_service.get_location_info(ip_address, category="bot" if visitor_info.get("is_crawler") else "human")

        # Populate session geo fields if missing (only if geo_info available and human prioritized)
        try:
            if geo_info:
                if not session.country:
                    session.country = geo_info.get("country_code")
                if not session.country_name:
                    session.country_name = geo_info.get("country_name")
                if not session.city:
                    session.city = geo_info.get("city")
                if session.latitude is None:
                    session.latitude = geo_info.get("latitude")
                if session.longitude is None:
                    session.longitude = geo_info.get("longitude")
                if not session.timezone:
                    session.timezone = geo_info.get("timezone")
                if not session.isp:
                    session.isp = geo_info.get("isp")
                if not session.organization:
                    session.organization = geo_info.get("organization")
                if not session.asn:
                    session.asn = geo_info.get("asn")
        except Exception:
            pass
        
        # Deduplicate: if a recent visit with same session/page exists, reuse it
        existing_visit: Optional[Visit] = None
        try:
            if page_url:
                # Consider duplicates within the last 30 seconds
                cutoff_ts = datetime.now(timezone.utc) - timedelta(seconds=30)
                existing_visit = (
                    db.query(Visit)
                    .filter(
                        Visit.session_id == session_id,
                        Visit.page_url == page_url,
                        Visit.timestamp >= cutoff_ts,
                    )
                    .order_by(Visit.timestamp.desc())
                    .first()
                )
        except Exception:
            existing_visit = None

        if existing_visit:
            # Optionally merge custom headers/data to existing record and return it
            updated = False
            if headers:
                try:
                    merged = existing_visit.request_headers or {}
                    merged.update({k: v for k, v in (headers or {}).items() if k not in merged})
                    existing_visit.request_headers = merged
                    updated = True
                except Exception:
                    pass
            if custom_data:
                try:
                    rh = existing_visit.request_headers or {}
                    custom = rh.get("custom_data", {})
                    if isinstance(custom, dict):
                        custom.update(custom_data)
                    else:
                        custom = custom_data
                    rh["custom_data"] = custom
                    existing_visit.request_headers = rh
                    updated = True
                except Exception:
                    pass
            if tracking_id and not existing_visit.tracking_id:
                existing_visit.tracking_id = tracking_id
                updated = True
            try:
                # Always commit to persist session last_visit and any visit updates
                if updated:
                    db.add(existing_visit)
                db.commit()
                if updated:
                    db.refresh(existing_visit)
            except Exception:
                db.rollback()
            return existing_visit

        # Populate visit geo from session (inherit from session if available)
        visit_country = geo_info.get("country_code") if geo_info else None
        visit_city = geo_info.get("city") if geo_info else None
        
        # If geo_info not available, try to get from session
        if not visit_country and session:
            visit_country = session.country
            visit_city = session.city
        
        visit = Visit(
            session_id=session_id,
            client_id=client_id,  # Store client_id for unified tracking
            ip_address=ip_address,
            user_agent=user_agent[:1000],
            page_url=page_url[:2000] if page_url else None,
            referrer=referrer[:2000] if referrer else None,
            page_domain=page_info.get("domain"),
            crawler_type=visitor_info["crawler_name"],
            crawler_confidence=visitor_info["confidence"],
            is_bot=visitor_info["is_crawler"],
            request_headers=headers if headers else {},
            country=visit_country,
            city=visit_city,
            tracking_id=tracking_id,
            source=utm_info.get("source"),
            medium=utm_info.get("medium"),
            campaign=utm_info.get("campaign"),
            protocol=page_info.get("protocol"),
            port=page_info.get("port"),
            path=page_info.get("path"),
            query_params=page_info.get("query_params", {}),
            # Client-side data fields
            client_side_timezone=client_side_data.get('timezone') if client_side_data else None,
            client_side_language=client_side_data.get('language') if client_side_data else None,
            client_side_screen_resolution=client_side_data.get('screen_resolution') if client_side_data else None,
            client_side_viewport_size=client_side_data.get('viewport_size') if client_side_data else None,
            client_side_device_memory=client_side_data.get('device_memory') if client_side_data else None,
            client_side_connection_type=client_side_data.get('connection_type') if client_side_data else None
        )
        
        # Add custom data if provided
        if custom_data:
            visit.request_headers.update({"custom_data": custom_data})
        
        db.add(visit)
        # Increment session visit_count only when creating a new visit
        try:
            session.visit_count = (session.visit_count or 0) + 1
            db.add(session)
        except Exception:
            pass
        db.commit()
        db.refresh(visit)
        
        # Real-time summary update
        if client_id:
            try:
                self._update_summaries(db, client_id)
            except Exception as e:
                logger.error("Failed to update summaries", client_id=client_id, error=str(e))

        # Log the visit with full details
        logger.info(
            "Visit tracked",
            visit_id=visit.id,
            category=visitor_info["category"],
            user_agent=user_agent[:200],  # Truncate for logging
            ip_address=ip_address,
            page_url=page_url,
            is_crawler=visitor_info["is_crawler"],
            crawler_name=visitor_info["crawler_name"],
            confidence=visitor_info["confidence"]
        )
        
        return visit

    async def track_event(
        self,
        db: Session,
        ip_address: str,
        user_agent: str,
        event_type: str,
        page_url: Optional[str] = None,
        referrer: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        visit_id: Optional[int] = None,
        tracking_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_side_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Track fine-grained events like scroll, click, navigation."""
        page_info = self._extract_page_info(page_url or "")
        page_domain = page_info.get("domain")
        referrer_domain = None
        if referrer:
            try:
                referrer_domain = urlparse(referrer).netloc
            except Exception:
                referrer_domain = None

        # Session derived from client_id when available to avoid NAT collisions
        session_id = self._generate_session_id(ip_address, user_agent, page_domain, client_id)

        # Categorize and geo for event context
        visitor_info = self._categorize_visitor(user_agent)
        geo_info = await self.geo_service.get_location_info(ip_address, category="bot" if visitor_info.get("is_crawler") else "human")
        utm_info = self._extract_utm(page_info, referrer)

        # Define effective_session_id early - it will be refined later if needed
        effective_session_id = session_id
        
        # If visit_id not provided, try to link to the most recent matching visit
        linked_visit: Optional[Visit] = None
        if visit_id:
            linked_visit = db.query(Visit).filter(Visit.id == visit_id).first()
        if not linked_visit and page_url:
            # First attempt: find a visit created moments ago for this IP/page regardless of session
            try:
                cutoff = datetime.now(timezone.utc) - timedelta(seconds=90)
                candidate = (
                    db.query(Visit)
                    .filter(
                        Visit.page_url == page_url,
                        Visit.ip_address == ip_address,
                        Visit.timestamp >= cutoff,
                    )
                    .order_by(Visit.timestamp.desc())
                    .first()
                )
                if candidate:
                    linked_visit = candidate
            except Exception:
                linked_visit = None
            # Fallback: find by previous IP/UA session id and page
            if not linked_visit:
                try:
                    previous_session_id = self._generate_session_id(ip_address, user_agent, page_domain)
                    linked_visit = (
                        db.query(Visit)
                        .filter(Visit.session_id == previous_session_id, Visit.page_url == page_url)
                        .order_by(Visit.timestamp.desc())
                        .first()
                    )
                except Exception:
                    linked_visit = None

        # Ensure a session row exists early (needed for visit creation)
        session_row = db.query(VisitSession).filter(VisitSession.id == effective_session_id).first()
        if not session_row:
            # Try to inherit location from existing sessions for this IP before creating new session
            existing_location = None
            try:
                # Look for recent sessions with this IP that have good location data
                recent_sessions = db.query(VisitSession).filter(
                    VisitSession.ip_address == ip_address,
                    VisitSession.last_visit >= datetime.now(timezone.utc) - timedelta(hours=24)
                ).all()

                for existing_session in recent_sessions:
                    if existing_session.country and existing_session.country != "XX":
                        existing_location = {
                            "country": existing_session.country,
                            "city": existing_session.city,
                            "country_name": existing_session.country_name
                        }
                        break

                # If no good session data, try recent visits
                if not existing_location:
                    recent_visits = db.query(Visit).filter(
                        Visit.ip_address == ip_address,
                        Visit.timestamp >= datetime.now(timezone.utc) - timedelta(hours=24)
                    ).all()

                    for existing_visit in recent_visits:
                        if existing_visit.country and existing_visit.country != "XX":
                            existing_location = {
                                "country": existing_visit.country,
                                "city": existing_visit.city
                            }
                            break
            except Exception:
                pass  # If inheritance fails, continue with geo_info

            # Create new session with inherited location if available
            session_row = VisitSession(
                id=effective_session_id,
                ip_address=ip_address,
                user_agent=user_agent[:500],
                client_id=client_id,  # Store client_id for unified tracking
                first_visit=datetime.now(timezone.utc),
                last_visit=datetime.now(timezone.utc),
                visit_count=0,
                country=existing_location.get("country") if existing_location else None,
                city=existing_location.get("city") if existing_location else None,
                country_name=existing_location.get("country_name") if existing_location else None
            )
            db.add(session_row)
            db.flush()  # Make sure session exists before creating visit
        
        # If still no visit, create one now using the event context (first interaction or new tab)
        if not linked_visit and page_url:
            try:
                base_session_id = effective_session_id

                # Populate visit geo with inheritance logic (same as session creation)
                visit_country = None
                visit_city = None

                # First, try to inherit from existing visits for this IP
                try:
                    recent_visits = db.query(Visit).filter(
                        Visit.ip_address == ip_address,
                        Visit.timestamp >= datetime.now(timezone.utc) - timedelta(hours=24)
                    ).all()

                    for existing_visit in recent_visits:
                        if existing_visit.country and existing_visit.country != "XX":
                            visit_country = existing_visit.country
                            visit_city = existing_visit.city
                            break

                    # If no good visit data, try sessions
                    if not visit_country:
                        recent_sessions = db.query(VisitSession).filter(
                            VisitSession.ip_address == ip_address,
                            VisitSession.last_visit >= datetime.now(timezone.utc) - timedelta(hours=24)
                        ).all()

                        for existing_session in recent_sessions:
                            if existing_session.country and existing_session.country != "XX":
                                visit_country = existing_session.country
                                visit_city = existing_session.city
                                break
                except Exception:
                    pass  # If inheritance fails, continue

                # Final fallback to current geo_info (but avoid "XX")
                if not visit_country and geo_info:
                    if geo_info.get("country_code") and geo_info.get("country_code") != "XX":
                        visit_country = geo_info.get("country_code")
                        visit_city = geo_info.get("city")

                # Try to get session for fallback geo (even if XX, more consistent)
                temp_session = db.query(VisitSession).filter(VisitSession.id == base_session_id).first()
                if not visit_country and temp_session:
                    visit_country = temp_session.country
                    visit_city = temp_session.city
                
                new_visit = Visit(
                    session_id=base_session_id,
                    client_id=client_id,  # Store client_id for unified tracking
                    ip_address=ip_address,
                    user_agent=user_agent[:1000],
                    page_url=page_url[:2000],
                    referrer=referrer[:2000] if referrer else None,
                    page_domain=page_info.get("domain"),
                    crawler_type=visitor_info["crawler_name"],
                    crawler_confidence=visitor_info["confidence"],
                    is_bot=visitor_info["is_crawler"],
                    request_headers={},
                    country=visit_country,
                    city=visit_city,
                    tracking_id=tracking_id,
                    source=utm_info.get("source"),
                    medium=utm_info.get("medium"),
                    campaign=utm_info.get("campaign"),
                    protocol=page_info.get("protocol"),
                    port=page_info.get("port"),
                    path=page_info.get("path"),
                    query_params=page_info.get("query_params", {})
                )
                db.add(new_visit)
                db.commit()
                db.refresh(new_visit)
                linked_visit = new_visit
                try:
                    session_row.visit_count = (session_row.visit_count or 0) + 1
                    db.add(session_row)
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                db.rollback()

        # If we found a linked visit and have a client_id, migrate the visit to the CID-scoped session
        if linked_visit:
            effective_session_id = linked_visit.session_id
            if client_id:
                target_session_id = self._generate_session_id(ip_address, user_agent, page_domain, client_id)
                if linked_visit.session_id != target_session_id:
                    # Ensure target session exists
                    target_session = db.query(VisitSession).filter(VisitSession.id == target_session_id).first()
                    if not target_session:
                        target_session = VisitSession(
                            id=target_session_id,
                            ip_address=ip_address,
                            user_agent=user_agent[:500],
                            client_id=client_id,  # Store client_id for unified tracking
                            first_visit=datetime.now(timezone.utc),
                            last_visit=datetime.now(timezone.utc),
                            visit_count=0
                        )
                        db.add(target_session)
                    # Reassign visit
                    linked_visit.session_id = target_session_id
                    effective_session_id = target_session_id
                    try:
                        db.add(linked_visit)
                        db.commit()
                        db.refresh(linked_visit)
                    except Exception:
                        db.rollback()

        # Update session row with latest timestamp and geo data
        # Re-fetch if effective_session_id changed due to client_id migration
        if effective_session_id != session_id:
            session_row = db.query(VisitSession).filter(VisitSession.id == effective_session_id).first()
            if not session_row:
                session_row = VisitSession(
                    id=effective_session_id,
                    ip_address=ip_address,
                    user_agent=user_agent[:500],
                    client_id=client_id,  # Store client_id for unified tracking
                    first_visit=datetime.now(timezone.utc),
                    last_visit=datetime.now(timezone.utc),
                    visit_count=0
                )
                db.add(session_row)
        
        # Update last_visit timestamp
        session_row.last_visit = datetime.now(timezone.utc)
        db.add(session_row)

        # Populate missing geo fields with inheritance logic (don't overwrite known values)
        try:
            # Only update if session doesn't have good location data
            needs_location_update = (
                not session_row.country or
                session_row.country == "XX" or
                not session_row.city or
                session_row.city == "Unknown"
            )

            if needs_location_update:
                # Try to inherit from existing sessions/visits for this IP
                inherited_location = None

                try:
                    # Look for recent sessions with this IP that have good location data
                    recent_sessions = db.query(VisitSession).filter(
                        VisitSession.ip_address == ip_address,
                        VisitSession.last_visit >= datetime.now(timezone.utc) - timedelta(hours=24),
                        VisitSession.id != session_row.id  # Don't inherit from self
                    ).all()

                    for existing_session in recent_sessions:
                        if existing_session.country and existing_session.country != "XX":
                            inherited_location = {
                                "country": existing_session.country,
                                "city": existing_session.city,
                                "country_name": existing_session.country_name
                            }
                            break

                    # If no good session data, try recent visits
                    if not inherited_location:
                        recent_visits = db.query(Visit).filter(
                            Visit.ip_address == ip_address,
                            Visit.timestamp >= datetime.now(timezone.utc) - timedelta(hours=24)
                        ).all()

                        for existing_visit in recent_visits:
                            if existing_visit.country and existing_visit.country != "XX":
                                inherited_location = {
                                    "country": existing_visit.country,
                                    "city": existing_visit.city
                                }
                                break
                except Exception:
                    pass  # If inheritance fails, continue with geo_info

                # Use inherited location if available, otherwise use current geo_info
                if inherited_location:
                    if not session_row.country or session_row.country == "XX":
                        session_row.country = inherited_location["country"]
                    if not session_row.city or session_row.city == "Unknown":
                        session_row.city = inherited_location["city"]
                    if not session_row.country_name and inherited_location.get("country_name"):
                        session_row.country_name = inherited_location["country_name"]
                elif geo_info:
                    # Only use geo_info if it's not "XX" or if we have no other choice
                    if geo_info.get("country_code") and geo_info.get("country_code") != "XX":
                        if not session_row.country or session_row.country == "XX":
                            session_row.country = geo_info.get("country_code")
                        if not session_row.city or session_row.city == "Unknown":
                            session_row.city = geo_info.get("city")
                        if not session_row.country_name:
                            session_row.country_name = geo_info.get("country_name")
                    elif not session_row.country:  # If we have no location data at all, use whatever we have
                        session_row.country = geo_info.get("country_code")
                        session_row.city = geo_info.get("city")
                        session_row.country_name = geo_info.get("country_name")

                # Update other geo fields from geo_info if not already set
                if geo_info:
                    if session_row.latitude is None:
                        session_row.latitude = geo_info.get("latitude")
                    if session_row.longitude is None:
                        session_row.longitude = geo_info.get("longitude")
                    if not session_row.timezone:
                        session_row.timezone = geo_info.get("timezone")
                    if not session_row.isp:
                        session_row.isp = geo_info.get("isp")
                    if not session_row.organization:
                        session_row.organization = geo_info.get("organization")
                    if not session_row.asn:
                        session_row.asn = geo_info.get("asn")

        except Exception:
            pass

        # Backfill session with client-side data from event
        if client_side_data and session_row:
            try:
                if not session_row.client_side_timezone and client_side_data.get('timezone'):
                    session_row.client_side_timezone = client_side_data.get('timezone')
                    logger.debug(f"Backfilled session timezone: {client_side_data.get('timezone')}")
                if not session_row.client_side_language and client_side_data.get('language'):
                    session_row.client_side_language = client_side_data.get('language')
                if not session_row.client_side_screen_resolution and client_side_data.get('screen_resolution'):
                    session_row.client_side_screen_resolution = client_side_data.get('screen_resolution')
                if not session_row.client_side_viewport_size and client_side_data.get('viewport_size'):
                    session_row.client_side_viewport_size = client_side_data.get('viewport_size')
                if not session_row.client_side_device_memory and client_side_data.get('device_memory'):
                    session_row.client_side_device_memory = client_side_data.get('device_memory')
                if not session_row.client_side_connection_type and client_side_data.get('connection_type'):
                    session_row.client_side_connection_type = client_side_data.get('connection_type')
                logger.info(f"Session client-side data backfilled", session_id=session_row.id[:20] if session_row.id else None)
            except Exception as e:
                logger.error(f"Failed to backfill session client-side data: {e}", session_id=session_row.id[:20] if session_row else None)

        # If we still couldn't link to a recent visit, merge any very recent visits for this IP into this session
        if not linked_visit:
            try:
                merge_cutoff = datetime.now(timezone.utc) - timedelta(seconds=180)
                recent_visits = (
                    db.query(Visit)
                    .filter(
                        Visit.ip_address == ip_address,
                        Visit.timestamp >= merge_cutoff,
                        Visit.session_id != effective_session_id,
                    )
                    .all()
                )
                changed = False
                for rv in recent_visits:
                    rv.session_id = effective_session_id
                    db.add(rv)
                    changed = True
                if changed:
                    db.commit()
            except Exception:
                db.rollback()

        # Try to inherit location from existing data before using current geo lookup
        event_country = None
        event_city = None

        # Priority 1: Use linked visit if it has good location data
        if linked_visit and linked_visit.country and linked_visit.country != "XX":
            event_country = linked_visit.country
            event_city = linked_visit.city

        # Priority 2: Use session if it has good location data
        elif session_row and session_row.country and session_row.country != "XX":
            event_country = session_row.country
            event_city = session_row.city

        # Priority 3: Try to inherit from other recent sessions/visits for this IP
        if not event_country:
            try:
                # Look for recent sessions with this IP that have good location data
                recent_sessions = db.query(VisitSession).filter(
                    VisitSession.ip_address == ip_address,
                    VisitSession.last_visit >= datetime.now(timezone.utc) - timedelta(hours=24)
                ).all()

                for existing_session in recent_sessions:
                    if existing_session.country and existing_session.country != "XX":
                        event_country = existing_session.country
                        event_city = existing_session.city
                        break

                # If no good session data, try recent visits
                if not event_country:
                    recent_visits = db.query(Visit).filter(
                        Visit.ip_address == ip_address,
                        Visit.timestamp >= datetime.now(timezone.utc) - timedelta(hours=24)
                    ).all()

                    for existing_visit in recent_visits:
                        if existing_visit.country and existing_visit.country != "XX":
                            event_country = existing_visit.country
                            event_city = existing_visit.city
                            break
            except Exception:
                pass  # If inheritance fails, continue

        # Priority 4: Final fallback to current geo lookup (avoid "XX" if possible)
        if not event_country and geo_info:
            if geo_info.get("country_code") and geo_info.get("country_code") != "XX":
                event_country = geo_info.get("country_code")
                event_city = geo_info.get("city")
            elif session_row and session_row.country:
                # Even if session has "XX", it's more consistent than event_data "XX"
                event_country = session_row.country
                event_city = session_row.city

        # Priority 5: Last resort - use whatever we have, even if it's XX
        if not event_country and geo_info:
            event_country = geo_info.get("country_code")
            event_city = geo_info.get("city")

        enriched_data = {
            **(data or {}),
            "tracking_id": tracking_id,
            "source": utm_info.get("source"),
            "medium": utm_info.get("medium"),
            "campaign": utm_info.get("campaign"),
            "crawler_type": visitor_info.get("crawler_name"),
            "is_bot": visitor_info.get("is_crawler"),
            "country": event_country,
            "city": event_city,
            "tracking_method": "javascript",
        }

        event_payload = {
            "session_id": effective_session_id,
            "visit_id": linked_visit.id if linked_visit else None,
            "client_id": client_id,
            "event_type": event_type,
            "page_url": page_url[:2000] if page_url else None,
            "referrer": referrer[:2000] if referrer else None,
            "path": page_info.get("path"),
            "page_domain": page_domain,
            "referrer_domain": referrer_domain,
            "tracking_id": tracking_id,
            "source": utm_info.get("source"),
            "medium": utm_info.get("medium"),
            "campaign": utm_info.get("campaign"),
            "event_data": enriched_data,
            "client_side_timezone": client_side_data.get('timezone') if client_side_data else None,
            "client_side_language": client_side_data.get('language') if client_side_data else None,
            "client_side_screen_resolution": client_side_data.get('screen_resolution') if client_side_data else None,
            "client_side_viewport_size": client_side_data.get('viewport_size') if client_side_data else None,
            "client_side_device_memory": client_side_data.get('device_memory') if client_side_data else None,
            "client_side_connection_type": client_side_data.get('connection_type') if client_side_data else None,
        }

        event_id = None
        queued = False

        if event_batcher.enabled:
            queued = await event_batcher.enqueue(event_payload)

        if not queued:
            event = VisitEvent(**event_payload)
            db.add(event)
            db.commit()
            db.refresh(event)
            event_id = event.id
            
            # Real-time summary update
            if client_id:
                try:
                    self._update_summaries(db, client_id)
                except Exception as e:
                    logger.error("Failed to update summaries on event", client_id=client_id, error=str(e))

        # If we created or linked a visit, opportunistically backfill visit geo, tracking fields, and client-side data from event
        if linked_visit:
            try:
                changed = False
                # Backfill geo data
                if geo_info:
                    if not linked_visit.country and geo_info.get("country_code"):
                        linked_visit.country = geo_info.get("country_code"); changed = True
                    if not linked_visit.city and geo_info.get("city"):
                        linked_visit.city = geo_info.get("city"); changed = True
                # Backfill tracking ID
                if tracking_id and not linked_visit.tracking_id:
                    linked_visit.tracking_id = tracking_id; changed = True
                # Backfill client-side data from event to visit
                if client_side_data:
                    if not linked_visit.client_side_timezone and client_side_data.get('timezone'):
                        linked_visit.client_side_timezone = client_side_data.get('timezone'); changed = True
                    if not linked_visit.client_side_language and client_side_data.get('language'):
                        linked_visit.client_side_language = client_side_data.get('language'); changed = True
                    if not linked_visit.client_side_screen_resolution and client_side_data.get('screen_resolution'):
                        linked_visit.client_side_screen_resolution = client_side_data.get('screen_resolution'); changed = True
                    if not linked_visit.client_side_viewport_size and client_side_data.get('viewport_size'):
                        linked_visit.client_side_viewport_size = client_side_data.get('viewport_size'); changed = True
                    if not linked_visit.client_side_device_memory and client_side_data.get('device_memory'):
                        linked_visit.client_side_device_memory = client_side_data.get('device_memory'); changed = True
                    if not linked_visit.client_side_connection_type and client_side_data.get('connection_type'):
                        linked_visit.client_side_connection_type = client_side_data.get('connection_type'); changed = True
                if changed:
                    logger.info(f"Visit client-side data backfilled", visit_id=linked_visit.id)
                    db.add(linked_visit)
                    db.commit()
            except Exception as e:
                logger.error(f"Failed to backfill visit client-side data: {e}", visit_id=linked_visit.id if linked_visit else None)
                db.rollback()

        logger.info(
            "Event tracked",
            event_id=event_id,
            queued=queued,
            event_type=event_type,
            page_url=page_url,
            visit_id=linked_visit.id if linked_visit else None,
        )

        return {"event_id": event_id, "queued": queued}
    
    async def get_visit_by_id(self, db: Session, visit_id: int) -> Optional[Visit]:
        """Get a visit by ID."""
        return db.query(Visit).filter(Visit.id == visit_id).first()
    
    async def get_recent_visits(
        self,
        db: Session,
        limit: int = 100,
        crawler_type: Optional[str] = None,
        hours: int = 24
    ) -> List[Visit]:
        """Get recent visits within specified time window."""
        query = db.query(Visit).filter(
            Visit.timestamp >= datetime.now(timezone.utc).replace(
                hour=datetime.now(timezone.utc).hour - hours
            )
        )
        
        if crawler_type:
            query = query.filter(Visit.crawler_type == crawler_type)
        
        return query.order_by(Visit.timestamp.desc()).limit(limit).all()
    
    async def get_session_stats(
        self,
        db: Session,
        session_id: str
    ) -> Dict[str, Any]:
        """Get statistics for a specific session."""
        session = db.query(VisitSession).filter(
            VisitSession.id == session_id
        ).first()
        
        if not session:
            return {}
        
        visits = db.query(Visit).filter(Visit.session_id == session_id).all()
        
        return {
            "session_id": session.id,
            "total_visits": len(visits),
            "first_visit": session.first_visit,
            "last_visit": session.last_visit,
            "country": session.country_name,
            "city": session.city,
            "unique_domains": len(set(v.page_domain for v in visits if v.page_domain)),
            "unique_paths": len(set(v.path for v in visits if v.path))
        }

    def _update_summaries(self, db: Session, client_id: str):
        """Update JourneySummary and LeadSummary for a specific client_id.
        Enhanced to match the robust SQL query logic.
        """
        # In tracking context, client_id is already the unique user key
        # 1. Get chronological visits
        visits = db.query(Visit).filter(Visit.client_id == client_id).order_by(Visit.timestamp.asc()).all()
        if not visits: return

        first = visits[0]
        
        # 2. Identify conversion time (/schedule)
        conversion_ts = None
        conversion_page = None
        conversion_path = None
        for v in visits:
            # Check for /schedule path (fuzzy regex equivalent)
            if v.path and ('/schedule' in v.path):
                conversion_ts = v.timestamp
                conversion_page = v.page_url
                conversion_path = v.path
                break

        # 3. Get chronological paths (deduplicated)
        path_list = []
        last_path = None
        for v in visits:
            if conversion_ts and v.timestamp > conversion_ts:
                break
            if v.path != last_path:
                path_list.append(v.path or "")
                last_path = v.path
        
        journey_to_schedule = " → ".join(path_list)
        
        # 4. Aggregate form data (Events + URL Params)
        form_data = {}
        captured_data_parts = []
        
        # Pull from visit query_params (all visits up to conversion)
        for v in visits:
            if conversion_ts and v.timestamp > conversion_ts:
                break
            qp = v.query_params or {}
            for k in ['email', 'name', 'company', 'organization', 'user_email']:
                if k in qp:
                    val = qp[k][0] if isinstance(qp[k], list) else qp[k]
                    if val:
                        form_data[k] = val
        
        # Pull from events
        events = db.query(VisitEvent).filter(VisitEvent.client_id == client_id).order_by(VisitEvent.timestamp.asc()).all()
        latest_form_ts = None
        for ev in events:
            if conversion_ts and ev.timestamp > conversion_ts:
                break
            
            ed = ev.event_data or {}
            event_has_data = False
            # form_input style
            if ev.event_type == 'form_input' and 'field_name' in ed:
                k, v = str(ed.get('field_name')).lower(), ed.get('field_value')
                if v:
                    form_data[k] = v
                    captured_data_parts.append(f"{ev.path or 'unknown'}: {k}={v}")
                    event_has_data = True
            
            # form_submit style
            if ev.event_type == 'form_submit' and 'form_values' in ed:
                vals = ed.get('form_values') or {}
                if isinstance(vals, dict):
                    for k, v in vals.items():
                        kl = str(k).lower()
                        form_data[kl] = v
                        captured_data_parts.append(f"{ev.path or 'unknown'}: {kl}={v}")
                        event_has_data = True
            
            if event_has_data:
                latest_form_ts = ev.timestamp

        captured_data_str = " | ".join(captured_data_parts)
        form_data_shared = " | ".join([f"{k}: {v}" for k, v in form_data.items()])
        if not form_data_shared and not captured_data_str:
            form_data_shared = "No info shared"

        # Heuristic profile extraction
        email = None
        name = None
        for k, v in form_data.items():
            if not email and ('email' in k or 'mail' in k): email = v
            if not name and ('name' in k or 'user' in k or 'full' in k): name = v

        # 5. Update JourneySummary
        journey = db.query(JourneySummary).filter(JourneySummary.client_id == client_id).first()
        if not journey:
            journey = JourneySummary(client_id=client_id)
            db.add(journey)
        
        journey.first_seen = first.timestamp
        journey.last_seen = conversion_ts or visits[-1].timestamp
        journey.visit_count = len(visits)
        journey.entry_page = first.page_url
        journey.exit_page = conversion_page or visits[-1].page_url
        journey.path_sequence = journey_to_schedule
        journey.email = email
        journey.name = name
        journey.has_captured_data = 1 if (conversion_ts or captured_data_parts) else 0
        journey.source = first.source
        journey.medium = first.medium
        journey.campaign = first.campaign

        # 6. Update LeadSummary if has data
        if conversion_ts or email or name or captured_data_parts:
            lead = db.query(LeadSummary).filter(LeadSummary.client_id == client_id).first()
            if not lead:
                lead = LeadSummary(client_id=client_id)
                db.add(lead)
            
            lead.email = email
            lead.name = name
            lead.captured_at = conversion_ts or latest_form_ts or first.timestamp
            lead.captured_page = conversion_page or visits[-1].page_url
            lead.captured_path = conversion_path or visits[-1].path
            lead.form_data_shared = form_data_shared
            lead.captured_data = captured_data_str
            lead.source = first.source
            lead.medium = first.medium
            lead.campaign = first.campaign
            lead.first_referrer = first.referrer
            try:
                from urllib.parse import urlparse
                lead.first_referrer_domain = urlparse(first.referrer).netloc if first.referrer else None
            except: pass
            lead.first_seen = first.timestamp
            lead.last_seen = conversion_ts or visits[-1].timestamp
        
        db.commit()
