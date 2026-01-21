"""Backfill service for journey and lead summaries."""
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, and_
import structlog
import json

from app.models.summary import LeadSummary, JourneySummary
from app.models.visit import Visit, VisitEvent

logger = structlog.get_logger()

class BackfillService:
    """Service to backfill summary tables using robust SQL logic."""

    def backfill_all(self, db: Session, days: int = 90):
        """Perform a full backfill of both summary tables."""
        logger.info("Starting full backfill", days=days)
        
        # Optimized version of the user's query to avoid timeouts
        # Using a single pass for journey data instead of subqueries in SELECT
        sql = text("""
        WITH user_base AS (
            SELECT 
                COALESCE(client_id, session_id) as unique_user_key,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen,
                COUNT(*) as visit_count
            FROM visits
            WHERE timestamp >= NOW() - INTERVAL '1 day' * :days
            GROUP BY 1
        ),
        schedule_converters AS (
            SELECT 
                COALESCE(client_id, session_id) as unique_user_key,
                MIN(timestamp) as first_scheduled_at
            FROM visits
            WHERE path ~* '^.*/schedule([/?#]|$)'
              AND timestamp >= NOW() - INTERVAL '1 day' * :days
            GROUP BY 1
        ),
        converters_with_params AS (
            SELECT DISTINCT ON (COALESCE(client_id, session_id))
                COALESCE(client_id, session_id) as unique_user_key,
                (
                    SELECT STRING_AGG(key || ': ' || (value->>0), ' | ') 
                    FROM jsonb_each(query_params::jsonb) 
                    WHERE key IN ('email', 'name', 'company', 'organization', 'user_email')
                ) as extracted_params,
                page_url as schedule_url,
                path as schedule_path
            FROM visits
            WHERE path ~* '^.*/schedule([/?#]|$)'
              AND timestamp >= NOW() - INTERVAL '1 day' * :days
            ORDER BY COALESCE(client_id, session_id), timestamp ASC
        ),
        user_attribution AS (
            SELECT DISTINCT ON (unique_user_key)
                COALESCE(client_id, session_id) as unique_user_key,
                referrer as original_referrer,
                source as original_utm_source,
                medium as original_utm_medium,
                campaign as original_utm_campaign,
                page_url as entry_page
            FROM visits
            WHERE timestamp >= NOW() - INTERVAL '1 day' * :days
            ORDER BY COALESCE(client_id, session_id), timestamp ASC
        ),
        journey_paths AS (
            SELECT 
                unique_user_key,
                STRING_AGG(path, ' → ' ORDER BY timestamp ASC) as path_sequence,
                (ARRAY_AGG(page_url ORDER BY timestamp DESC))[1] as last_page_url
            FROM (
                SELECT 
                    COALESCE(v.client_id, v.session_id) as unique_user_key,
                    v.path,
                    v.page_url,
                    v.timestamp,
                    CASE WHEN v.path = LAG(v.path) OVER (PARTITION BY COALESCE(v.client_id, v.session_id) ORDER BY v.timestamp ASC) 
                         THEN 1 ELSE 0 END as is_duplicate
                FROM visits v
                WHERE v.timestamp >= NOW() - INTERVAL '1 day' * :days
            ) t
            WHERE is_duplicate = 0
            GROUP BY unique_user_key
        ),
        forms_captured AS (
            SELECT 
                COALESCE(ve.client_id, ve.session_id) as unique_user_key,
                STRING_AGG(
                    DISTINCT (ve.path || ': ' || ve.event_data::text), 
                    ' | '
                ) as journey_forms,
                MAX(ve.timestamp) as latest_form_ts
            FROM visit_events ve
            WHERE ve.timestamp >= NOW() - INTERVAL '1 day' * :days
              AND (ve.event_type = 'form_submit' OR ve.event_type = 'form_input')
            GROUP BY 1
        )
        SELECT 
            ub.unique_user_key,
            ub.first_seen,
            ub.last_seen,
            ub.visit_count,
            ua.entry_page,
            ua.original_referrer,
            ua.original_utm_source,
            ua.original_utm_medium,
            ua.original_utm_campaign,
            jp.path_sequence,
            jp.last_page_url,
            sc.first_scheduled_at as conversion_at,
            cp.extracted_params,
            cp.schedule_url,
            cp.schedule_path,
            fc.journey_forms as captured_data,
            fc.latest_form_ts,
            COALESCE(cp.extracted_params, fc.journey_forms, 'No info shared') as form_data_shared
        FROM user_base ub
        LEFT JOIN user_attribution ua ON ub.unique_user_key = ua.unique_user_key
        LEFT JOIN journey_paths jp ON ub.unique_user_key = jp.unique_user_key
        LEFT JOIN schedule_converters sc ON ub.unique_user_key = sc.unique_user_key
        LEFT JOIN converters_with_params cp ON ub.unique_user_key = cp.unique_user_key
        LEFT JOIN forms_captured fc ON ub.unique_user_key = fc.unique_user_key
        """)

        # Set a long statement timeout for this session
        db.execute(text("SET statement_timeout = 180000")) # 3 minutes
        
        results = db.execute(sql, {"days": days}).mappings().all()
        
        # Clear existing summaries
        db.query(LeadSummary).delete()
        db.query(JourneySummary).delete()
        
        leads_added = 0
        journeys_added = 0
        
        for row in results:
            cid = row['unique_user_key']
            
            # Extract basic profile info
            email, name = self._extract_profile(row['form_data_shared'])
            
            # 1. Add to JourneySummary
            journey = JourneySummary(
                client_id=cid,
                first_seen=row['first_seen'],
                last_seen=row['conversion_at'] or row['last_seen'],
                visit_count=row['visit_count'],
                entry_page=row['entry_page'],
                exit_page=row['last_page_url'],
                path_sequence=row['path_sequence'],
                email=email,
                name=name,
                has_captured_data=1 if (row['conversion_at'] or row['captured_data']) else 0,
                source=row['original_utm_source'],
                medium=row['original_utm_medium'],
                campaign=row['original_utm_campaign']
            )
            db.add(journey)
            journeys_added += 1
            
            # 2. Add to LeadSummary
            if row['conversion_at'] or email or name:
                lead = LeadSummary(
                    client_id=cid,
                    email=email,
                    name=name,
                    captured_at=row['conversion_at'] or row['latest_form_ts'] or row['first_seen'],
                    captured_page=row['schedule_url'] or row['last_page_url'],
                    captured_path=row['schedule_path'] or row['last_page_url'],
                    form_data_shared=row['form_data_shared'],
                    captured_data=row['captured_data'],
                    source=row['original_utm_source'],
                    medium=row['original_utm_medium'],
                    campaign=row['original_utm_campaign'],
                    first_referrer=row['original_referrer'],
                    first_referrer_domain=self._extract_domain(row['original_referrer']),
                    first_seen=row['first_seen'],
                    last_seen=row['conversion_at'] or row['last_seen']
                )
                db.add(lead)
                leads_added += 1
        
        db.commit()
        logger.info("Backfill completed with optimized logic", journeys=journeys_added, leads=leads_added)
        return {"journeys": journeys_added, "leads": leads_added}

    def _extract_domain(self, url: str) -> str:
        if not url: return None
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc or None
        except: return None

    def _extract_profile(self, data_str: str):
        if not data_str or data_str == 'No info shared':
            return None, None
        
        email = None
        name = None
        
        # 1. Try JSON parsing first (for individual form submissions)
        try:
            # If the whole string is a JSON array/object from a single event
            if data_str.strip().startswith('{') or data_str.strip().startswith('['):
                clean_json = data_str.split(' | ')[0].split(': ', 1)[-1] if ': {' in data_str else data_str
                parsed = json.loads(clean_json)
                if isinstance(parsed, dict):
                    vals = parsed.get('form_values') or parsed.get('values') or parsed
                    for k, v in vals.items():
                        kl = str(k).lower()
                        if not email and ('email' in kl or 'mail' in kl) and '@' in str(v): email = str(v)
                        if not name and ('name' in kl or 'user' in kl or 'full' in kl): name = str(v)
        except: pass

        # 2. Robust regex fallback for aggregated strings
        import re
        if not email:
            email_match = re.search(r'[\'"]?email[\'"]?\s*[:=]\s*[\'"]?([^\'"\s,|]+@[^\'"\s,|]+)[\'"]?', data_str, re.I)
            if email_match: email = email_match.group(1)
        
        if not name:
            name_match = re.search(r'[\'"]?name[\'"]?\s*[:=]\s*[\'"]?([^\'"|]+)[\'"]?', data_str, re.I)
            if name_match: 
                val = name_match.group(1).strip()
                if val.lower() not in ['null', 'undefined', 'unknown', '', '[object object]']:
                    name = val

        return email, name
