"""IP enrichment service — persists company-level data for visitor IPs."""
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import httpx
import structlog
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.models.enrichment import IpEnrichment

logger = structlog.get_logger()

_PRIVATE_IPS = {"127.0.0.1", "::1", ""}
_PROVIDER = "ipapi"
_API_BASE = "https://api.apilayer.com/ipapi"


class IpEnrichmentService:
    """Enriches visitor IPs with company identity and network flags.

    Uses apilayer ipapi. On the free plan (5k req/month) security flags
    (is_proxy, is_tor, etc.) are not returned and stored as NULL.
    company_domain is derived from connection.sld + connection.tld.
    """

    def _clean_str(self, val) -> Optional[str]:
        if val is None:
            return None
        s = str(val).strip().lower()
        if s in ("", "nan", "null", "none", "n/a", "unknown"):
            return None
        return s

    def _clean_str_raw(self, val) -> Optional[str]:
        """Same as _clean_str but preserves original casing."""
        if val is None:
            return None
        s = str(val).strip()
        if s.lower() in ("", "nan", "null", "none", "n/a", "unknown"):
            return None
        return s

    def _normalize(self, ip: str, raw: dict, first_seen_at, last_seen_at) -> dict:
        """Map apilayer ipapi response to ip_enrichment columns."""
        now = datetime.now(timezone.utc)
        conn = raw.get("connection") or {}
        sec = raw.get("security") or {}

        sld = self._clean_str(conn.get("sld"))
        tld = self._clean_str(conn.get("tld"))
        company_domain = f"{sld}.{tld}" if sld and tld else None

        # is_vpn: apilayer signals VPN via anonymizer_status or vpn_service presence
        anonymizer = self._clean_str(sec.get("anonymizer_status"))
        vpn_service = self._clean_str(sec.get("vpn_service"))
        is_vpn: Optional[bool] = None
        if sec:
            is_vpn = bool(vpn_service) or anonymizer in ("active", "suspect")

        is_proxy: Optional[bool] = None
        if "is_proxy" in sec:
            is_proxy = bool(sec["is_proxy"])

        is_tor: Optional[bool] = None
        if "is_tor" in sec:
            is_tor = bool(sec["is_tor"])

        # hosting_facility is a boolean on paid plans
        hosting = sec.get("hosting_facility")
        is_datacenter: Optional[bool] = bool(hosting) if hosting is not None else None

        return {
            "ip": ip,
            "company_domain": company_domain,
            "company_name": self._clean_str_raw(conn.get("isp") or conn.get("org")),
            "company_type": self._clean_str_raw(conn.get("organization_type")),
            "country": self._clean_str(raw.get("country_code")),
            "is_datacenter": is_datacenter,
            "is_vpn": is_vpn,
            "is_proxy": is_proxy,
            "is_tor": is_tor,
            "source": _PROVIDER,
            "enriched_at": now,
            "ttl_expires_at": now + timedelta(days=settings.ip_enrichment_ttl_days),
            "status": "success",
            "error_code": None,
            "error_message": None,
            "attempt_count": 0,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
        }

    def _call_api(self, ip: str) -> dict:
        """Call apilayer ipapi for one IP. Returns raw JSON dict.

        Retries up to 3 times with exponential backoff on transient errors.
        Raises ValueError for permanent failures (4xx excl. 429).
        """
        if not settings.ipapi_api_key:
            raise RuntimeError("CRAWLDOCTOR_IPAPI_API_KEY is not configured")

        headers = {"apikey": settings.ipapi_api_key}
        url = f"{_API_BASE}/{ip}"

        for attempt in range(3):
            try:
                with httpx.Client(timeout=httpx.Timeout(10.0)) as client:
                    resp = client.get(url, headers=headers)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429 or resp.status_code >= 500:
                    # Transient — backoff and retry
                    wait = 2 ** attempt
                    logger.warning(
                        "ipapi transient error, retrying",
                        ip=ip, status=resp.status_code, attempt=attempt + 1, wait=wait
                    )
                    time.sleep(wait)
                    continue

                # 4xx (excl 429) = permanent failure
                raise ValueError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                wait = 2 ** attempt
                logger.warning(
                    "ipapi network error, retrying",
                    ip=ip, error=str(exc), attempt=attempt + 1, wait=wait
                )
                if attempt < 2:
                    time.sleep(wait)
                else:
                    raise

        raise RuntimeError(f"ipapi: exceeded retries for {ip}")

    def _get_candidates(self, db: Session, batch_size: int) -> list[dict]:
        """Return IPs that need enrichment: missing, TTL-expired, or retriable errors."""
        rows = db.execute(text("""
            SELECT DISTINCT ON (v.ip_address)
                v.ip_address,
                MIN(v.timestamp) OVER (PARTITION BY v.ip_address) AS first_seen_at,
                MAX(v.timestamp) OVER (PARTITION BY v.ip_address) AS last_seen_at
            FROM visits v
            WHERE v.is_bot = false
              AND v.ip_address IS NOT NULL
              AND v.ip_address NOT IN ('127.0.0.1', '::1', '')
              AND NOT EXISTS (
                SELECT 1 FROM ip_enrichment e
                WHERE e.ip = v.ip_address
                  AND e.status = 'success'
                  AND e.ttl_expires_at > now()
              )
              AND NOT EXISTS (
                SELECT 1 FROM ip_enrichment e
                WHERE e.ip = v.ip_address
                  AND e.status = 'error'
                  AND e.attempt_count >= :max_attempts
              )
            LIMIT :batch_size
        """), {"batch_size": batch_size, "max_attempts": settings.ip_enrichment_max_attempts}).fetchall()

        return [
            {"ip": r.ip_address, "first_seen_at": r.first_seen_at, "last_seen_at": r.last_seen_at}
            for r in rows
        ]

    def _upsert(self, db: Session, data: dict):
        """Upsert one row. On conflict: update all columns, preserve earliest first_seen_at."""
        stmt = insert(IpEnrichment).values(**data)
        update_cols = {
            k: stmt.excluded[k]
            for k in data
            if k not in ("ip", "first_seen_at")
        }
        update_cols["first_seen_at"] = func.least(
            IpEnrichment.first_seen_at, stmt.excluded.first_seen_at
        )
        stmt = stmt.on_conflict_do_update(index_elements=["ip"], set_=update_cols)
        db.execute(stmt)
        db.commit()

    def _upsert_error(self, db: Session, ip: str, error_code: str, message: str,
                      first_seen_at, last_seen_at):
        """Record a failed enrichment attempt, incrementing attempt_count."""
        now = datetime.now(timezone.utc)
        existing = db.query(IpEnrichment).filter(IpEnrichment.ip == ip).first()
        attempt_count = (existing.attempt_count + 1) if existing else 1
        status = "error" if attempt_count < settings.ip_enrichment_max_attempts else "failed"

        data = {
            "ip": ip,
            "company_domain": None,
            "company_name": None,
            "company_type": None,
            "country": None,
            "is_datacenter": None,
            "is_vpn": None,
            "is_proxy": None,
            "is_tor": None,
            "source": _PROVIDER,
            "enriched_at": now,
            "ttl_expires_at": None,
            "status": status,
            "error_code": error_code[:50] if error_code else None,
            "error_message": message[:500] if message else None,
            "attempt_count": attempt_count,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
        }
        self._upsert(db, data)

    def run_batch(self, db: Session, batch_size: Optional[int] = None) -> dict:
        """Enrich one batch of candidate IPs. Returns per-batch stats."""
        if not settings.ip_enrichment_enabled:
            return {"skipped": True, "reason": "ip_enrichment_enabled=false"}

        if not settings.ipapi_api_key:
            logger.warning("ip enrichment skipped: CRAWLDOCTOR_IPAPI_API_KEY not set")
            return {"skipped": True, "reason": "api_key_missing"}

        batch_size = batch_size or settings.ip_enrichment_batch_size
        candidates = self._get_candidates(db, batch_size)

        stats = {"candidates": len(candidates), "success": 0, "error": 0, "failed": 0}
        min_interval = 1.0 / max(settings.ip_enrichment_max_rps, 0.01)

        for candidate in candidates:
            ip = candidate["ip"]
            t0 = time.monotonic()

            try:
                raw = self._call_api(ip)
                data = self._normalize(ip, raw, candidate["first_seen_at"], candidate["last_seen_at"])
                self._upsert(db, data)
                stats["success"] += 1
                logger.info("ip enriched", ip=ip, company_domain=data.get("company_domain"))
            except ValueError as exc:
                # Permanent API error (4xx)
                stats["failed"] += 1
                logger.warning("ip enrichment permanent failure", ip=ip, error=str(exc))
                try:
                    self._upsert_error(db, ip, "api_4xx", str(exc),
                                       candidate["first_seen_at"], candidate["last_seen_at"])
                except Exception as write_exc:
                    logger.error("ip enrichment: failed to write error sentinel",
                                 ip=ip, error=str(write_exc))
                    try:
                        db.rollback()
                    except Exception:
                        pass
            except Exception as exc:
                # Transient / network error, or DB failure on the success path
                stats["error"] += 1
                logger.warning("ip enrichment transient error", ip=ip, error=str(exc))
                try:
                    db.rollback()  # clear any failed transaction before writing sentinel
                except Exception:
                    pass
                try:
                    self._upsert_error(db, ip, "transient", str(exc),
                                       candidate["first_seen_at"], candidate["last_seen_at"])
                except Exception as write_exc:
                    logger.error("ip enrichment: failed to write error sentinel",
                                 ip=ip, error=str(write_exc))
                    try:
                        db.rollback()
                    except Exception:
                        pass

            # Rate limit: sleep for the remainder of the inter-request interval
            elapsed = time.monotonic() - t0
            remainder = min_interval - elapsed
            if remainder > 0:
                time.sleep(remainder)

        logger.info("ip enrichment batch complete", **stats)
        return stats

    def run_backfill(self, db: Session, max_batches: int = 10) -> dict:
        """Run multiple batches for initial backfill.

        max_batches caps total API calls to max_batches * batch_size to
        protect the free-plan monthly quota.
        """
        totals = {"batches": 0, "candidates": 0, "success": 0, "error": 0, "failed": 0}

        for _ in range(max_batches):
            result = self.run_batch(db)
            if result.get("skipped"):
                break
            totals["batches"] += 1
            totals["candidates"] += result["candidates"]
            totals["success"] += result["success"]
            totals["error"] += result["error"]
            totals["failed"] += result["failed"]
            if result["candidates"] == 0:
                break  # nothing left to enrich

        logger.info("ip enrichment backfill complete", **totals)
        return totals

    def get_coverage_stats(self, db: Session) -> dict:
        """Return enrichment coverage metrics."""
        rows = db.execute(text("""
            SELECT
                COUNT(DISTINCT v.ip_address)                             AS candidates_total,
                COUNT(DISTINCT e.ip) FILTER (WHERE e.status = 'success') AS enriched_success,
                COUNT(DISTINCT e.ip) FILTER (WHERE e.status = 'error')   AS enriched_error,
                COUNT(DISTINCT e.ip) FILTER (WHERE e.status = 'failed')  AS enriched_failed
            FROM visits v
            LEFT JOIN ip_enrichment e ON e.ip = v.ip_address
            WHERE v.is_bot = false
              AND v.ip_address IS NOT NULL
              AND v.ip_address NOT IN ('127.0.0.1', '::1', '')
        """)).fetchone()

        total = rows.candidates_total or 0
        success = rows.enriched_success or 0
        coverage = round(success / total * 100, 1) if total else 0.0

        return {
            "candidates_total": total,
            "enriched_success_total": success,
            "enriched_error_total": rows.enriched_error or 0,
            "enriched_failed_total": rows.enriched_failed or 0,
            "enriched_skipped_total": total - success - (rows.enriched_error or 0) - (rows.enriched_failed or 0),
            "coverage_percent": coverage,
        }
