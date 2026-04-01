"""Unit tests for the journey-based session model.

Covers:
  - Internal domain classifier
  - External-entry detection
  - Session ID generation (no domain / no day)
  - Cross-domain continuity
  - Heartbeat / scroll / visibility never start sessions
  - Null-referrer fallback logic
  - Backfill service journey assignment
"""

import hashlib
import pytest
from datetime import datetime, timezone, timedelta

from app.utils.domains import classify_domain, is_internal_domain, is_internal_url
from app.services.tracking import TrackingService


# =====================================================================
# Domain classifier
# =====================================================================

class TestDomainClassifier:

    # -- exact matches --
    def test_exact_maxim(self):
        assert classify_domain("getmaxim.ai") == "maxim"

    def test_exact_bifrost(self):
        assert classify_domain("getbifrost.ai") == "bifrost"

    # -- subdomain matches --
    def test_subdomain_www_maxim(self):
        assert classify_domain("www.getmaxim.ai") == "maxim"

    def test_subdomain_app_maxim(self):
        assert classify_domain("app.getmaxim.ai") == "maxim"

    def test_subdomain_docs_bifrost(self):
        assert classify_domain("docs.getbifrost.ai") == "bifrost"

    def test_subdomain_bifrost_site_maxim(self):
        # bifrost-site.getmaxim.ai is a maxim subdomain
        assert classify_domain("bifrost-site.getmaxim.ai") == "maxim"

    # -- external domains --
    def test_google_external(self):
        assert classify_domain("google.com") is None

    def test_linkedin_external(self):
        assert classify_domain("www.linkedin.com") is None

    def test_similar_not_internal(self):
        # Must not match via substring — "notgetmaxim.ai" is NOT internal
        assert classify_domain("notgetmaxim.ai") is None

    def test_suffix_attack(self):
        # "evil.com.getmaxim.ai" IS a subdomain of getmaxim.ai
        assert classify_domain("evil.com.getmaxim.ai") == "maxim"

    # -- edge cases --
    def test_none(self):
        assert classify_domain(None) is None

    def test_empty(self):
        assert classify_domain("") is None

    def test_case_insensitive(self):
        assert classify_domain("WWW.GETMAXIM.AI") == "maxim"

    def test_with_port(self):
        assert classify_domain("getmaxim.ai:443") == "maxim"

    # -- is_internal_domain --
    def test_is_internal_true(self):
        assert is_internal_domain("www.getmaxim.ai") is True

    def test_is_internal_false(self):
        assert is_internal_domain("github.com") is False

    # -- is_internal_url --
    def test_is_internal_url_full(self):
        assert is_internal_url("https://docs.getbifrost.ai/guide") is True

    def test_is_internal_url_external(self):
        assert is_internal_url("https://www.google.com/search?q=maxim") is False

    def test_is_internal_url_none(self):
        assert is_internal_url(None) is False


# =====================================================================
# External-entry detection
# =====================================================================

class TestExternalEntryDetection:
    """Tests for TrackingService._is_external_entry."""

    def setup_method(self):
        self.svc = TrackingService()

    # -- referrer domain present --
    def test_internal_referrer_continues(self):
        assert self.svc._is_external_entry(
            referrer="https://www.getmaxim.ai/pricing",
            referrer_domain="www.getmaxim.ai",
            page_domain="docs.getbifrost.ai",
        ) is False

    def test_external_referrer_new_session(self):
        assert self.svc._is_external_entry(
            referrer="https://www.google.com/search",
            referrer_domain="www.google.com",
            page_domain="getmaxim.ai",
        ) is True

    def test_bifrost_to_maxim_continues(self):
        assert self.svc._is_external_entry(
            referrer="https://getbifrost.ai/demo",
            referrer_domain="getbifrost.ai",
            page_domain="app.getmaxim.ai",
        ) is False

    # -- heartbeat / continuity events --
    def test_heartbeat_never_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            event_type="heartbeat",
        ) is False

    def test_scroll_never_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            event_type="scroll",
        ) is False

    def test_visibility_never_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            event_type="visibility",
        ) is False

    # -- no referrer, source fallback --
    def test_no_referrer_internal_source_continues(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            source="www.getbifrost.ai",
        ) is False

    def test_no_referrer_google_source_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            source="google",
        ) is True

    def test_no_referrer_linkedin_source_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            source="linkedin",
        ) is True

    def test_no_referrer_unknown_source_new_session(self):
        """Unknown source value — still implies someone sent the user here."""
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            source="some-partner-site",
        ) is True

    # -- no referrer, no source, medium/campaign fallback --
    def test_no_referrer_no_source_medium_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            medium="cpc",
        ) is True

    def test_no_referrer_no_source_campaign_new_session(self):
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            campaign="spring-launch",
        ) is True

    # -- zero signal --
    def test_zero_signal_new_session(self):
        """No referrer, no source, no utm → new session."""
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
        ) is True

    # -- page_view event type with external referrer --
    def test_page_view_external_referrer(self):
        assert self.svc._is_external_entry(
            referrer="https://twitter.com/link",
            referrer_domain="twitter.com",
            page_domain="getmaxim.ai",
            event_type="page_view",
        ) is True

    # -- click event with no referrer --
    def test_click_zero_signal_new_session(self):
        """Non-continuity event with zero signal → new session."""
        assert self.svc._is_external_entry(
            referrer=None,
            referrer_domain=None,
            page_domain="getmaxim.ai",
            event_type="click",
        ) is True


# =====================================================================
# Session ID generation
# =====================================================================

class TestSessionIdGeneration:
    """Tests for TrackingService._generate_session_id."""

    def setup_method(self):
        self.svc = TrackingService()

    def test_deterministic(self):
        a = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id="abc", journey_seq=0)
        b = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id="abc", journey_seq=0)
        assert a == b

    def test_different_journey_seq(self):
        a = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id="abc", journey_seq=0)
        b = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id="abc", journey_seq=1)
        assert a != b

    def test_no_domain_in_hash(self):
        """Session ID must NOT vary by page_domain — old behavior removed."""
        # The function no longer accepts page_domain at all;
        # same inputs → same output regardless of what domain the user is on.
        sid = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id="abc", journey_seq=0)
        assert len(sid) == 32

    def test_no_day_in_hash(self):
        """Session ID must NOT contain a day bucket."""
        # The hash is fully deterministic from (identity, journey_seq).
        # Running on different dates with same inputs gives the same result.
        sid = self.svc._generate_session_id("1.2.3.4", "ua", client_id="xyz", journey_seq=3)
        expected = hashlib.sha256("cid:xyz:journey:3".encode()).hexdigest()[:32]
        assert sid == expected

    def test_fallback_to_ip_ua(self):
        """Without client_id, falls back to ip + user_agent."""
        sid = self.svc._generate_session_id("1.2.3.4", "Mozilla/5.0", client_id=None, journey_seq=0)
        expected = hashlib.sha256("ipua:1.2.3.4:Mozilla/5.0:journey:0".encode()).hexdigest()[:32]
        assert sid == expected

    def test_length(self):
        sid = self.svc._generate_session_id("1.2.3.4", "ua", client_id="c", journey_seq=0)
        assert len(sid) == 32


# =====================================================================
# Backfill journey assignment logic (unit-level, no DB)
# =====================================================================

class TestBackfillJourneyAssignment:
    """Test SessionBackfillService._should_start_new_journey in isolation."""

    def setup_method(self):
        from app.services.session_backfill import SessionBackfillService
        self.svc = SessionBackfillService()

    def _rec(self, **overrides):
        base = {
            "type": "visit",
            "id": 1,
            "old_session_id": "old",
            "client_id": "cid-1",
            "ip_address": "1.2.3.4",
            "user_agent": "Mozilla/5.0",
            "timestamp": datetime(2026, 3, 1, tzinfo=timezone.utc),
            "page_url": "https://getmaxim.ai/pricing",
            "page_domain": "getmaxim.ai",
            "referrer": None,
            "referrer_domain": None,
            "source": None,
            "medium": None,
            "campaign": None,
        }
        base.update(overrides)
        return base

    def _meta(self, **overrides):
        base = {
            "entry_referrer_domain": "getmaxim.ai",
            "last_visit": datetime(2026, 3, 1, tzinfo=timezone.utc),
        }
        base.update(overrides)
        return base

    def test_first_record_always_new(self):
        assert self.svc._should_start_new_journey(self._rec(), None) is True

    def test_internal_referrer_continues(self):
        rec = self._rec(referrer_domain="www.getmaxim.ai")
        assert self.svc._should_start_new_journey(rec, self._meta()) is False

    def test_external_referrer_new(self):
        rec = self._rec(referrer_domain="google.com")
        assert self.svc._should_start_new_journey(rec, self._meta()) is True

    def test_cross_family_continues(self):
        rec = self._rec(referrer_domain="docs.getbifrost.ai", page_domain="getmaxim.ai")
        assert self.svc._should_start_new_journey(rec, self._meta()) is False

    def test_heartbeat_continues(self):
        rec = self._rec(event_type="heartbeat", referrer_domain=None)
        assert self.svc._should_start_new_journey(rec, self._meta()) is False

    def test_zero_signal_continues(self):
        # Zero-signal page entries (direct visits, bookmarks) continue the
        # current session in backfill to avoid over-fragmentation.
        rec = self._rec(referrer_domain=None, source=None, medium=None, campaign=None)
        assert self.svc._should_start_new_journey(rec, self._meta()) is False

    def test_utm_campaign_only_new(self):
        rec = self._rec(referrer_domain=None, source=None, campaign="spring-launch")
        assert self.svc._should_start_new_journey(rec, self._meta()) is True

    def test_internal_source_continues(self):
        rec = self._rec(referrer_domain=None, source="getmaxim.ai")
        assert self.svc._should_start_new_journey(rec, self._meta()) is False
