"""Simplified tracking API for logging ALL visits."""
from datetime import datetime
from typing import Optional
import json
from fastapi import APIRouter, Request, Response, Depends, Query, HTTPException
from sqlalchemy.orm import Session
import structlog
from rjsmin import jsmin

from app.database import get_db
from app.services.tracking import TrackingService
from app.utils.rate_limiting import RateLimiter

logger = structlog.get_logger()

router = APIRouter(prefix="/track", tags=["tracking"])
tracking_service = TrackingService()
rate_limiter = RateLimiter()
def _get_client_ip(request: Request) -> str:
    """Extract client IP considering reverse proxy headers."""
    try:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            # XFF may contain multiple IPs, take the first
            ip = xff.split(",")[0].strip()
            if ip:
                return ip
        xri = request.headers.get("x-real-ip")
        if xri:
            return xri.strip()
        fwd = request.headers.get("forwarded")
        if fwd and "for=" in fwd:
            # e.g., for=1.2.3.4;proto=https;by=...
            try:
                part = [p for p in fwd.split(";") if p.strip().lower().startswith("for=")][0]
                ip = part.split("=", 1)[1].strip().strip('"')
                # Remove optional port
                if ip.startswith("[") and "]" in ip:
                    ip = ip[1:ip.index("]")]
                else:
                    ip = ip.split(":")[0]
                if ip:
                    return ip
            except Exception:
                pass
    except Exception:
        pass
    return request.client.host

@router.get("/js")
async def track_js(
    request: Request,
    db: Session = Depends(get_db),
    tid: Optional[str] = Query(None, description="Tracking ID"),
    page: Optional[str] = Query(None, description="Page identifier")
):
    """JavaScript tracking endpoint with client-side instrumentation and single-fire guard."""
    try:
        client_ip = _get_client_ip(request)
        if not await rate_limiter.is_allowed(client_ip, "js_track"):
            return Response(content="/* Rate limited */", media_type="application/javascript")
        
        user_agent = request.headers.get("user-agent", "")
        referrer = request.headers.get("referer")
        page_url = page or referrer
        
        # Do not create a visit on script load; the actual page_view event will create it
        logger.info("JavaScript tracker served")
        
        # Return enhanced JavaScript that instruments events and prevents duplicate initial fires
        js_template = """
/* CrawlDoctor JS instrumentation */
(function(){
  try {
    var CD = window.CrawlDoctor || (window.CrawlDoctor = {});
    if (CD._loaded) return; // prevent duplicates
    CD._loaded = true;

    // Single-fire guard for initial page_view across fallbacks
    if (!sessionStorage.getItem('cd_page_view_sent')) {
      sessionStorage.setItem('cd_page_view_sent', '1');
    }

    var TID = __TID__;
    var PAGE_URL = __PAGE_URL__;
    var VISIT_ID = __VISIT_ID__;

    // Determine tracker origin from current script src
    var __src = (function(){
      var s = document.currentScript; 
      if (!s) { var list = document.getElementsByTagName('script'); s = list[list.length - 1]; }
      return s && s.src || '';
    })();
    var __origin = (function(){
      try { return new URL(__src).origin; } catch(e) { return (location.protocol + '//' + location.host); }
    })();

    // Compute base domain (eTLD+1 heuristic) for cross-subdomain continuity
    function getBaseDomain(host) {
      try {
        var parts = (host || location.hostname).split('.');
        if (parts.length <= 2) return parts.join('.');
        // Heuristic: last 2 labels; handle common ccTLDs (co.uk, com.au)
        var tld2 = parts.slice(-2).join('.');
        var tld3 = parts.slice(-3).join('.');
        var ccLike = ['co.uk','org.uk','ac.uk','gov.uk','com.au','net.au','co.nz'];
        if (ccLike.indexOf(tld3) >= 0) return tld3;
        return tld2;
      } catch(e) { return location.hostname; }
    }

    var BASE_DOMAIN = getBaseDomain(location.hostname);
    // Per-site anonymous client id to avoid cross-device/session collisions
    var CID_KEY = 'cd_cid_' + (TID || BASE_DOMAIN);
    var CID = null;
    
    // 1. Try to get CID from URL parameters (Cross-domain handoff)
    try {
      var urlParams = new URLSearchParams(window.location.search);
      if (urlParams.has('cd_cid')) {
        CID = urlParams.get('cd_cid');
        
        // Clean up ONLY our parameter from the URL
        urlParams.delete('cd_cid');
        var newSearch = urlParams.toString();
        var newUrl = window.location.pathname + (newSearch ? '?' + newSearch : '') + window.location.hash;
        window.history.replaceState({}, document.title, newUrl);
      }
    } catch(e) {}

    // 2. Try to get CID from LocalStorage or Cookie if not in URL
    try {
      if (!CID) CID = localStorage.getItem(CID_KEY);
      
      if (!CID) {
        // Fallback: Check cookie
        var match = document.cookie.match(new RegExp('(^| )cd_cid=([^;]+)'));
        if (match) CID = match[2];
      }

      if (!CID) {
        // Generata new CID
        CID = ([1e7]+-1e3+-4e3+-8e3+-1e11).toString().replace(/[018]/g, function(c){
          return (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
        });
      }
      
      // Persist CID
      localStorage.setItem(CID_KEY, CID);
      try {
        var expires = new Date();
        expires.setFullYear(expires.getFullYear() + 1);
        document.cookie = 'cd_cid=' + CID + '; path=/; domain=' + BASE_DOMAIN + '; expires=' + expires.toUTCString() + '; samesite=Lax';
      } catch(e) {}
    } catch(e) {}

    // Cross-domain Link Decorator
    // Automatically appends 'cd_cid' to links pointing to sibling subdomains
    document.addEventListener('mousedown', function(event) {
      try {
        var a = event.target.closest('a');
        if (!a || !a.href) return;
        
        var linkUrl = new URL(a.href);
        // Only decorate links to the same base domain but different origin (e.g. www -> app)
        if (linkUrl.hostname.indexOf(BASE_DOMAIN) !== -1 && linkUrl.origin !== window.location.origin) {
          linkUrl.searchParams.set('cd_cid', CID);
          a.href = linkUrl.toString();
        }
      } catch(e) {}
    }, true);

    // Helper to extract page metadata
    function getPageMetadata() {
      var meta = {};
      try {
        meta.title = document.title;
        var metas = document.getElementsByTagName('meta');
        for (var i = 0; i < metas.length; i++) {
          var m = metas[i];
          var name = m.getAttribute('name') || m.getAttribute('property');
          if (name && (name.indexOf('description') >= 0 || name.indexOf('og:title') >= 0 || name.indexOf('keywords') >= 0)) {
            meta[name] = m.getAttribute('content');
          }
        }
      } catch(e) {}
      return meta;
    }

    // Collect client-side data (cached after first call)
    var CLIENT_DATA_CACHE = null;
    function getClientSideData() {
      if (CLIENT_DATA_CACHE) return CLIENT_DATA_CACHE;
      var data = {};
      try { data.timezone = Intl.DateTimeFormat().resolvedOptions().timeZone; } catch(e) {}
      try { data.language = navigator.language || navigator.userLanguage; } catch(e) {}
      try { data.screen_resolution = window.screen.width + 'x' + window.screen.height; } catch(e) {}
      try { data.viewport_size = window.innerWidth + 'x' + window.innerHeight; } catch(e) {}
      try { if (navigator.deviceMemory) data.device_memory = navigator.deviceMemory + 'GB'; } catch(e) {}
      try { if (navigator.connection) data.connection_type = navigator.connection.effectiveType || navigator.connection.type; } catch(e) {}
      CLIENT_DATA_CACHE = data;
      return data;
    }

    var __in_post_event = false;
    function postEvent(type, data) {
      if (__in_post_event) return;
      __in_post_event = true;
      try {
        var payload = {
          event_type: type,
          page_url: window.location.href,
          referrer: document.referrer || null,
          data: data || {},
          visit_id: VISIT_ID,
          tid: TID,
          cid: CID,
          client_side_data: getClientSideData(),
          page_metadata: getPageMetadata()
        };
        var url = __origin + '/track/event?tid=' + encodeURIComponent(TID || '');
        var payloadStr = JSON.stringify(payload);
        
        // Try sendBeacon first (most reliable for page unload)
        if (navigator.sendBeacon) {
          try {
            var sent = navigator.sendBeacon(url, payloadStr);
            if (sent) { __in_post_event = false; return; }
          } catch(e) {}
        }
        
        // Fallback to fetch
        if (window.fetch) {
          try {
            fetch(url, { 
              method: 'POST', 
              body: payloadStr, 
              headers: { 'Content-Type': 'text/plain' }, 
              keepalive: true 
            }).catch(function(err) {}).finally(function(){ __in_post_event = false; });
          } catch(e) { __in_post_event = false; }
        } else {
          __in_post_event = false;
        }
      } catch(e) { __in_post_event = false; }
    }

    // Page view event (guarded per page)
    var pvKey = 'cd_pv_' + location.pathname + location.search;
    if (!sessionStorage.getItem(pvKey)) {
      sessionStorage.setItem(pvKey, Date.now());
      postEvent('page_view', { viewport: { w: window.innerWidth, h: window.innerHeight }, tracking_method: 'javascript', cid: CID });
    }

    // Click events (throttled minimal payload)
    document.addEventListener('click', function(ev) {
      try {
        var t = ev.target;
        var a = (t && t.closest) ? t.closest('a,button,[role="button"]') : null;
        if (!a) return; // Only track actual clickable elements
        var href = a.tagName === 'A' ? a.href : null;
        var text = a.innerText || a.getAttribute('aria-label') || a.name || a.id || null;
        var id = a.id || null;
        var cls = a.className || null;
        
        postEvent('click', { 
          href: href || null, 
          text: text, 
          id: id, 
          class: cls, 
          tracking_method: 'javascript' 
        });
      } catch(e) {}
    }, { passive: true });

    // Scroll events (throttled)
    var scrollTimer = null;
    window.addEventListener('scroll', function() {
      if (scrollTimer) return;
      scrollTimer = setTimeout(function() {
        scrollTimer = null;
        var y = window.scrollY || document.documentElement.scrollTop || 0;
        var h = document.documentElement.scrollHeight || 0;
        var vp = window.innerHeight || 0;
        var pct = h ? Math.round(((y + vp) / h) * 100) : 0;
        postEvent('scroll', { y: y, percent: pct, tracking_method: 'javascript' });
      }, 1000);
    }, { passive: true });

    // Visibility change
    document.addEventListener('visibilitychange', function() {
      postEvent('visibility', { state: document.visibilityState, tracking_method: 'javascript' });
    });

    // Track time on page and engagement
    var __pageEnterTime = Date.now();
    var __lastEngagementTime = Date.now();
    function getEngagementData() {
      var now = Date.now();
      return {
        time_on_page_ms: now - __pageEnterTime,
        idle_time_ms: now - __lastEngagementTime,
        engaged: (now - __lastEngagementTime) < 30000,
        tracking_method: 'javascript'
      };
    }
    
    // Update engagement on common interactions
    function updateEngagement() { __lastEngagementTime = Date.now(); }
    document.addEventListener('click', updateEngagement, { passive: true });
    document.addEventListener('scroll', updateEngagement, { passive: true });
    document.addEventListener('keypress', updateEngagement, { passive: true });

    // Periodic heartbeat for "time on site" tracking
    setInterval(function() {
      if (document.visibilityState === 'visible') {
        postEvent('heartbeat', getEngagementData());
      }
    }, 30000); // Every 30s

    // Navigation away (refined)
    window.addEventListener('beforeunload', function() {
      try {
        var nav = performance && performance.getEntriesByType ? performance.getEntriesByType('navigation')[0] : null;
        var data = getEngagementData();
        data.type = nav && nav.type || 'unknown';
        postEvent('navigate', data);
      } catch(e) {}
    });

    // SPA navigation tracking
    var __lastUrl = location.href;
    function emitNav(type) {
      try {
        var to = location.href;
        var from = __lastUrl;
        if (to !== from) {
          postEvent('navigation', { type: type, from: from, to: to, tracking_method: 'javascript' });
          __lastUrl = to;
        }
      } catch(e) {}
    }
    try {
      var _ps = history.pushState;
      history.pushState = function() {
        _ps.apply(history, arguments);
        emitNav('pushState');
      };
      var _rs = history.replaceState;
      history.replaceState = function() {
        _rs.apply(history, arguments);
        emitNav('replaceState');
      };
      window.addEventListener('popstate', function() { emitNav('popstate'); });
      window.addEventListener('hashchange', function() { emitNav('hashchange'); });
    } catch(e) {}

    // Sensitive-field masking rules
    function isSensitiveField(el, name, type) {
      var n = (name || '').toLowerCase();
      var t = (type || '').toLowerCase();
      if (t === 'password') return true;
      var keywords = ['password','pass','pwd','ssn','social','credit','card','cc','cvc','cvv','otp','token','secret','api_key','apikey','recaptcha','cvn','card_number','cvv2'];
      for (var i = 0; i < keywords.length; i++) {
        if (n.indexOf(keywords[i]) >= 0) return true;
      }
      return false;
    }

    function sanitizeValue(val, el, name, type) {
      var v = (val || '');
      var sensitive = isSensitiveField(el, name, type);
      if (sensitive) {
        return { value: null, masked: true, length: v.length }; // Don't store sensitive data at all
      }
      var trimmed = ('' + v).trim();
      var limit = 1000;
      var truncated = trimmed.length > limit ? trimmed.slice(0, limit) : trimmed;
      return { value: truncated, masked: false, length: trimmed.length, truncated: trimmed.length > limit };
    }

    // Form tracking: broad capture for all interactive fields
    function shouldSkipField(el, type) {
      try {
        if (!el) return true;
        if (el.getAttribute && el.getAttribute('aria-hidden') === 'true') return true;
        var t = (type || '').toLowerCase();
        // Allow common input types even if they have negative tabIndex (common in custom UI kits)
        if (t === 'hidden' || t === 'password' || t === 'submit' || t === 'button' || t === 'reset') return true;
      } catch(e) {}
      return false;
    }

    // Track individual input changes (captures data even if form not submitted)
    var __inputCache = {};
    function emitInputTracking(el) {
      try {
        if (!el) return;
        var type = (el.getAttribute('type') || el.tagName || '').toLowerCase();
        if (shouldSkipField(el, type)) return;
        
        var name = el.name || el.id || el.getAttribute('placeholder') || el.getAttribute('aria-label') || 'unnamed';
        var val = type === 'checkbox' || type === 'radio' ? (el.checked ? '1' : '0') : (el.value || '').trim();
        
        if (!val || val === __inputCache[name]) return;
        __inputCache[name] = val;
        
        var safe = sanitizeValue(val, el, name, type);
        if (safe.value === null) return;
        
        postEvent('form_input', {
          field_name: name,
          field_type: type,
          field_value: safe.value,
          tracking_method: 'javascript'
        });
      } catch(e) {}
    }

    document.addEventListener('blur', function(ev) {
      var t = ev.target;
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT')) {
        emitInputTracking(t);
      }
    }, true);

    document.addEventListener('change', function(ev) {
      var t = ev.target;
      if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT')) {
        emitInputTracking(t);
      }
    }, true);

    function collectFormValues(form) {
      var inputs = form.querySelectorAll('input,textarea,select');
      var filled = 0;
      var formData = {};
      inputs.forEach(function(i) {
        var type = (i.getAttribute('type') || '').toLowerCase();
        if (shouldSkipField(i, type)) return;
        var name = i.name || i.id || ('field_' + Math.random().toString(36).substr(2,5));
        var raw = type === 'checkbox' || type === 'radio' ? (i.checked ? '1' : '0') : (i.value || '').trim();
        if (!raw) return;
        var safe = sanitizeValue(raw, i, name, type);
        if (safe.value !== null) { // Only add if not masked
          filled += 1;
          formData[name] = safe.value;
        }
      });
      return { filled: filled, values: formData };
    }

    function emitFormSubmit(form) {
      try {
        if (!form) return;
        var now = Date.now();
        if (form._cd_last_submit_ts && (now - form._cd_last_submit_ts) < 1000) return;
        form._cd_last_submit_ts = now;
        var fid = form.id || null;
        var fName = form.getAttribute('name') || null;
        var action = form.getAttribute('action') || null;
        var method = (form.getAttribute('method') || 'GET').toUpperCase();
        var collected = collectFormValues(form);
        postEvent('form_submit', {
          id: fid,
          name: fName,
          action: action,
          method: method,
          filled_fields: collected.filled,
          form_values: collected.values,
          tracking_method: 'javascript'
        });
      } catch(e) {}
    }

    // Capture submit (standard)
    document.addEventListener('submit', function(ev) {
      try {
        emitFormSubmit(ev.target);
      } catch(e) {}
    }, true);

    // Capture submit button clicks (some SPA handlers bypass submit event)
    document.addEventListener('click', function(ev) {
      try {
        var btn = ev.target && ev.target.closest ? ev.target.closest('button, input') : null;
        if (!btn) return;
        var type = (btn.getAttribute('type') || '').toLowerCase();
        if (type !== 'submit') return;
        var form = btn.form || (btn.closest ? btn.closest('form') : null);
        if (!form) return;
        if (form.checkValidity && !form.checkValidity()) return;
        setTimeout(function(){ emitFormSubmit(form); }, 0);
      } catch(e) {}
    }, true);

    // Network-level form submit capture (Framer/SPA often bypasses submit event)
    function shouldSkipNetworkField(name) {
      var n = (name || '').toLowerCase();
      if (!n) return true;
      if (n.indexOf('g-recaptcha') === 0 || n.indexOf('recaptcha') >= 0) return true; // reCAPTCHA
      if (n === 'phc_' || n.indexOf('distinct_id') >= 0) return true; // Analytics noise
      return false;
    }
    
    function isAnalyticsRequest(url) {
      if (!url) return false;
      var u = ('' + url).toLowerCase();
      // Skip PostHog, Segment, Google Analytics, Amplitude, Mixpanel, etc.
      var patterns = ['posthog', 'segment', 'google-analytics', 'amplitude', 'mixpanel', '/api/analytics', '/track', '/batch', '/capture', '/engage', '/collect'];
      for (var i = 0; i < patterns.length; i++) {
        if (u.indexOf(patterns[i]) >= 0) return true;
      }
      return false;
    }

    function extractFormValuesFromBody(body, contentType) {
      var values = {};
      try {
        if (!body) return values;
        var ct = (contentType || '').toLowerCase();
        
        if (typeof FormData !== 'undefined' && body instanceof FormData) {
          body.forEach(function(val, key){
            if (shouldSkipNetworkField(key)) return;
            var safe = sanitizeValue(String(val || ''), null, key, 'text');
            if (safe.value !== null) values[key] = safe.value;
          });
        } else if (typeof URLSearchParams !== 'undefined' && body instanceof URLSearchParams) {
          body.forEach(function(val, key){
            if (shouldSkipNetworkField(key)) return;
            var safe = sanitizeValue(String(val || ''), null, key, 'text');
            if (safe.value !== null) values[key] = safe.value;
          });
        } else if (typeof body === 'string' && ct.indexOf('multipart/form-data') >= 0) {
          var bMatch = (contentType || '').match(/boundary=([^;]+)/i);
          var boundary = bMatch ? bMatch[1] : null;
          if (boundary) {
            var parts = body.split('--' + boundary);
            parts.forEach(function(part) {
              if (part.indexOf('name="') >= 0) {
                var nameMatch = part.match(/name="([^"]+)"/);
                var valueParts = part.split('\r\n\r\n');
                if (nameMatch && valueParts.length > 1) {
                  var name = nameMatch[1];
                  var val = valueParts[1].split('\r\n')[0].trim();
                  if (shouldSkipNetworkField(name)) return;
                  var safe = sanitizeValue(val, null, name, 'text');
                  if (safe.value !== null) values[name] = safe.value;
                }
              }
            });
          }
        } else if (ct.indexOf('application/json') >= 0 || ct.indexOf('text/plain') >= 0 || (typeof body === 'string' && (body.indexOf('{') === 0 || body.indexOf('[') === 0))) {
          try {
            var json = typeof body === 'string' ? JSON.parse(body) : body;
            
            function flatten(obj, prefix) {
              if (!obj || typeof obj !== 'object') return;
              for (var key in obj) {
                if (!obj.hasOwnProperty(key)) continue;
                var val = obj[key];
                var name = prefix ? prefix + '.' + key : key;
                
                if (val === null || val === undefined) continue;
                
                if (typeof val === 'object' && !Array.isArray(val)) {
                  flatten(val, name);
                } else if (Array.isArray(val)) {
                  val.forEach(function(item, i) {
                    if (item && typeof item === 'object') flatten(item, name + '[' + i + ']');
                    else {
                      var safe = sanitizeValue(String(item), null, name, 'text');
                      if (safe.value !== null) values[name + '[' + i + ']'] = safe.value;
                    }
                  });
                } else {
                  if (shouldSkipNetworkField(key)) continue;
                  var safe = sanitizeValue(String(val), null, name, 'text');
                  if (safe.value !== null) values[name] = safe.value;
                  if (key === 'event' && !values['_event_name']) values['_event_name'] = val;
                }
              }
            }

            if (Array.isArray(json)) {
              json.forEach(function(item, i) { flatten(item, 'item[' + i + ']'); });
            } else {
              flatten(json, '');
            }
          } catch(e) {}
        } else if (typeof body === 'string' && ct.indexOf('application/x-www-form-urlencoded') >= 0) {
          try {
             if (typeof URLSearchParams !== 'undefined') {
               var p = new URLSearchParams(body);
               p.forEach(function(val, key){
                 if (shouldSkipNetworkField(key)) return;
                 var safe = sanitizeValue(String(val || ''), null, key, 'text');
                 if (safe.value !== null) values[key] = safe.value;
               });
             }
          } catch(e) {}
        }
      } catch(e) {}
      return values;
    }

    function emitNetworkFormSubmit(url, method, values) {
      try {
        if (isAnalyticsRequest(url)) return; // Skip analytics tracking requests
        var keys = Object.keys(values || {});
        if (!keys.length) return;
        
        var eventType = 'form_submit';
        if (values._event_name) {
          eventType = values._event_name;
          delete values._event_name;
        }

        postEvent(eventType, {
          id: null,
          name: null,
          action: url || null,
          method: (method || 'POST').toUpperCase(),
          filled_fields: Object.keys(values).length,
          form_values: values,
          tracking_method: 'javascript'
        });
      } catch(e) {}
    }

    // Patch fetch
    try {
      var _fetch = window.fetch;
      if (_fetch) {
        window.fetch = function(input, init) {
          try {
            var req = input;
            var url = (typeof req === 'string') ? req : (req && req.url);
            var method = (init && init.method) || (req && req.method) || 'GET';
            var headers = (init && init.headers) || (req && req.headers) || {};
            var contentType = '';
            try {
              if (headers && headers.get) contentType = headers.get('content-type') || '';
              else if (headers && headers['content-type']) contentType = headers['content-type'];
            } catch(e) {}
            var body = init && init.body;
            var values = extractFormValuesFromBody(body, contentType);
            if (method && method.toUpperCase() !== 'GET') {
              emitNetworkFormSubmit(url, method, values);
            }
          } catch(e) {}
          return _fetch.apply(this, arguments);
        };
      }
    } catch(e) {}

    // Patch XMLHttpRequest
    try {
      var _open = XMLHttpRequest.prototype.open;
      var _send = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(method, url) {
        this.__cd_method = method;
        this.__cd_url = url;
        return _open.apply(this, arguments);
      };
      XMLHttpRequest.prototype.send = function(body) {
        try {
          var method = this.__cd_method || 'GET';
          if (method.toUpperCase() !== 'GET') {
            var values = extractFormValuesFromBody(body, '');
            emitNetworkFormSubmit(this.__cd_url, method, values);
          }
        } catch(e) {}
        return _send.apply(this, arguments);
      };
    } catch(e) {}

    // Capture FormData submissions triggered via fetch/XHR (Framer often uses this path)
    function extractFormData(fd) {
      try {
        var out = {};
        var filled = 0;
        fd.forEach(function(value, key) {
          if (shouldSkipNetworkField(key)) return; // Use unified skip logic
          var raw = '';
          try {
            raw = (typeof value === 'string') ? value : (value && value.name ? value.name : '');
          } catch(e) {}
          raw = (raw || '').trim();
          if (!raw) return;
          var safe = sanitizeValue(raw, null, key, 'text');
          if (safe.value !== null) {
            filled += 1;
            out[key] = safe.value;
          }
        });
        return { filled: filled, values: out };
      } catch(e) {}
      return { filled: 0, values: {} };
    }

    function emitFormSubmitFromNetwork(url, method, fd) {
      try {
        if (isAnalyticsRequest(url)) return; // Skip analytics tracking requests
        if (!fd) return;
        var collected = extractFormData(fd);
        if (!collected || collected.filled === 0) return;
        postEvent('form_submit', {
          id: null,
          name: null,
          action: url || null,
          method: (method || 'POST').toUpperCase(),
          filled_fields: collected.filled,
          form_values: collected.values,
          tracking_method: 'javascript'
        });
      } catch(e) {}
    }

    // Fetch/XHR overrides removed to prevent infinite recursion

    // Add MutationObserver to watch for dynamically added forms (Framer)
    try {
      var observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
          mutation.addedNodes.forEach(function(node) {
            if (node.nodeType === 1 && node.tagName === 'FORM') {
              node.addEventListener('submit', function(ev) {
                emitFormSubmit(ev.target);
              }, true);
            }
          });
        });
      });
      observer.observe(document.body, { childList: true, subtree: true });
    } catch(e) {}

  } catch (e) {}
})();
"""

        js_content = (
            js_template
            .replace("__TID__", json.dumps(tid or ""))
            .replace("__PAGE_URL__", json.dumps(page_url or ""))
            .replace("__VISIT_ID__", "null")
        )
        # Minify the JavaScript to reduce payload size without changing behavior
        js_minified = jsmin(js_content)
        
        return Response(
            content=js_minified,
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Access-Control-Allow-Origin": "*"
            }
        )
        
    except Exception as e:
        logger.error("JavaScript tracking failed", error=str(e))
        return Response(
            content="/* CrawlDoctor tracking error */",
            media_type="application/javascript"
        )


@router.post("/event")
async def track_event(
    request: Request,
    db: Session = Depends(get_db),
    tid: Optional[str] = Query(None, description="Tracking ID")
):
    """Record granular client-side events (click, scroll, navigation, etc.)."""
    try:
        client_ip = request.client.host
        if not await rate_limiter.is_allowed(client_ip, "event_track"):
            return Response(content="Rate limited", status_code=429)

        # Support both JSON and text/plain bodies
        try:
            payload = await request.json()
        except Exception:
            try:
                body = await request.body()
                payload = json.loads(body.decode('utf-8') or '{}')
            except Exception:
                payload = {}
        event_type = payload.get("event_type")
        page_url = payload.get("page_url")
        referrer = payload.get("referrer")
        data = payload.get("data")
        visit_id = payload.get("visit_id")
        client_id = payload.get("cid")
        client_side_data = payload.get("client_side_data")

        if not event_type:
            raise HTTPException(status_code=400, detail="event_type is required")

        user_agent = request.headers.get("user-agent", "")

        result = await tracking_service.track_event(
            db=db,
            ip_address=client_ip,
            user_agent=user_agent,
            event_type=event_type,
            page_url=page_url,
            referrer=referrer,
            data=data,
            visit_id=visit_id,
            tracking_id=tid,
            client_id=client_id,
            client_side_data=client_side_data,
        )

        return {"status": "tracked", "event_id": result.get("event_id"), "queued": result.get("queued")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Event tracking failed", error=str(e))
        return {"status": "error", "message": str(e)}


@router.get("/status")
async def tracking_status():
    """Health check endpoint for tracking service."""
    return {
        "status": "healthy",
        "service": "tracking",
        "timestamp": datetime.now().isoformat()
    }
