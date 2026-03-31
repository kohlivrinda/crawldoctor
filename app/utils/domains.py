"""Internal domain classification for Maxim/Bifrost unified session tracking."""

from urllib.parse import urlparse
from typing import Optional

# Internal domain families — matched by exact hostname or suffix
_INTERNAL_DOMAINS = {
    "maxim": {
        "exact": {"getmaxim.ai"},
        "suffix": ".getmaxim.ai",
    },
    "bifrost": {
        "exact": {"getbifrost.ai"},
        "suffix": ".getbifrost.ai",
    },
}


def _normalize_hostname(raw: str) -> str:
    """Lowercase and strip port from a hostname string."""
    h = raw.strip().lower()
    # Strip port if present (but not IPv6 brackets)
    if h and not h.startswith("[") and ":" in h:
        h = h.rsplit(":", 1)[0]
    return h


def classify_domain(hostname: Optional[str]) -> Optional[str]:
    """Return the internal family name ('maxim' | 'bifrost') or None if external.

    Uses normalized hostname matching — never substring contains.
    """
    if not hostname:
        return None

    h = _normalize_hostname(hostname)
    if not h:
        return None

    for family, rules in _INTERNAL_DOMAINS.items():
        if h in rules["exact"]:
            return family
        if h.endswith(rules["suffix"]):
            return family

    return None


def is_internal_domain(hostname: Optional[str]) -> bool:
    """Return True when *hostname* belongs to any internal domain family."""
    return classify_domain(hostname) is not None


def is_internal_url(url: Optional[str]) -> bool:
    """Return True when the host portion of *url* is an internal domain."""
    if not url:
        return False
    try:
        return is_internal_domain(urlparse(url).netloc)
    except Exception:
        return False
