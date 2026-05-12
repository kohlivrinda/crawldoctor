"""Geolocation service for IP address lookup."""
import os
import json
from typing import Optional, Dict, Any
import time
import re
import httpx
import structlog

try:
    import geoip2.database
    import geoip2.errors
    GEOIP2_AVAILABLE = True
except ImportError:
    GEOIP2_AVAILABLE = False

from app.config import settings
from app.utils.rate_limiting import RateLimiter

logger = structlog.get_logger()


class GeoLocationService:
    """Service for IP geolocation using GeoIP2 database."""
    
    def __init__(self):
        self.db_reader = None
        # In-memory cache for IP -> geo result
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl_seconds: int = 24 * 60 * 60  # 24h
        self._cache_last_cleanup: float = time.time()
        self._cache_max_entries: int = 10000
        # Global rate limiter for outbound geo API (ip-api.com)
        self._geo_rate_limiter = RateLimiter()
        self._init_geoip_database()
    
    def _init_geoip_database(self):
        """Initialize GeoIP2 database reader."""
        if not GEOIP2_AVAILABLE:
            logger.warning("GeoIP2 library not available, geolocation will be disabled")
            return
        
        if not settings.geoip_database_path:
            logger.warning("GeoIP database path not configured")
            return
        
        if not os.path.exists(settings.geoip_database_path):
            logger.warning(
                "GeoIP database file not found",
                path=settings.geoip_database_path
            )
            return
        
        try:
            self.db_reader = geoip2.database.Reader(settings.geoip_database_path)
            logger.info("GeoIP2 database initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize GeoIP2 database", error=str(e))
    
    async def get_location_info(self, ip_address: str, category: str = "human") -> Dict[str, Any]:
        """
        Get geographic information for an IP address.
        
        Args:
            ip_address: IP address to lookup
            category: 'human' or 'bot' to prioritize rate budget
            
        Returns:
            Dictionary with location information
        """
        if not ip_address:
            return {}
        
        # Handle local/private IPs
        if self._is_private_ip(ip_address):
            return {
                "country_code": "XX",
                "country_name": "Private/Local",
                "city": "Unknown",
                "latitude": None,
                "longitude": None,
                "timezone": None,
                "isp": "Private Network",
                "organization": "Private Network",
                "asn": None
            }
        
        if not self.db_reader:
            return await self._get_fallback_location_info_async(ip_address, category)
        
        try:
            # Try City database first (most detailed)
            response = self.db_reader.city(ip_address)
            
            location_info = {
                "country_code": response.country.iso_code,
                "country_name": response.country.name,
                "city": response.city.name,
                "latitude": float(response.location.latitude) if response.location.latitude else None,
                "longitude": float(response.location.longitude) if response.location.longitude else None,
                "timezone": response.location.time_zone,
                "accuracy_radius": response.location.accuracy_radius,
                "postal_code": response.postal.code,
                "subdivision": response.subdivisions.most_specific.name if response.subdivisions else None,
                "subdivision_code": response.subdivisions.most_specific.iso_code if response.subdivisions else None
            }
            
            # Try to get ISP information if available
            try:
                isp_response = self.db_reader.isp(ip_address)
                location_info.update({
                    "isp": isp_response.isp,
                    "organization": isp_response.organization,
                    "asn": isp_response.autonomous_system_number,
                    "asn_organization": isp_response.autonomous_system_organization
                })
            except (geoip2.errors.AddressNotFoundError, AttributeError):
                # ISP database might not be available or IP not found
                location_info.update({
                    "isp": None,
                    "organization": None,
                    "asn": None,
                    "asn_organization": None
                })
            
            return location_info
            
        except geoip2.errors.AddressNotFoundError:
            logger.debug("IP address not found in GeoIP database", ip=ip_address)
            return self._get_unknown_location_info()
        except Exception as e:
            logger.error("Error looking up IP location", ip=ip_address, error=str(e))
            return await self._get_fallback_location_info_async(ip_address, category)
    
    def _is_private_ip(self, ip_address: str) -> bool:
        """Check if IP address is private/local."""
        import ipaddress
        
        try:
            ip = ipaddress.ip_address(ip_address)
            return ip.is_private or ip.is_loopback or ip.is_link_local
        except ValueError:
            return False
    
    async def _get_fallback_location_info_async(self, ip_address: str, category: str = "human") -> Dict[str, Any]:
        """Get fallback location info using external API when GeoIP is unavailable.

        Primary: ip-api.com (free) with 40 rpm limit and in-memory caching.
        Secondary: ipapi.co if ip-api.com fails.
        """
        # Cache lookup
        cached = self._cache_get(ip_address)
        if cached is not None:
            return cached

        # Enforce global 40 rpm limit for outgoing requests
        # For bots, skip external calls entirely to preserve budget
        if (category or "human").lower() != "human":
            return self._get_unknown_location_info()
        try:
            # Separate budgets for humans vs bots to avoid starving humans
            action = "geo_ip_api_human"
            limit = 80
            allowed = await self._geo_rate_limiter.is_allowed(
                identifier="global", action=action, limit=limit, window=60
            )
        except Exception:
            allowed = True

        if not allowed:
            return self._get_unknown_location_info()

        # Attempt ip-api.com (free tier is HTTP only)
        try:
            timeout = httpx.Timeout(1.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                url = f"http://ip-api.com/json/{ip_address}?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,asname"
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "success":
                        asn_number = None
                        try:
                            m = re.match(r"^AS(\\d+)", data.get("as") or "")
                            if m:
                                asn_number = int(m.group(1))
                        except Exception:
                            asn_number = None
                        mapped = {
                            "country_code": data.get("countryCode"),
                            "country_name": data.get("country"),
                            "city": data.get("city"),
                            "latitude": data.get("lat"),
                            "longitude": data.get("lon"),
                            "timezone": data.get("timezone"),
                            "isp": data.get("isp"),
                            "organization": data.get("org") or data.get("asname"),
                            "asn": asn_number,
                            "accuracy_radius": None,
                            "postal_code": data.get("zip"),
                            "subdivision": data.get("regionName"),
                            "subdivision_code": data.get("region"),
                            "asn_organization": data.get("asname") or data.get("org")
                        }
                        self._cache_set(ip_address, mapped)
                        return mapped
        except Exception as e:
            logger.debug("ip-api.com lookup failed", error=str(e))

        # Secondary fallback: ipapi.co
        try:
            timeout = httpx.Timeout(1.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                url = f"https://ipapi.co/{ip_address}/json/"
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    mapped = {
                        "country_code": data.get("country_code"),
                        "country_name": data.get("country_name"),
                        "city": data.get("city"),
                        "latitude": data.get("latitude"),
                        "longitude": data.get("longitude"),
                        "timezone": data.get("timezone"),
                        "isp": data.get("org"),
                        "organization": data.get("org"),
                        "asn": data.get("asn"),
                        "accuracy_radius": None,
                        "postal_code": data.get("postal"),
                        "subdivision": data.get("region"),
                        "subdivision_code": data.get("region_code"),
                        "asn_organization": data.get("asn")
                    }
                    self._cache_set(ip_address, mapped)
                    return mapped
        except Exception as e:
            logger.debug("ipapi.co lookup failed", error=str(e))

        return self._get_unknown_location_info()

    def _cache_get(self, ip_address: str) -> Optional[Dict[str, Any]]:
        """Get cached geo info if not expired."""
        try:
            now = time.time()
            # Periodic cleanup
            if now - self._cache_last_cleanup > 300:
                self._cleanup_cache(now)
            entry = self._cache.get(ip_address)
            if not entry:
                return None
            if entry.get("_expires_at", 0) < now:
                # Expired
                del self._cache[ip_address]
                return None
            return entry.get("data")
        except Exception:
            return None

    def _cache_set(self, ip_address: str, data: Dict[str, Any]):
        """Store geo info with TTL; enforce max entries."""
        try:
            now = time.time()
            self._cache[ip_address] = {"data": data, "_expires_at": now + self._cache_ttl_seconds}
            if len(self._cache) > self._cache_max_entries:
                self._cleanup_cache(now, aggressive=True)
        except Exception:
            pass

    def _cleanup_cache(self, now: Optional[float] = None, aggressive: bool = False):
        """Cleanup expired entries; if aggressive, drop oldest half."""
        try:
            self._cache_last_cleanup = now or time.time()
            # Remove expired
            expired_keys = [k for k, v in self._cache.items() if v.get("_expires_at", 0) < self._cache_last_cleanup]
            for k in expired_keys:
                self._cache.pop(k, None)
            if aggressive and len(self._cache) > self._cache_max_entries:
                # Drop roughly half of remaining entries (arbitrary order)
                to_drop = list(self._cache.keys())[: len(self._cache) // 2]
                for k in to_drop:
                    self._cache.pop(k, None)
        except Exception:
            pass
    
    def _get_unknown_location_info(self) -> Dict[str, Any]:
        """Return unknown location info structure."""
        return {
            "country_code": None,
            "country_name": None,
            "city": None,
            "latitude": None,
            "longitude": None,
            "timezone": None,
            "isp": None,
            "organization": None,
            "asn": None,
            "accuracy_radius": None,
            "postal_code": None,
            "subdivision": None,
            "subdivision_code": None,
            "asn_organization": None
        }
    
    async def bulk_lookup(self, ip_addresses: list[str]) -> Dict[str, Dict[str, Any]]:
        """
        Perform bulk IP geolocation lookup.
        
        Args:
            ip_addresses: List of IP addresses to lookup
            
        Returns:
            Dictionary mapping IP addresses to location info
        """
        results = {}
        
        for ip in ip_addresses:
            try:
                results[ip] = await self.get_location_info(ip)
            except Exception as e:
                logger.error("Error in bulk lookup", ip=ip, error=str(e))
                results[ip] = self._get_unknown_location_info()
        
        return results
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get information about the loaded GeoIP database."""
        if not self.db_reader:
            return {
                "available": False,
                "reason": "Database not loaded"
            }
        
        try:
            metadata = self.db_reader.metadata()
            return {
                "available": True,
                "database_type": metadata.database_type,
                "build_epoch": metadata.build_epoch,
                "description": metadata.description.get('en', 'No description'),
                "languages": list(metadata.languages),
                "node_count": metadata.node_count,
                "record_size": metadata.record_size
            }
        except Exception as e:
            return {
                "available": True,
                "error": str(e)
            }
    
    def close(self):
        """Close the GeoIP database reader."""
        if self.db_reader:
            try:
                self.db_reader.close()
                logger.info("GeoIP database closed")
            except Exception as e:
                logger.error("Error closing GeoIP database", error=str(e))
