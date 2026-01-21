"""Rate limiting utilities for API protection."""
import time
from typing import Dict, Optional
import redis
import structlog

from app.config import settings

logger = structlog.get_logger()


class RateLimiter:
    """Redis-based rate limiter for API endpoints."""
    
    def __init__(self):
        self.redis_client = None
        self.memory_cache = {}  # Fallback when Redis is unavailable
        self._init_redis()
    
    def _init_redis(self):
        """Initialize Redis connection."""
        try:
            if settings.redis_url.startswith("memory://"):
                logger.info("Redis disabled; using memory-based rate limiting")
                self.redis_client = None
                return
            self.redis_client = redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Redis rate limiter initialized successfully")
        except Exception as e:
            logger.warning("Redis not available, using memory-based rate limiting", error=str(e))
            self.redis_client = None
    
    async def is_allowed(
        self,
        identifier: str,
        action: str,
        limit: Optional[int] = None,
        window: Optional[int] = None
    ) -> bool:
        """
        Check if action is allowed for the given identifier.
        
        Args:
            identifier: Unique identifier (IP, user ID, etc.)
            action: Action being performed (pixel_track, beacon_track, etc.)
            limit: Custom rate limit (requests per window)
            window: Custom time window in seconds
            
        Returns:
            True if action is allowed, False if rate limited
        """
        # Use default limits if not specified
        if limit is None:
            limit = settings.rate_limit_requests
        if window is None:
            window = settings.rate_limit_window
        
        # Create rate limit key
        key = f"rate_limit:{action}:{identifier}"
        current_time = int(time.time())
        window_start = current_time - (current_time % window)
        
        try:
            if self.redis_client:
                return await self._check_redis_limit(key, window_start, window, limit)
            else:
                return self._check_memory_limit(key, window_start, window, limit)
        except Exception as e:
            logger.error("Rate limiting error", error=str(e))
            # Allow request if rate limiting fails
            return True
    
    async def _check_redis_limit(
        self,
        key: str,
        window_start: int,
        window: int,
        limit: int
    ) -> bool:
        """Check rate limit using Redis."""
        try:
            # Use Redis pipeline for atomic operations
            pipe = self.redis_client.pipeline()
            
            # Remove old entries
            pipe.zremrangebyscore(key, 0, window_start - window)
            
            # Count current requests
            pipe.zcard(key)
            
            # Add current request
            pipe.zadd(key, {str(time.time()): time.time()})
            
            # Set expiration
            pipe.expire(key, window * 2)
            
            results = pipe.execute()
            current_count = results[1]
            
            return current_count < limit
            
        except Exception as e:
            logger.error("Redis rate limiting error", error=str(e))
            return True
    
    def _check_memory_limit(
        self,
        key: str,
        window_start: int,
        window: int,
        limit: int
    ) -> bool:
        """Check rate limit using memory cache (fallback)."""
        try:
            current_time = time.time()
            
            # Clean up old entries
            if key not in self.memory_cache:
                self.memory_cache[key] = []
            
            # Remove old timestamps
            cutoff_time = current_time - window
            self.memory_cache[key] = [
                timestamp for timestamp in self.memory_cache[key]
                if timestamp > cutoff_time
            ]
            
            # Check if under limit
            if len(self.memory_cache[key]) >= limit:
                return False
            
            # Add current request
            self.memory_cache[key].append(current_time)
            
            # Cleanup memory cache periodically
            if len(self.memory_cache) > 10000:
                self._cleanup_memory_cache()
            
            return True
            
        except Exception as e:
            logger.error("Memory rate limiting error", error=str(e))
            return True
    
    def _cleanup_memory_cache(self):
        """Clean up old entries from memory cache."""
        try:
            current_time = time.time()
            cutoff_time = current_time - (settings.rate_limit_window * 2)
            
            keys_to_remove = []
            for key, timestamps in self.memory_cache.items():
                # Remove old timestamps
                new_timestamps = [
                    timestamp for timestamp in timestamps
                    if timestamp > cutoff_time
                ]
                
                if new_timestamps:
                    self.memory_cache[key] = new_timestamps
                else:
                    keys_to_remove.append(key)
            
            # Remove empty entries
            for key in keys_to_remove:
                del self.memory_cache[key]
                
            logger.debug(f"Cleaned up {len(keys_to_remove)} old rate limit entries")
            
        except Exception as e:
            logger.error("Memory cache cleanup error", error=str(e))
    
    async def get_limit_info(
        self,
        identifier: str,
        action: str,
        limit: Optional[int] = None,
        window: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Get current rate limit information for identifier.
        
        Returns:
            Dictionary with limit info (remaining, reset_time, etc.)
        """
        if limit is None:
            limit = settings.rate_limit_requests
        if window is None:
            window = settings.rate_limit_window
        
        key = f"rate_limit:{action}:{identifier}"
        current_time = int(time.time())
        window_start = current_time - (current_time % window)
        
        try:
            if self.redis_client:
                # Count current requests
                current_count = self.redis_client.zcard(key)
            else:
                # Use memory cache
                if key not in self.memory_cache:
                    current_count = 0
                else:
                    cutoff_time = time.time() - window
                    current_count = len([
                        t for t in self.memory_cache[key] if t > cutoff_time
                    ])
            
            remaining = max(0, limit - current_count)
            reset_time = window_start + window
            
            return {
                "limit": limit,
                "remaining": remaining,
                "reset_time": reset_time,
                "window": window
            }
            
        except Exception as e:
            logger.error("Error getting rate limit info", error=str(e))
            return {
                "limit": limit,
                "remaining": limit,
                "reset_time": current_time + window,
                "window": window
            }
    
    async def reset_limit(self, identifier: str, action: str):
        """Reset rate limit for specific identifier and action."""
        key = f"rate_limit:{action}:{identifier}"
        
        try:
            if self.redis_client:
                self.redis_client.delete(key)
            else:
                if key in self.memory_cache:
                    del self.memory_cache[key]
            
            logger.info(f"Rate limit reset for {key}")
            
        except Exception as e:
            logger.error("Error resetting rate limit", error=str(e))
    
    def close(self):
        """Close Redis connection."""
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis rate limiter closed")
            except Exception as e:
                logger.error("Error closing Redis connection", error=str(e))
