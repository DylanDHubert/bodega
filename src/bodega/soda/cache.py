"""
Caching layer for doc_store library.

This module provides in-memory and optional Redis-based caching for 
read-heavy operations to improve performance.
"""

import json
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from loguru import logger

from .config import get_config
from .exceptions import CacheError, CacheConnectionError
from .utils import timing_context


class CacheBackend(ABC):
    """Abstract base class for cache backends."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        pass
    
    @abstractmethod
    def clear(self) -> bool:
        """Clear all cache entries."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        pass


class MemoryCache(CacheBackend):
    """In-memory cache implementation."""
    
    def __init__(self, default_ttl: int = 3600):
        """
        Initialize memory cache.
        
        Args:
            default_ttl: Default TTL in seconds
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.default_ttl = default_ttl
        logger.info("Memory cache initialized")
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired."""
        if entry.get('expires_at') is None:
            return False
        return datetime.utcnow().timestamp() > entry['expires_at']
    
    def _cleanup_expired(self) -> None:
        """Remove expired entries."""
        expired_keys = []
        for key, entry in self.cache.items():
            if self._is_expired(entry):
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from memory cache."""
        entry = self.cache.get(key)
        if not entry:
            return None
        
        if self._is_expired(entry):
            del self.cache[key]
            return None
        
        entry['last_accessed'] = datetime.utcnow().timestamp()
        return entry['value']
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in memory cache."""
        try:
            ttl = ttl or self.default_ttl
            expires_at = datetime.utcnow().timestamp() + ttl if ttl > 0 else None
            
            self.cache[key] = {
                'value': value,
                'created_at': datetime.utcnow().timestamp(),
                'last_accessed': datetime.utcnow().timestamp(),
                'expires_at': expires_at
            }
            
            # Periodic cleanup
            if len(self.cache) % 100 == 0:
                self._cleanup_expired()
            
            return True
        except Exception as e:
            logger.error(f"Failed to set cache key {key}: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from memory cache."""
        if key in self.cache:
            del self.cache[key]
            return True
        return False
    
    def clear(self) -> bool:
        """Clear all memory cache entries."""
        self.cache.clear()
        logger.info("Memory cache cleared")
        return True
    
    def exists(self, key: str) -> bool:
        """Check if key exists in memory cache."""
        entry = self.cache.get(key)
        if not entry:
            return False
        
        if self._is_expired(entry):
            del self.cache[key]
            return False
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self.cache)
        total_size = sum(len(str(entry['value'])) for entry in self.cache.values())
        
        # Count expired entries
        expired_count = sum(1 for entry in self.cache.values() if self._is_expired(entry))
        
        return {
            'backend': 'memory',
            'total_entries': total_entries,
            'expired_entries': expired_count,
            'active_entries': total_entries - expired_count,
            'total_size_bytes': total_size,
            'default_ttl': self.default_ttl
        }


class RedisCache(CacheBackend):
    """Redis-based cache implementation."""
    
    def __init__(self, redis_url: str, default_ttl: int = 3600):
        """
        Initialize Redis cache.
        
        Args:
            redis_url: Redis connection URL
            default_ttl: Default TTL in seconds
        """
        try:
            import redis
            self.redis = redis.from_url(redis_url)
            self.default_ttl = default_ttl
            
            # Test connection
            self.redis.ping()
            logger.info(f"Redis cache initialized: {redis_url}")
            
        except ImportError:
            raise CacheConnectionError("Redis package not installed. Install with: pip install redis")
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to Redis: {str(e)}")
    
    def _serialize(self, value: Any) -> str:
        """Serialize value for Redis storage."""
        return json.dumps(value, default=str)
    
    def _deserialize(self, value: str) -> Any:
        """Deserialize value from Redis storage."""
        return json.loads(value)
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from Redis cache."""
        try:
            value = self.redis.get(key)
            if value is None:
                return None
            return self._deserialize(value.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to get Redis key {key}: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in Redis cache."""
        try:
            ttl = ttl or self.default_ttl
            serialized = self._serialize(value)
            
            if ttl > 0:
                return self.redis.setex(key, ttl, serialized)
            else:
                return self.redis.set(key, serialized)
        except Exception as e:
            logger.error(f"Failed to set Redis key {key}: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from Redis cache."""
        try:
            return bool(self.redis.delete(key))
        except Exception as e:
            logger.error(f"Failed to delete Redis key {key}: {str(e)}")
            return False
    
    def clear(self) -> bool:
        """Clear all Redis cache entries."""
        try:
            self.redis.flushdb()
            logger.info("Redis cache cleared")
            return True
        except Exception as e:
            logger.error(f"Failed to clear Redis cache: {str(e)}")
            return False
    
    def exists(self, key: str) -> bool:
        """Check if key exists in Redis cache."""
        try:
            return bool(self.redis.exists(key))
        except Exception as e:
            logger.error(f"Failed to check Redis key {key}: {str(e)}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics."""
        try:
            info = self.redis.info()
            return {
                'backend': 'redis',
                'used_memory': info.get('used_memory', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'default_ttl': self.default_ttl
            }
        except Exception as e:
            logger.error(f"Failed to get Redis stats: {str(e)}")
            return {'backend': 'redis', 'error': str(e)}


class DocumentCache:
    """
    High-level document caching interface.
    
    Provides caching for document content and metadata to optimize
    read-heavy operations in the main_app.
    """
    
    def __init__(self, backend: Optional[CacheBackend] = None):
        """
        Initialize document cache.
        
        Args:
            backend: Cache backend (auto-configured if None)
        """
        if backend is None:
            backend = self._create_default_backend()
        
        self.backend = backend
        self.hit_count = 0
        self.miss_count = 0
        self.start_time = datetime.utcnow()
        
        logger.info(f"DocumentCache initialized with {backend.__class__.__name__}")
    
    def _create_default_backend(self) -> CacheBackend:
        """Create default cache backend based on configuration."""
        config = get_config()
        
        if config.enable_redis_cache and config.redis_url:
            try:
                return RedisCache(config.redis_url, config.cache_ttl_seconds)
            except CacheConnectionError as e:
                logger.warning(f"Failed to initialize Redis cache, falling back to memory: {str(e)}")
        
        return MemoryCache(config.cache_ttl_seconds)
    
    def _make_key(self, prefix: str, *args: str) -> str:
        """Create cache key with prefix."""
        return f"doc_store:{prefix}:" + ":".join(args)
    
    def get_document_content(self, doc_id: str) -> Optional[Dict[str, str]]:
        """
        Get cached document content.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Document content dict or None if not cached
        """
        key = self._make_key("content", doc_id)
        content = self.backend.get(key)
        
        if content is not None:
            self.hit_count += 1
            logger.debug(f"Cache HIT for document content: {doc_id}")
            return content
        else:
            self.miss_count += 1
            logger.debug(f"Cache MISS for document content: {doc_id}")
            return None
    
    def set_document_content(self, doc_id: str, content: Dict[str, str], ttl: Optional[int] = None) -> bool:
        """
        Cache document content.
        
        Args:
            doc_id: Document ID
            content: Document content dict
            ttl: TTL in seconds (optional)
            
        Returns:
            True if cached successfully
        """
        key = self._make_key("content", doc_id)
        success = self.backend.set(key, content, ttl)
        
        if success:
            logger.debug(f"Cached document content: {doc_id}")
        
        return success
    
    def get_document_list(self, list_type: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached document list.
        
        Args:
            list_type: Type of list (e.g., 'final', 'draft', 'raw')
            
        Returns:
            Document list or None if not cached
        """
        key = self._make_key("list", list_type)
        doc_list = self.backend.get(key)
        
        if doc_list is not None:
            self.hit_count += 1
            logger.debug(f"Cache HIT for document list: {list_type}")
            return doc_list
        else:
            self.miss_count += 1
            logger.debug(f"Cache MISS for document list: {list_type}")
            return None
    
    def set_document_list(self, list_type: str, doc_list: List[Dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """
        Cache document list.
        
        Args:
            list_type: Type of list
            doc_list: List of documents
            ttl: TTL in seconds (optional)
            
        Returns:
            True if cached successfully
        """
        key = self._make_key("list", list_type)
        success = self.backend.set(key, doc_list, ttl)
        
        if success:
            logger.debug(f"Cached document list: {list_type} ({len(doc_list)} items)")
        
        return success
    
    def get_system_health(self) -> Optional[Dict[str, Any]]:
        """Get cached system health info."""
        key = self._make_key("health", "system")
        health = self.backend.get(key)
        
        if health is not None:
            self.hit_count += 1
            logger.debug("Cache HIT for system health")
            return health
        else:
            self.miss_count += 1
            logger.debug("Cache MISS for system health")
            return None
    
    def set_system_health(self, health: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Cache system health info."""
        key = self._make_key("health", "system")
        # Use shorter TTL for health info
        health_ttl = ttl or 300  # 5 minutes default
        success = self.backend.set(key, health, health_ttl)
        
        if success:
            logger.debug("Cached system health")
        
        return success
    
    def invalidate_document(self, doc_id: str) -> bool:
        """
        Invalidate cached data for a specific document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            True if invalidated successfully
        """
        content_key = self._make_key("content", doc_id)
        success = self.backend.delete(content_key)
        
        # Also invalidate lists since they might contain this document
        self.invalidate_lists()
        
        if success:
            logger.debug(f"Invalidated cache for document: {doc_id}")
        
        return success
    
    def invalidate_lists(self) -> bool:
        """Invalidate all cached document lists."""
        list_types = ['final', 'draft', 'raw', 'processing', 'failed']
        success_count = 0
        
        for list_type in list_types:
            key = self._make_key("list", list_type)
            if self.backend.delete(key):
                success_count += 1
        
        logger.debug(f"Invalidated {success_count} document lists")
        return success_count > 0
    
    def invalidate_all(self) -> bool:
        """Clear all cache entries."""
        success = self.backend.clear()
        if success:
            self.hit_count = 0
            self.miss_count = 0
            logger.info("Invalidated all cache entries")
        return success
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache performance statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        total_requests = self.hit_count + self.miss_count
        hit_rate = (self.hit_count / total_requests) if total_requests > 0 else 0
        uptime = (datetime.utcnow() - self.start_time).total_seconds()
        
        stats = {
            'hits': self.hit_count,
            'misses': self.miss_count,
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'uptime_seconds': uptime,
            'requests_per_second': total_requests / uptime if uptime > 0 else 0
        }
        
        # Add backend-specific stats
        backend_stats = self.backend.get_stats()
        stats.update(backend_stats)
        
        return stats


# Global cache instance
_cache_instance: Optional[DocumentCache] = None


def get_cache() -> DocumentCache:
    """
    Get the global document cache instance.
    
    Returns:
        DocumentCache instance
    """
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = DocumentCache()
    return _cache_instance


def create_cache(backend: Optional[CacheBackend] = None) -> DocumentCache:
    """
    Create a new document cache instance.
    
    Args:
        backend: Optional cache backend
        
    Returns:
        DocumentCache instance
    """
    return DocumentCache(backend)


def clear_global_cache() -> bool:
    """
    Clear the global cache instance.
    
    Returns:
        True if cleared successfully
    """
    global _cache_instance
    if _cache_instance is not None:
        success = _cache_instance.invalidate_all()
        _cache_instance = None
        return success
    return True 