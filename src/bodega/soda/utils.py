"""
Utility functions for doc_store library.

This module contains common helper functions used throughout the library.
"""

import time
import uuid
import hashlib
from typing import Any, Dict, Optional, Callable, TypeVar, Union
from functools import wraps
from datetime import datetime, timedelta

from loguru import logger

from .exceptions import RetryExhaustedError

T = TypeVar('T')


def generate_doc_id(content: Optional[str] = None) -> str:
    """
    Generate a unique document ID.
    
    Args:
        content: Optional content to hash for deterministic ID
        
    Returns:
        str: Unique document ID
    """
    if content:
        # Generate deterministic ID based on content hash
        hash_obj = hashlib.sha256(content.encode('utf-8'))
        return hash_obj.hexdigest()[:16]
    else:
        # Generate random UUID-based ID
        return str(uuid.uuid4()).replace('-', '')[:16]


def validate_doc_id(doc_id: str) -> bool:
    """
    Validate document ID format.
    
    Args:
        doc_id: Document ID to validate
        
    Returns:
        bool: True if valid
    """
    if not doc_id or not isinstance(doc_id, str):
        return False
    
    # Should be alphanumeric, 16 characters long
    return len(doc_id) == 16 and doc_id.isalnum()


def validate_version(version: str) -> bool:
    """
    Validate version format.
    
    Args:
        version: Version string to validate (e.g., "v1", "v2")
        
    Returns:
        bool: True if valid
    """
    if not version or not isinstance(version, str):
        return False
    
    # Should be "v" followed by a number
    if not version.startswith('v'):
        return False
    
    try:
        int(version[1:])
        return True
    except ValueError:
        return False


def parse_version_number(version: str) -> int:
    """
    Parse version string to number.
    
    Args:
        version: Version string (e.g., "v1")
        
    Returns:
        int: Version number
        
    Raises:
        ValueError: If version format is invalid
    """
    if not validate_version(version):
        raise ValueError(f"Invalid version format: {version}")
    
    return int(version[1:])


def format_version(version_num: int) -> str:
    """
    Format version number to string.
    
    Args:
        version_num: Version number
        
    Returns:
        str: Formatted version string (e.g., "v1")
    """
    return f"v{version_num}"


def sanitize_s3_key(key: str) -> str:
    """
    Sanitize S3 key to ensure it's valid.
    
    Args:
        key: S3 key to sanitize
        
    Returns:
        str: Sanitized S3 key
    """
    # Remove leading/trailing slashes and spaces
    key = key.strip().strip('/')
    
    # Replace problematic characters
    replacements = {
        ' ': '_',
        '\\': '/',
        '../': '',
        './': '',
    }
    
    for old, new in replacements.items():
        key = key.replace(old, new)
    
    # Ensure no double slashes
    while '//' in key:
        key = key.replace('//', '/')
    
    return key


def build_s3_key(doc_id: str, *parts: str) -> str:
    """
    Build S3 key from components.
    
    Args:
        doc_id: Document ID
        *parts: Additional path components
        
    Returns:
        str: Complete S3 key
    """
    key_parts = [doc_id] + list(parts)
    key = '/'.join(str(part) for part in key_parts if part)
    return sanitize_s3_key(key)


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts
        base_delay: Initial delay between attempts (seconds)
        max_delay: Maximum delay between attempts (seconds)
        backoff_factor: Exponential backoff multiplier
        exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_attempts - 1:
                        # Last attempt failed
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts: {str(e)}")
                        raise RetryExhaustedError(
                            f"Function {func.__name__} failed after {max_attempts} attempts",
                            operation=func.__name__,
                            attempts=max_attempts,
                            last_error=e
                        )
                    
                    # Calculate delay for next attempt
                    delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                    
                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt + 1}/{max_attempts}: {str(e)}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def timing_context(operation_name: str) -> 'TimingContext':
    """
    Create a timing context manager for performance measurement.
    
    Args:
        operation_name: Name of the operation being timed
        
    Returns:
        TimingContext: Context manager for timing
    """
    return TimingContext(operation_name)


class TimingContext:
    """Context manager for timing operations."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        logger.debug(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            logger.info(f"Operation '{self.operation_name}' completed in {duration:.3f}s")
        else:
            logger.error(f"Operation '{self.operation_name}' failed after {duration:.3f}s: {exc_val}")
    
    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the operation."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


def chunk_list(lst: list, chunk_size: int) -> list:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        list: List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def is_expired(timestamp: datetime, ttl_seconds: int) -> bool:
    """
    Check if a timestamp is expired based on TTL.
    
    Args:
        timestamp: Timestamp to check
        ttl_seconds: Time-to-live in seconds
        
    Returns:
        bool: True if expired
    """
    expiry_time = timestamp + timedelta(seconds=ttl_seconds)
    return datetime.utcnow() > expiry_time


def safe_get(dictionary: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary with nested key support.
    
    Args:
        dictionary: Dictionary to search
        key: Key or nested key path (e.g., "a.b.c")
        default: Default value if key not found
        
    Returns:
        Value from dictionary or default
    """
    try:
        keys = key.split('.')
        value = dictionary
        for k in keys:
            value = value[k]
        return value
    except (KeyError, TypeError, AttributeError):
        return default


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        str: Formatted size (e.g., "1.5 MB")
    """
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.1f} {units[unit_index]}"


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate string if it exceeds maximum length.
    
    Args:
        text: Text to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncating
        
    Returns:
        str: Truncated string
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix 