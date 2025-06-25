"""
Custom exceptions for doc_store library.

This module defines all custom exceptions used throughout the library
for better error handling and debugging.
"""

from typing import Optional, Dict, Any


class DocStoreError(Exception):
    """Base exception for all doc_store related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} (Details: {details_str})"
        return self.message


class ConfigurationError(DocStoreError):
    """Raised when there are configuration-related errors."""
    
    def __init__(self, message: str, config_key: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.config_key = config_key
        details = details or {}
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, details)


class S3OperationError(DocStoreError):
    """Base exception for S3 operation errors."""
    
    def __init__(self, message: str, operation: Optional[str] = None, bucket: Optional[str] = None, 
                 key: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.operation = operation
        self.bucket = bucket
        self.key = key
        details = details or {}
        if operation:
            details["operation"] = operation
        if bucket:
            details["bucket"] = bucket
        if key:
            details["key"] = key
        super().__init__(message, details)


class S3ConnectionError(S3OperationError):
    """Raised when unable to connect to S3."""
    pass


class S3PermissionError(S3OperationError):
    """Raised when S3 operation fails due to insufficient permissions."""
    pass


class S3ObjectNotFoundError(S3OperationError):
    """Raised when trying to access a non-existent S3 object."""
    pass


class S3BucketNotFoundError(S3OperationError):
    """Raised when trying to access a non-existent S3 bucket."""
    pass


class DocumentStateError(DocStoreError):
    """Base exception for document state-related errors."""
    
    def __init__(self, message: str, doc_id: Optional[str] = None, current_state: Optional[str] = None,
                 target_state: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.doc_id = doc_id
        self.current_state = current_state
        self.target_state = target_state
        details = details or {}
        if doc_id:
            details["doc_id"] = doc_id
        if current_state:
            details["current_state"] = current_state
        if target_state:
            details["target_state"] = target_state
        super().__init__(message, details)


class InvalidStateTransitionError(DocumentStateError):
    """Raised when attempting an invalid document state transition."""
    pass


class DocumentNotFoundError(DocumentStateError):
    """Raised when trying to access a document that doesn't exist."""
    pass


class DocumentAlreadyExistsError(DocumentStateError):
    """Raised when trying to create a document that already exists."""
    pass


class DocumentVersionError(DocStoreError):
    """Base exception for document version-related errors."""
    
    def __init__(self, message: str, doc_id: Optional[str] = None, version: Optional[str] = None,
                 details: Optional[Dict[str, Any]] = None):
        self.doc_id = doc_id
        self.version = version
        details = details or {}
        if doc_id:
            details["doc_id"] = doc_id
        if version:
            details["version"] = version
        super().__init__(message, details)


class VersionNotFoundError(DocumentVersionError):
    """Raised when trying to access a document version that doesn't exist."""
    pass


class VersionAlreadyExistsError(DocumentVersionError):
    """Raised when trying to create a version that already exists."""
    pass


class CacheError(DocStoreError):
    """Base exception for caching-related errors."""
    pass


class CacheConnectionError(CacheError):
    """Raised when unable to connect to cache backend (e.g., Redis)."""
    pass


class ValidationError(DocStoreError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None,
                 details: Optional[Dict[str, Any]] = None):
        self.field = field
        self.value = value
        details = details or {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = value
        super().__init__(message, details)


class RetryExhaustedError(DocStoreError):
    """Raised when all retry attempts have been exhausted."""
    
    def __init__(self, message: str, operation: Optional[str] = None, attempts: Optional[int] = None,
                 last_error: Optional[Exception] = None, details: Optional[Dict[str, Any]] = None):
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error
        details = details or {}
        if operation:
            details["operation"] = operation
        if attempts:
            details["attempts"] = attempts
        if last_error:
            details["last_error"] = str(last_error)
        super().__init__(message, details)


class HealthCheckError(DocStoreError):
    """Raised when health check operations fail."""
    pass


class AtomicOperationError(DocStoreError):
    """Raised when atomic operations fail and need rollback."""
    
    def __init__(self, message: str, operation: Optional[str] = None, partial_state: Optional[Dict[str, Any]] = None,
                 details: Optional[Dict[str, Any]] = None):
        self.operation = operation
        self.partial_state = partial_state or {}
        details = details or {}
        if operation:
            details["operation"] = operation
        if partial_state:
            details["partial_state"] = partial_state
        super().__init__(message, details) 