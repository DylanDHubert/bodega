"""
Doc Store - S3-based document storage and state management library.

This library provides a shared API for managing PDF documents through their
processing lifecycle using S3 object tags for state management.

Usage:
    from doc_store import DocumentStore
    
    store = DocumentStore(bucket_name='doc-store-prod')
    raw_docs = store.list_raw_documents()
    store.mark_document_processing(doc_id)
"""

# Phase 2 & 3 Complete - Full API Available
from .document_store import DocumentStore, create_document_store
from .document_states import DocumentState, DocumentStateManager
from .cache import DocumentCache, get_cache, create_cache
from .s3_ops import get_s3_client
from .utils import generate_doc_id, validate_doc_id, timing_context

from .config import DocStoreConfig, get_config, load_config
from .exceptions import (
    DocStoreError,
    S3OperationError,
    DocumentStateError,
    InvalidStateTransitionError,
    DocumentNotFoundError,
    VersionNotFoundError,
    VersionAlreadyExistsError,
    CacheError,
    ConfigurationError,
)

__version__ = "0.3.0"  # Phase 3 Complete
__author__ = "Document Processing Team"

__all__ = [
    # Main API Classes
    "DocumentStore",
    "DocumentState",
    "DocumentStateManager", 
    "DocumentCache",
    
    # Factory Functions
    "create_document_store",
    "create_cache",
    
    # Configuration
    "DocStoreConfig",
    "get_config", 
    "load_config",
    
    # Utilities
    "get_s3_client",
    "get_cache",
    "generate_doc_id",
    "validate_doc_id",
    "timing_context",
    
    # Exceptions
    "DocStoreError",
    "S3OperationError",
    "DocumentStateError",
    "InvalidStateTransitionError",
    "DocumentNotFoundError",
    "VersionNotFoundError",
    "VersionAlreadyExistsError",
    "CacheError",
    "ConfigurationError",
] 