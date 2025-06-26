"""
Bodega - Full RAG Processing Pipeline

A complete document processing pipeline that combines PDF parsing, 
enhancement, JSON extraction, and AWS storage with state management.

Components:
- pbj: PDF processing pipeline (Peanut, Butter, Jelly, Toast)
- soda: AWS document storage and state management
"""

# Import main components from pbj (nested src structure)
from .pbj.src.pbj.sandwich import Sandwich
from .pbj.src.pbj.config import create_config, PipelineConfig

# Import main components from soda (doc_store) 
from .soda.doc_store.document_store import DocumentStore, create_document_store
from .soda.doc_store.document_states import DocumentState, DocumentStateManager
from .soda.doc_store.config import DocStoreConfig, get_config as get_soda_config

# Import main Bodega orchestrator
from .bodega import Bodega

__version__ = "0.1.0"
__author__ = "Bodega Team"

__all__ = [
    # Main Bodega orchestrator
    "Bodega",
    
    # PB&J Pipeline components
    "Sandwich",
    "create_config", 
    "PipelineConfig",
    
    # Soda (Document Store) components
    "DocumentStore",
    "create_document_store",
    "DocumentState",
    "DocumentStateManager",
    "DocStoreConfig",
    "get_soda_config",
] 