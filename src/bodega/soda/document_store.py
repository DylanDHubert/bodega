"""
High-level DocumentStore API for doc_store library.

This module provides the main DocumentStore class that serves as the primary
interface for document storage and state management operations.
"""

import json
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

from .config import get_config
from .s3_ops import (
    get_s3_client,
    put_object_content,
    get_object_content,
    object_exists,
    list_objects_with_prefix,
    delete_object
)
from .document_states import (
    DocumentState,
    DocumentStateManager,
    get_document_state as get_state,
    transition_document_state as transition_state
)
from .exceptions import (
    DocumentNotFoundError,
    DocumentAlreadyExistsError,
    DocumentStateError,
    InvalidStateTransitionError,
    VersionNotFoundError,
    VersionAlreadyExistsError,
    S3OperationError
)
from .utils import (
    generate_doc_id,
    validate_doc_id,
    build_s3_key,
    timing_context,
    format_file_size
)
from .cache import DocumentCache, get_cache


class DocumentStore:
    """
    High-level API for document storage and state management.
    
    This class provides a clean interface for all document operations needed
    by the pdf_pipeline, pdf_inspector, and main_app repositories.
    """
    
    def __init__(self, bucket_name: Optional[str] = None, aws_region: Optional[str] = None, cache: Optional[DocumentCache] = None):
        """
        Initialize DocumentStore.
        
        Args:
            bucket_name: S3 bucket name (defaults to config)
            aws_region: AWS region (defaults to config)
            cache: Cache instance (defaults to global cache)
        """
        config = get_config()
        self.bucket = bucket_name or config.get_bucket_name()
        self.aws_region = aws_region or config.aws_region
        self.state_manager = DocumentStateManager(self.bucket)
        self.cache = cache or get_cache()
        
        logger.info(f"DocumentStore initialized for bucket: {self.bucket}")
    
    # ==========================================
    # Raw Document Operations (for pdf_pipeline)
    # ==========================================
    
    def list_raw_documents(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all documents in RAW state awaiting processing.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of document info dictionaries
            
        Example:
            docs = store.list_raw_documents()
            for doc in docs:
                print(f"Doc ID: {doc['doc_id']}, uploaded: {doc['uploaded_at']}")
        """
        try:
            with timing_context("list_raw_documents"):
                raw_docs = []
                documents = self.state_manager.list_documents_by_state(
                    [DocumentState.RAW], 
                    prefix="raw/",
                    max_results=limit
                )
                
                for doc_key, state, tags in documents:
                    # Extract doc_id from key: raw/{doc_id}/original.pdf
                    doc_id = doc_key.split('/')[1] if '/' in doc_key else doc_key
                    
                    doc_info = {
                        'doc_id': doc_id,
                        'doc_key': doc_key,
                        'state': state.value,
                        'uploaded_at': tags.get('state_changed_at'),
                        'original_filename': tags.get('original_filename', 'unknown'),
                        'file_size': tags.get('file_size', 'unknown'),
                        'tags': tags
                    }
                    raw_docs.append(doc_info)
                
                logger.info(f"Found {len(raw_docs)} raw documents")
                return raw_docs
                
        except Exception as e:
            logger.error(f"Failed to list raw documents: {str(e)}")
            raise DocumentStateError(f"Failed to list raw documents: {str(e)}")
    
    def mark_document_processing(self, doc_id: str, processor_info: Optional[Dict[str, str]] = None) -> bool:
        """
        Mark a document as currently being processed.
        
        Args:
            doc_id: Document ID
            processor_info: Optional info about the processor
            
        Returns:
            True if successful
            
        Raises:
            DocumentNotFoundError: If document doesn't exist
            InvalidStateTransitionError: If not in RAW state
            
        Example:
            success = store.mark_document_processing(
                "abc123", 
                {"processor": "gpu-worker-1", "started_by": "pipeline_v2"}
            )
        """
        try:
            with timing_context(f"mark_document_processing(doc_id={doc_id})"):
                doc_key = f"raw/{doc_id}/original.pdf"
                
                metadata = processor_info or {}
                metadata['processing_started_at'] = datetime.utcnow().isoformat() + 'Z'
                
                success = self.state_manager.transition_document_state(
                    doc_key,
                    DocumentState.PROCESSING,
                    metadata=metadata
                )
                
                if success:
                    logger.info(f"Document {doc_id} marked as processing")
                return success
                
        except Exception as e:
            logger.error(f"Failed to mark document {doc_id} as processing: {str(e)}")
            raise
    
    def mark_document_processed(self, doc_id: str, processing_info: Optional[Dict[str, str]] = None) -> bool:
        """
        Mark a document as successfully processed.
        
        Args:
            doc_id: Document ID
            processing_info: Optional info about the processing results
            
        Returns:
            True if successful
            
        Example:
            success = store.mark_document_processed(
                "abc123",
                {"processing_time": "3.2s", "pages": "45", "output_version": "v1"}
            )
        """
        try:
            with timing_context(f"mark_document_processed(doc_id={doc_id})"):
                doc_key = f"raw/{doc_id}/original.pdf"
                
                metadata = processing_info or {}
                metadata['processing_completed_at'] = datetime.utcnow().isoformat() + 'Z'
                
                success = self.state_manager.transition_document_state(
                    doc_key,
                    DocumentState.PROCESSED,
                    metadata=metadata
                )
                
                if success:
                    logger.info(f"Document {doc_id} marked as processed")
                return success
                
        except Exception as e:
            logger.error(f"Failed to mark document {doc_id} as processed: {str(e)}")
            raise
    
    def mark_document_failed(self, doc_id: str, error_msg: str, error_details: Optional[Dict[str, str]] = None) -> bool:
        """
        Mark a document as failed processing.
        
        Args:
            doc_id: Document ID
            error_msg: Error message describing the failure
            error_details: Optional additional error details
            
        Returns:
            True if successful
            
        Example:
            success = store.mark_document_failed(
                "abc123",
                "PDF parsing failed: corrupted file",
                {"error_code": "PARSE_ERROR", "retry_count": "2"}
            )
        """
        try:
            with timing_context(f"mark_document_failed(doc_id={doc_id})"):
                doc_key = f"raw/{doc_id}/original.pdf"
                
                metadata = error_details or {}
                metadata.update({
                    'error_message': error_msg,
                    'failed_at': datetime.utcnow().isoformat() + 'Z'
                })
                
                success = self.state_manager.transition_document_state(
                    doc_key,
                    DocumentState.FAILED,
                    metadata=metadata
                )
                
                if success:
                    logger.warning(f"Document {doc_id} marked as failed: {error_msg}")
                return success
                
        except Exception as e:
            logger.error(f"Failed to mark document {doc_id} as failed: {str(e)}")
            raise
    
    # ==========================================
    # Version Management
    # ==========================================
    
    def get_next_version_number(self, doc_id: str) -> str:
        """
        Get the next version number for a document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Next version string (e.g., "v1", "v2", etc.)
        """
        try:
            with timing_context(f"get_next_version_number(doc_id={doc_id})"):
                # List existing versions
                prefix = f"processed/{doc_id}/"
                objects = list_objects_with_prefix(self.bucket, prefix)
                
                # Find highest version number
                max_version = 0
                for obj in objects:
                    key = obj['Key']
                    # Look for version pattern: processed/{doc_id}/v{n}/
                    parts = key.split('/')
                    if len(parts) >= 3 and parts[2].startswith('v'):
                        try:
                            version_num = int(parts[2][1:])  # Remove 'v' prefix
                            max_version = max(max_version, version_num)
                        except ValueError:
                            continue
                
                next_version = f"v{max_version + 1}"
                logger.debug(f"Next version for document {doc_id}: {next_version}")
                return next_version
                
        except Exception as e:
            logger.error(f"Failed to get next version for document {doc_id}: {str(e)}")
            raise DocumentStateError(f"Failed to get next version: {str(e)}")
    
    def create_document_version(
        self,
        doc_id: str,
        md_content: str,
        json_content: Union[str, Dict[str, Any]],
        version: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Create a new version of processed document outputs.
        
        Args:
            doc_id: Document ID
            md_content: Markdown content
            json_content: JSON content (string or dict)
            version: Version string (auto-generated if None)
            metadata: Optional metadata
            
        Returns:
            Version string that was created
            
        Example:
            version = store.create_document_version(
                "abc123",
                "# Document Title\n\nContent here...",
                {"title": "Document Title", "pages": 45},
                metadata={"creator": "pipeline_v2", "model": "gpt-4"}
            )
        """
        try:
            with timing_context(f"create_document_version(doc_id={doc_id})"):
                # Get version number
                if version is None:
                    version = self.get_next_version_number(doc_id)
                
                # Validate version doesn't exist
                md_key = f"processed/{doc_id}/{version}/output.md"
                if object_exists(self.bucket, md_key):
                    raise VersionAlreadyExistsError(
                        f"Version {version} already exists for document {doc_id}",
                        doc_id=doc_id,
                        version=version
                    )
                
                # Prepare content
                if isinstance(json_content, dict):
                    json_str = json.dumps(json_content, indent=2)
                else:
                    json_str = json_content
                
                # Prepare tags
                tags = {
                    'stage': DocumentState.DRAFT.value,
                    'doc_id': doc_id,
                    'version': version,
                    'created_at': datetime.utcnow().isoformat() + 'Z'
                }
                
                # Add metadata to tags
                if metadata:
                    for key, value in metadata.items():
                        tags[f"meta_{key}"] = value
                
                # Upload both files atomically
                json_key = f"processed/{doc_id}/{version}/output.json"
                
                # Upload markdown file
                md_result = put_object_content(
                    self.bucket,
                    md_key,
                    md_content,
                    content_type='text/markdown',
                    tags=tags
                )
                
                try:
                    # Upload JSON file
                    json_result = put_object_content(
                        self.bucket,
                        json_key,
                        json_str,
                        content_type='application/json',
                        tags=tags
                    )
                    
                    logger.info(f"Created document version {version} for {doc_id}")
                    logger.debug(f"MD size: {format_file_size(md_result['Size'])}, JSON size: {format_file_size(json_result['Size'])}")
                    
                    return version
                    
                except Exception as e:
                    # Rollback: delete the MD file if JSON upload failed
                    try:
                        delete_object(self.bucket, md_key)
                        logger.warning(f"Rolled back MD file after JSON upload failure")
                    except Exception:
                        pass
                    raise
                
        except Exception as e:
            logger.error(f"Failed to create document version: {str(e)}")
            raise
    
    def list_document_versions(self, doc_id: str) -> List[Dict[str, Any]]:
        """
        List all versions of a document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            List of version info dictionaries
            
        Example:
            versions = store.list_document_versions("abc123")
            for v in versions:
                print(f"Version {v['version']}: {v['state']} ({v['created_at']})")
        """
        try:
            with timing_context(f"list_document_versions(doc_id={doc_id})"):
                prefix = f"processed/{doc_id}/"
                objects = list_objects_with_prefix(self.bucket, prefix)
                
                versions = {}
                for obj in objects:
                    key = obj['Key']
                    # Parse: processed/{doc_id}/v{n}/output.{ext}
                    parts = key.split('/')
                    if len(parts) >= 4 and parts[2].startswith('v'):
                        version = parts[2]
                        
                        if version not in versions:
                            versions[version] = {
                                'version': version,
                                'doc_id': doc_id,
                                'created_at': None,
                                'state': None,
                                'has_md': False,
                                'has_json': False,
                                'tags': {}
                            }
                        
                        # Check file type
                        if key.endswith('.md'):
                            versions[version]['has_md'] = True
                        elif key.endswith('.json'):
                            versions[version]['has_json'] = True
                
                # Get tags and state for each version
                version_list = []
                for version_info in versions.values():
                    try:
                        # Get tags from MD file (both files should have same tags)
                        md_key = f"processed/{doc_id}/{version_info['version']}/output.md"
                        if object_exists(self.bucket, md_key):
                            from .s3_ops import get_object_tags
                            tags = get_object_tags(self.bucket, md_key)
                            version_info['tags'] = tags
                            version_info['created_at'] = tags.get('created_at')
                            version_info['state'] = tags.get('stage')
                    except Exception as e:
                        logger.warning(f"Failed to get tags for version {version_info['version']}: {str(e)}")
                    
                    version_list.append(version_info)
                
                # Sort by version number
                version_list.sort(key=lambda x: int(x['version'][1:]) if x['version'][1:].isdigit() else 0)
                
                logger.info(f"Found {len(version_list)} versions for document {doc_id}")
                return version_list
                
        except Exception as e:
            logger.error(f"Failed to list versions for document {doc_id}: {str(e)}")
            raise DocumentStateError(f"Failed to list document versions: {str(e)}")
    
    # ==========================================
    # Draft/Final Operations (for pdf_inspector)
    # ==========================================
    
    def list_draft_documents(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all document versions in DRAFT state awaiting approval.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of draft document info
            
        Example:
            drafts = store.list_draft_documents()
            for draft in drafts:
                print(f"Doc {draft['doc_id']} v{draft['version']}: {draft['created_at']}")
        """
        try:
            with timing_context("list_draft_documents"):
                draft_docs = []
                documents = self.state_manager.list_documents_by_state(
                    [DocumentState.DRAFT],
                    prefix="processed/",
                    max_results=limit * 2  # Get more since we'll filter
                )
                
                for doc_key, state, tags in documents:
                    # Parse: processed/{doc_id}/v{n}/output.{ext}
                    parts = doc_key.split('/')
                    if len(parts) >= 4:
                        doc_id = parts[1]
                        version = parts[2]
                        file_type = parts[3].split('.')[1] if '.' in parts[3] else 'unknown'
                        
                        # Only include MD files to avoid duplicates
                        if file_type == 'md':
                            doc_info = {
                                'doc_id': doc_id,
                                'version': version,
                                'state': state.value,
                                'created_at': tags.get('created_at'),
                                'creator': tags.get('meta_creator'),
                                'model': tags.get('meta_model'),
                                'tags': tags,
                                'md_key': doc_key,
                                'json_key': doc_key.replace('output.md', 'output.json')
                            }
                            draft_docs.append(doc_info)
                            
                            if len(draft_docs) >= limit:
                                break
                
                # Sort by creation date (newest first)
                draft_docs.sort(key=lambda x: x['created_at'] or '', reverse=True)
                
                logger.info(f"Found {len(draft_docs)} draft documents")
                return draft_docs
                
        except Exception as e:
            logger.error(f"Failed to list draft documents: {str(e)}")
            raise DocumentStateError(f"Failed to list draft documents: {str(e)}")
    
    def get_document_version_content(self, doc_id: str, version: str) -> Dict[str, str]:
        """
        Get the content of a specific document version.
        
        Args:
            doc_id: Document ID
            version: Version string (e.g., "v1")
            
        Returns:
            Dictionary with 'md' and 'json' content
            
        Raises:
            VersionNotFoundError: If version doesn't exist
            
        Example:
            content = store.get_document_version_content("abc123", "v1")
            print("Markdown:", content['md'][:100])
            print("JSON:", content['json'][:100])
        """
        try:
            with timing_context(f"get_document_version_content(doc_id={doc_id}, version={version})"):
                md_key = f"processed/{doc_id}/{version}/output.md"
                json_key = f"processed/{doc_id}/{version}/output.json"
                
                # Check if version exists
                if not object_exists(self.bucket, md_key):
                    raise VersionNotFoundError(
                        f"Version {version} not found for document {doc_id}",
                        doc_id=doc_id,
                        version=version
                    )
                
                # Get content
                md_content = get_object_content(self.bucket, md_key).decode('utf-8')
                json_content = get_object_content(self.bucket, json_key).decode('utf-8')
                
                logger.debug(f"Retrieved content for {doc_id} {version}")
                return {
                    'md': md_content,
                    'json': json_content
                }
                
        except (VersionNotFoundError, S3OperationError):
            raise
        except Exception as e:
            logger.error(f"Failed to get document version content: {str(e)}")
            raise DocumentStateError(f"Failed to get document version content: {str(e)}")
    
    def approve_document_version(self, doc_id: str, version: str, approver_info: Optional[Dict[str, str]] = None) -> bool:
        """
        Approve a document version (DRAFT -> FINAL).
        
        This is an atomic operation that:
        1. Sets the specified version to FINAL
        2. Archives any previously FINAL version
        3. Updates the current version pointer
        
        Args:
            doc_id: Document ID
            version: Version to approve
            approver_info: Optional info about the approver
            
        Returns:
            True if successful
            
        Example:
            success = store.approve_document_version(
                "abc123", "v2",
                {"approved_by": "john.doe", "review_notes": "Looks good"}
            )
        """
        try:
            with timing_context(f"approve_document_version(doc_id={doc_id}, version={version})"):
                md_key = f"processed/{doc_id}/{version}/output.md"
                json_key = f"processed/{doc_id}/{version}/output.json"
                
                # Verify version exists and is in DRAFT state
                if not object_exists(self.bucket, md_key):
                    raise VersionNotFoundError(
                        f"Version {version} not found for document {doc_id}",
                        doc_id=doc_id,
                        version=version
                    )
                
                current_state = get_state(self.bucket, md_key)
                if current_state != DocumentState.DRAFT:
                    raise InvalidStateTransitionError(
                        f"Cannot approve version {version}: current state is {current_state.value if current_state else 'unknown'}, expected draft"
                    )
                
                # Find currently FINAL version to archive
                versions = self.list_document_versions(doc_id)
                current_final = None
                for v in versions:
                    if v['state'] == DocumentState.FINAL.value:
                        current_final = v['version']
                        break
                
                # Archive current final version if exists
                if current_final and current_final != version:
                    logger.info(f"Archiving current final version {current_final}")
                    old_md_key = f"processed/{doc_id}/{current_final}/output.md"
                    old_json_key = f"processed/{doc_id}/{current_final}/output.json"
                    
                    transition_state(self.bucket, old_md_key, DocumentState.ARCHIVED)
                    transition_state(self.bucket, old_json_key, DocumentState.ARCHIVED)
                
                # Approve new version
                metadata = approver_info or {}
                metadata['approved_at'] = datetime.utcnow().isoformat() + 'Z'
                
                success_md = self.state_manager.transition_document_state(
                    md_key, DocumentState.FINAL, metadata=metadata
                )
                success_json = self.state_manager.transition_document_state(
                    json_key, DocumentState.FINAL, metadata=metadata
                )
                
                if success_md and success_json:
                    # Update current version pointer
                    self._update_current_version_pointer(doc_id, version)
                    
                    # Invalidate cache for this document and lists
                    self.cache.invalidate_document(doc_id)
                    
                    logger.info(f"Approved document {doc_id} version {version}")
                    return True
                else:
                    logger.error(f"Failed to approve document {doc_id} version {version}")
                    return False
                
        except Exception as e:
            logger.error(f"Failed to approve document version: {str(e)}")
            raise
    
    def _update_current_version_pointer(self, doc_id: str, version: str) -> None:
        """Update the current version pointer file."""
        try:
            pointer_key = f"processed/{doc_id}/current_version.txt"
            put_object_content(
                self.bucket,
                pointer_key,
                version,
                tags={'stage': 'metadata', 'doc_id': doc_id, 'points_to': version}
            )
            logger.debug(f"Updated current version pointer for {doc_id} to {version}")
        except Exception as e:
            logger.warning(f"Failed to update version pointer: {str(e)}")
    
    # ==========================================
    # Final Document Operations (for main_app)
    # ==========================================
    
    def list_final_documents(self, limit: int = 1000, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        List all documents with FINAL approved versions.
        
        Args:
            limit: Maximum number of documents to return
            use_cache: Whether to use cached results
            
        Returns:
            List of final document info
            
        Example:
            finals = store.list_final_documents()
            for doc in finals:
                print(f"Doc {doc['doc_id']} v{doc['version']}: {doc['approved_at']}")
        """
        try:
            with timing_context("list_final_documents"):
                # Try cache first
                if use_cache:
                    cached_result = self.cache.get_document_list('final')
                    if cached_result is not None:
                        # Filter by limit in case cache has more
                        return cached_result[:limit]
                
                final_docs = []
                documents = self.state_manager.list_documents_by_state(
                    [DocumentState.FINAL],
                    prefix="processed/",
                    max_results=limit * 2
                )
                
                seen_docs = set()
                for doc_key, state, tags in documents:
                    parts = doc_key.split('/')
                    if len(parts) >= 4:
                        doc_id = parts[1]
                        version = parts[2]
                        file_type = parts[3].split('.')[1] if '.' in parts[3] else 'unknown'
                        
                        # Only include MD files and avoid duplicates
                        if file_type == 'md' and doc_id not in seen_docs:
                            seen_docs.add(doc_id)
                            
                            doc_info = {
                                'doc_id': doc_id,
                                'version': version,
                                'state': state.value,
                                'approved_at': tags.get('approved_at'),
                                'created_at': tags.get('created_at'),
                                'approved_by': tags.get('meta_approved_by'),
                                'tags': tags,
                                'md_key': doc_key,
                                'json_key': doc_key.replace('output.md', 'output.json')
                            }
                            final_docs.append(doc_info)
                            
                            if len(final_docs) >= limit:
                                break
                
                # Sort by approval date (newest first)
                final_docs.sort(key=lambda x: x['approved_at'] or '', reverse=True)
                
                # Cache the results
                if use_cache and final_docs:
                    self.cache.set_document_list('final', final_docs)
                
                logger.info(f"Found {len(final_docs)} final documents")
                return final_docs
                
        except Exception as e:
            logger.error(f"Failed to list final documents: {str(e)}")
            raise DocumentStateError(f"Failed to list final documents: {str(e)}")
    
    def get_final_document_content(self, doc_id: str, use_cache: bool = True) -> Optional[Dict[str, str]]:
        """
        Get the content of the current FINAL version of a document.
        
        Args:
            doc_id: Document ID
            use_cache: Whether to use cached content
            
        Returns:
            Dictionary with 'md' and 'json' content, or None if no final version
            
        Example:
            content = store.get_final_document_content("abc123")
            if content:
                print("Final version content:", content['md'][:100])
        """
        try:
            with timing_context(f"get_final_document_content(doc_id={doc_id})"):
                # Try cache first
                if use_cache:
                    cached_content = self.cache.get_document_content(doc_id)
                    if cached_content is not None:
                        return cached_content
                
                # Get current version
                current_version = self.get_current_document_version(doc_id)
                if not current_version:
                    return None
                
                content = self.get_document_version_content(doc_id, current_version)
                
                # Cache the content
                if use_cache and content:
                    self.cache.set_document_content(doc_id, content)
                
                return content
                
        except Exception as e:
            logger.error(f"Failed to get final document content for {doc_id}: {str(e)}")
            raise DocumentStateError(f"Failed to get final document content: {str(e)}")
    
    def get_current_document_version(self, doc_id: str) -> Optional[str]:
        """
        Get the current FINAL version of a document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Current version string, or None if no final version
        """
        try:
            with timing_context(f"get_current_document_version(doc_id={doc_id})"):
                # Try to read version pointer file first
                pointer_key = f"processed/{doc_id}/current_version.txt"
                try:
                    if object_exists(self.bucket, pointer_key):
                        version_content = get_object_content(self.bucket, pointer_key).decode('utf-8').strip()
                        
                        # Verify this version is actually FINAL
                        md_key = f"processed/{doc_id}/{version_content}/output.md"
                        if object_exists(self.bucket, md_key):
                            state = get_state(self.bucket, md_key)
                            if state == DocumentState.FINAL:
                                return version_content
                except Exception:
                    pass  # Fall back to searching
                
                # Fall back: search for any FINAL version
                versions = self.list_document_versions(doc_id)
                for version_info in versions:
                    if version_info['state'] == DocumentState.FINAL.value:
                        return version_info['version']
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get current document version for {doc_id}: {str(e)}")
            return None
    
    # ==========================================
    # Health and Monitoring
    # ==========================================
    
    def get_system_health(self) -> Dict[str, Any]:
        """
        Get overall system health information.
        
        Returns:
            Health status dictionary
            
        Example:
            health = store.get_system_health()
            print(f"System status: {health['status']}")
            print(f"Total documents: {health['total_documents']}")
        """
        try:
            with timing_context("get_system_health"):
                # Get state statistics
                stats = self.state_manager.get_state_statistics()
                
                # Find stuck documents
                stuck_processing = self.list_stuck_documents(DocumentState.PROCESSING, timeout_minutes=10)
                
                # Calculate health status
                total_docs = sum(stats.values()) - stats.get('no_state', 0)
                failed_docs = stats.get('failed', 0)
                stuck_docs = len(stuck_processing)
                
                if stuck_docs > 0 or failed_docs > total_docs * 0.1:  # >10% failure rate
                    status = 'unhealthy'
                elif failed_docs > 0 or stuck_docs > 0:
                    status = 'degraded'
                else:
                    status = 'healthy'
                
                health_info = {
                    'status': status,
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'total_documents': total_docs,
                    'state_breakdown': stats,
                    'stuck_documents': stuck_docs,
                    'failed_documents': failed_docs,
                    'issues': []
                }
                
                if stuck_docs > 0:
                    health_info['issues'].append(f"{stuck_docs} documents stuck in processing")
                if failed_docs > 0:
                    health_info['issues'].append(f"{failed_docs} documents failed processing")
                
                logger.info(f"System health check: {status} ({total_docs} total docs)")
                return health_info
                
        except Exception as e:
            logger.error(f"Failed to get system health: {str(e)}")
            return {
                'status': 'error',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'error': str(e)
            }
    
    def list_stuck_documents(self, state: DocumentState, timeout_minutes: int = 10) -> List[Dict[str, Any]]:
        """
        Find documents stuck in a state longer than timeout.
        
        Args:
            state: State to check for stuck documents
            timeout_minutes: Minutes before considering stuck
            
        Returns:
            List of stuck document info
        """
        try:
            stuck_docs = self.state_manager.find_stuck_documents(state, timeout_minutes)
            
            result = []
            for doc_key, state_changed_at, tags in stuck_docs:
                # Extract doc_id from key
                doc_id = doc_key.split('/')[1] if '/' in doc_key else 'unknown'
                
                result.append({
                    'doc_id': doc_id,
                    'doc_key': doc_key,
                    'state': state.value,
                    'stuck_since': state_changed_at.isoformat() + 'Z' if state_changed_at != datetime.min else 'unknown',
                    'stuck_duration_minutes': (datetime.utcnow() - state_changed_at).total_seconds() / 60 if state_changed_at != datetime.min else float('inf'),
                    'tags': tags
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to list stuck documents: {str(e)}")
            raise DocumentStateError(f"Failed to list stuck documents: {str(e)}")
    
    def reset_document_to_raw(self, doc_id: str, reason: Optional[str] = None) -> bool:
        """
        Reset a document back to RAW state (emergency recovery).
        
        Args:
            doc_id: Document ID
            reason: Optional reason for reset
            
        Returns:
            True if successful
            
        Example:
            success = store.reset_document_to_raw(
                "abc123", 
                "Stuck in processing for >30 minutes"
            )
        """
        try:
            with timing_context(f"reset_document_to_raw(doc_id={doc_id})"):
                doc_key = f"raw/{doc_id}/original.pdf"
                
                success = self.state_manager.reset_document_state(
                    doc_key,
                    DocumentState.RAW,
                    reason=reason
                )
                
                if success:
                    logger.warning(f"Reset document {doc_id} to RAW state. Reason: {reason}")
                
                return success
                
        except Exception as e:
            logger.error(f"Failed to reset document {doc_id}: {str(e)}")
            raise DocumentStateError(f"Failed to reset document: {str(e)}")

    # ==========================================
    # Batch Operations (for efficiency)
    # ==========================================
    
    def get_multiple_documents(self, doc_ids: List[str]) -> Dict[str, Optional[Dict[str, str]]]:
        """
        Get final content for multiple documents in a single operation.
        
        Args:
            doc_ids: List of document IDs
            
        Returns:
            Dictionary mapping doc_id to content (or None if not found)
            
        Example:
            contents = store.get_multiple_documents(["abc123", "def456", "ghi789"])
            for doc_id, content in contents.items():
                if content:
                    print(f"Doc {doc_id}: {len(content['md'])} chars")
        """
        try:
            with timing_context(f"get_multiple_documents(count={len(doc_ids)})"):
                results = {}
                
                for doc_id in doc_ids:
                    try:
                        content = self.get_final_document_content(doc_id)
                        results[doc_id] = content
                    except Exception as e:
                        logger.warning(f"Failed to get content for document {doc_id}: {str(e)}")
                        results[doc_id] = None
                
                successful = sum(1 for v in results.values() if v is not None)
                logger.info(f"Retrieved content for {successful}/{len(doc_ids)} documents")
                return results
                
        except Exception as e:
            logger.error(f"Failed to get multiple documents: {str(e)}")
            raise DocumentStateError(f"Failed to get multiple documents: {str(e)}")
    
    def batch_mark_documents_processing(self, doc_ids: List[str], processor_info: Optional[Dict[str, str]] = None) -> Dict[str, bool]:
        """
        Mark multiple documents as processing in batch.
        
        Args:
            doc_ids: List of document IDs
            processor_info: Optional processor information
            
        Returns:
            Dictionary mapping doc_id to success status
            
        Example:
            results = store.batch_mark_documents_processing(
                ["abc123", "def456"], 
                {"processor": "batch-worker-1"}
            )
        """
        try:
            with timing_context(f"batch_mark_documents_processing(count={len(doc_ids)})"):
                results = {}
                
                for doc_id in doc_ids:
                    try:
                        success = self.mark_document_processing(doc_id, processor_info)
                        results[doc_id] = success
                    except Exception as e:
                        logger.warning(f"Failed to mark document {doc_id} as processing: {str(e)}")
                        results[doc_id] = False
                
                successful = sum(1 for v in results.values() if v)
                logger.info(f"Marked {successful}/{len(doc_ids)} documents as processing")
                return results
                
        except Exception as e:
            logger.error(f"Failed to batch mark documents as processing: {str(e)}")
            raise DocumentStateError(f"Failed to batch mark documents as processing: {str(e)}")
    
    def list_documents_by_multiple_states(self, states: List[DocumentState], prefix: str = "", limit: int = 1000) -> List[Dict[str, Any]]:
        """
        List documents in multiple states efficiently.
        
        Args:
            states: List of states to include
            prefix: S3 key prefix to filter by
            limit: Maximum number of documents to return
            
        Returns:
            List of document info dictionaries
            
        Example:
            docs = store.list_documents_by_multiple_states([
                DocumentState.RAW, DocumentState.PROCESSING, DocumentState.FAILED
            ])
        """
        try:
            with timing_context(f"list_documents_by_multiple_states(states={[s.value for s in states]})"):
                return self.state_manager.list_documents_by_state(states, prefix, limit)
                
        except Exception as e:
            logger.error(f"Failed to list documents by multiple states: {str(e)}")
            raise DocumentStateError(f"Failed to list documents by multiple states: {str(e)}")
    
    def health_check_all_documents(self) -> Dict[str, Any]:
        """
        Comprehensive health check across all document states.
        
        Returns:
            Detailed health status including stuck documents in all states
            
        Example:
            health = store.health_check_all_documents()
            for issue in health['detailed_issues']:
                print(f"Issue: {issue}")
        """
        try:
            with timing_context("health_check_all_documents"):
                # Get basic health info
                health_info = self.get_system_health()
                
                # Check for stuck documents in each relevant state
                detailed_issues = []
                
                # Check processing state (10 min timeout)
                stuck_processing = self.list_stuck_documents(DocumentState.PROCESSING, 10)
                if stuck_processing:
                    detailed_issues.append(f"{len(stuck_processing)} documents stuck in PROCESSING > 10 minutes")
                
                # Check for very old failed documents (24 hours)
                stuck_failed = self.list_stuck_documents(DocumentState.FAILED, 24 * 60)
                if stuck_failed:
                    detailed_issues.append(f"{len(stuck_failed)} documents failed > 24 hours ago")
                
                # Check for old drafts (7 days)
                stuck_drafts = self.list_stuck_documents(DocumentState.DRAFT, 7 * 24 * 60)
                if stuck_drafts:
                    detailed_issues.append(f"{len(stuck_drafts)} drafts pending approval > 7 days")
                
                health_info['detailed_issues'] = detailed_issues
                health_info['stuck_processing'] = stuck_processing
                health_info['stuck_failed'] = stuck_failed
                health_info['stuck_drafts'] = stuck_drafts
                
                logger.info(f"Comprehensive health check completed: {len(detailed_issues)} issues found")
                return health_info
                
        except Exception as e:
            logger.error(f"Failed to perform comprehensive health check: {str(e)}")
            return {
                'status': 'error',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'error': str(e)
            }


# Convenience function for easy initialization
def create_document_store(bucket_name: Optional[str] = None, aws_region: Optional[str] = None) -> DocumentStore:
    """
    Create a DocumentStore instance with optional custom settings.
    
    Args:
        bucket_name: S3 bucket name (defaults to config)
        aws_region: AWS region (defaults to config)
        
    Returns:
        DocumentStore instance
        
    Example:
        store = create_document_store()
        # or
        store = create_document_store("my-custom-bucket", "us-west-2")
    """
    return DocumentStore(bucket_name=bucket_name, aws_region=aws_region) 