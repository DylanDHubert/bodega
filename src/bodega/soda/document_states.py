"""
Document state management for doc_store library.

This module provides high-level document state operations using S3 tags,
including atomic state transitions and business rule validation.
"""

from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

from loguru import logger

from .s3_ops import (
    get_object_tags,
    put_object_tags,
    list_objects_with_tags,
    object_exists
)
from .exceptions import (
    DocumentStateError,
    S3OperationError,
    InvalidStateTransitionError,
    DocumentNotFoundError
)
from .utils import timing_context


class DocumentState(Enum):
    """
    Document states for different stages of the pipeline.
    
    PDF Document States (for raw documents):
    - RAW: Uploaded PDF awaiting processing
    - PROCESSING: Currently being processed by pipeline
    - PROCESSED: Successfully processed, outputs created
    - FAILED: Processing failed, requires manual intervention
    
    Output Document States (for processed outputs):
    - DRAFT: Output created but not yet approved
    - FINAL: Approved output, ready for use
    - ARCHIVED: Previously final version, superseded by new version
    """
    # PDF states
    RAW = "raw"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    
    # Output states
    DRAFT = "draft"
    FINAL = "final"
    ARCHIVED = "archived"


@dataclass
class StateTransition:
    """Definition of a valid state transition."""
    from_state: DocumentState
    to_state: DocumentState
    description: str
    requires_approval: bool = False


# Valid state transitions
VALID_TRANSITIONS = [
    # PDF processing flow
    StateTransition(DocumentState.RAW, DocumentState.PROCESSING, "Start processing PDF"),
    StateTransition(DocumentState.PROCESSING, DocumentState.PROCESSED, "Processing completed successfully"),
    StateTransition(DocumentState.PROCESSING, DocumentState.FAILED, "Processing failed"),
    StateTransition(DocumentState.FAILED, DocumentState.RAW, "Reset for reprocessing"),
    StateTransition(DocumentState.FAILED, DocumentState.PROCESSING, "Retry processing"),
    
    # Output approval flow
    StateTransition(DocumentState.DRAFT, DocumentState.FINAL, "Approve draft version", requires_approval=True),
    StateTransition(DocumentState.FINAL, DocumentState.ARCHIVED, "Archive when superseded"),
    StateTransition(DocumentState.ARCHIVED, DocumentState.FINAL, "Restore archived version", requires_approval=True),
    
    # Emergency transitions
    StateTransition(DocumentState.PROCESSED, DocumentState.RAW, "Reset processed document"),
]


class DocumentStateManager:
    """Manager for document state operations using S3 tags."""
    
    def __init__(self, bucket: str):
        """
        Initialize state manager.
        
        Args:
            bucket: S3 bucket name
        """
        self.bucket = bucket
        self._valid_transitions = self._build_transition_map()
    
    def _build_transition_map(self) -> Dict[DocumentState, Set[DocumentState]]:
        """Build map of valid state transitions."""
        transition_map = {}
        for transition in VALID_TRANSITIONS:
            if transition.from_state not in transition_map:
                transition_map[transition.from_state] = set()
            transition_map[transition.from_state].add(transition.to_state)
        return transition_map
    
    def get_document_state(self, doc_key: str) -> Optional[DocumentState]:
        """
        Get current state of a document.
        
        Args:
            doc_key: S3 object key
            
        Returns:
            DocumentState or None if no state tag found
            
        Raises:
            DocumentNotFoundError: If document doesn't exist
            DocumentStateError: If operation fails
        """
        try:
            with timing_context(f"get_document_state(key={doc_key})"):
                if not object_exists(self.bucket, doc_key):
                    raise DocumentNotFoundError(
                        f"Document not found: s3://{self.bucket}/{doc_key}",
                        doc_id=doc_key
                    )
                
                tags = get_object_tags(self.bucket, doc_key)
                stage_tag = tags.get('stage')
                
                if not stage_tag:
                    logger.warning(f"Document has no stage tag: s3://{self.bucket}/{doc_key}")
                    return None
                
                try:
                    state = DocumentState(stage_tag)
                    logger.debug(f"Document s3://{self.bucket}/{doc_key} is in state: {state.value}")
                    return state
                except ValueError:
                    logger.error(f"Invalid state tag '{stage_tag}' on document: s3://{self.bucket}/{doc_key}")
                    return None
                    
        except (DocumentNotFoundError, S3OperationError):
            raise
        except Exception as e:
            raise DocumentStateError(
                f"Failed to get document state: {str(e)}",
                doc_id=doc_key
            )
    
    def transition_document_state(
        self,
        doc_key: str,
        to_state: DocumentState,
        metadata: Optional[Dict[str, str]] = None,
        force: bool = False
    ) -> bool:
        """
        Transition document to new state with validation.
        
        Args:
            doc_key: S3 object key
            to_state: Target state
            metadata: Optional metadata to include in tags
            force: Skip validation if True
            
        Returns:
            bool: True if transition was successful
            
        Raises:
            DocumentNotFoundError: If document doesn't exist
            InvalidStateTransitionError: If transition is not valid
            DocumentStateError: If operation fails
        """
        try:
            with timing_context(f"transition_document_state(key={doc_key}, to_state={to_state.value})"):
                # Get current state
                current_state = self.get_document_state(doc_key)
                
                # Validate transition unless forced
                if not force and current_state is not None:
                    if not self._is_valid_transition(current_state, to_state):
                        raise InvalidStateTransitionError(
                            f"Invalid transition from {current_state.value} to {to_state.value}",
                            doc_id=doc_key,
                            current_state=current_state.value,
                            target_state=to_state.value
                        )
                
                # Prepare new tags
                new_tags = {'stage': to_state.value}
                
                # Add timestamp
                new_tags['state_changed_at'] = datetime.utcnow().isoformat() + 'Z'
                
                # Add previous state if transitioning
                if current_state is not None:
                    new_tags['previous_stage'] = current_state.value
                
                # Add optional metadata
                if metadata:
                    for key, value in metadata.items():
                        # Prefix metadata to avoid conflicts
                        new_tags[f"meta_{key}"] = value
                
                # Apply state change atomically
                put_object_tags(self.bucket, doc_key, new_tags)
                
                transition_desc = f"{current_state.value if current_state else 'None'} → {to_state.value}"
                logger.info(f"State transition successful for s3://{self.bucket}/{doc_key}: {transition_desc}")
                
                return True
                
        except (DocumentNotFoundError, InvalidStateTransitionError, S3OperationError):
            raise
        except Exception as e:
            raise DocumentStateError(
                f"Failed to transition document state: {str(e)}",
                doc_id=doc_key
            )
    
    def _is_valid_transition(self, from_state: DocumentState, to_state: DocumentState) -> bool:
        """Check if state transition is valid."""
        if from_state not in self._valid_transitions:
            return False
        return to_state in self._valid_transitions[from_state]
    
    def list_documents_by_state(
        self,
        states: List[DocumentState],
        prefix: str = "",
        max_results: int = 1000
    ) -> List[Tuple[str, DocumentState, Dict[str, str]]]:
        """
        List documents in specific states.
        
        Args:
            states: List of states to filter by
            prefix: S3 key prefix to filter by
            max_results: Maximum number of results
            
        Returns:
            List of (doc_key, state, all_tags) tuples
            
        Raises:
            DocumentStateError: If operation fails
        """
        try:
            with timing_context(f"list_documents_by_state(states={[s.value for s in states]}, prefix={prefix})"):
                results = []
                
                # Get all documents with tags in the prefix
                all_objects = list_objects_with_tags(self.bucket, prefix)
                
                for doc_key, tags in all_objects:
                    stage_tag = tags.get('stage')
                    if not stage_tag:
                        continue
                    
                    try:
                        doc_state = DocumentState(stage_tag)
                        if doc_state in states:
                            results.append((doc_key, doc_state, tags))
                            
                            if len(results) >= max_results:
                                break
                                
                    except ValueError:
                        logger.warning(f"Invalid state tag '{stage_tag}' on document: s3://{self.bucket}/{doc_key}")
                        continue
                
                logger.info(f"Found {len(results)} documents in states {[s.value for s in states]}")
                return results
                
        except S3OperationError:
            raise
        except Exception as e:
            raise DocumentStateError(
                f"Failed to list documents by state: {str(e)}"
            )
    
    def list_documents_by_single_state(
        self,
        state: DocumentState,
        prefix: str = "",
        max_results: int = 1000
    ) -> List[str]:
        """
        List document keys in a specific state.
        
        Args:
            state: State to filter by
            prefix: S3 key prefix to filter by
            max_results: Maximum number of results
            
        Returns:
            List of document keys
        """
        results = self.list_documents_by_state([state], prefix, max_results)
        return [doc_key for doc_key, _, _ in results]
    
    def find_stuck_documents(
        self,
        state: DocumentState,
        timeout_minutes: int = 10,
        prefix: str = ""
    ) -> List[Tuple[str, datetime, Dict[str, str]]]:
        """
        Find documents stuck in a state longer than timeout.
        
        Args:
            state: State to check for stuck documents
            timeout_minutes: Minutes before considering stuck
            prefix: S3 key prefix to filter by
            
        Returns:
            List of (doc_key, state_changed_at, all_tags) tuples
            
        Raises:
            DocumentStateError: If operation fails
        """
        try:
            with timing_context(f"find_stuck_documents(state={state.value}, timeout={timeout_minutes}min)"):
                cutoff_time = datetime.utcnow() - timedelta(minutes=timeout_minutes)
                stuck_docs = []
                
                # Get all documents in the specified state
                documents = self.list_documents_by_state([state], prefix)
                
                for doc_key, doc_state, tags in documents:
                    state_changed_str = tags.get('state_changed_at')
                    if not state_changed_str:
                        # No timestamp means very old document - definitely stuck
                        logger.warning(f"Document has no state timestamp: s3://{self.bucket}/{doc_key}")
                        stuck_docs.append((doc_key, datetime.min, tags))
                        continue
                    
                    try:
                        # Parse ISO timestamp
                        state_changed_at = datetime.fromisoformat(state_changed_str.replace('Z', '+00:00'))
                        if state_changed_at.replace(tzinfo=None) < cutoff_time:
                            stuck_docs.append((doc_key, state_changed_at.replace(tzinfo=None), tags))
                    except ValueError as e:
                        logger.warning(f"Invalid timestamp format '{state_changed_str}' on document: s3://{self.bucket}/{doc_key}")
                        stuck_docs.append((doc_key, datetime.min, tags))
                
                logger.info(f"Found {len(stuck_docs)} documents stuck in {state.value} state for >{timeout_minutes}min")
                return stuck_docs
                
        except S3OperationError:
            raise
        except Exception as e:
            raise DocumentStateError(
                f"Failed to find stuck documents: {str(e)}"
            )
    
    def reset_document_state(
        self,
        doc_key: str,
        to_state: DocumentState = DocumentState.RAW,
        reason: Optional[str] = None
    ) -> bool:
        """
        Reset document state (emergency recovery).
        
        Args:
            doc_key: S3 object key
            to_state: State to reset to (default: RAW)
            reason: Optional reason for reset
            
        Returns:
            bool: True if reset was successful
        """
        try:
            with timing_context(f"reset_document_state(key={doc_key}, to_state={to_state.value})"):
                current_state = self.get_document_state(doc_key)
                
                metadata = {'reset_reason': reason or 'Manual reset'}
                if current_state:
                    metadata['reset_from'] = current_state.value
                
                # Force the transition (skip validation)
                success = self.transition_document_state(
                    doc_key,
                    to_state,
                    metadata=metadata,
                    force=True
                )
                
                if success:
                    logger.warning(f"RESET document state: s3://{self.bucket}/{doc_key} → {to_state.value}")
                    if reason:
                        logger.warning(f"Reset reason: {reason}")
                
                return success
                
        except Exception as e:
            logger.error(f"Failed to reset document state: {str(e)}")
            raise DocumentStateError(
                f"Failed to reset document state: {str(e)}",
                doc_id=doc_key
            )
    
    def get_state_statistics(self, prefix: str = "") -> Dict[str, int]:
        """
        Get count of documents in each state.
        
        Args:
            prefix: S3 key prefix to filter by
            
        Returns:
            Dict mapping state names to counts
            
        Raises:
            DocumentStateError: If operation fails
        """
        try:
            with timing_context(f"get_state_statistics(prefix={prefix})"):
                stats = {state.value: 0 for state in DocumentState}
                stats['no_state'] = 0
                
                # Get all documents with tags
                all_objects = list_objects_with_tags(self.bucket, prefix)
                
                for doc_key, tags in all_objects:
                    stage_tag = tags.get('stage')
                    if not stage_tag:
                        stats['no_state'] += 1
                        continue
                    
                    if stage_tag in stats:
                        stats[stage_tag] += 1
                    else:
                        logger.warning(f"Unknown state '{stage_tag}' on document: s3://{self.bucket}/{doc_key}")
                        stats[stage_tag] = stats.get(stage_tag, 0) + 1
                
                logger.info(f"State statistics for s3://{self.bucket}/{prefix}: {stats}")
                return stats
                
        except S3OperationError:
            raise
        except Exception as e:
            raise DocumentStateError(
                f"Failed to get state statistics: {str(e)}"
            )


# Convenience functions for common operations

def get_document_state(bucket: str, doc_key: str) -> Optional[DocumentState]:
    """Get document state (convenience function)."""
    manager = DocumentStateManager(bucket)
    return manager.get_document_state(doc_key)


def transition_document_state(
    bucket: str,
    doc_key: str,
    to_state: DocumentState,
    metadata: Optional[Dict[str, str]] = None
) -> bool:
    """Transition document state (convenience function)."""
    manager = DocumentStateManager(bucket)
    return manager.transition_document_state(doc_key, to_state, metadata)


def list_documents_by_state(
    bucket: str,
    states: List[DocumentState],
    prefix: str = ""
) -> List[str]:
    """List documents by state (convenience function)."""
    manager = DocumentStateManager(bucket)
    results = manager.list_documents_by_state(states, prefix)
    return [doc_key for doc_key, _, _ in results] 