"""
Bodega - Complete RAG Processing Pipeline Orchestrator

Combines PDF processing (PB&J) with AWS document storage and state management.
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from .pbj.sandwich import Sandwich
from .pbj.config import create_config as create_pbj_config
from .soda.document_store import DocumentStore, create_document_store
from .soda.document_states import DocumentState
from .soda.s3_ops import put_object_content


class Bodega:
    """
    🏪 Bodega - Complete RAG Processing Pipeline
    
    Combines the PB&J PDF processing pipeline with soda document storage
    to provide a complete document processing solution with AWS integration.
    """
    
    def __init__(
        self,
        aws_bucket: Optional[str] = None,
        aws_region: Optional[str] = None,
        pbj_config: Optional[Dict[str, Any]] = None,
        use_premium: bool = False,
        openai_model: str = "gpt-4"
    ):
        """
        Initialize Bodega with both PB&J and soda components.
        
        Args:
            aws_bucket: S3 bucket name for document storage
            aws_region: AWS region for S3 operations
            pbj_config: Configuration for PB&J pipeline
            use_premium: Use LlamaParse premium mode
            openai_model: OpenAI model for processing
        """
        # Initialize soda (document storage)
        self.soda = create_document_store(
            bucket_name=aws_bucket,
            aws_region=aws_region
        )
        
        # Initialize PB&J pipeline
        pbj_settings = pbj_config or {}
        pbj_settings.update({
            'use_premium_mode': use_premium,
            'openai_model': openai_model
        })
        
        self.pbj_config = create_pbj_config(**pbj_settings)
        self.sandwich = Sandwich(config=self.pbj_config)
        
        print(f"Bodega initialized with bucket: {self.soda.bucket}")
    
    def process_document(
        self, 
        pdf_path: str, 
        doc_id: Optional[str] = None,
        upload_to_aws: bool = True
    ) -> Dict[str, Any]:
        """
        Process a PDF document through the complete Bodega pipeline.
        
        Args:
            pdf_path: Path to the PDF file
            doc_id: Optional document ID (auto-generated if not provided)
            upload_to_aws: Whether to upload results to AWS
            
        Returns:
            Dictionary with processing results and metadata
        """
        start_time = datetime.now()
        
        try:
            # Generate document ID if not provided
            if not doc_id:
                doc_id = self._generate_doc_id(pdf_path)
            
            print(f"Starting Bodega processing for document: {doc_id}")
            
            # Step 1: Mark document as processing in soda
            if upload_to_aws:
                self.soda.mark_document_processing(doc_id, {
                    "processor": "bodega",
                    "started_at": start_time.isoformat()
                })
            
            # Step 2: Process with PB&J pipeline
            print("Running PB&J pipeline...")
            pbj_result = self.sandwich.process(pdf_path)
            
            # Step 3: Upload intermediate results to AWS
            if upload_to_aws:
                print("Uploading intermediate results to AWS...")
                self._upload_intermediate_results(doc_id, pbj_result)
            
            # Step 4: Mark document as processed
            if upload_to_aws:
                self.soda.mark_document_processed(doc_id, {
                    "processing_time": f"{(datetime.now() - start_time).total_seconds():.2f}s",
                    "pages_processed": pbj_result.get('data_summary', {}).get('total_pages', 0),
                    "pbj_version": "1.0"
                })
            
            # Step 5: Create document version in soda
            if upload_to_aws:
                self._create_document_version(doc_id, pbj_result)
            
            # Compile final result
            result = {
                "doc_id": doc_id,
                "processing_info": {
                    "started_at": start_time.isoformat(),
                    "completed_at": datetime.now().isoformat(),
                    "total_time_seconds": (datetime.now() - start_time).total_seconds(),
                    "uploaded_to_aws": upload_to_aws
                },
                "pbj_pipeline": pbj_result,
                "soda_storage": {
                    "bucket": self.soda.bucket,
                    "document_state": "PROCESSED" if upload_to_aws else "LOCAL_ONLY"
                }
            }
            
            print(f"Bodega processing completed for {doc_id}")
            return result
            
        except Exception as e:
            print(f"Bodega processing failed for {doc_id}: {str(e)}")
            
            # Mark document as failed in soda
            if upload_to_aws and doc_id:
                self.soda.mark_document_failed(doc_id, str(e))
            
            raise
    
    def list_pending_documents(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List documents awaiting processing.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of document info dictionaries
        """
        return self.soda.list_raw_documents(limit=limit)
    
    def get_processed_documents(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List documents that have been processed.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of processed document info dictionaries
        """
        return self.soda.list_final_documents(limit=limit)
    
    def get_document_content(self, doc_id: str) -> Optional[Dict[str, str]]:
        """
        Get the processed content for a specific document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Document content dictionary or None if not found
        """
        return self.soda.get_final_document_content(doc_id)
    
    def _generate_doc_id(self, pdf_path: str) -> str:
        """Generate a unique document ID based on filename and timestamp."""
        filename = Path(pdf_path).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{filename}_{timestamp}"
    
    def _upload_intermediate_results(self, doc_id: str, pbj_result: Dict[str, Any]) -> None:
        """Upload intermediate processing results to AWS."""
        try:
            document_folder = pbj_result.get('pipeline_info', {}).get('document_folder')
            if not document_folder or not Path(document_folder).exists():
                print(f"No document folder found for {doc_id}")
                return
            
            # Upload the entire document folder structure
            folder_path = Path(document_folder)
            for file_path in folder_path.rglob('*'):
                if file_path.is_file():
                    # Create S3 key based on relative path
                    relative_path = file_path.relative_to(folder_path)
                    s3_key = f"processed/{doc_id}/{relative_path}"
                    
                    # Upload file content
                    with open(file_path, 'rb') as f:
                        content = f.read()
                    
                    put_object_content(
                        bucket=self.soda.bucket,
                        key=s3_key,
                        content=content,
                        content_type=self._get_content_type(file_path)
                    )
                    
                    print(f"Uploaded {s3_key}")
            
            print(f"Uploaded intermediate results for {doc_id}")
            
        except Exception as e:
            print(f"Failed to upload intermediate results for {doc_id}: {str(e)}")
            raise
    
    def _create_document_version(self, doc_id: str, pbj_result: Dict[str, Any]) -> None:
        """Create a document version in soda with processed content."""
        try:
            # Get the final output content
            document_folder = pbj_result.get('pipeline_info', {}).get('document_folder')
            final_output_path = Path(document_folder) / "final_output.json"
            
            if final_output_path.exists():
                with open(final_output_path, 'r', encoding='utf-8') as f:
                    json_content = f.read()
                
                # Create markdown summary
                md_content = self._create_markdown_summary(pbj_result)
                
                # Create document version
                version = self.soda.create_document_version(
                    doc_id=doc_id,
                    md_content=md_content,
                    json_content=json_content,
                    metadata={
                        "pbj_pipeline_version": "1.0",
                        "pages_processed": str(pbj_result.get('data_summary', {}).get('total_pages', 0)),
                        "processing_model": pbj_result.get('pipeline_info', {}).get('openai_model', 'unknown')
                    }
                )
                
                print(f"Created document version {version} for {doc_id}")
            else:
                print(f"No final output found for {doc_id}")
                
        except Exception as e:
            print(f"Failed to create document version for {doc_id}: {str(e)}")
            raise
    
    def _create_markdown_summary(self, pbj_result: Dict[str, Any]) -> str:
        """Create a markdown summary of the processing results."""
        pipeline_info = pbj_result.get('pipeline_info', {})
        data_summary = pbj_result.get('data_summary', {})
        
        summary = f"""# Document Processing Summary

## Pipeline Information
- **Processing Time**: {pipeline_info.get('total_processing_time_seconds', 0):.2f} seconds
- **OpenAI Model**: {pipeline_info.get('openai_model', 'unknown')}
- **LlamaParse Mode**: {pipeline_info.get('llamaparse_mode', 'unknown')}

## Data Summary
- **Total Pages**: {data_summary.get('total_pages', 0)}
- **Total Tables**: {data_summary.get('total_tables', 0)}
- **Unique Keywords**: {data_summary.get('unique_keywords', 0)}

## Page Titles
{chr(10).join(f"- {title}" for title in data_summary.get('page_titles', []))}

## Processing Stages
1. **Peanut (Parse)**: PDF → Markdown
2. **Butter (Better)**: Markdown → Enhanced Markdown  
3. **Jelly (JSON)**: Enhanced Markdown → Structured JSON
4. **Toast (Format)**: Column-based → Row-based JSON

*Processed by Bodega RAG Pipeline*
"""
        return summary
    
    def _get_content_type(self, file_path: Path) -> str:
        """Get the appropriate content type for a file."""
        suffix = file_path.suffix.lower()
        content_types = {
            '.json': 'application/json',
            '.md': 'text/markdown',
            '.txt': 'text/plain',
            '.pdf': 'application/pdf',
            '.html': 'text/html'
        }
        return content_types.get(suffix, 'application/octet-stream')
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health information for both PB&J and soda components."""
        health = {
            "bodega_version": "0.1.0",
            "timestamp": datetime.now().isoformat(),
            "soda_health": self.soda.get_system_health(),
            "pbj_config": {
                "output_base_dir": self.pbj_config.output_base_dir,
                "use_premium_mode": self.pbj_config.use_premium_mode,
                "openai_model": self.pbj_config.openai_model
            }
        }
        return health 