"""
Bodega - Complete RAG Processing Pipeline Orchestrator

Combines PDF processing (PB&J) with AWS document storage and state management.

Workflow:
1. Process PDF with PB&J pipeline
2. Upload intermediate results to AWS (Soda)
3. Launch Inspector (Streamlit) for manual review/correction
4. Upload final approved data to AWS
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
from .inspector.launch import launch_inspector_app


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
    
    def process_complete_pipeline(
        self,
        pdf_path: str,
        doc_id: Optional[str] = None,
        launch_inspector: bool = True,
        auto_upload_final: bool = False
    ) -> Dict[str, Any]:
        """
        Complete end-to-end pipeline: PDF → PB&J → AWS → Inspector → Final AWS
        
        Args:
            pdf_path: Path to the PDF file
            doc_id: Optional document ID (auto-generated if not provided)
            launch_inspector: Whether to launch the Inspector app after processing
            auto_upload_final: Whether to automatically upload final output after Inspector
            
        Returns:
            Dictionary with complete pipeline results
        """
        print("🏪 STARTING COMPLETE BODEGA PIPELINE")
        print("=" * 60)
        
        pipeline_start = datetime.now()
        
        try:
            # Step A: Process PDF with PB&J
            print("\n📄 STEP A: Processing PDF with PB&J Pipeline")
            print("-" * 40)
            result = self.process_document(pdf_path, doc_id, upload_to_aws=True)
            
            document_folder = result['pbj_pipeline']['pipeline_info']['document_folder']
            doc_id = result['doc_id']
            
            print(f"✅ PB&J Processing Complete - Document ID: {doc_id}")
            print(f"📁 Output Folder: {document_folder}")
            
            # Step B: Upload to AWS (already done in process_document)
            print(f"\n☁️  STEP B: Uploaded to AWS (Bucket: {self.soda.bucket})")
            print("-" * 40)
            print(f"✅ Intermediate results uploaded to s3://{self.soda.bucket}/processed/{doc_id}/")
            print(f"✅ Document state: {result['soda_storage']['document_state']}")
            
            # Step C: Launch Inspector
            if launch_inspector:
                print(f"\n🔍 STEP C: Launching Inspector for Manual Review")
                print("-" * 40)
                print(f"🚀 Opening Streamlit Inspector app...")
                print(f"📂 Review folder: {document_folder}")
                print(f"🌐 Inspector will open in your browser")
                print(f"📋 After review, use 'Export Final' in the Inspector")
                
                # Launch the inspector
                self.launch_inspector(document_folder=document_folder)
                
                if auto_upload_final:
                    print(f"\n⏳ Waiting for Inspector review to complete...")
                    print(f"💡 After completing review in Inspector, the final upload will happen automatically")
                    # Note: In a real implementation, you might want to wait for user confirmation
                    # or check for the existence of inspector export files
                else:
                    print(f"\n📝 Manual Upload Required:")
                    print(f"   After completing review in Inspector, run:")
                    print(f"   bodega.upload_final_inspected_output(document_folder='{document_folder}')")
            
            # Compile final pipeline result
            pipeline_end = datetime.now()
            total_time = (pipeline_end - pipeline_start).total_seconds()
            
            pipeline_result = {
                "pipeline_info": {
                    "completed_at": pipeline_end.isoformat(),
                    "total_pipeline_time_seconds": total_time,
                    "pipeline_steps": ["PB&J Processing", "AWS Upload", "Inspector Launch"],
                    "status": "COMPLETE"
                },
                "document_info": {
                    "doc_id": doc_id,
                    "pdf_path": pdf_path,
                    "document_folder": document_folder,
                    "aws_bucket": self.soda.bucket
                },
                "pbj_result": result,
                "next_steps": {
                    "inspector_launched": launch_inspector,
                    "final_upload_required": not auto_upload_final,
                    "final_upload_command": f"bodega.upload_final_inspected_output(document_folder='{document_folder}')"
                }
            }
            
            print(f"\n🎉 COMPLETE PIPELINE FINISHED!")
            print("=" * 60)
            print(f"📊 Total Pipeline Time: {total_time:.2f} seconds")
            print(f"📄 Document ID: {doc_id}")
            print(f"📁 Local Folder: {document_folder}")
            print(f"☁️  AWS Bucket: {self.soda.bucket}")
            
            if launch_inspector:
                print(f"\n🔍 Inspector launched - complete your review in the browser")
                if not auto_upload_final:
                    print(f"📤 After review, run the final upload command shown above")
            
            return pipeline_result
            
        except Exception as e:
            print(f"❌ Complete pipeline failed: {str(e)}")
            raise
    
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
    
    def launch_inspector(self, document_folder: Optional[str] = None, port: int = 8501):
        """
        Launch the Streamlit inspector app for manual review of a processed document folder.
        Args:
            document_folder: Path to the processed document folder to review
            port: Port to run the Streamlit app on (default: 8501)
        """
        if document_folder is None:
            # Default to the most recent processed folder
            processed_dir = Path(self.pbj_config.output_base_dir)
            folders = sorted(processed_dir.glob("*/"), key=os.path.getmtime, reverse=True)
            if not folders:
                print("No processed document folders found.")
                return
            document_folder = str(folders[0])
        print(f"Launching inspector for folder: {document_folder}")
        launch_inspector_app(document_folder=document_folder, port=port)

    def upload_final_inspected_output(self, document_folder: Optional[str] = None, doc_id: Optional[str] = None):
        """
        Upload the inspector's final output (after manual review) to AWS and update document state.
        Args:
            document_folder: Path to the reviewed document folder (if None, use most recent)
            doc_id: Document ID (if None, inferred from folder name)
        """
        # Determine folder
        if document_folder is None:
            processed_dir = Path(self.pbj_config.output_base_dir)
            folders = sorted(processed_dir.glob("*/"), key=os.path.getmtime, reverse=True)
            if not folders:
                print("No processed document folders found.")
                return
            document_folder = str(folders[0])
        folder_path = Path(document_folder)
        if not folder_path.exists():
            print(f"Document folder not found: {document_folder}")
            return
        # Infer doc_id if not provided
        if doc_id is None:
            doc_id = folder_path.name
        print(f"Uploading inspector-approved output for doc_id: {doc_id}")
        # List of files to upload
        files_to_upload = [
            "final_output.json",
            "inspector_metadata.json",
            "pipeline_summary.json",
            "document_metadata.json",
            folder_path.name + ".pdf"
        ]
        for fname in files_to_upload:
            fpath = folder_path / fname
            if fpath.exists():
                s3_key = f"final/{doc_id}/{fname}"
                with open(fpath, "rb") as f:
                    content = f.read()
                put_object_content(
                    bucket=self.soda.bucket,
                    key=s3_key,
                    content=content,
                    content_type=self._get_content_type(fpath)
                )
                print(f"Uploaded {s3_key}")
            else:
                print(f"File not found, skipping: {fpath}")
        # Update document state to FINAL
        try:
            self.soda.state_manager.transition_document_state(
                f"raw/{doc_id}/original.pdf",
                DocumentState.FINAL,
                metadata={"finalized_at": datetime.now().isoformat()}
            )
            print(f"Document {doc_id} marked as FINAL.")
        except Exception as e:
            print(f"Failed to update document state: {e}") 