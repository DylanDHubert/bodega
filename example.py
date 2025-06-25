#!/usr/bin/env python3
"""
Bodega Example - Complete RAG Processing Pipeline

This example demonstrates how to use the integrated Bodega system
that combines PB&J PDF processing with soda document storage.
"""

import os
from pathlib import Path
from src.bodega import Bodega

def main():
    """Example usage of the Bodega RAG processing pipeline."""
    
    # Configuration
    aws_bucket = os.getenv("DOC_BUCKET", "your-s3-bucket-name")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    
    # Initialize Bodega
    print("🏪 Initializing Bodega RAG Pipeline...")
    bodega = Bodega(
        aws_bucket=aws_bucket,
        aws_region=aws_region,
        use_premium=False,  # Set to True for LlamaParse premium
        openai_model="gpt-4"
    )
    
    # Check system health
    print("\n📊 System Health Check:")
    health = bodega.get_system_health()
    print(f"Bodega Version: {health['bodega_version']}")
    print(f"Soda Bucket: {health['soda_health'].get('bucket_name', 'Not configured')}")
    print(f"PB&J Output Dir: {health['pbj_config']['output_base_dir']}")
    
    # List pending documents (if any)
    print("\n📋 Pending Documents:")
    pending_docs = bodega.list_pending_documents(limit=5)
    if pending_docs:
        for doc in pending_docs:
            print(f"  - {doc['doc_id']} (uploaded: {doc['uploaded_at']})")
    else:
        print("  No pending documents found")
    
    # Example: Process a PDF document
    pdf_path = "example_document.pdf"  # Replace with your PDF path
    
    if Path(pdf_path).exists():
        print(f"\n🔄 Processing document: {pdf_path}")
        
        try:
            # Process the document through the complete pipeline
            result = bodega.process_document(
                pdf_path=pdf_path,
                upload_to_aws=True  # Set to False for local-only processing
            )
            
            print(f"\n✅ Processing completed!")
            print(f"Document ID: {result['doc_id']}")
            print(f"Processing Time: {result['processing_info']['total_time_seconds']:.2f} seconds")
            print(f"Pages Processed: {result['pbj_pipeline']['data_summary']['total_pages']}")
            print(f"Uploaded to AWS: {result['processing_info']['uploaded_to_aws']}")
            
            # Get the processed content
            content = bodega.get_document_content(result['doc_id'])
            if content:
                print(f"\n📄 Document Content Available:")
                print(f"  - Markdown Summary: {len(content.get('md_content', ''))} characters")
                print(f"  - JSON Data: {len(content.get('json_content', ''))} characters")
            
        except Exception as e:
            print(f"❌ Processing failed: {str(e)}")
    
    else:
        print(f"\n⚠️  PDF file not found: {pdf_path}")
        print("Please provide a valid PDF file path to test the pipeline.")
    
    # List processed documents
    print("\n📚 Processed Documents:")
    processed_docs = bodega.get_processed_documents(limit=5)
    if processed_docs:
        for doc in processed_docs:
            print(f"  - {doc['doc_id']} (version: {doc.get('current_version', 'unknown')})")
    else:
        print("  No processed documents found")

def example_without_aws():
    """Example using Bodega for local processing only."""
    
    print("🏪 Initializing Bodega for Local Processing...")
    bodega = Bodega(
        aws_bucket=None,  # No AWS bucket for local-only processing
        use_premium=False,
        openai_model="gpt-4"
    )
    
    pdf_path = "example_document.pdf"
    
    if Path(pdf_path).exists():
        print(f"🔄 Processing document locally: {pdf_path}")
        
        result = bodega.process_document(
            pdf_path=pdf_path,
            upload_to_aws=False  # Local processing only
        )
        
        print(f"✅ Local processing completed!")
        print(f"Document ID: {result['doc_id']}")
        print(f"Output saved to: {result['pbj_pipeline']['pipeline_info']['document_folder']}")

if __name__ == "__main__":
    print("=" * 60)
    print("🏪 BODEGA RAG PROCESSING PIPELINE EXAMPLE")
    print("=" * 60)
    
    # Check if AWS credentials are available
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("DOC_BUCKET"):
        print("AWS credentials detected - running full pipeline example")
        main()
    else:
        print("AWS credentials not detected - running local-only example")
        print("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and DOC_BUCKET for full functionality")
        example_without_aws()
    
    print("\n" + "=" * 60)
    print("Example completed!")
    print("=" * 60) 