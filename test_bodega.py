#!/usr/bin/env python3
"""
Bodega Complete Pipeline Test
Test the complete end-to-end Bodega RAG processing pipeline with test_data.pdf

Pipeline Steps:
A. Take PDF → B. Process with PB&J → C. Upload to AWS → D. Launch Inspector → E. Upload Final to AWS
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from bodega.bodega import Bodega
except ImportError:
    # Fallback import
    sys.path.insert(0, str(Path(__file__).parent))
    from src.bodega.bodega import Bodega

def test_complete_pipeline():
    """Test the complete end-to-end Bodega pipeline."""
    
    print("🏪 BODEGA COMPLETE PIPELINE TEST")
    print("=" * 60)
    print("Pipeline: PDF → PB&J → AWS → Inspector → Final AWS")
    print("=" * 60)
    
    # Check if test file exists
    test_pdf = "test_data.pdf"
    if not Path(test_pdf).exists():
        print(f"❌ Test file not found: {test_pdf}")
        return
    
    print(f"📄 Test file: {test_pdf}")
    print(f"📏 File size: {Path(test_pdf).stat().st_size / 1024:.1f} KB")
    
    # Initialize Bodega with dummy AWS bucket for testing
    print("\n🔧 Initializing Bodega...")
    try:
        bodega = Bodega(
            aws_bucket="dummy-bucket",  # Dummy bucket for testing
            aws_region="us-east-1",
            use_premium=False,
            openai_model="gpt-4"
        )
        print("✅ Bodega initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize Bodega: {str(e)}")
        return
    
    # Check system health
    print("\n📊 System Health Check:")
    try:
        health = bodega.get_system_health()
        print(f"  Bodega Version: {health['bodega_version']}")
        print(f"  PB&J Output Dir: {health['pbj_config']['output_base_dir']}")
        print(f"  OpenAI Model: {health['pbj_config']['openai_model']}")
        print(f"  LlamaParse Mode: {'Premium' if health['pbj_config']['use_premium_mode'] else 'Standard'}")
        print(f"  AWS Bucket: {health['soda_health'].get('bucket_name', 'Not configured')}")
    except Exception as e:
        print(f"  ⚠️  Health check failed: {str(e)}")
    
    # Run the complete pipeline
    print(f"\n🔄 Running Complete Pipeline...")
    print("=" * 60)
    
    try:
        # This will run the complete pipeline: PDF → PB&J → AWS → Inspector
        result = bodega.process_complete_pipeline(
            pdf_path=test_pdf,
            launch_inspector=True,  # Launch Inspector after processing
            auto_upload_final=False  # Manual final upload after Inspector review
        )
        
        print(f"\n🎉 COMPLETE PIPELINE SUCCESS!")
        print("=" * 60)
        print(f"📄 Document ID: {result['document_info']['doc_id']}")
        print(f"📁 Local Folder: {result['document_info']['document_folder']}")
        print(f"☁️  AWS Bucket: {result['document_info']['aws_bucket']}")
        print(f"⏱️  Total Time: {result['pipeline_info']['total_pipeline_time_seconds']:.2f} seconds")
        
        # Show next steps
        print(f"\n📋 NEXT STEPS:")
        print(f"1. 🔍 Complete review in Inspector (browser should be open)")
        print(f"2. 📤 After review, run the final upload command:")
        print(f"   {result['next_steps']['final_upload_command']}")
        
        return result
        
    except Exception as e:
        print(f"❌ Complete pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def test_step_by_step():
    """Test the pipeline step by step for debugging."""
    
    print("\n🔧 STEP-BY-STEP PIPELINE TEST")
    print("=" * 60)
    
    test_pdf = "test_data.pdf"
    if not Path(test_pdf).exists():
        print(f"❌ Test file not found: {test_pdf}")
        return
    
    # Initialize Bodega
    bodega = Bodega(
        aws_bucket="dummy-bucket",
        aws_region="us-east-1",
        use_premium=False,
        openai_model="gpt-4"
    )
    
    try:
        # Step A: Process PDF with PB&J
        print("\n📄 STEP A: Processing PDF with PB&J")
        print("-" * 40)
        result = bodega.process_document(test_pdf, upload_to_aws=True)
        print(f"✅ PB&J Complete - Document ID: {result['doc_id']}")
        
        # Step B: Upload to AWS (already done above)
        print(f"\n☁️  STEP B: Uploaded to AWS")
        print("-" * 40)
        print(f"✅ Intermediate results uploaded")
        print(f"✅ Document state: {result['soda_storage']['document_state']}")
        
        # Step C: Launch Inspector
        print(f"\n🔍 STEP C: Launching Inspector")
        print("-" * 40)
        document_folder = result['pbj_pipeline']['pipeline_info']['document_folder']
        print(f"🚀 Opening Inspector for: {document_folder}")
        bodega.launch_inspector(document_folder=document_folder)
        
        # Step D: Instructions for final upload
        print(f"\n📤 STEP D: Final Upload Instructions")
        print("-" * 40)
        print(f"1. Complete review in Inspector")
        print(f"2. Use 'Export Final' in Inspector")
        print(f"3. Run final upload command:")
        print(f"   bodega.upload_final_inspected_output(document_folder='{document_folder}')")
        
        return result
        
    except Exception as e:
        print(f"❌ Step-by-step test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def test_inspector_only():
    """Test launching Inspector for an existing processed document."""
    print("\n🔍 INSPECTOR-ONLY TEST")
    print("=" * 60)
    
    # Find the most recent processed document
    processed_dir = Path("processed_documents")
    if not processed_dir.exists():
        print("❌ No processed documents found")
        return
    
    folders = [f for f in processed_dir.iterdir() if f.is_dir()]
    if not folders:
        print("❌ No document folders found")
        return
    
    latest_folder = max(folders, key=lambda x: x.stat().st_mtime)
    print(f"📁 Found document folder: {latest_folder.name}")
    
    # Initialize Bodega
    bodega = Bodega(
        aws_bucket="dummy-bucket",
        aws_region="us-east-1"
    )
    
    # Launch Inspector
    print(f"🚀 Launching Inspector for: {latest_folder}")
    bodega.launch_inspector(document_folder=str(latest_folder))

if __name__ == "__main__":
    print("Starting Bodega Complete Pipeline Test...")
    print("=" * 60)
    
    # Check environment
    print("🔍 Environment Check:")
    required_vars = ["LLAMAPARSE_API_KEY", "OPENAI_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {missing_vars}")
        print("💡 Set these in your .env file or environment")
        print("   For testing, you can use dummy values for AWS credentials")
    else:
        print("✅ All required environment variables found")
    
    # Run the complete pipeline test
    result = test_complete_pipeline()
    
    if result:
        print(f"\n🎉 Test completed successfully!")
        print(f"📁 Check the output in: {result['document_info']['document_folder']}")
        print(f"🔍 Inspector should be open in your browser")
    else:
        print(f"\n❌ Test failed!")
        print(f"💡 You can also try:")
        print(f"   - test_step_by_step() for debugging")
        print(f"   - test_inspector_only() for existing documents")
    
    print("=" * 60) 