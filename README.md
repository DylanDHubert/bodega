# 🏪 Bodega - Complete RAG Processing Pipeline

A comprehensive document processing pipeline that combines PDF parsing, AI enhancement, structured data extraction, and manual review workflows.

## 🚀 Complete Pipeline Overview

Bodega provides a complete end-to-end solution:

```
PDF Input → PB&J Processing → AWS Upload → Inspector Review → Final AWS Upload
```

### Pipeline Steps:

1. **📄 PDF Input** - Take any PDF document
2. **🥪 PB&J Processing** - Parse, enhance, and structure the content
3. **☁️ AWS Upload** - Store intermediate results in S3
4. **🔍 Inspector Review** - Manual review and correction via Streamlit
5. **📤 Final Upload** - Upload approved final results to AWS with FINAL tag

## 🏗️ Architecture

Bodega integrates three core components:

- **🥪 PB&J (Peanut Butter & Jelly)** - PDF processing pipeline
- **🥤 Soda** - AWS document storage and state management  
- **🔍 Inspector** - Streamlit-based manual review interface

## 📦 Installation

```bash
# Clone the repository
git clone <repository-url>
cd bodega

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file with your API credentials:

```bash
# Required for PDF processing
LLAMAPARSE_API_KEY=your_llamaparse_key
OPENAI_API_KEY=your_openai_key

# Required for AWS storage
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1

# Optional: S3 bucket name (defaults to 'bodega-documents')
BODEGA_BUCKET=your-bucket-name
```

### Configuration File

Edit `config.yaml` to customize processing settings:

```yaml
# PB&J Pipeline Settings
pbj:
  output_base_dir: "processed_documents"
  use_premium_mode: false
  openai_model: "gpt-4"
  
# AWS Storage Settings  
aws:
  bucket_name: "bodega-documents"
  region: "us-east-1"
  
# Inspector Settings
inspector:
  port: 8501
  auto_open_browser: true
```

## 🚀 Quick Start

### Complete End-to-End Pipeline

```python
from bodega.bodega import Bodega

# Initialize Bodega
bodega = Bodega(
    aws_bucket="your-bucket-name",
    aws_region="us-east-1"
)

# Run complete pipeline: PDF → PB&J → AWS → Inspector → Final AWS
result = bodega.process_complete_pipeline(
    pdf_path="document.pdf",
    launch_inspector=True,  # Opens Inspector after processing
    auto_upload_final=False  # Manual final upload after review
)

print(f"Pipeline complete! Document ID: {result['document_info']['doc_id']}")
```

### Step-by-Step Processing

```python
# Step 1: Process PDF with PB&J
result = bodega.process_document("document.pdf", upload_to_aws=True)

# Step 2: Launch Inspector for review
document_folder = result['pbj_pipeline']['pipeline_info']['document_folder']
bodega.launch_inspector(document_folder=document_folder)

# Step 3: Upload final approved output
bodega.upload_final_inspected_output(document_folder=document_folder)
```

## 📊 Pipeline Capabilities

### Stage 1: PB&J Processing
- **Input**: PDF documents
- **Output**: Structured JSON + Enhanced Markdown
- **Storage**: Local `processed_documents/` folder
- **Features**:
  - PDF parsing with LlamaParse
  - AI-powered content enhancement
  - Table extraction and formatting
  - Keyword extraction
  - Page-by-page processing

### Stage 2: AWS Upload
- **Input**: Processed document folder
- **Output**: S3 objects in `processed/{doc_id}/`
- **Storage**: AWS S3 bucket
- **Features**:
  - Complete folder structure preservation
  - Document state management
  - Metadata tracking
  - Version control

### Stage 3: Inspector Review
- **Input**: Processed document folder
- **Output**: Corrected/approved data
- **Interface**: Streamlit web app
- **Features**:
  - Visual document review
  - Data correction interface
  - Export final results
  - Quality assurance workflow

### Stage 4: Final Upload
- **Input**: Inspector-approved data
- **Output**: S3 objects in `final/{doc_id}/`
- **Storage**: AWS S3 bucket with FINAL state
- **Features**:
  - Final approved data storage
  - Document state transition to FINAL
  - Audit trail completion

## 📁 File Structure

```
bodega/
├── src/bodega/
│   ├── bodega.py              # Main orchestrator
│   │   ├── bodega.py          # Main PB&J orchestrator
│   │   ├── peanut.py          # PDF parsing
│   │   ├── butter.py          # Content enhancement
│   │   ├── jelly.py           # JSON structuring
│   │   └── toast.py           # Data formatting
│   ├── soda/                  # AWS document storage
│   │   ├── document_store.py  # Main storage interface
│   │   ├── s3_ops.py          # S3 operations
│   │   └── document_states.py # State management
│   └── inspector/             # Manual review interface
│       ├── sandwich_inspector_app.py  # Streamlit app
│       └── launch.py          # App launcher
├── processed_documents/       # Local processing output
├── config.yaml               # Configuration
├── requirements.txt          # Dependencies
└── test_bodega.py           # Test script
```

## 🧪 Testing

Run the complete pipeline test:

```bash
# Set environment variables
export LLAMAPARSE_API_KEY="your_key"
export OPENAI_API_KEY="your_key"
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"

# Run test
python test_bodega.py
```

The test will:
1. Process `test_data.pdf` with PB&J
2. Upload intermediate results to AWS
3. Launch Inspector for manual review
4. Provide instructions for final upload

## 🔍 Inspector Interface

The Inspector provides a web-based interface for:

- **Document Review**: Visual inspection of processed content
- **Data Correction**: Edit extracted tables and text
- **Quality Assurance**: Verify processing accuracy
- **Export**: Generate final approved output

Launch Inspector:
```python
bodega.launch_inspector(document_folder="path/to/processed/doc")
```

## 📈 Document States

Documents progress through these states:

1. **RAW** - Initial upload
2. **PROCESSING** - PB&J pipeline running
3. **PROCESSED** - Pipeline complete, ready for review
4. **FINAL** - Inspector approved, final upload complete

## 🛠️ Advanced Usage

### Custom PB&J Configuration

```python
pbj_config = {
    'use_premium_mode': True,
    'openai_model': 'gpt-4-turbo',
    'output_base_dir': 'custom_output'
}

bodega = Bodega(
    aws_bucket="my-bucket",
    pbj_config=pbj_config
)
```

### Batch Processing

```python
# Process multiple documents
documents = ["doc1.pdf", "doc2.pdf", "doc3.pdf"]

for doc in documents:
    result = bodega.process_document(doc, upload_to_aws=True)
    print(f"Processed: {result['doc_id']}")
```

### Document Management

```python
# List pending documents
pending = bodega.list_pending_documents()

# Get processed documents
processed = bodega.get_processed_documents()

# Retrieve document content
content = bodega.get_document_content("doc_id")
```

## 🔧 Troubleshooting

### Common Issues

1. **Missing API Keys**
   - Ensure `LLAMAPARSE_API_KEY` and `OPENAI_API_KEY` are set
   - Check `.env` file or environment variables

2. **AWS Permissions**
   - Verify S3 bucket access permissions
   - Check AWS credentials and region

3. **Inspector Not Launching**
   - Ensure Streamlit is installed: `pip install streamlit`
   - Check port availability (default: 8501)

4. **Processing Failures**
   - Check PDF file format and size
   - Verify API rate limits
   - Review error logs in console output

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

bodega = Bodega(aws_bucket="test-bucket")
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Support

For issues and questions:
- Check the troubleshooting section
- Review error logs
- Open an issue on GitHub