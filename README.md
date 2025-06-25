# Bodega

A complete RAG (Retrieval-Augmented Generation) processing pipeline that combines PDF parsing, enhancement, JSON extraction, and AWS document storage with state management.

## Overview

Bodega integrates two powerful components:
- **PB&J Pipeline**: Complete PDF processing with 4 stages (Peanut, Butter, Jelly, Toast)
- **Soda**: AWS S3-based document storage with state management

## Quick Start

```python
from src.bodega import Bodega

# Initialize Bodega with AWS integration
bodega = Bodega(
    aws_bucket="your-s3-bucket",
    aws_region="us-east-1",
    use_premium=False,
    openai_model="gpt-4"
)

# Process a PDF through the complete pipeline
result = bodega.process_document("document.pdf")
print(f"Document ID: {result['doc_id']}")
print(f"Processing Time: {result['processing_info']['total_time_seconds']:.2f}s")
```

## Pipeline Flow

1. **PDF Input** → **PB&J Processing** (4-stage pipeline)
2. **Intermediate Results** → **AWS Storage** (Soda)
3. **Document State Management** → **Final Results**
4. **Content Retrieval** → **RAG Applications**

## PB&J Pipeline Stages

1. **Peanut (Parse)**: PDF → Markdown using LlamaParse
2. **Butter (Better)**: Markdown → Enhanced Markdown using OpenAI
3. **Jelly (JSON)**: Enhanced Markdown → Structured JSON using OpenAI
4. **Toast (Format)**: Column-based → Row-based JSON conversion

## Soda Document States

- **RAW**: Uploaded PDF awaiting processing
- **PROCESSING**: Currently being processed
- **PROCESSED**: Successfully processed, outputs created
- **FINAL**: Approved output, ready for use

## Configuration

### Environment Variables

```bash
# Required for AWS integration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
DOC_BUCKET=your-s3-bucket-name
AWS_REGION=us-east-1

# Required for PDF processing
LLAMAPARSE_API_KEY=your_llamaparse_key
OPENAI_API_KEY=your_openai_key
```

### Programmatic Configuration

```python
from src.bodega import Bodega

bodega = Bodega(
    aws_bucket="my-documents-bucket",
    aws_region="us-west-2",
    pbj_config={
        "output_base_dir": "processed_docs",
        "use_premium_mode": True,
        "openai_model": "gpt-4-turbo"
    }
)
```

## Usage Examples

### Process a Document

```python
# Process with AWS upload
result = bodega.process_document(
    pdf_path="document.pdf",
    upload_to_aws=True
)

# Local processing only
result = bodega.process_document(
    pdf_path="document.pdf", 
    upload_to_aws=False
)
```

### List Documents

```python
# List pending documents
pending = bodega.list_pending_documents(limit=10)

# List processed documents
processed = bodega.get_processed_documents(limit=10)
```

### Retrieve Content

```python
# Get processed content
content = bodega.get_document_content("doc_123")
if content:
    print(f"Markdown: {content['md_content']}")
    print(f"JSON: {content['json_content']}")
```

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
export LLAMAPARSE_API_KEY="your_key"
export OPENAI_API_KEY="your_key"
export DOC_BUCKET="your_bucket"
```

## Requirements

- Python 3.8+
- LlamaParse API key
- OpenAI API key
- AWS credentials (for S3 storage)
- S3 bucket for document storage

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│   PDF Input     │    │   AWS S3        │
│   (Local/URL)   │    │   (Soda)        │
└─────────┬───────┘    └─────────┬───────┘
          │                      │
          └──────────┬───────────┘
                     │
          ┌──────────▼───────────┐
          │    Bodega            │
          │  (Orchestrator)      │
          └──────────┬───────────┘
                     │
          ┌──────────▼───────────┐
          │   PB&J Pipeline      │
          │  (4-stage process)   │
          └──────────┬───────────┘
                     │
          ┌──────────▼───────────┐
          │   Final Output       │
          │  (JSON + Metadata)   │
          └──────────────────────┘
```

## Example Output Structure

```
processed_documents/
└── document_20241201_143022/
    ├── original.pdf
    ├── document_metadata.json
    ├── pipeline_summary.json
    ├── final_output.json
    ├── 01_parsed_markdown/
    ├── 02_enhanced_markdown/
    └── 03_cleaned_json/
```

## API Keys Required

- **LlamaParse**: [https://cloud.llamaindex.ai/](https://cloud.llamaindex.ai/)
- **OpenAI**: [https://platform.openai.com/account/api-keys](https://platform.openai.com/account/api-keys)