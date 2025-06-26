# Document Accuracy Inspector

**A Visual Quality Control Tool for Reviewing Processed PDF Documents**

The Document Accuracy Inspector is a Streamlit web application designed for reviewing and editing documents that have already been processed through a PDF extraction pipeline. It provides a side-by-side comparison interface to verify the accuracy of extracted data against original PDFs, make corrections, and export clean final results.

---

## **What Does It Do?**

- **Load Processed Documents**: Review documents from the processed_documents folder
- **Side-by-Side Verification**: Compare extracted data against original PDF pages
- **Manual Corrections**: Edit table data (JSON format) and markdown content
- **Portfolio Tagging**: Categorize documents (ts knee, knee, hips)
- **Quality Control**: Approve, flag, or edit each page with visual progress tracking
- **Export Clean Results**: Generate consolidated final output files

---

## **Setup Guide**

### **Step 1: Install Dependencies**

```bash
# Install required packages
pip install -r requirements_inspector.txt
```

### **Step 2: Prepare Input Data**

The app expects processed documents in the following structure:

```
processed_documents/
└── document_name_YYYYMMDD_HHMMSS/
    ├── document_name.pdf                    # Original PDF file
    ├── document_metadata.json               # Processing metadata
    ├── pipeline_summary.json                # Pipeline information
    ├── inspector_metadata.json              # Review tracking (created by app)
    ├── 01_parsed_markdown/                  # Raw extracted markdown
    │   ├── page_1.md
    │   ├── page_2.md
    │   └── ...
    ├── 02_enhanced_markdown/                # Enhanced markdown content
    │   ├── page_1.md
    │   ├── page_2.md
    │   └── ...
    └── 03_cleaned_json/                     # Structured data for each page
        ├── page_1.json
        ├── page_2.json
        └── ...
```

### **Step 3: Run the Application**

```bash
streamlit run sandwich_inspector_app.py
```

The app will open in your browser at `http://localhost:8501`

---

## **How to Use**

### **Basic Workflow**

#### **1. Load a Document**

- Select a processed document from the sidebar dropdown
- Click "Load Document" to start reviewing
- The app will load all pages and display the first page

#### **2. Review Pages**

**Left Panel - Ground Truth:**
- Original PDF page for reference

**Right Panel - Extracted Data:**
- Tables tab: View/edit extracted table data
- Markdown tab: View/edit page content
- Fixed height containers keep PDF visible while scrolling

#### **3. Edit Content (Optional)**

**Table Editing:**
- **View Mode**: Tables displayed as clean dataframes
- **Edit Mode**: Raw JSON editing for precise control
- Real-time validation with error feedback

**Markdown Editing:**
- **View Mode**: Scrollable read-only content (500px height)
- **Edit Mode**: Full text area for content modification

#### **4. Portfolio Tagging**

- Select category: "ts knee", "knee", or "hips"
- Tags are saved immediately and persist per document
- Included in final export metadata

#### **5. Quality Control**

For each page:
- **Approve Page**: Mark as accurately extracted
- **Flag Issues**: Mark for later review
- **Save Changes**: Save current edits
- **Edit Mode**: Toggle between view/edit modes

#### **6. Navigation**

- **Previous/Next buttons**: Navigate sequentially
- **Page dropdown**: Jump to specific pages
- **Quick navigation**: Click page numbers with status indicators
- **Progress tracking**: Visual completion status

#### **7. Final Export**

Click "Export Final" to create consolidated output with:
- Clean JSON with all corrections
- Consolidated markdown content
- Original PDF and metadata
- Portfolio tag information

---

## **Expected Input Structure**

### **Page JSON Format (03_cleaned_json/page_X.json)**

```json
{
  "page_id": "page_1",
  "title": "Page Title",
  "summary": "Page summary",
  "keywords": ["keyword1", "keyword2", "keyword3"],
  "tables": [
    {
      "title": "Table Title",
      "data": [
        {
          "Column 1": "Value 1",
          "Column 2": "Value 2",
          "Column 3": "Value 3"
        },
        {
          "Column 1": "Value 4",
          "Column 2": "Value 5",
          "Column 3": "Value 6"
        }
      ],
      "table_id": "table_1",
      "description": "Table description",
      "metadata": {
        "row_count": 2,
        "column_count": 3
      }
    }
  ],
  "raw_content": "Markdown content for the page...",
  "processing_metadata": {
    "source_file": "path/to/source",
    "processed_at": "2025-01-24T14:24:15.095699",
    "model_used": "gpt-4"
  }
}
```

### **Enhanced Markdown Format (02_enhanced_markdown/page_X.md)**

Plain markdown content that can be edited in the interface.

---

## **Output Structure**

### **Final Export Folder**

When you export, the app creates:

```
final_pdfname_YYYYMMDD_HHMMSS/
├── pdfname.pdf                    # Original PDF
├── pdfname_final.json             # Consolidated JSON (all pages)
├── pdfname_final.md               # Consolidated markdown (all pages)
├── document_metadata.json         # Original processing metadata
├── pipeline_summary.json          # Pipeline information
└── inspector_metadata.json        # Review metadata with portfolio tag
```

### **Consolidated JSON Structure**

```json
{
  "document_info": {
    "document_name": "document_name",
    "export_date": "2025-01-24T15:30:45.123456",
    "total_pages": 4,
    "portfolio": "knee",
    "review_status": {
      "approved_pages": 3,
      "flagged_pages": 1
    }
  },
  "pages": [
    {
      "page_number": 1,
      "title": "Page Title",
      "keywords": ["keyword1", "keyword2"],
      "tables": [
        {
          "title": "Table Title",
          "data": [
            {"Column 1": "Value 1", "Column 2": "Value 2"},
            {"Column 1": "Value 3", "Column 2": "Value 4"}
          ]
        }
      ]
    }
  ]
}
```

### **Inspector Metadata**

```json
{
  "page_statuses": {
    "0": "approved",
    "1": "approved", 
    "2": "flagged",
    "3": "approved"
  },
  "flagged_pages": [2],
  "portfolio": "knee",
  "last_updated": "2025-01-24T15:30:45.123456",
  "total_pages": 4
}
```

---

## **Features**

### **Visual Interface**
- Side-by-side PDF and data comparison
- Clean, professional design optimized for accuracy review
- Fixed-height scrollable content areas
- Real-time status indicators

### **Editing Capabilities**
- JSON table editing with validation
- Markdown content editing
- Auto-save functionality
- Undo-friendly workflow

### **Quality Control**
- Page-by-page approval workflow
- Flag problematic pages for review
- Progress tracking with visual indicators
- Batch export capabilities

### **Organization**
- Portfolio tagging system
- Persistent document state
- Unique timestamped outputs
- Comprehensive metadata tracking

---

## **Requirements**

- Python 3.8+
- Streamlit
- pandas
- PyMuPDF (for PDF viewing)
- pathlib2
- dataclasses-json

---

## **Troubleshooting**

### **Document Won't Load**
- Check that the processed_documents folder structure is correct
- Ensure page JSON files exist in 03_cleaned_json/
- Verify JSON format matches expected structure

### **Tables Don't Display**
- Check that table data is in the correct format (list of dictionaries)
- Verify JSON syntax is valid
- Ensure table "data" field contains the row information

### **PDF Not Showing**
- Confirm PDF file exists in the document folder
- Check that PDF filename matches the document name pattern
- Verify PyMuPDF is properly installed

---

*Document Accuracy Inspector - Ensuring reliable document processing quality control.* 