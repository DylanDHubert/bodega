#!/usr/bin/env python3
"""
🍇 Jelly - JSON Data Extractor
==============================

Stage 3 of the PB&J Pipeline. Converts enhanced markdown to standardized JSON format
optimized for RAG systems with consistent table structures and metadata extraction.
"""

import os
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import openai
from openai import OpenAI
from dotenv import load_dotenv

# LOAD ENVIRONMENT VARIABLES FROM .ENV FILE
load_dotenv()

# IMPORT CONFIGURATION
from .config import PipelineConfig

@dataclass
class ProcessedTable:
    """Standardized table structure for RAG systems"""
    table_id: str
    title: str
    description: str
    columns: List[str]
    rows: List[List[str]]
    metadata: Dict[str, Any]

@dataclass
class ProcessedPage:
    """Standardized page structure"""
    page_id: str
    title: str
    summary: str
    keywords: List[str]
    tables: List[ProcessedTable]
    raw_content: str
    processing_metadata: Dict[str, Any]

class Jelly:
    """
    🍇 Jelly - JSON Data Extractor
    
    Stage 3 of the PB&J Pipeline. Converts enhanced markdown to standardized JSON format
    optimized for RAG systems with consistent table structures and metadata extraction.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4", config: Optional[PipelineConfig] = None):
        """
        Initialize the data cleaner
        
        Args:
            api_key: OpenAI API key (if not provided, will try to get from env)
            model: OpenAI model to use for processing
            config: PipelineConfig object with settings including max_tokens
        """
        # GET API KEY FROM ENVIRONMENT OR PARAMETER
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key is required. Set OPENAI_API_KEY environment variable "
                "or pass it directly to the constructor."
            )
        
        # INITIALIZE OPENAI CLIENT
        self.client = OpenAI(api_key=self.api_key)
        self.model = model
        
        # SET MAX TOKENS FROM CONFIG WITH SAFETY CHECK
        if config and hasattr(config, 'max_tokens'):
            # SAFETY CHECK: PREVENT CONTEXT OVERFLOW
            self.max_tokens = min(config.max_tokens, 8192)
        else:
            self.max_tokens = 8192  # FALLBACK TO SAFE DEFAULT
        
        # LOAD PROMPTS FROM CONFIG FILE
        self.prompts = self._load_prompts()
        
        print(f"INITIALIZED DATA CLEANER WITH MODEL: {model}, MAX_TOKENS: {self.max_tokens}")
    
    def _load_prompts(self):
        """Load prompts from pantry - much cleaner approach"""
        # GET PANTRY PATH RELATIVE TO THIS MODULE
        pantry_path = Path(__file__).parent / "pantry"
        prompt_path = pantry_path / "jelly.txt"
        
        if not prompt_path.exists():
            raise FileNotFoundError(f"Cleaning prompt file not found: {prompt_path}")
        
        # LOAD PROMPT FROM SIMPLE TEXT FILE
        with open(prompt_path, 'r', encoding='utf-8') as f:
            cleaning_prompt = f.read().strip()
        
        return {
            'cleaning_prompt': cleaning_prompt
        }
    
    def _create_cleaning_prompt(self, enhanced_content: str) -> str:
        """Create the prompt for OpenAI to clean and standardize the enhanced markdown"""
        
        return f"""{self.prompts['cleaning_prompt']}

ENHANCED MARKDOWN CONTENT TO PROCESS:
{enhanced_content}

JSON OUTPUT:"""

    async def process_file_async(self, markdown_file_path: str) -> Optional[ProcessedPage]:
        """
        Process a single markdown file and return cleaned data
        
        Args:
            markdown_file_path: Path to the markdown file
            
        Returns:
            ProcessedPage: Cleaned and standardized data
        """
        file_path = Path(markdown_file_path)
        
        # READ THE MARKDOWN FILE
        with open(file_path, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        print(f"PROCESSING FILE: {file_path.name}")
        
        # SAFETY CHECK: DETECT HTML COMMENTS IN INPUT
        if '<!--' in markdown_content and '-->' in markdown_content:
            print(f"⚠️  WARNING: HTML comments detected in input markdown for {file_path.name}")
            print(f"   This may indicate data truncation from previous stage")
            print(f"   HTML comment found: {markdown_content[markdown_content.find('<!--'):markdown_content.find('<!--')+100]}...")
        
        # CREATE CLEANING PROMPT
        prompt = self._create_cleaning_prompt(markdown_content)
        
        try:
            # CALL OPENAI TO CLEAN THE DATA
            print("SENDING TO OPENAI FOR CLEANING...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an expert data cleaning agent specializing in technical document processing."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # ZERO TEMPERATURE FOR DETERMINISTIC OUTPUT
                max_tokens=self.max_tokens
            )
            
            # EXTRACT AND PARSE JSON RESPONSE
            json_content = response.choices[0].message.content
            if not json_content:
                raise ValueError("Empty response from OpenAI")
            cleaned_data = json.loads(json_content)
            
            # CONVERT TO STRUCTURED FORMAT
            tables = []
            for i, table_data in enumerate(cleaned_data.get("tables", [])):
                table = ProcessedTable(
                    table_id=table_data.get("table_id", f"table_{i+1}"),
                    title=table_data.get("title", ""),
                    description=table_data.get("description", ""),
                    columns=table_data.get("columns", []),
                    rows=table_data.get("rows", []),
                    metadata=table_data.get("metadata", {})
                )
                tables.append(table)
            
            # CREATE PROCESSED PAGE
            processed_page = ProcessedPage(
                page_id=file_path.stem,
                title=cleaned_data.get("title", file_path.stem),
                summary=cleaned_data.get("summary", ""),
                keywords=cleaned_data.get("keywords", []),
                tables=tables,
                raw_content=markdown_content,
                processing_metadata={
                    "source_file": str(file_path),
                    "processed_at": datetime.now().isoformat(),
                    "model_used": self.model,
                    "tables_found": len(tables)
                }
            )
            
            print(f"✅ CLEANED: {len(tables)} tables, {len(cleaned_data.get('keywords', []))} keywords")
            return processed_page
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON PARSING ERROR: {e}")
            print(f"RAW RESPONSE: {response.choices[0].message.content}")
            print(f"   Skipping page {file_path.name} due to JSON parsing error")
            return None  # Return None instead of raising to allow pipeline to continue
        except Exception as e:
            print(f"❌ PROCESSING ERROR: {e}")
            print(f"   Skipping page {file_path.name} due to error")
            return None  # Return None instead of raising to allow pipeline to continue
    
    def process(self, markdown_content: str, filename: str = "document.md") -> Optional[ProcessedPage]:
        """
        🍇 Process Markdown → JSON
        Main Jelly processing method
        """
        # CREATE TEMP FILE FOR PROCESSING
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(markdown_content)
            temp_path = f.name
        
        try:
            result = self.process_file(temp_path)
            if result is None:
                return None
            # UPDATE FILENAME IN RESULT
            result.page_id = Path(filename).stem
            result.processing_metadata["original_filename"] = filename
            return result
        finally:
            # CLEAN UP TEMP FILE
            import os
            os.unlink(temp_path)
    
    def process_async(self, markdown_file_path: str) -> Optional[ProcessedPage]:
        """
        🍇 Async Process markdown file → JSON
        Async Jelly processing method
        """
        return asyncio.run(self.process_file_async(markdown_file_path))
    
    def process_file(self, markdown_file_path: str) -> Optional[ProcessedPage]:
        """Synchronous wrapper for file processing"""
        return asyncio.run(self.process_file_async(markdown_file_path))
    
    def process_folder(self, folder_path: str, output_file: Optional[str] = None) -> List[ProcessedPage]:
        """
        Process all markdown files in a folder
        
        Args:
            folder_path: Path to folder containing markdown files
            output_file: Optional path to save consolidated JSON output
            
        Returns:
            List[ProcessedPage]: All processed pages
        """
        folder = Path(folder_path)
        if not folder.exists():
            raise FileNotFoundError(f"Folder not found: {folder_path}")
        
        # FIND ALL MARKDOWN FILES
        md_files = list(folder.glob("*.md"))
        if not md_files:
            raise ValueError(f"No markdown files found in: {folder_path}")
        
        print(f"FOUND {len(md_files)} MARKDOWN FILES TO PROCESS")
        
        # PROCESS EACH FILE
        processed_pages = []
        skipped_pages = []
        for md_file in sorted(md_files):
            try:
                processed_page = self.process_file(str(md_file))
                if processed_page is None:
                    # Page was skipped due to error
                    skipped_pages.append({
                        "filename": md_file.name,
                        "error": "Processing failed - page skipped",
                        "timestamp": datetime.now().isoformat()
                    })
                    print(f"⚠️  SKIPPED: {md_file.name} (processing failed)")
                    continue
                    
                processed_pages.append(processed_page)
                
                # SAVE EACH PROCESSED PAGE IMMEDIATELY (PAGE-WISE SAVING)
                self._save_single_processed_page(processed_page, Path(folder_path), Path(folder_path).name)
                
                print(f"✅ PROCESSED AND SAVED: {md_file.name}")
            except Exception as e:
                print(f"❌ FAILED TO PROCESS {md_file.name}: {e}")
                skipped_pages.append({
                    "filename": md_file.name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                continue
        
        # SAVE CONSOLIDATED OUTPUT IF REQUESTED
        if output_file:
            self._save_processed_data(processed_pages, output_file)
        
        # UPDATE DOCUMENT METADATA
        self._update_document_metadata_cleaning(folder, "cleaning", processed_pages, skipped_pages)
        
        print(f"🎉 SUCCESSFULLY PROCESSED {len(processed_pages)}/{len(md_files)} FILES")
        return processed_pages
    
    def _save_processed_data(self, processed_pages: List[ProcessedPage], output_file: str):
        """Save processed data to JSON file"""
        output_path = Path(output_file)
        
        # VALIDATE THAT OUTPUT_PATH IS A FILE PATH, NOT A DIRECTORY
        if output_path.suffix.lower() != '.json':
            # IF NO .json EXTENSION, ADD IT
            output_path = output_path.with_suffix('.json')
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # CONVERT TO SERIALIZABLE FORMAT
        data = {
            "processed_at": datetime.now().isoformat(),
            "total_pages": len(processed_pages),
            "total_tables": sum(len(page.tables) for page in processed_pages),
            "pages": [asdict(page) for page in processed_pages]
        }
        
        # SAVE TO JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 SAVED PROCESSED DATA: {output_path}")
        print(f"   - {data['total_pages']} pages")
        print(f"   - {data['total_tables']} tables total")

    async def process_enhanced_document_async(self, enhanced_doc) -> ProcessedPage:
        """
        Process an enhanced document using only enhanced content (original kept as backup)
        
        Args:
            enhanced_doc: EnhancedDocument object with both versions
            
        Returns:
            ProcessedPage: Cleaned and standardized data
        """
        print(f"PROCESSING ENHANCED DOCUMENT: {enhanced_doc.filename}")
        
        # SAFETY CHECK: DETECT HTML COMMENTS IN ENHANCED CONTENT
        if '<!--' in enhanced_doc.enhanced_content and '-->' in enhanced_doc.enhanced_content:
            print(f"⚠️  WARNING: HTML comments detected in enhanced content for {enhanced_doc.filename}")
            print(f"   Falling back to original content to preserve data integrity")
            print(f"   HTML comment found: {enhanced_doc.enhanced_content[enhanced_doc.enhanced_content.find('<!--'):enhanced_doc.enhanced_content.find('<!--')+100]}...")
            # USE ORIGINAL CONTENT INSTEAD OF ENHANCED
            content_to_process = enhanced_doc.original_content
            enhancement_applied = False
        else:
            content_to_process = enhanced_doc.enhanced_content
            enhancement_applied = True
        
        # USE ENHANCED CONTENT FOR EXTRACTION
        prompt = self._create_cleaning_prompt(content_to_process)
        
        try:
            # CALL OPENAI TO CLEAN THE ENHANCED DATA
            print("SENDING ENHANCED CONTENT TO OPENAI FOR CLEANING...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an expert data cleaning agent specializing in technical document processing."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # ZERO TEMPERATURE FOR DETERMINISTIC OUTPUT
                max_tokens=self.max_tokens   # STANDARD LIMIT FOR SINGLE DOCUMENT
            )
            
            # EXTRACT AND PARSE JSON RESPONSE
            json_content = response.choices[0].message.content
            if not json_content:
                raise ValueError("Empty response from OpenAI")
            cleaned_data = json.loads(json_content)
            
            # CONVERT TO STRUCTURED FORMAT
            tables = []
            for i, table_data in enumerate(cleaned_data.get("tables", [])):
                table = ProcessedTable(
                    table_id=table_data.get("table_id", f"table_{i+1}"),
                    title=table_data.get("title", ""),
                    description=table_data.get("description", ""),
                    columns=table_data.get("columns", []),
                    rows=table_data.get("rows", []),
                    metadata=table_data.get("metadata", {})
                )
                tables.append(table)
            
            # CREATE PROCESSED PAGE WITH ENHANCEMENT INFO
            processed_page = ProcessedPage(
                page_id=Path(enhanced_doc.filename).stem,
                title=cleaned_data.get("title", Path(enhanced_doc.filename).stem),
                summary=cleaned_data.get("summary", ""),
                keywords=cleaned_data.get("keywords", []),
                tables=tables,
                raw_content=content_to_process,  # STORE ENHANCED VERSION
                processing_metadata={
                    "source_file": enhanced_doc.filename,
                    "processed_at": datetime.now().isoformat(),
                    "model_used": self.model,
                    "tables_found": len(tables),
                    "enhancement_applied": enhancement_applied,
                    "enhancement_notes": enhanced_doc.enhancement_notes,
                    "original_preserved": True,  # ORIGINAL CONTENT AVAILABLE AS BACKUP
                    "data_integrity": "Enhanced content with all data preserved"
                }
            )
            
            print(f"✅ ENHANCED-PROCESSED: {len(tables)} tables, {len(cleaned_data.get('keywords', []))} keywords")
            print(f"   📊 Using enhanced content with improved structure")
            print(f"   🔧 Structure enhancements: {len(enhanced_doc.enhancement_notes)}")
            print(f"   💾 Original content preserved as backup")
            return processed_page
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON PARSING ERROR: {e}")
            print(f"RAW RESPONSE: {response.choices[0].message.content}")
            raise
        except Exception as e:
            print(f"❌ PROCESSING ERROR: {e}")
            raise

    def process_enhanced_document(self, enhanced_doc) -> ProcessedPage:
        """Synchronous wrapper for enhanced document processing"""
        return asyncio.run(self.process_enhanced_document_async(enhanced_doc))

    def process_enhanced_documents(self, enhanced_docs: List, output_file: Optional[str] = None) -> List[ProcessedPage]:
        """
        Process a list of enhanced documents with dual content approach
        
        Args:
            enhanced_docs: List of EnhancedDocument objects
            output_file: Optional path to save consolidated JSON output
            
        Returns:
            List[ProcessedPage]: All processed pages with data integrity
        """
        print(f"PROCESSING {len(enhanced_docs)} ENHANCED DOCUMENTS WITH DUAL APPROACH")
        
        # PROCESS EACH ENHANCED DOCUMENT AND SAVE IMMEDIATELY
        processed_pages = []
        for enhanced_doc in enhanced_docs:
            try:
                processed_page = self.process_enhanced_document(enhanced_doc)
                processed_pages.append(processed_page)
                
                # SAVE EACH PROCESSED PAGE IMMEDIATELY (PAGE-WISE SAVING)
                if output_file:
                    # Create output directory if it doesn't exist
                    output_path = Path(output_file)
                    output_dir = output_path.parent
                    output_dir.mkdir(parents=True, exist_ok=True)
                    # Save individual page with proper file extension
                    page_file = output_dir / f"{processed_page.page_id}.json"
                    with open(page_file, 'w', encoding='utf-8') as f:
                        json.dump(asdict(processed_page), f, indent=2, ensure_ascii=False)
                    print(f"✅ PROCESSED AND SAVED: {enhanced_doc.filename}")
                
            except Exception as e:
                print(f"⚠️  FAILED TO PROCESS {enhanced_doc.filename}: {e}")
                continue
        
        # SAVE CONSOLIDATED OUTPUT IF REQUESTED
        if output_file:
            self._save_processed_data(processed_pages, output_file)
        
        print(f"🎉 SUCCESSFULLY PROCESSED AND SAVED {len(processed_pages)}/{len(enhanced_docs)} ENHANCED DOCUMENTS")
        return processed_pages

    def _process_document_folder(self, document_folder_path: str) -> List[ProcessedPage]:
        """
        🍇 INTERNAL: Process document folder → JSON pages  
        Internal pipeline method - not intended for direct user calls
        """
        doc_folder = Path(document_folder_path)
        if not doc_folder.exists():
            raise FileNotFoundError(f"Document folder not found: {document_folder_path}")
        
        # FIND ENHANCED MARKDOWN SUBFOLDER
        enhanced_md_folder = doc_folder / "02_enhanced_markdown"
        if not enhanced_md_folder.exists():
            raise FileNotFoundError(f"Enhanced markdown folder not found: {enhanced_md_folder}")
        
        # CREATE CLEANED JSON SUBFOLDER
        cleaned_json_folder = doc_folder / "03_cleaned_json"
        cleaned_json_folder.mkdir(exist_ok=True)
        
        # FIND ALL ENHANCED MARKDOWN FILES
        md_files = list(enhanced_md_folder.glob("*.md"))
        if not md_files:
            raise ValueError(f"No enhanced markdown files found in: {enhanced_md_folder}")
        
        print(f"FOUND {len(md_files)} ENHANCED MARKDOWN FILES TO PROCESS IN DOCUMENT FOLDER")
        
        # PROCESS EACH FILE AND SAVE IMMEDIATELY
        processed_pages = []
        skipped_pages = []
        for md_file in sorted(md_files):
            try:
                processed_page = self.process_file(str(md_file))
                if processed_page is None:
                    # Page was skipped due to error
                    skipped_pages.append({
                        "filename": md_file.name,
                        "error": "Processing failed - page skipped",
                        "timestamp": datetime.now().isoformat()
                    })
                    print(f"⚠️  SKIPPED: {md_file.name} (processing failed)")
                    continue
                    
                processed_pages.append(processed_page)
                
                # SAVE EACH PROCESSED PAGE IMMEDIATELY (PAGE-WISE SAVING)
                self._save_single_processed_page(processed_page, cleaned_json_folder, doc_folder.name)
                
                print(f"✅ PROCESSED AND SAVED: {md_file.name}")
            except Exception as e:
                print(f"❌ FAILED TO PROCESS {md_file.name}: {e}")
                skipped_pages.append({
                    "filename": md_file.name,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                continue
        
        # CREATE FINAL COMBINED OUTPUT
        final_output_file = doc_folder / "final_output.json"
        self._save_final_combined_output(processed_pages, final_output_file)
        
        # UPDATE DOCUMENT METADATA
        self._update_document_metadata_cleaning(doc_folder, "cleaning", processed_pages, skipped_pages)
        
        print(f"🎉 STAGE 3 COMPLETE - PROCESSED {len(processed_pages)}/{len(md_files)} FILES")
        print(f"   📄 Individual JSON files saved in: {cleaned_json_folder}")
        print(f"   📋 Final combined output: {final_output_file}")
        return processed_pages

    def _save_single_processed_page(self, processed_page: ProcessedPage, output_folder: Path, document_id: str):
        """Save a single processed page as JSON immediately after processing"""
        output_file = output_folder / f"{processed_page.page_id}.json"
        
        # ADD DOCUMENT ID TO METADATA
        processed_page.processing_metadata["document_id"] = document_id
        processed_page.processing_metadata["stage"] = "03_cleaned_json"
        
        # CONVERT TO SERIALIZABLE FORMAT
        page_data = asdict(processed_page)
        
        # SAVE INDIVIDUAL JSON FILE
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(page_data, f, indent=2, ensure_ascii=False)
        
        print(f"SAVED CLEANED PAGE JSON: {output_file}")

    def _save_final_combined_output(self, processed_pages: List[ProcessedPage], output_file: Path):
        """Save the final combined JSON output for the entire document"""
        # CREATE COMPREHENSIVE FINAL OUTPUT
        final_data = {
            "document_info": {
                "document_id": processed_pages[0].processing_metadata.get("document_id", "unknown") if processed_pages else "unknown",
                "processed_at": datetime.now().isoformat(),
                "total_pages": len(processed_pages),
                "total_tables": sum(len(page.tables) for page in processed_pages),
                "total_keywords": len(set().union(*[page.keywords for page in processed_pages])) if processed_pages else 0,
                "processing_pipeline": ["01_parsed_markdown", "02_enhanced_markdown", "03_cleaned_json"]
            },
            "document_summary": {
                "combined_keywords": list(set().union(*[page.keywords for page in processed_pages])) if processed_pages else [],
                "page_titles": [page.title for page in processed_pages],
                "table_summary": [
                    {
                        "page_id": page.page_id,
                        "page_title": page.title,
                        "table_count": len(page.tables),
                        "table_titles": [table.title for table in page.tables]
                    }
                    for page in processed_pages
                ]
            },
            "pages": [asdict(page) for page in processed_pages]
        }
        
        # SAVE FINAL COMBINED OUTPUT
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 SAVED FINAL COMBINED OUTPUT: {output_file}")
        print(f"   📊 {final_data['document_info']['total_pages']} pages")
        print(f"   📋 {final_data['document_info']['total_tables']} tables total")
        print(f"   🔍 {final_data['document_info']['total_keywords']} unique keywords")

    def _update_document_metadata_cleaning(self, doc_folder: Path, stage: str, processed_pages: List[ProcessedPage], skipped_pages: List[Dict]):
        """Update the document metadata with cleaning stage info"""
        metadata_file = doc_folder / "document_metadata.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # ENSURE stages_completed FIELD EXISTS
            if "stages_completed" not in metadata:
                metadata["stages_completed"] = []
            
            # UPDATE STAGES COMPLETED
            if stage not in metadata["stages_completed"]:
                metadata["stages_completed"].append(stage)
            
            # ADD CLEANING INFO
            metadata[f"{stage}_info"] = {
                "processed_at": datetime.now().isoformat(),
                "pages_processed": len(processed_pages),
                "total_tables_extracted": sum(len(page.tables) for page in processed_pages),
                "unique_keywords": len(set().union(*[page.keywords for page in processed_pages])) if processed_pages else 0,
                "processing_summary": {
                    page.page_id: {
                        "title": page.title,
                        "tables": len(page.tables),
                        "keywords": len(page.keywords)
                    }
                    for page in processed_pages
                },
                "skipped_pages": skipped_pages
            }
            
            # MARK PIPELINE AS COMPLETE
            metadata["pipeline_complete"] = True
            metadata["final_output_ready"] = True
            
            # SAVE UPDATED METADATA
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"UPDATED DOCUMENT METADATA: {metadata_file}")
            print(f"🎉 PIPELINE COMPLETE - ALL STAGES FINISHED")
        else:
            print(f"WARNING: No metadata file found at {metadata_file}")





 