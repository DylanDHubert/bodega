"""
🥜 Peanut - PDF to Markdown Parser
==================================

Stage 1 of the PB&J Pipeline. Extracts raw content from PDFs using LlamaParse API
and returns clean markdown documents optimized for technical and table-heavy content.
"""

import os
import json
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from llama_parse import LlamaParse, ResultType

# IMPORT OUR CONFIGURATION SYSTEM
from .config import PipelineConfig, create_config

@dataclass
class ParsedDocument:
    """Container for parsed document data"""
    content: str
    filename: str
    parse_timestamp: datetime
    parsing_time_seconds: float

class Peanut:
    """
    🥜 Peanut - PDF to Markdown Parser
    
    Stage 1 of the PB&J Pipeline. Extracts raw content from PDFs using LlamaParse API
    and returns clean markdown documents optimized for technical and table-heavy content.
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None, api_key: Optional[str] = None, use_premium: bool = False):
        """
        Initialize the PDF processor
        
        Args:
            config: PipelineConfig object with all settings
            api_key: LlamaParse API key (if not provided, will try to get from config)
            use_premium: Whether to use Premium mode for better parsing quality (costs more)
        """
        # LOAD CONFIGURATION - PRIORITY: config parameter > api_key/use_premium > defaults
        if config:
            self.config = config
        else:
            # CREATE CONFIG WITH PROVIDED PARAMETERS
            self.config = create_config(
                llamaparse_api_key=api_key,
                use_premium_mode=use_premium
            )
        
        # LOAD PROMPTS FROM CONFIG FILE
        prompts = self._load_prompts()
        
        # CONFIGURE LLAMAPARSE WITH OPTIMIZED SETTINGS FOR TECHNICAL DOCUMENTS
        # USING NEW API PARAMETERS INSTEAD OF DEPRECATED parsing_instruction
        self.parser = LlamaParse(
            api_key=self.config.llamaparse_api_key,  # CONFIG VALIDATION ENSURES THIS IS NOT NONE
            result_type=ResultType.MD,  # OUTPUT FORMAT AS MARKDOWN
            verbose=self.config.enable_verbose_logging,  # ENABLE VERBOSE LOGGING
            language="en",           # SET LANGUAGE TO ENGLISH
            
            # PAGE BREAK CONFIGURATION - LLAMAPARSE HAS BUILT-IN PAGE SEPARATOR SUPPORT
            page_separator=self.config.page_separator,  # USE CONFIGURABLE PAGE SEPARATOR
            
            # NEW API: USE system_prompt_append INSTEAD OF DEPRECATED parsing_instruction
            # THIS APPENDS TO LLAMAPARSE'S SYSTEM PROMPT INSTEAD OF REPLACING IT
            system_prompt_append=prompts['system_prompt'],
            
            # ADD USER PROMPT FOR TABLE AND DATA EXTRACTION
            user_prompt=prompts['user_prompt'],
            
            # PREMIUM MODE SETTINGS FOR BETTER PARSING QUALITY
            premium_mode=self.config.use_premium_mode,  # ENABLE PREMIUM MODE FOR COMPLEX DOCUMENTS
            
            # OTHER OPTIMIZATIONS FOR TECHNICAL DOCUMENTS
            output_tables_as_HTML=True,  # USE HTML TABLES FOR COMPLEX TABLE STRUCTURES
            
            # ADDITIONAL OPTIONS FOR BETTER CHECKBOX/BULLET DETECTION
            disable_ocr=False,  # ENSURE OCR IS ENABLED TO DETECT FAINT MARKS
            skip_diagonal_text=False,  # DON'T SKIP DIAGONAL TEXT THAT MIGHT BE CHECKMARKS
            
            max_timeout=self.config.max_timeout,  # CONFIGURABLE TIMEOUT
        )
        
        # STORE MODE FOR REPORTING
        self.use_premium = self.config.use_premium_mode
    
    def _load_prompts(self):
        """Load prompts from pantry - much cleaner approach"""
        # GET PANTRY PATH RELATIVE TO THIS MODULE
        pantry_path = Path(__file__).parent / "pantry"
        system_prompt_path = pantry_path / "pea.txt"
        user_prompt_path = pantry_path / "nut.txt"
        
        # CHECK IF FILES EXIST
        if not system_prompt_path.exists():
            raise FileNotFoundError(f"System prompt file not found: {system_prompt_path}")
        if not user_prompt_path.exists():
            raise FileNotFoundError(f"User prompt file not found: {user_prompt_path}")
        
        # LOAD PROMPTS FROM SIMPLE TEXT FILES
        with open(system_prompt_path, 'r', encoding='utf-8') as f:
            system_prompt = f.read().strip()
        
        with open(user_prompt_path, 'r', encoding='utf-8') as f:
            user_prompt = f.read().strip()
        
        return {
            'system_prompt': system_prompt,
            'user_prompt': user_prompt
        }
    
    async def parse_pdf_async(self, pdf_path: str) -> List[ParsedDocument]:
        """
        Asynchronously parse a PDF file using LlamaParse
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            List[ParsedDocument]: List of parsed documents (one per page/section if split)
        """
        # VALIDATE PDF FILE EXISTS AND IS CORRECT FORMAT
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        if not pdf_file.suffix.lower() == '.pdf':
            raise ValueError(f"File must be a PDF: {pdf_path}")
        
        # START PROCESSING WITH TIMING
        mode_text = "PREMIUM" if self.use_premium else "BALANCED"
        print(f"STARTING PDF PARSING WITH {mode_text} MODE: {pdf_file.name}")
        start_time = datetime.now()
        
        try:
            # SEND PDF TO LLAMAPARSE API - LET LLAMAPARSE HANDLE ALL THE HEAVY LIFTING
            print("SENDING PDF TO LLAMAPARSE API...")
            documents = await self.parser.aload_data(str(pdf_file))
            
            # CHECK IF WE GOT ANY CONTENT BACK
            if not documents:
                raise ValueError("NO CONTENT EXTRACTED FROM PDF")
            
            # CALCULATE PROCESSING TIME
            end_time = datetime.now()
            parsing_time = (end_time - start_time).total_seconds()
            
            # CONVERT LLAMAPARSE DOCUMENTS TO OUR FORMAT
            parsed_docs = []
            for i, doc in enumerate(documents):
                # CREATE PARSED DOCUMENT OBJECT
                parsed_doc = ParsedDocument(
                    content=doc.text,  # LLAMAPARSE ALREADY RETURNS CLEAN MARKDOWN WITH PAGE BREAKS
                    filename=f"{pdf_file.stem}_part_{i+1}.md" if len(documents) > 1 else f"{pdf_file.stem}.md",
                    parse_timestamp=end_time,
                    parsing_time_seconds=parsing_time
                )
                parsed_docs.append(parsed_doc)
            
            # PRINT SUCCESS STATISTICS
            print(f"SUCCESSFULLY PARSED PDF IN {parsing_time:.2f} SECONDS")
            print(f"EXTRACTED {len(parsed_docs)} DOCUMENT(S)")
            for i, doc in enumerate(parsed_docs):
                print(f"   - PART {i+1}: {len(doc.content)} CHARACTERS")
            
            return parsed_docs
            
        except Exception as e:
            # PRINT ERROR AND RE-RAISE
            print(f"ERROR PARSING PDF {pdf_file.name}: {str(e)}")
            raise
    
    def process(self, pdf_path: str) -> List[ParsedDocument]:
        """
        🥜 Process PDF → Markdown
        Main Peanut processing method
        """
        return asyncio.run(self.parse_pdf_async(pdf_path))
    
    def process_async(self, pdf_path: str) -> List[ParsedDocument]:
        """
        🥜 Async Process PDF → Markdown
        Async Peanut processing method
        """
        return asyncio.run(self.parse_pdf_async(pdf_path))
    
    def save_parsed_documents(self, parsed_docs: List[ParsedDocument], output_dir: Optional[str] = None, source_pdf_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Save parsed documents to organized per-document directory structure
        
        Args:
            parsed_docs: List of ParsedDocument objects to save
            output_dir: Custom output directory (if None, will use config default)
            source_pdf_path: Path to original PDF file to copy to output
            
        Returns:
            Dict[str, Any]: Information about the created directory structure
        """
        if not parsed_docs:
            raise ValueError("NO PARSED DOCUMENTS TO SAVE")
        
        # DETERMINE OUTPUT PATH USING CONFIGURATION SYSTEM
        if source_pdf_path:
            pdf_filename = Path(source_pdf_path).name
        else:
            pdf_filename = parsed_docs[0].filename.replace('.md', '.pdf')
        
        # GET OUTPUT PATH FROM CONFIGURATION
        document_folder = self.config.get_output_path(pdf_filename, output_dir)
        
        # CREATE DIRECTORY STRUCTURE
        document_folder.mkdir(parents=True, exist_ok=True)
        
        # CREATE SUBFOLDERS FOR ORGANIZED OUTPUT
        parsed_markdown_folder = document_folder / "01_parsed_markdown"
        parsed_markdown_folder.mkdir(exist_ok=True)
        
        # COPY ORIGINAL PDF IF PROVIDED
        if source_pdf_path and Path(source_pdf_path).exists():
            import shutil
            pdf_dest = document_folder / Path(source_pdf_path).name
            shutil.copy2(source_pdf_path, pdf_dest)
            print(f"📄 COPIED ORIGINAL PDF TO: {pdf_dest}")
        
        # SAVE PARSED DOCUMENTS
        saved_files = []
        for i, doc in enumerate(parsed_docs):
            # CREATE FILENAME WITH PAGE NUMBER IF MULTIPLE DOCUMENTS
            if len(parsed_docs) > 1:
                filename = f"page_{i+1}.md"
            else:
                filename = doc.filename
            
            file_path = parsed_markdown_folder / filename
            
            # SAVE MARKDOWN CONTENT
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(doc.content)
            
            saved_files.append(str(file_path))
            print(f"💾 SAVED PARSED DOCUMENT: {file_path}")
        
        # CREATE METADATA FILE
        metadata = {
            "processing_info": {
                "processor": "Peanut (LlamaParse)",
                "processing_time": datetime.now().isoformat(),
                "total_documents": len(parsed_docs),
                "use_premium_mode": self.use_premium,
                "page_separator": self.config.page_separator
            },
            "document_info": {
                "source_pdf": source_pdf_path,
                "parsed_files": saved_files,
                "total_characters": sum(len(doc.content) for doc in parsed_docs)
            },
            "folder_structure": {
                "main_folder": str(document_folder),
                "parsed_markdown_folder": str(parsed_markdown_folder)
            }
        }
        
        metadata_file = document_folder / "document_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"📋 SAVED METADATA: {metadata_file}")
        
        return {
            "main_folder": str(document_folder),
            "parsed_markdown_folder": str(parsed_markdown_folder),
            "saved_files": saved_files,
            "metadata_file": str(metadata_file),
            "total_documents": len(parsed_docs)
        }


