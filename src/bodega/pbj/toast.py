#!/usr/bin/env python3
"""
🍞 Toast - JSON Format Converter
================================

Stage 4 of the PB&J Pipeline. Converts column-based table format to row-based dictionary
format for better accessibility and easier querying in downstream applications.

INPUT FORMAT (from PB&J):
{
  "table": {
    "columns": ["Size", "Anterior Posterior", ...],
    "rows": [["1", "51.7", ...], ["2", "53.7", ...]]
  }
}

OUTPUT FORMAT (toasted):
{
  "table": {
    "rows": [
      {"Size": "1", "Anterior Posterior": "51.7", ...},
      {"Size": "2", "Anterior Posterior": "53.7", ...}
    ]
  }
}
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
from datetime import datetime


class Toast:
    """
    🍞 Toast - JSON Format Converter
    
    Stage 4 of the PB&J Pipeline. Converts column-based table format to row-based dictionary
    format for better accessibility and querying in downstream applications.
    """
    
    def __init__(self):
        """Initialize the toaster"""
        print("🍞 TOAST INITIALIZED - Ready to convert JSON formats")
    
    def convert_table(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a single table from column-based to row-based format
        
        Args:
            table_data: Table with 'columns' and 'rows' arrays
            
        Returns:
            Dict: Table with 'rows' as list of dictionaries
        """
        if not isinstance(table_data, dict):
            return table_data
        
        # Check if this looks like a table with columns and rows
        if "columns" not in table_data or "rows" not in table_data:
            return table_data
        
        columns = table_data.get("columns", [])
        rows = table_data.get("rows", [])
        
        # Convert rows from arrays to dictionaries
        converted_rows = []
        for row in rows:
            if isinstance(row, list) and len(row) == len(columns):
                # Create dictionary mapping column names to values
                row_dict = dict(zip(columns, row))
                converted_rows.append(row_dict)
            else:
                # If row format is unexpected, keep as is
                converted_rows.append(row)
        
        # Create new table structure without columns array
        converted_table = table_data.copy()
        converted_table["rows"] = converted_rows
        converted_table.pop("columns", None)  # Remove columns array
        
        return converted_table
    
    def convert_page(self, page_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert all tables in a page
        
        Args:
            page_data: Page data with tables
            
        Returns:
            Dict: Page data with converted tables
        """
        if not isinstance(page_data, dict):
            return page_data
        
        converted_page = page_data.copy()
        
        # Convert tables in the page
        if "tables" in converted_page and isinstance(converted_page["tables"], list):
            converted_tables = []
            for table in converted_page["tables"]:
                converted_table = self.convert_table(table)
                converted_tables.append(converted_table)
            converted_page["tables"] = converted_tables
        
        return converted_page
    
    def convert_pbj_output(self, pbj_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert entire PB&J pipeline output
        
        Args:
            pbj_data: Complete PB&J pipeline output
            
        Returns:
            Dict: Converted data with row-based table format
        """
        print("🍞 CONVERTING PB&J OUTPUT TO TOASTED FORMAT")
        
        converted_data = pbj_data.copy()
        
        # Add conversion metadata
        converted_data["toast_info"] = {
            "converted_at": datetime.now().isoformat(),
            "converter": "PB&J Toast",
            "format": "row-based dictionaries",
            "original_format": "column-based arrays"
        }
        
        # Convert pages if they exist
        if "pages" in converted_data and isinstance(converted_data["pages"], list):
            converted_pages = []
            for page in converted_data["pages"]:
                converted_page = self.convert_page(page)
                converted_pages.append(converted_page)
            converted_data["pages"] = converted_pages
            
            print(f"✅ CONVERTED {len(converted_pages)} PAGES")
        
        # Convert any standalone tables
        if "tables" in converted_data and isinstance(converted_data["tables"], list):
            converted_tables = []
            for table in converted_data["tables"]:
                converted_table = self.convert_table(table)
                converted_tables.append(converted_table)
            converted_data["tables"] = converted_tables
            
            print(f"✅ CONVERTED {len(converted_tables)} STANDALONE TABLES")
        
        print("🍞 TOASTING COMPLETE!")
        return converted_data
    
    def convert_file(self, input_file: str, output_file: Optional[str] = None) -> str:
        """
        Convert a JSON file from PB&J format to toasted format
        
        Args:
            input_file: Path to input JSON file
            output_file: Path to output JSON file (optional, overwrites input if None)
            
        Returns:
            str: Path to output file
        """
        input_path = Path(input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Load input data
        print(f"🍞 LOADING: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as f:
            pbj_data = json.load(f)
        
        # Convert data
        toasted_data = self.convert_pbj_output(pbj_data)
        
        # Determine output path
        if output_file is None:
            output_path = input_path  # Overwrite input file
        else:
            output_path = Path(output_file)
        
        # Save converted data
        print(f"🍞 SAVING: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(toasted_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ TOASTED DATA SAVED: {output_path}")
        return str(output_path)
    
    def convert_document_folder(self, document_folder: str) -> str:
        """
        Convert final_output.json in a document folder
        
        Args:
            document_folder: Path to document folder
            
        Returns:
            str: Path to converted file
        """
        doc_path = Path(document_folder)
        final_output = doc_path / "final_output.json"
        
        if not final_output.exists():
            raise FileNotFoundError(f"final_output.json not found in: {document_folder}")
        
        return self.convert_file(str(final_output))
