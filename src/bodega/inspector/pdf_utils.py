#!/usr/bin/env python3
"""
🥪 PDF Utilities for Sandwich Inspector
=======================================

Utilities for handling PDF files and displaying them in the Streamlit app.
"""

import streamlit as st
import base64
from pathlib import Path
from typing import Optional, Union
import fitz  # PyMuPDF for PDF handling

def display_pdf_page(pdf_path: Union[str, Path], page_num: int = 0) -> bool:
    """
    Display a specific page of a PDF file in Streamlit
    
    Args:
        pdf_path: Path to the PDF file
        page_num: Page number to display (0-indexed)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            st.error(f"PDF file not found: {pdf_path}")
            return False
        
        # Open PDF and get page
        doc = fitz.open(str(pdf_path))
        if page_num >= len(doc):
            st.error(f"Page {page_num + 1} not found in PDF (total pages: {len(doc)})")
            return False
        
        page = doc[page_num]
        
        # Convert page to image
        mat = fitz.Matrix(2.0, 2.0)  # Zoom factor for better quality
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        
        # Display image
        st.image(img_data, caption=f"Page {page_num + 1}", use_container_width=True)
        
        doc.close()
        return True
        
    except Exception as e:
        st.error(f"Error displaying PDF: {str(e)}")
        return False

def get_pdf_page_count(pdf_path: Union[str, Path]) -> int:
    """
    Get the total number of pages in a PDF
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        int: Number of pages, or 0 if error
    """
    try:
        doc = fitz.open(str(pdf_path))
        page_count = len(doc)
        doc.close()
        return page_count
    except:
        return 0

def create_pdf_download_link(pdf_path: Union[str, Path], filename: str = "document.pdf") -> str:
    """
    Create a download link for a PDF file
    
    Args:
        pdf_path: Path to the PDF file
        filename: Name for the downloaded file
        
    Returns:
        str: HTML download link
    """
    try:
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        
        b64_pdf = base64.b64encode(pdf_data).decode()
        href = f'<a href="data:application/pdf;base64,{b64_pdf}" download="{filename}" target="_blank">📥 Download PDF</a>'
        return href
    except:
        return "❌ Download not available"

def extract_pdf_text(pdf_path: Union[str, Path], page_num: Optional[int] = None) -> str:
    """
    Extract text from PDF page(s)
    
    Args:
        pdf_path: Path to the PDF file
        page_num: Specific page number (0-indexed), or None for all pages
        
    Returns:
        str: Extracted text
    """
    try:
        doc = fitz.open(str(pdf_path))
        
        if page_num is not None:
            if page_num < len(doc):
                text = doc[page_num].get_text()
            else:
                text = ""
        else:
            text = ""
            for page in doc:
                text += page.get_text() + "\n"
        
        doc.close()
        return text
        
    except Exception as e:
        return f"Error extracting text: {str(e)}"

def pdf_page_thumbnail(pdf_path: Union[str, Path], page_num: int, max_width: int = 200) -> bytes:
    """
    Generate a thumbnail image of a PDF page
    
    Args:
        pdf_path: Path to the PDF file
        page_num: Page number (0-indexed)
        max_width: Maximum width of thumbnail
        
    Returns:
        bytes: PNG image data
    """
    try:
        doc = fitz.open(str(pdf_path))
        page = doc[page_num]
        
        # Calculate zoom to achieve desired width
        page_rect = page.rect
        zoom = max_width / page_rect.width
        mat = fitz.Matrix(zoom, zoom)
        
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        
        doc.close()
        return img_data
        
    except Exception as e:
        return b""

class PDFViewer:
    """Enhanced PDF viewer component for Streamlit"""
    
    def __init__(self, pdf_path: Union[str, Path]):
        self.pdf_path = Path(pdf_path)
        self.page_count = get_pdf_page_count(self.pdf_path)
    
    def render(self, page_num: int = 0, show_controls: bool = True) -> bool:
        """
        Render the PDF viewer with optional controls
        
        Args:
            page_num: Page number to display (0-indexed)
            show_controls: Whether to show navigation controls
            
        Returns:
            bool: True if successful
        """
        if not self.pdf_path.exists():
            st.error("📄 PDF file not found")
            return False
        
        if self.page_count == 0:
            st.error("📄 Could not read PDF file")
            return False
        
        # Display page info
        if show_controls:
            st.markdown(f"**📖 Document:** {self.pdf_path.name}")
            st.markdown(f"**📊 Page {page_num + 1} of {self.page_count}**")
        
        # Display the PDF page
        success = display_pdf_page(self.pdf_path, page_num)
        
        if show_controls and success:
            # Download link
            download_link = create_pdf_download_link(self.pdf_path, self.pdf_path.name)
            st.markdown(download_link, unsafe_allow_html=True)
        
        return success
    
    def get_page_text(self, page_num: int) -> str:
        """Get text content from a specific page"""
        return extract_pdf_text(self.pdf_path, page_num) 