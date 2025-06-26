#!/usr/bin/env python3
"""
Inspector Adapter Module
========================

Provides functions to launch and interface with the cloned Inspector application
without modifying the cloned repository.
"""

import os
import sys
import subprocess
from pathlib import Path
from typing import Optional


def launch_inspector_app(
    document_folder: Optional[str] = None,
    port: int = 8501,
    auto_open_browser: bool = True
) -> None:
    """
    Launch the Streamlit Inspector application from the cloned repo.
    
    Args:
        document_folder: Path to the processed document folder
        port: Port to run Streamlit on (default: 8501)
        auto_open_browser: Whether to automatically open browser
    """
    # Get the inspector directory
    inspector_dir = Path(__file__).parent / "inspector"
    app_script = inspector_dir / "sandwich_inspector_app.py"
    
    if not app_script.exists():
        raise FileNotFoundError(f"Inspector app not found: {app_script}")
    
    # Set environment variable for document folder if provided
    env = os.environ.copy()
    if document_folder:
        env["INSPECTOR_DOCUMENT_FOLDER"] = str(document_folder)
        print(f"🔍 Inspector will load document folder: {document_folder}")
    
    # Build streamlit command
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        str(app_script),
        "--server.port", str(port)
    ]
    
    if not auto_open_browser:
        cmd.extend(["--server.headless", "true"])
    
    print(f"🚀 Launching Inspector on port {port}...")
    print(f"🌐 Open browser to: http://localhost:{port}")
    
    try:
        # Launch streamlit from project root so it can find processed_documents/
        project_root = Path(__file__).parent.parent.parent  # Go up from src/bodega/ to project root
        
        # Launch streamlit in background
        subprocess.Popen(
            cmd,
            env=env,
            cwd=project_root,  # Run from project root, not inspector dir
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("✅ Inspector launched successfully!")
        
    except Exception as e:
        print(f"❌ Failed to launch Inspector: {e}")
        raise


def is_inspector_running(port: int = 8501) -> bool:
    """
    Check if Inspector is running on the specified port.
    
    Args:
        port: Port to check
        
    Returns:
        True if Inspector is running, False otherwise
    """
    try:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex(('localhost', port))
            return result == 0
    except Exception:
        return False


def stop_inspector(port: int = 8501) -> None:
    """
    Attempt to stop Inspector running on the specified port.
    
    Args:
        port: Port where Inspector is running
    """
    try:
        # Try to find and kill streamlit processes
        subprocess.run(
            ["pkill", "-f", f"streamlit.*{port}"],
            capture_output=True,
            text=True
        )
        print(f"🛑 Stopped Inspector on port {port}")
    except Exception as e:
        print(f"⚠️ Could not stop Inspector: {e}") 