import subprocess
import sys
import os
from pathlib import Path

def launch_inspector_app(document_folder=None, port=8501):
    """
    Launch the Streamlit inspector app for manual review.
    Optionally specify a document folder to open by default.
    """
    app_path = Path(__file__).parent / "sandwich_inspector_app.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", str(port)]
    if document_folder:
        cmd += ["--", "--document_folder", str(document_folder)]
    env = os.environ.copy()
    
    # Run in background (non-blocking)
    try:
        process = subprocess.Popen(cmd, env=env)
        print(f"🚀 Streamlit Inspector launched in background (PID: {process.pid})")
        print(f"🌐 Inspector will be available at: http://localhost:{port}")
        print(f"💡 To stop the Inspector, run: kill {process.pid}")
        return process
    except Exception as e:
        print(f"❌ Failed to launch Inspector: {e}")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Launch the Sandwich Inspector Streamlit app.")
    parser.add_argument("--document_folder", type=str, default=None, help="Path to a processed document folder to review.")
    parser.add_argument("--port", type=int, default=8501, help="Port to run the Streamlit app on.")
    args = parser.parse_args()
    launch_inspector_app(document_folder=args.document_folder, port=args.port) 