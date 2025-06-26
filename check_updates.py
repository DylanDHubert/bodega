#!/usr/bin/env python3
"""
🔍 Quick Repository Update Checker
==================================

Lightweight script to check for repository updates and prompt user.
Can be imported and used in other scripts.
"""

import subprocess
from pathlib import Path
from typing import Dict, List, Tuple
import sys

def check_repo_updates(quiet: bool = False) -> bool:
    """
    Quick check for repository updates.
    Returns True if updates are available, False otherwise.
    """
    # Import configuration from setup_repos.py
    try:
        from setup_repos import REPOSITORIES, RepoManager
    except ImportError:
        if not quiet:
            print("⚠️ setup_repos.py not found. Cannot check for updates.")
        return False
    
    repo_manager = RepoManager(verbose=not quiet)
    updates_available = False
    repos_with_updates = []
    
    for repo_name, config in REPOSITORIES.items():
        target_path = Path(config["target_dir"])
        
        if target_path.exists() and repo_manager.is_git_repo(target_path):
            can_check, commits_behind = repo_manager.check_for_updates(target_path)
            if can_check and commits_behind > 0:
                updates_available = True
                repos_with_updates.append((repo_name, commits_behind))
    
    if updates_available and not quiet:
        print("\n🔄 Repository Updates Available!")
        print("=" * 40)
        for repo_name, commits in repos_with_updates:
            print(f"📦 {repo_name}: {commits} commits behind")
        
        print("\nRun 'python setup_repos.py' to update repositories.")
    
    return updates_available

def prompt_for_updates() -> bool:
    """
    Check for updates and prompt user to update.
    Returns True if user wants to continue, False if they want to update first.
    """
    if not check_repo_updates(quiet=True):
        return True  # No updates available, continue
    
    print("\n🔄 Some repositories have updates available.")
    print("You can continue with current versions or update first.")
    
    response = input("\nOptions:\n  [c] Continue with current versions\n  [u] Update repositories first\n  [q] Quit\n\nChoice [c/u/q]: ").lower().strip()
    
    if response in ['u', 'update']:
        print("\n🔧 Running repository updates...")
        try:
            subprocess.run([sys.executable, "setup_repos.py"], check=True)
            print("✅ Updates complete! You can now run your command again.")
            return False  # Don't continue, user should re-run
        except subprocess.CalledProcessError:
            print("❌ Update failed. Continuing with current versions.")
            return True
    elif response in ['q', 'quit', 'exit']:
        print("👋 Goodbye!")
        sys.exit(0)
    else:
        print("📄 Continuing with current versions...")
        return True

if __name__ == "__main__":
    """When run directly, just check and display status"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check for repository updates")
    parser.add_argument("--quiet", action="store_true", help="Only return exit code")
    parser.add_argument("--prompt", action="store_true", help="Prompt user for action")
    
    args = parser.parse_args()
    
    if args.prompt:
        should_continue = prompt_for_updates()
        sys.exit(0 if should_continue else 1)
    else:
        has_updates = check_repo_updates(quiet=args.quiet)
        sys.exit(1 if has_updates else 0) 