#!/usr/bin/env python3
"""
🔧 Bodega Repository Setup Script
================================

Automatically clones and updates required repository dependencies.
Run this script to ensure you have the latest versions of all components.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import argparse

# Repository configuration
REPOSITORIES = {
    "pbj": {
        "url": "https://github.com/DylanDHubert/peanut_butter_jelly.git",
        "target_dir": "src/bodega/pbj",
        "branch": "main",
        "description": "PDF processing pipeline (PB&J)"
    },
    "inspector": {
        "url": "https://github.com/lheitman0/sandwhich_inspector.git",
        "target_dir": "src/bodega/inspector",
        "branch": "main",
        "description": "Streamlit-based document review interface"
    },
    "soda": {
        "url": "https://github.com/lheitman0/doc_store.git",
        "target_dir": "src/bodega/soda",
        "branch": "main",
        "description": "AWS document storage system"
    }
}

class RepoManager:
    """Manages repository cloning and updates"""
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.root_dir = Path(__file__).parent
        
    def log(self, message: str, level: str = "INFO"):
        """Log messages with timestamps"""
        if self.verbose:
            prefix = "✅" if level == "SUCCESS" else "ℹ️" if level == "INFO" else "⚠️" if level == "WARNING" else "❌"
            print(f"{prefix} {message}")
    
    def run_command(self, command: List[str], cwd: Path = None) -> Tuple[bool, str]:
        """Run a shell command and return success status and output"""
        try:
            result = subprocess.run(
                command,
                cwd=cwd or self.root_dir,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return False, e.stderr.strip()
    
    def is_git_repo(self, path: Path) -> bool:
        """Check if a directory is a git repository"""
        return (path / ".git").exists()
    
    def get_current_branch(self, repo_path: Path) -> str:
        """Get the current branch of a git repository"""
        success, output = self.run_command(["git", "branch", "--show-current"], cwd=repo_path)
        return output if success else "unknown"
    
    def get_remote_url(self, repo_path: Path) -> str:
        """Get the remote URL of a git repository"""
        success, output = self.run_command(["git", "remote", "get-url", "origin"], cwd=repo_path)
        return output if success else "unknown"
    
    def check_for_updates(self, repo_path: Path) -> Tuple[bool, int]:
        """Check if there are updates available for a repository"""
        if not self.is_git_repo(repo_path):
            return False, 0
        
        # Fetch latest changes
        self.run_command(["git", "fetch"], cwd=repo_path)
        
        # Check how many commits behind
        success, output = self.run_command(
            ["git", "rev-list", "--count", "HEAD..origin/HEAD"], 
            cwd=repo_path
        )
        
        if success and output.isdigit():
            return True, int(output)
        return False, 0
    
    def clone_repository(self, repo_name: str, config: Dict) -> bool:
        """Clone a repository to the specified location"""
        target_path = self.root_dir / config["target_dir"]
        
        if target_path.exists():
            self.log(f"Directory {config['target_dir']} already exists", "WARNING")
            return False
        
        # Create parent directory if needed
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.log(f"Cloning {repo_name} from {config['url']}")
        success, output = self.run_command([
            "git", "clone", 
            "--branch", config["branch"],
            config["url"], 
            str(target_path)
        ])
        
        if success:
            self.log(f"Successfully cloned {repo_name}", "SUCCESS")
            return True
        else:
            self.log(f"Failed to clone {repo_name}: {output}", "ERROR")
            return False
    
    def update_repository(self, repo_name: str, config: Dict) -> bool:
        """Update an existing repository"""
        target_path = self.root_dir / config["target_dir"]
        
        if not self.is_git_repo(target_path):
            self.log(f"{config['target_dir']} is not a git repository", "ERROR")
            return False
        
        self.log(f"Updating {repo_name}")
        
        # Fetch and pull latest changes
        success1, _ = self.run_command(["git", "fetch"], cwd=target_path)
        success2, output = self.run_command(["git", "pull"], cwd=target_path)
        
        if success1 and success2:
            self.log(f"Successfully updated {repo_name}", "SUCCESS")
            return True
        else:
            self.log(f"Failed to update {repo_name}: {output}", "ERROR")
            return False
    
    def setup_all_repos(self, force_update: bool = False) -> Dict[str, bool]:
        """Setup all configured repositories"""
        results = {}
        
        self.log("🔧 Setting up Bodega repository dependencies")
        self.log("=" * 50)
        
        for repo_name, config in REPOSITORIES.items():
            self.log(f"\n📦 Processing {repo_name}: {config['description']}")
            
            target_path = self.root_dir / config["target_dir"]
            
            if target_path.exists() and self.is_git_repo(target_path):
                if force_update:
                    results[repo_name] = self.update_repository(repo_name, config)
                else:
                    # Check if updates are available
                    can_check, commits_behind = self.check_for_updates(target_path)
                    if can_check and commits_behind > 0:
                        self.log(f"{repo_name} is {commits_behind} commits behind", "WARNING")
                        response = input(f"Update {repo_name}? [y/N]: ").lower().strip()
                        if response in ['y', 'yes']:
                            results[repo_name] = self.update_repository(repo_name, config)
                        else:
                            self.log(f"Skipping update for {repo_name}")
                            results[repo_name] = True
                    else:
                        self.log(f"{repo_name} is up to date", "SUCCESS")
                        results[repo_name] = True
            else:
                # Clone the repository
                results[repo_name] = self.clone_repository(repo_name, config)
        
        return results
    
    def check_repo_status(self) -> None:
        """Check the status of all repositories"""
        self.log("📊 Repository Status Report")
        self.log("=" * 50)
        
        for repo_name, config in REPOSITORIES.items():
            target_path = self.root_dir / config["target_dir"]
            
            if target_path.exists() and self.is_git_repo(target_path):
                branch = self.get_current_branch(target_path)
                remote_url = self.get_remote_url(target_path)
                can_check, commits_behind = self.check_for_updates(target_path)
                
                status = "✅ Up to date"
                if can_check and commits_behind > 0:
                    status = f"⚠️ {commits_behind} commits behind"
                
                self.log(f"\n📦 {repo_name}")
                self.log(f"   Path: {config['target_dir']}")
                self.log(f"   Branch: {branch}")
                self.log(f"   Remote: {remote_url}")
                self.log(f"   Status: {status}")
            else:
                self.log(f"\n📦 {repo_name}")
                self.log(f"   Path: {config['target_dir']}")
                self.log(f"   Status: ❌ Not cloned")

def main():
    parser = argparse.ArgumentParser(description="Manage Bodega repository dependencies")
    parser.add_argument("--force-update", action="store_true", help="Force update all repositories without prompting")
    parser.add_argument("--status", action="store_true", help="Check status of all repositories")
    parser.add_argument("--quiet", action="store_true", help="Reduce output verbosity")
    
    args = parser.parse_args()
    
    repo_manager = RepoManager(verbose=not args.quiet)
    
    if args.status:
        repo_manager.check_repo_status()
    else:
        results = repo_manager.setup_all_repos(force_update=args.force_update)
        
        # Summary
        repo_manager.log("\n" + "=" * 50)
        repo_manager.log("📋 Setup Summary")
        repo_manager.log("=" * 50)
        
        for repo_name, success in results.items():
            status = "✅ Success" if success else "❌ Failed"
            repo_manager.log(f"   {repo_name}: {status}")
        
        if all(results.values()):
            repo_manager.log("\n🎉 All repositories are ready!", "SUCCESS")
        else:
            repo_manager.log("\n⚠️ Some repositories had issues. Check the output above.", "WARNING")

if __name__ == "__main__":
    main() 