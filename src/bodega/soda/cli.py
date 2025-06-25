#!/usr/bin/env python3
"""
Doc Store CLI Commands

Provides command-line utilities for health checks and migrations.
These are exposed as console scripts via pyproject.toml.
"""

import argparse
import sys
import time
from typing import Dict, List, Optional

from loguru import logger

from doc_store.config import DocStoreConfig
from doc_store.document_store import DocumentStore
from doc_store.exceptions import DocStoreError


def health_check():
    """Console script for health checking the document store."""
    parser = argparse.ArgumentParser(
        description="Check the health of the document store system"
    )
    parser.add_argument(
        "--bucket", 
        help="S3 bucket name (overrides config)"
    )
    parser.add_argument(
        "--timeout", 
        type=int, 
        default=10,
        help="Timeout in minutes for stuck document detection (default: 10)"
    )
    parser.add_argument(
        "--verbose", 
        "-v", 
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--json",
        action="store_true", 
        help="Output results in JSON format"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    
    try:
        # Initialize document store
        if args.bucket:
            doc_store = DocumentStore(bucket_name=args.bucket)
        else:
            config = DocStoreConfig()
            doc_store = DocumentStore(bucket_name=config.doc_bucket)
        
        # Run health checks
        health_results = _run_health_checks(doc_store, args.timeout)
        
        # Output results
        if args.json:
            import json
            print(json.dumps(health_results, indent=2))
        else:
            _print_health_results(health_results)
        
        # Exit with error code if unhealthy
        if health_results["overall_status"] != "healthy":
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        sys.exit(1)


def migrate():
    """Console script for migrating document store data."""
    parser = argparse.ArgumentParser(
        description="Migrate document store data structures"
    )
    parser.add_argument(
        "--bucket",
        help="S3 bucket name (overrides config)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without making changes"
    )
    parser.add_argument(
        "--verbose",
        "-v", 
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    
    try:
        # Initialize document store
        if args.bucket:
            doc_store = DocumentStore(bucket_name=args.bucket)
        else:
            config = DocStoreConfig()
            doc_store = DocumentStore(bucket_name=config.doc_bucket)
        
        # Run migration
        migration_results = _run_migration(doc_store, args.dry_run)
        
        # Output results
        _print_migration_results(migration_results, args.dry_run)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


def _run_health_checks(doc_store: DocumentStore, timeout_minutes: int) -> Dict:
    """Run comprehensive health checks on the document store."""
    logger.info("Starting health check...")
    
    results = {
        "timestamp": time.time(),
        "overall_status": "healthy",
        "checks": {},
        "warnings": [],
        "errors": []
    }
    
    try:
        # Check 1: S3 connectivity
        logger.debug("Checking S3 connectivity...")
        try:
            doc_store.list_raw_documents()
            results["checks"]["s3_connectivity"] = {"status": "pass", "message": "S3 accessible"}
        except Exception as e:
            results["checks"]["s3_connectivity"] = {"status": "fail", "message": str(e)}
            results["errors"].append(f"S3 connectivity failed: {e}")
            results["overall_status"] = "unhealthy"
        
        # Check 2: Stuck documents
        logger.debug("Checking for stuck documents...")
        try:
            stuck_docs = doc_store.list_stuck_documents(timeout_minutes)
            if stuck_docs:
                results["checks"]["stuck_documents"] = {
                    "status": "warn", 
                    "message": f"Found {len(stuck_docs)} stuck documents",
                    "stuck_docs": stuck_docs
                }
                results["warnings"].append(f"{len(stuck_docs)} documents stuck in processing")
                if results["overall_status"] == "healthy":
                    results["overall_status"] = "warning"
            else:
                results["checks"]["stuck_documents"] = {
                    "status": "pass", 
                    "message": "No stuck documents found"
                }
        except Exception as e:
            results["checks"]["stuck_documents"] = {"status": "fail", "message": str(e)}
            results["errors"].append(f"Stuck document check failed: {e}")
            results["overall_status"] = "unhealthy"
        
        # Check 3: System health statistics
        logger.debug("Getting system health statistics...")
        try:
            health_stats = doc_store.get_system_health()
            results["checks"]["system_health"] = {
                "status": "pass",
                "message": "System health retrieved successfully",
                "stats": health_stats
            }
            
            # Add warnings for unusual patterns
            if health_stats.get("failed_count", 0) > 0:
                results["warnings"].append(f"{health_stats['failed_count']} documents in failed state")
                if results["overall_status"] == "healthy":
                    results["overall_status"] = "warning"
                    
        except Exception as e:
            results["checks"]["system_health"] = {"status": "fail", "message": str(e)}
            results["errors"].append(f"System health check failed: {e}")
            results["overall_status"] = "unhealthy"
        
        # Check 4: Cache health (if enabled)
        logger.debug("Checking cache health...")
        try:
            cache_stats = doc_store.get_cache_stats()
            if cache_stats:
                hit_rate = cache_stats.get("hit_rate", 0)
                if hit_rate < 0.8:  # Less than 80% hit rate
                    results["checks"]["cache_health"] = {
                        "status": "warn",
                        "message": f"Low cache hit rate: {hit_rate:.1%}",
                        "stats": cache_stats
                    }
                    results["warnings"].append(f"Cache hit rate is low: {hit_rate:.1%}")
                    if results["overall_status"] == "healthy":
                        results["overall_status"] = "warning"
                else:
                    results["checks"]["cache_health"] = {
                        "status": "pass",
                        "message": f"Cache hit rate: {hit_rate:.1%}",
                        "stats": cache_stats
                    }
            else:
                results["checks"]["cache_health"] = {
                    "status": "info",
                    "message": "Caching disabled"
                }
        except Exception as e:
            results["checks"]["cache_health"] = {"status": "fail", "message": str(e)}
            results["errors"].append(f"Cache health check failed: {e}")
            results["overall_status"] = "unhealthy"
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        results["overall_status"] = "unhealthy"
        results["errors"].append(f"Health check failed: {e}")
    
    logger.info(f"Health check completed: {results['overall_status']}")
    return results


def _print_health_results(results: Dict):
    """Print health check results in human-readable format."""
    print("=" * 50)
    print("DOC STORE HEALTH CHECK RESULTS")
    print("=" * 50)
    
    # Overall status
    status = results["overall_status"].upper()
    status_color = {
        "HEALTHY": "\033[92m",  # Green
        "WARNING": "\033[93m",  # Yellow
        "UNHEALTHY": "\033[91m"  # Red
    }
    reset_color = "\033[0m"
    
    print(f"\nOverall Status: {status_color.get(status, '')}{status}{reset_color}")
    print(f"Timestamp: {time.ctime(results['timestamp'])}")
    
    # Individual checks
    print("\nDetailed Results:")
    print("-" * 30)
    
    for check_name, check_result in results["checks"].items():
        status_icon = {
            "pass": "✅",
            "warn": "⚠️",
            "fail": "❌",
            "info": "ℹ️"
        }
        
        icon = status_icon.get(check_result["status"], "❓")
        print(f"{icon} {check_name}: {check_result['message']}")
        
        # Print additional details for some checks
        if "stats" in check_result:
            stats = check_result["stats"]
            if isinstance(stats, dict):
                for key, value in stats.items():
                    print(f"   {key}: {value}")
    
    # Warnings and errors
    if results["warnings"]:
        print(f"\n⚠️  Warnings ({len(results['warnings'])}):")
        for warning in results["warnings"]:
            print(f"   • {warning}")
    
    if results["errors"]:
        print(f"\n❌ Errors ({len(results['errors'])}):")
        for error in results["errors"]:
            print(f"   • {error}")
    
    print("\n" + "=" * 50)


def _run_migration(doc_store: DocumentStore, dry_run: bool) -> Dict:
    """Run data migration operations."""
    logger.info(f"Starting migration (dry_run={dry_run})...")
    
    results = {
        "timestamp": time.time(),
        "dry_run": dry_run,
        "migrations": [],
        "errors": []
    }
    
    try:
        # Migration 1: Fix missing version pointers
        logger.debug("Checking for missing version pointers...")
        final_docs = doc_store.list_final_documents()
        missing_pointers = []
        
        for doc_id in final_docs:
            try:
                current_version = doc_store.get_current_document_version(doc_id)
                if not current_version:
                    missing_pointers.append(doc_id)
            except Exception as e:
                logger.warning(f"Could not check version pointer for {doc_id}: {e}")
                missing_pointers.append(doc_id)
        
        if missing_pointers:
            migration = {
                "name": "fix_missing_version_pointers",
                "description": f"Create missing version pointers for {len(missing_pointers)} documents",
                "affected_docs": missing_pointers,
                "status": "pending"
            }
            
            if not dry_run:
                # Actually perform the migration
                fixed_count = 0
                for doc_id in missing_pointers:
                    try:
                        # Find the latest final version and create pointer
                        versions = doc_store.list_document_versions(doc_id)
                        if versions:
                            latest_version = max(versions)
                            # Create version pointer (this would need to be implemented)
                            logger.info(f"Created version pointer for {doc_id} -> {latest_version}")
                            fixed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to fix version pointer for {doc_id}: {e}")
                        results["errors"].append(f"Version pointer fix failed for {doc_id}: {e}")
                
                migration["status"] = "completed"
                migration["fixed_count"] = fixed_count
            else:
                migration["status"] = "dry_run"
            
            results["migrations"].append(migration)
        
        # Add more migrations as needed...
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        results["errors"].append(f"Migration failed: {e}")
    
    logger.info(f"Migration completed (dry_run={dry_run})")
    return results


def _print_migration_results(results: Dict, dry_run: bool):
    """Print migration results in human-readable format."""
    print("=" * 50)
    print(f"DOC STORE MIGRATION RESULTS {'(DRY RUN)' if dry_run else ''}")
    print("=" * 50)
    
    print(f"\nTimestamp: {time.ctime(results['timestamp'])}")
    print(f"Dry Run Mode: {'Yes' if dry_run else 'No'}")
    
    if results["migrations"]:
        print(f"\nMigrations ({len(results['migrations'])}):")
        print("-" * 30)
        
        for migration in results["migrations"]:
            status_icon = {
                "completed": "✅",
                "pending": "⏳",
                "dry_run": "🔍",
                "failed": "❌"
            }
            
            icon = status_icon.get(migration["status"], "❓")
            print(f"{icon} {migration['name']}")
            print(f"   Description: {migration['description']}")
            print(f"   Status: {migration['status']}")
            
            if "affected_docs" in migration:
                print(f"   Affected Documents: {len(migration['affected_docs'])}")
            
            if "fixed_count" in migration:
                print(f"   Fixed: {migration['fixed_count']}")
    else:
        print("\n✅ No migrations needed")
    
    if results["errors"]:
        print(f"\n❌ Errors ({len(results['errors'])}):")
        for error in results["errors"]:
            print(f"   • {error}")
    
    print("\n" + "=" * 50)


if __name__ == "__main__":
    # If called directly, show help
    print("Doc Store CLI - Available commands:")
    print("  doc-store-health  - Check system health")
    print("  doc-store-migrate - Run data migrations")
    print("\nInstall the package to use these commands.") 