#!/usr/bin/env python3
"""
Migration: Copy files from output_directory to storage and EFS directories.

This command copies all files and subfolders from pipeline.output_directory
to pipeline.storage_directory and pipeline.EFS_directory, preserving the
same file structure. These target directories are typically S3 buckets or
other blob storage services and EFS mounts for downstream processing.

Usage:
    python commands/05_migration.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import configure_logging, get_log_file_path, load_config

LOG = logging.getLogger("commands.migrate")


def copy_directory_tree(source: Path, destination: Path) -> tuple[int, int]:
    """
    Copy entire directory tree from source to destination, preserving structure.
    
    Args:
        source: Source directory path
        destination: Destination directory path
        
    Returns:
        Tuple of (files_copied, directories_created)
    """
    files_copied = 0
    directories_created = 0
    
    if not source.exists():
        LOG.error("Source directory does not exist: %s", source)
        return (0, 0)
    
    if not source.is_dir():
        LOG.error("Source path is not a directory: %s", source)
        return (0, 0)
    
    # Create destination directory if it doesn't exist
    destination.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        LOG.error("Failed to create destination directory: %s", destination)
        return (0, 0)
    
    LOG.info("Copying directory tree: %s -> %s", source, destination)
    
    # Use shutil.copytree with dirs_exist_ok=True to handle existing directories
    # We'll copy manually to get better logging
    try:
        for item in source.rglob("*"):
            # Calculate relative path from source
            relative_path = item.relative_to(source)
            dest_path = destination / relative_path
            
            if item.is_dir():
                # Create directory in destination
                dest_path.mkdir(parents=True, exist_ok=True)
                directories_created += 1
                LOG.debug("Created directory: %s", dest_path)
            elif item.is_file():
                # Copy file
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest_path)
                files_copied += 1
                if files_copied % 100 == 0:
                    LOG.info("Copied %d files so far...", files_copied)
        
        LOG.info("✓ Successfully copied %d files and %d directories from %s to %s", 
                files_copied, directories_created, source, destination)
        return (files_copied, directories_created)
        
    except Exception as exc:  # noqa: BLE001
        LOG.error("✗ Failed to copy directory tree from %s to %s: %s", source, destination, exc)
        LOG.exception("Full error details:")
        raise


def main() -> int:
    """Entrypoint for the migration command."""
    parser = argparse.ArgumentParser(
        description="Copy files from output_directory to storage and EFS directories"
    )
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config file
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "migration")
    configure_logging(config, log_file=log_file)
    
    # Get pipeline configuration
    pipeline_config = config.get("pipeline", {})
    
    # Get source directory (output_directory)
    output_directory = pipeline_config.get("output_directory")
    if not output_directory:
        LOG.error("Config missing 'pipeline.output_directory'")
        return 1
    
    source_dir = Path(output_directory)
    if not source_dir.exists():
        LOG.error("Output directory does not exist: %s", source_dir)
        return 1
    
    if not source_dir.is_dir():
        LOG.error("Output directory path is not a directory: %s", source_dir)
        return 1
    
    # Get target directories
    storage_directory = pipeline_config.get("storage_directory")
    efs_directory = pipeline_config.get("EFS_directory")
    
    target_directories = []
    if storage_directory:
        target_directories.append(("storage", Path(storage_directory)))
    if efs_directory:
        target_directories.append(("EFS", Path(efs_directory)))
    
    if not target_directories:
        LOG.error("No target directories specified in config (storage_directory or EFS_directory)")
        return 1
    
    LOG.info("=== Starting Migration ===")
    LOG.info("Source directory: %s", source_dir)
    for name, path in target_directories:
        LOG.info("Target directory (%s): %s", name, path)
    
    # Check if source directory has any files
    file_count = sum(1 for _ in source_dir.rglob("*") if _.is_file())
    if file_count == 0:
        LOG.warning("Source directory is empty: %s", source_dir)
        return 0
    
    LOG.info("Found %d files in source directory", file_count)
    
    # Copy to each target directory
    total_files_copied = 0
    total_dirs_created = 0
    failed_targets = []
    
    for target_name, target_path in target_directories:
        try:
            LOG.info("--- Copying to %s directory: %s ---", target_name, target_path)
            files_copied, dirs_created = copy_directory_tree(source_dir, target_path)
            total_files_copied += files_copied
            total_dirs_created += dirs_created
            LOG.info("✓ Successfully migrated to %s: %d files, %d directories", 
                    target_name, files_copied, dirs_created)
        except Exception as exc:  # noqa: BLE001
            LOG.error("✗ Failed to migrate to %s directory: %s", target_name, exc)
            failed_targets.append((target_name, str(exc)))
    
    # Summary
    LOG.info("=== Migration Summary ===")
    LOG.info("Total files copied: %d", total_files_copied)
    LOG.info("Total directories created: %d", total_dirs_created)
    
    if failed_targets:
        LOG.error("Failed to migrate to %d target(s):", len(failed_targets))
        for target_name, error in failed_targets:
            LOG.error("  %s: %s", target_name, error)
        return 1
    
    LOG.info("✓ Migration completed successfully to all target directories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
