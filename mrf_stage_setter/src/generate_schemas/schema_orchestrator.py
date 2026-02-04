"""Pipeline orchestration for schema detection and generation."""
from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

from src.shared.file_mover import move_files_to_analyzed
from src.detect_shapes.structure_analyzer import run_shape_analysis
from src.generate_schemas.schema_groups_db import (
    ensure_schema_groups_table,
    get_all_file_names_from_schema_groups,
    get_files_grouped_by_min_file_name,
    get_files_grouped_by_schema_sig,
    get_non_min_file_names,
    get_unique_min_file_names,
    hash_schema_sig_to_folder_name,
)

LOG = logging.getLogger("src.schema_orchestrator")


def run_full_pipeline(
    connection_string: str,
    spark: None = None,
    input_directory: Path | None = None,
    schema_output_directory: Path | None = None,
    batch_size: int = 1000,
    fetch_batch_size: int = 1000,
    drop_table_if_exists: bool = False,
    refresh_schema_groups: bool = True,
    download_path: Path | None = None,
    analyzed_directory: Path | None = None,
    files_output_directory: Path | None = None,
    enable_detect: bool = True,
    enable_group: bool = True,
) -> int:
    """
    Run the full pipeline: detect shapes and refresh schema groups.
    
    Files are moved and organized into subfolders by schema group after grouping.
    
    Args:
        connection_string: PostgreSQL connection string
        spark: Not used (kept for API compatibility)
        input_directory: Directory containing .json.gz files (required if group enabled)
        schema_output_directory: Not used (kept for API compatibility)
        batch_size: Batch size for shape analysis inserts
        fetch_batch_size: Batch size for fetching rows from database
        drop_table_if_exists: Whether to drop mrf_analysis table before creating (not used in this function, kept for API compatibility)
        refresh_schema_groups: Whether to refresh schema_groups table
        download_path: Optional directory path for URL downloads (deprecated, kept for compatibility)
        analyzed_directory: Optional directory to move analyzed files to (if None, uses input_directory / "analyzed")
        files_output_directory: Directory to move files to after grouping (organized into subfolders by schema group)
        enable_detect: If True, run shape detection step
        enable_group: If True, run schema group refresh step
        
    Returns:
        Analysis result: 0 on success, non-zero on error
    """
    # Step 1: Detect shapes
    if enable_detect:
        LOG.info("=== Step 1: Detecting shapes ===")
        analysis_result = run_shape_analysis(
            connection_string=connection_string,
            batch_size=batch_size,
            fetch_batch_size=fetch_batch_size,
            download_path=download_path,
        )
        
        if analysis_result[0] < 0:  # Error occurred
            LOG.error("Shape analysis failed, aborting pipeline")
            return (1, False)
        
        LOG.info("Shape analysis complete: %d records inserted, %d files analyzed", analysis_result[0], analysis_result[1])
    else:
        LOG.info("=== Step 1: Skipping shape detection (disabled) ===")
    
    # Step 2: Get current unique min_file_names before refresh (only if grouping is enabled)
    min_file_names_before = set()
    if enable_group:
        LOG.info("=== Step 2: Checking for new schema groups ===")
        min_file_names_before = get_unique_min_file_names(connection_string)
        LOG.info("Found %d unique min_file_name values before refresh", len(min_file_names_before))
    
    # Step 3: Refresh schema groups
    if enable_group:
        LOG.info("=== Step 3: Refreshing schema_groups table ===")
        try:
            ensure_schema_groups_table(connection_string, refresh=refresh_schema_groups)
            LOG.info("Schema groups table refreshed")
        except Exception as exc:  # noqa: BLE001
            LOG.exception("Error refreshing schema_groups table: %s", exc)
            return 1
        
        # Step 3a: Move files after grouping (if files_output_directory is provided)
        # This allows files to be moved after schema grouping is complete
        if files_output_directory and input_directory:
            LOG.info("=== Moving files after schema grouping ===")
            try:
                files_in_groups = get_all_file_names_from_schema_groups(connection_string)
                LOG.info("Found %d file(s) in schema_groups table", len(files_in_groups))
                
                if files_in_groups:
                    # Filter to only files that exist in input directory
                    # Database stores original file names (without prefixes)
                    # But files in directory may have _ingested_analyzed_ prefix
                    # So we need to match by trying both the original name and prefixed versions
                    from src.shared.file_prefix import (
                        has_all_prefixes, 
                        PREFIX_INGESTED, PREFIX_ANALYZED,
                        add_prefix
                    )
                    from src.shared.json_reader import is_json_file
                    
                    # First, get all actual files in the input directory
                    actual_files = {
                        f.name: f 
                        for f in input_directory.iterdir() 
                        if f.is_file() and is_json_file(f)
                    }
                    LOG.info("Found %d actual file(s) in input directory: %s", len(actual_files), input_directory)
                    if actual_files:
                        # Log some example files
                        example_files = list(actual_files.keys())[:10]
                        LOG.info("Example files found: %s", example_files)
                        # Count files with _ingested_analyzed_ prefix
                        prefixed_count = sum(1 for f in actual_files.values() 
                                           if has_all_prefixes(f, [PREFIX_INGESTED, PREFIX_ANALYZED]))
                        LOG.info("Files with _ingested_analyzed_ prefix: %d out of %d", prefixed_count, len(actual_files))
                    
                    # Match database file names to actual files
                    files_to_move = []
                    for db_file_name in files_in_groups:
                        matched = False
                        
                        # Try exact match first
                        if db_file_name in actual_files:
                            file_path = actual_files[db_file_name]
                            if has_all_prefixes(file_path, [PREFIX_INGESTED, PREFIX_ANALYZED]):
                                files_to_move.append(db_file_name)
                                matched = True
                                continue
                        
                        # Try with _ingested_analyzed_ prefix (REQUIRED - files must have both prefixes)
                        prefixed_name = add_prefix(Path(db_file_name), PREFIX_INGESTED)
                        prefixed_name = add_prefix(prefixed_name, PREFIX_ANALYZED)
                        if prefixed_name.name in actual_files:
                            file_path = actual_files[prefixed_name.name]
                            # Verify it actually has both prefixes (safety check)
                            if has_all_prefixes(file_path, [PREFIX_INGESTED, PREFIX_ANALYZED]):
                                files_to_move.append(db_file_name)  # Use original name for tracking
                                matched = True
                                continue
                        
                        # DO NOT match files with only _ingested_ prefix - they haven't been analyzed yet!
                        # Files must have _ingested_analyzed_ prefix to be moved
                    
                    LOG.info("Found %d file(s) in input directory to move (out of %d total in groups)", 
                            len(files_to_move), len(files_in_groups))
                    
                    if len(files_to_move) == 0:
                        LOG.warning("No files matched! This could mean:")
                        LOG.warning("  1. Files in directory don't match DB file names")
                        LOG.warning("  2. Files don't have required _ingested_analyzed_ prefix")
                        # Show some DB file names for comparison
                        LOG.warning("Sample DB file names from schema_groups:")
                        for db_file_name in list(files_in_groups)[:10]:
                            LOG.warning("  - %s", db_file_name)
                        # Show some actual file names for comparison
                        if actual_files:
                            LOG.warning("Sample actual file names in directory:")
                            for actual_name in list(actual_files.keys())[:10]:
                                LOG.warning("  - %s", actual_name)
                    
                    if len(files_to_move) < len(files_in_groups):
                        LOG.warning("Some files from schema_groups table not found in input directory:")
                        missing = set(files_in_groups) - set(files_to_move)
                        for missing_file in list(missing)[:10]:  # Show first 10
                            LOG.warning("  - %s (not found in directory)", missing_file)
                        if len(missing) > 10:
                            LOG.warning("  ... and %d more", len(missing) - 10)
                    
                    if files_to_move:
                        LOG.info("Moving %d file(s) to output directory: %s", 
                                len(files_to_move), files_output_directory)
                        files_output_directory.mkdir(parents=True, exist_ok=True)
                        
                        moved_count = 0
                        failed_count = 0
                        successfully_moved_files = []  # Track which files were actually moved (using DB file names)
                        file_name_mapping = {}  # Map DB file names to actual file paths
                        
                        # Build mapping of DB file names to actual file paths
                        # Only map files that have _ingested_analyzed_ prefix (files_to_move already filtered)
                        for db_file_name in files_to_move:
                            matched = False
                            
                            # Try exact match first
                            if db_file_name in actual_files:
                                file_path = actual_files[db_file_name]
                                # Verify it has required prefixes
                                if has_all_prefixes(file_path, [PREFIX_INGESTED, PREFIX_ANALYZED]):
                                    file_name_mapping[db_file_name] = file_path
                                    matched = True
                                    continue
                            
                            # Try with _ingested_analyzed_ prefix (REQUIRED)
                            prefixed_name = add_prefix(Path(db_file_name), PREFIX_INGESTED)
                            prefixed_name = add_prefix(prefixed_name, PREFIX_ANALYZED)
                            if prefixed_name.name in actual_files:
                                file_path = actual_files[prefixed_name.name]
                                # Verify it has required prefixes
                                if has_all_prefixes(file_path, [PREFIX_INGESTED, PREFIX_ANALYZED]):
                                    file_name_mapping[db_file_name] = file_path
                                    matched = True
                                    continue
                            
                            # DO NOT map files with only _ingested_ prefix - they haven't been analyzed yet!
                            
                            if not matched:
                                LOG.warning("Could not find actual file path for %s, skipping", db_file_name)
                        
                        # Move files using actual file paths
                        for db_file_name in files_to_move:
                            if db_file_name not in file_name_mapping:
                                LOG.warning("Could not find actual file path for %s, skipping", db_file_name)
                                failed_count += 1
                                continue
                            
                            source_path = file_name_mapping[db_file_name]
                            # Use the actual file name (with prefixes) for destination
                            dest_path = files_output_directory / source_path.name
                            
                            try:
                                # Use shutil.move for better Windows compatibility
                                shutil.move(str(source_path), str(dest_path))
                                moved_count += 1
                                successfully_moved_files.append(db_file_name)  # Track by DB name
                                LOG.info("Moved file: %s -> %s", source_path.name, dest_path)
                            except Exception as move_exc:  # noqa: BLE001
                                LOG.warning("Failed to move file %s to output directory: %s", source_path.name, move_exc)
                                failed_count += 1
                        
                        LOG.info("Moved %d file(s) to output directory (%d failed)", 
                                moved_count, failed_count)
                        
                        # Step 3b: Organize files into subfolders by schema group
                        # Only organize if we successfully moved at least one file
                        if moved_count > 0 and successfully_moved_files:
                            # Wait to ensure file system operations have completed
                            LOG.info("Waiting 10 seconds after moving files to output directory before organizing...")
                            time.sleep(10)
                            try:
                                LOG.info("=== Organizing files into schema group subfolders ===")
                                file_groups = get_files_grouped_by_schema_sig(connection_string)
                                
                                if file_groups:
                                    LOG.info("Found %d schema group(s) to organize files into", len(file_groups))
                                    organized_count = 0
                                    organized_failed = 0
                                    
                                    # Create a set of successfully moved files for quick lookup
                                    moved_files_set = set(successfully_moved_files)
                                    
                                    # List all files currently in output directory root (not in subfolders)
                                    # Files in subfolders are already organized, so we skip them
                                    actual_files_in_output = set()
                                    for item in files_output_directory.iterdir():
                                        if item.is_file():
                                            actual_files_in_output.add(item.name)
                                        elif item.is_dir():
                                            # Check if files are already in subfolders
                                            LOG.debug("Found existing subfolder: %s", item.name)
                                    LOG.info("Files currently in output directory root: %d file(s)", len(actual_files_in_output))
                                    if actual_files_in_output:
                                        LOG.debug("Files in root: %s", sorted(list(actual_files_in_output)[:10]))
                                    
                                    # Use hashed folder names based on schema_sig for deterministic naming
                                    # This ensures the same schema always maps to the same folder, even when running asynchronously
                                    for schema_sig, group_files in file_groups.items():
                                        hashed_folder_name = hash_schema_sig_to_folder_name(schema_sig)
                                        # Truncate schema_sig for logging (it can be very long)
                                        schema_sig_preview = schema_sig[:100] + "..." if len(schema_sig) > 100 else schema_sig
                                        LOG.info("Processing schema group (sig: %s) with %d file(s) -> folder: %s", 
                                                schema_sig_preview, len(group_files), hashed_folder_name)
                                        # Create subfolder with hashed name (deterministic based on schema_sig)
                                        subfolder = files_output_directory / hashed_folder_name
                                        try:
                                            subfolder.mkdir(parents=True, exist_ok=True)
                                            LOG.debug("Created/verified subfolder: %s", subfolder)
                                            # Wait after folder creation to ensure file system has updated
                                            LOG.debug("Waiting 10 seconds after creating subfolder %s...", subfolder.name)
                                            time.sleep(10)
                                        except Exception as mkdir_exc:  # noqa: BLE001
                                            LOG.error("Failed to create subfolder %s: %s", subfolder, mkdir_exc)
                                            continue  # Skip this group if we can't create the folder
                                        
                                        # Move files in this group to the subfolder
                                        # Only process files that were actually moved
                                        # Note: moved_files_set contains DB file names, but actual files have prefixes
                                        files_to_organize = [f for f in group_files if f in moved_files_set]
                                        LOG.info("  Organizing %d file(s) from this group (out of %d total in group)", 
                                                len(files_to_organize), len(group_files))
                                        
                                        # Build mapping of DB file names to actual file names in output directory
                                        actual_output_files = {f.name: f for f in files_output_directory.iterdir() if f.is_file()}
                                        db_to_actual_mapping = {}
                                        for db_file_name in files_to_organize:
                                            # Try to find the actual file (may have prefixes)
                                            for actual_name, actual_path in actual_output_files.items():
                                                # Check if actual file name contains the DB file name (with or without prefixes)
                                                base_name = db_file_name
                                                if base_name in actual_name or actual_name.endswith(base_name):
                                                    db_to_actual_mapping[db_file_name] = actual_path
                                                    break
                                        
                                        for file_name in files_to_organize:
                                            # Use actual file path if found, otherwise try DB name
                                            if file_name in db_to_actual_mapping:
                                                source_path = db_to_actual_mapping[file_name]
                                            else:
                                                source_path = files_output_directory / file_name
                                            
                                            # Check if file is already in a subfolder (from previous run)
                                            already_organized = False
                                            for existing_dir in files_output_directory.iterdir():
                                                if existing_dir.is_dir():
                                                    potential_path = existing_dir / file_name
                                                    if potential_path.exists():
                                                        LOG.debug("File %s already in subfolder %s, skipping", file_name, existing_dir.name)
                                                        already_organized = True
                                                        organized_count += 1  # Count as organized
                                                        break
                                            
                                            if already_organized:
                                                continue
                                            
                                            # Double-check file exists in root before moving
                                            if source_path.exists() and source_path.is_file():
                                                dest_path = subfolder / file_name
                                                # Ensure destination directory exists
                                                if not dest_path.parent.exists():
                                                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                                                
                                                # Verify destination parent exists before rename
                                                if not dest_path.parent.exists():
                                                    LOG.error("Failed to create destination directory: %s", dest_path.parent)
                                                    organized_failed += 1
                                                    continue
                                                
                                                # Use shutil.move instead of rename for better Windows compatibility
                                                # Retry logic for file system operations
                                                max_retries = 3
                                                retry_delay = 2
                                                moved = False
                                                
                                                # Convert to absolute paths to avoid any path resolution issues
                                                source_abs = source_path.resolve()
                                                dest_abs = dest_path.resolve()
                                                
                                                for attempt in range(max_retries):
                                                    try:
                                                        # Re-check file exists before each attempt
                                                        if not source_abs.exists():
                                                            LOG.warning("File %s no longer exists (attempt %d/%d)", 
                                                                       file_name, attempt + 1, max_retries)
                                                            break
                                                        
                                                        # Verify destination parent exists and is a directory
                                                        if not dest_abs.parent.exists():
                                                            LOG.warning("Destination parent does not exist: %s (attempt %d/%d)", 
                                                                       dest_abs.parent, attempt + 1, max_retries)
                                                            # Try to create it again
                                                            dest_abs.parent.mkdir(parents=True, exist_ok=True)
                                                            time.sleep(2)  # Wait after creating directory
                                                        
                                                        if not dest_abs.parent.exists() or not dest_abs.parent.is_dir():
                                                            LOG.error("Destination parent is not a valid directory: %s", dest_abs.parent)
                                                            if attempt < max_retries - 1:
                                                                time.sleep(retry_delay)
                                                                retry_delay *= 2
                                                            continue
                                                        
                                                        # Use absolute paths with shutil.move
                                                        shutil.move(str(source_abs), str(dest_abs))
                                                        
                                                        organized_count += 1
                                                        LOG.info("  Organized file: %s -> %s/%s", 
                                                                file_name, hashed_folder_name, file_name)
                                                        moved = True
                                                        break
                                                    except OSError as os_exc:  # noqa: BLE001
                                                        winerror = getattr(os_exc, 'winerror', None)
                                                        errno = getattr(os_exc, 'errno', None)
                                                        if errno == 2 or winerror == 3:  # File not found
                                                            if attempt < max_retries - 1:
                                                                LOG.debug("File move failed - file not found (attempt %d/%d), retrying in %d seconds", 
                                                                         attempt + 1, max_retries, retry_delay)
                                                                LOG.debug("  Source: %s (exists: %s)", source_abs, source_abs.exists())
                                                                LOG.debug("  Dest parent: %s (exists: %s, is_dir: %s)", 
                                                                         dest_abs.parent, dest_abs.parent.exists(), 
                                                                         dest_abs.parent.is_dir() if dest_abs.parent.exists() else False)
                                                                time.sleep(retry_delay)
                                                                retry_delay *= 2  # Exponential backoff
                                                                # Re-resolve paths in case something changed
                                                                source_abs = source_path.resolve()
                                                                dest_abs = dest_path.resolve()
                                                            else:
                                                                LOG.warning("File not found for move after %d attempts: %s", 
                                                                           max_retries, source_abs)
                                                                LOG.warning("  Source: %s (exists: %s, is_file: %s)", 
                                                                           source_abs, source_abs.exists(), 
                                                                           source_abs.is_file() if source_abs.exists() else False)
                                                                LOG.warning("  Dest parent: %s (exists: %s, is_dir: %s)", 
                                                                           dest_abs.parent, dest_abs.parent.exists(),
                                                                           dest_abs.parent.is_dir() if dest_abs.parent.exists() else False)
                                                        else:
                                                            LOG.warning("Failed to organize file %s (attempt %d/%d): %s (errno: %s, winerror: %s)", 
                                                                       file_name, attempt + 1, max_retries, os_exc, errno, winerror)
                                                            if attempt < max_retries - 1:
                                                                time.sleep(retry_delay)
                                                                retry_delay *= 2
                                                                source_abs = source_path.resolve()
                                                                dest_abs = dest_path.resolve()
                                                    except Exception as org_exc:  # noqa: BLE001
                                                        LOG.warning("Failed to organize file %s (attempt %d/%d): %s", 
                                                                   file_name, attempt + 1, max_retries, org_exc)
                                                        if attempt < max_retries - 1:
                                                            time.sleep(retry_delay)
                                                            retry_delay *= 2
                                                            source_abs = source_path.resolve()
                                                            dest_abs = dest_path.resolve()
                                                
                                                if not moved:
                                                    organized_failed += 1
                                                    LOG.warning("  Final check - Source: %s (exists: %s, is_file: %s), Dest parent: %s (exists: %s, is_dir: %s)", 
                                                               source_abs, source_abs.exists(), 
                                                               source_abs.is_file() if source_abs.exists() else False,
                                                               dest_abs.parent, dest_abs.parent.exists(),
                                                               dest_abs.parent.is_dir() if dest_abs.parent.exists() else False)
                                            else:
                                                LOG.debug("File %s not found in output directory root (expected at: %s)", 
                                                         file_name, source_path)
                                    
                                    LOG.info("Organized %d file(s) into schema group subfolders (%d failed)", 
                                            organized_count, organized_failed)
                                else:
                                    LOG.warning("No schema groups found for file organization")
                            except Exception as org_exc:  # noqa: BLE001
                                LOG.exception("Error organizing files into schema group subfolders (continuing): %s", org_exc)
                    else:
                        LOG.warning("No files found in input directory to move. Input directory: %s", input_directory)
                else:
                    LOG.warning("No files found in schema_groups table")
            except Exception as move_exc:  # noqa: BLE001
                LOG.exception("Error moving files after grouping (continuing): %s", move_exc)
        
        # Step 4: Get new unique min_file_names after refresh
        min_file_names_after = get_unique_min_file_names(connection_string)
        LOG.info("Found %d unique min_file_name values after refresh", len(min_file_names_after))
    else:
            LOG.info("=== Step 3: Skipping schema group refresh (disabled) ===")
    
    # Return success status
    # Note: File moving and organization by schema group is handled in Step 3a above
    return 0

