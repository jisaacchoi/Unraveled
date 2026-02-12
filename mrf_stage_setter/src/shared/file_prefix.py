"""File prefix management utilities for tracking processing state."""
from __future__ import annotations

import logging
from pathlib import Path

LOG = logging.getLogger("src.file_prefix")

# Prefix definitions
PREFIX_INGESTED = "_ingested_"
PREFIX_ANALYZED = "_analyzed_"
PREFIX_READY = "_ready_"


def add_prefix(file_path: Path, prefix: str) -> Path:
    """
    Add a prefix to a filename, preserving the original name structure.
    
    Handles special case: if adding _analyzed_ prefix and file already has _ingested_ prefix,
    inserts _analyzed_ after _ingested_ to avoid double underscore.
    
    Args:
        file_path: Path to the file
        prefix: Prefix to add (e.g., "_ingested_", "_analyzed_")
        
    Returns:
        New Path with prefix added to filename
        
    Examples:
        file.json.gz -> _ingested_file.json.gz
        _ingested_file.json.gz -> _ingested_analyzed_file.json.gz
    """
    # Extract the base filename
    filename = file_path.name
    LOG.debug("Adding prefix '%s' to filename: %s", prefix, filename)
    
    # Check if prefix already exists (to avoid duplicates)
    if filename.startswith(prefix):
        LOG.debug("File %s already has prefix %s, returning original path", file_path, prefix)
        return file_path
    
    # Special handling: if adding _analyzed_ and file has _ingested_ prefix, insert after _ingested_
    if prefix == PREFIX_ANALYZED and filename.startswith(PREFIX_INGESTED):
        # Check if _analyzed_ already exists after _ingested_ (to avoid double prefix)
        remaining_after_ingested = filename[len(PREFIX_INGESTED):]
        if (
            remaining_after_ingested.startswith(PREFIX_ANALYZED)
            or remaining_after_ingested.startswith(PREFIX_ANALYZED.lstrip("_"))
        ):
            # File already has _ingested_analyzed_ prefix, return original
            LOG.debug("File %s already has _ingested_analyzed_ prefix, returning original path", file_path)
            return file_path
        
        # Remove _ingested_ prefix, add both prefixes in correct order
        # Note: PREFIX_INGESTED ends with "_" and PREFIX_ANALYZED starts with "_", so we need to avoid double underscore
        # We'll use PREFIX_INGESTED (which ends with "_") + PREFIX_ANALYZED[1:] (skip the leading "_") + remaining
        remaining = filename[len(PREFIX_INGESTED):]
        # PREFIX_ANALYZED is "_analyzed_", so we use "analyzed_" to avoid double underscore
        analyzed_without_leading_underscore = PREFIX_ANALYZED[1:]  # "analyzed_"
        new_filename = PREFIX_INGESTED + analyzed_without_leading_underscore + remaining
        new_path = file_path.parent / new_filename
        LOG.debug("Inserting _analyzed_ after _ingested_: %s -> %s", file_path, new_path)
        return new_path
    
    # Add prefix to the beginning of the filename
    new_filename = prefix + filename
    new_path = file_path.parent / new_filename
    LOG.debug("Computed new path: %s -> %s", file_path, new_path)
    
    return new_path


def remove_prefix(file_path: Path, prefix: str) -> Path:
    """
    Remove a prefix from a filename if it exists.
    
    Args:
        file_path: Path to the file
        prefix: Prefix to remove
        
    Returns:
        New Path with prefix removed (or original if prefix not found)
    """
    filename = file_path.name
    
    if filename.startswith(prefix):
        new_filename = filename[len(prefix):]
        return file_path.parent / new_filename
    
    return file_path


def has_prefix(file_path: Path, prefix: str) -> bool:
    """Check if a file path has the given prefix."""
    return file_path.name.startswith(prefix)


def has_all_prefixes(file_path: Path, prefixes: list[str]) -> bool:
    """
    Check if a file path has all the specified prefixes (in order).
    
    Handles the case where prefixes share underscores (e.g., "_ingested_" + "_analyzed_" = "_ingested_analyzed_").
    
    Args:
        file_path: Path to the file
        prefixes: List of prefixes to check (e.g., ["_ingested_", "_analyzed_"])
        
    Returns:
        True if file has all prefixes in order, False otherwise
        
    Examples:
        has_all_prefixes(Path("_ingested_analyzed_file.json.gz"), ["_ingested_", "_analyzed_"]) -> True
        has_all_prefixes(Path("_ingested_file.json.gz"), ["_ingested_", "_analyzed_"]) -> False
    """
    filename = file_path.name
    current_pos = 0
    
    for prefix in prefixes:
        # Check if prefix exists starting at current_pos
        if current_pos + len(prefix) > len(filename):
            return False
        if filename[current_pos:current_pos + len(prefix)] == prefix:
            # Exact match at expected position
            current_pos += len(prefix)
        else:
            # Try finding the prefix (handles shared underscores between prefixes)
            # When previous prefix ends with "_" and current starts with "_", they share the underscore
            # So current prefix may start one position earlier
            search_start = max(0, current_pos - 1) if current_pos > 0 else current_pos
            pos = filename.find(prefix, search_start)
            if pos == -1:
                return False
            # Check if prefix is at expected position or one position earlier (shared underscore)
            if pos == current_pos:
                # Exact match
                current_pos += len(prefix)
            elif pos == current_pos - 1 and current_pos > 0:
                # Prefix starts one position earlier (shared underscore case)
                # Example: "_ingested_" (pos 0-9) followed by "_analyzed_" (pos 9-17)
                # After "_ingested_", current_pos = 10, but "_analyzed_" starts at 9
                current_pos = pos + len(prefix)
            else:
                return False
    
    return True


def has_exactly_prefix(file_path: Path, prefix: str) -> bool:
    """
    Check if a file path has exactly the specified prefix (and no additional prefixes after it).
    
    This is useful for Step 3 which should only process files with _ingested_ prefix,
    but not files that already have _ingested_analyzed_ prefix.
    
    Args:
        file_path: Path to the file
        prefix: Prefix to check (e.g., "_ingested_")
        
    Returns:
        True if file starts with prefix and the next character is not part of another known prefix
        
    Examples:
        has_exactly_prefix(Path("_ingested_file.json.gz"), "_ingested_") -> True
        has_exactly_prefix(Path("_ingested_analyzed_file.json.gz"), "_ingested_") -> False
    """
    filename = file_path.name
    if not filename.startswith(prefix):
        return False
    
    # Check if there's another prefix immediately after this one
    remaining = filename[len(prefix):]
    # Check if remaining starts with any other known prefix
    other_prefixes = [PREFIX_ANALYZED, PREFIX_READY]
    for other_prefix in other_prefixes:
        if remaining.startswith(other_prefix):
            return False
    
    return True


def rename_with_prefix(file_path: Path, prefix: str) -> Path:
    """
    Rename a file by adding a prefix to its filename.
    
    Args:
        file_path: Path to the file to rename
        prefix: Prefix to add
        
    Returns:
        New Path after renaming
        
    Raises:
        FileNotFoundError: If source file doesn't exist
        OSError: If rename fails
    """
    LOG.debug("Attempting to rename file with prefix '%s': %s", prefix, file_path)
    
    if not file_path.exists():
        error_msg = f"File not found: {file_path}"
        LOG.error("✗ Rename failed - %s", error_msg)
        raise FileNotFoundError(error_msg)
    
    new_path = add_prefix(file_path, prefix)
    
    # If add_prefix returned the original path (file already has prefix), no rename needed
    if new_path == file_path:
        LOG.debug("File %s already has prefix %s, no rename needed", file_path, prefix)
        return file_path
    
    # If new path already exists, don't rename
    if new_path.exists():
        LOG.warning("Target file already exists, skipping rename: %s -> %s (target exists)", 
                   file_path, new_path)
        return file_path
    
    # Rename the file
    try:
        LOG.info("Renaming file with prefix '%s': %s -> %s", prefix, file_path.name, new_path.name)
        LOG.debug("Full paths: %s -> %s", file_path, new_path)
        file_path.rename(new_path)
        LOG.info("✓ Successfully renamed: %s -> %s", file_path, new_path)
        # Verify the rename succeeded
        if not new_path.exists():
            LOG.error("✗ Rename appeared to succeed but target file does not exist: %s", new_path)
            raise OSError(f"Rename verification failed: target file not found: {new_path}")
        if file_path.exists():
            LOG.warning("⚠ Source file still exists after rename: %s (may indicate rename failure)", file_path)
        return new_path
    except OSError as e:
        error_msg = f"Failed to rename {file_path} to {new_path}: {e}"
        LOG.error("✗ Rename failed - %s", error_msg)
        LOG.exception("Full error details:")
        raise


def get_files_without_prefix(directory: Path, prefix: str, pattern: str = "*.json.gz") -> list[Path]:
    """
    Get all files in a directory that don't have the specified prefix.
    
    Args:
        directory: Directory to search
        prefix: Prefix to filter by (files without this prefix)
        pattern: File pattern to match (default: "*.json.gz")
        
    Returns:
        List of Path objects for files without the prefix
    """
    if not directory.exists():
        return []
    
    files = []
    for file_path in directory.glob(pattern):
        if file_path.is_file() and not has_prefix(file_path, prefix):
            files.append(file_path)
    
    return files


def get_files_with_prefix(directory: Path, prefix: str, pattern: str = "*.json.gz") -> list[Path]:
    """
    Get all files in a directory that have the specified prefix.
    
    Args:
        directory: Directory to search
        prefix: Prefix to filter by (files with this prefix)
        pattern: File pattern to match (default: "*.json.gz")
        
    Returns:
        List of Path objects for files with the prefix
    """
    if not directory.exists():
        return []
    
    files = []
    for file_path in directory.glob(pattern):
        if file_path.is_file() and has_prefix(file_path, prefix):
            files.append(file_path)
    
    return files
