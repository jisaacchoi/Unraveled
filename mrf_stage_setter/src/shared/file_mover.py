"""File movement utilities for pipeline operations."""
from __future__ import annotations

import logging
from pathlib import Path

LOG = logging.getLogger("src.file_mover")


def move_files_to_analyzed(
    input_directory: Path,
    file_names: list[str],
    analyzed_directory: Path | None = None,
) -> tuple[int, int]:
    """
    Move files to an analyzed directory.
    
    Args:
        input_directory: Directory containing the input files
        file_names: List of file names to move
        analyzed_directory: Directory to move files to (if None, uses input_directory / "analyzed")
        
    Returns:
        Tuple of (moved_count, failed_count)
    """
    if not file_names:
        LOG.debug("No files to move to analyzed directory")
        return (0, 0)
    
    if analyzed_directory is None:
        analyzed_dir = input_directory / "analyzed"
    else:
        analyzed_dir = analyzed_directory
    
    # Directory creation handled by ensure_directories_from_config()
    
    moved_count = 0
    failed_count = 0
    
    for file_name in file_names:
        source_path = input_directory / file_name
        dest_path = analyzed_dir / file_name
        
        if not source_path.exists():
            LOG.warning("File not found, skipping move: %s", source_path)
            failed_count += 1
            continue
        
        try:
            # Move file to analyzed subfolder
            source_path.rename(dest_path)
            moved_count += 1
            LOG.debug("Moved file to analyzed directory: %s -> %s", file_name, dest_path)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to move file %s to analyzed directory: %s", file_name, exc)
            failed_count += 1
    
    if moved_count > 0:
        LOG.info("Moved %d file(s) to analyzed directory: %s", moved_count, analyzed_dir)
    if failed_count > 0:
        LOG.warning("Failed to move %d file(s) to analyzed directory", failed_count)
    
    return (moved_count, failed_count)

