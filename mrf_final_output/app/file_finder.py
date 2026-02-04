"""File discovery logic for finding source files in group subdirectories."""
import logging
import re
from pathlib import Path
from typing import List, Tuple

LOG = logging.getLogger("app.file_finder")


def find_source_file(input_directory: Path, source_file: str) -> Tuple[Path, Path]:
    """
    Find source file in group subdirectories and return file path and schema path.
    
    Handles split files: if the file has _part#### suffix, finds all parts.
    
    Args:
        input_directory: Base directory containing group subdirectories
        source_file: Name of the source file to find
        
    Returns:
        Tuple of (file_path, schema_path) where:
        - file_path: Path to the first file found (or first part if split)
        - schema_path: Path to schema.json in the same group directory
        
    Raises:
        FileNotFoundError: If file or schema not found
    """
    LOG.info(f"Searching for source file: {source_file}")
    LOG.info(f"Input directory: {input_directory}")
    
    # Check if input_directory exists
    if not input_directory.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_directory}")
    
    # Find all group subdirectories
    group_dirs = [d for d in input_directory.iterdir() if d.is_dir() and d.name.startswith("group_")]
    
    if not group_dirs:
        raise FileNotFoundError(f"No group subdirectories found in {input_directory}")
    
    LOG.info(f"Found {len(group_dirs)} group subdirectories")
    
    # Search for the file in each group directory
    found_file = None
    found_group = None
    
    # Check if source_file has a _part#### suffix
    # Pattern: filename_part####.json.gz
    part_pattern = re.compile(r'^(.+?)_part\d{4}(\.json(?:\.gz)?)$')
    match = part_pattern.match(source_file)
    
    if match:
        # It's a split file - find all parts
        base_name = match.group(1) + match.group(2)
        LOG.info(f"Detected split file pattern. Base name: {base_name}")
        
        # Find all parts in the same group
        for group_dir in sorted(group_dirs):
            # Find all matching part files
            part_files = sorted(group_dir.glob(f"{base_name.replace('.json.gz', '')}_part*.json.gz"))
            
            if part_files:
                found_file = part_files[0]  # Return first part as representative
                found_group = group_dir
                LOG.info(f"Found split file in {group_dir.name}: {len(part_files)} parts")
                for part_file in part_files:
                    LOG.info(f"  - {part_file.name}")
                break
    else:
        # Regular file - search for exact match
        for group_dir in sorted(group_dirs):
            file_path = group_dir / source_file
            if file_path.exists():
                found_file = file_path
                found_group = group_dir
                LOG.info(f"Found file in {group_dir.name}: {file_path.name}")
                break
    
    if not found_file or not found_group:
        raise FileNotFoundError(
            f"Source file not found: {source_file} in any group subdirectory of {input_directory}"
        )
    
    # Find schema.json in the same group directory
    schema_path = found_group / "schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_path}. Required for processing."
        )
    
    LOG.info(f"Found schema: {schema_path}")
    
    return found_file, schema_path


def find_all_file_parts(input_directory: Path, source_file: str) -> List[Path]:
    """
    Find all parts of a split file.
    
    Args:
        input_directory: Base directory containing group subdirectories
        source_file: Name of the source file (may have _part#### suffix)
        
    Returns:
        List of paths to all file parts, sorted by part number
    """
    # Find the group directory and base name
    file_path, _ = find_source_file(input_directory, source_file)
    group_dir = file_path.parent
    
    # Extract base name (remove _part#### if present)
    part_pattern = re.compile(r'^(.+?)_part\d{4}(\.json(?:\.gz)?)$')
    match = part_pattern.match(source_file)
    
    if match:
        base_name = match.group(1) + match.group(2)
        # Find all parts
        part_files = sorted(group_dir.glob(f"{base_name.replace('.json.gz', '')}_part*.json.gz"))
        return part_files
    else:
        # Not a split file, return single file
        return [file_path]
