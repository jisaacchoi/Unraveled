"""File discovery logic for finding source files in group subdirectories."""
import logging
import re
from pathlib import Path
from typing import Iterable, List, Tuple

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
    
    # Find all group subdirectories (optional)
    group_dirs = [d for d in input_directory.iterdir() if d.is_dir() and d.name.startswith("group_")]
    if group_dirs:
        LOG.info(f"Found {len(group_dirs)} group subdirectories")
    else:
        LOG.info("No group subdirectories found; searching input_directory directly")
    
    # Search for the file in each group directory or in input_directory directly
    found_file = None
    found_group = None
    
    # Substring match: select all files whose name contains source_file
    candidate_files = []
    for group_dir in sorted(group_dirs):
        candidate_files.extend([p for p in group_dir.glob("*.json.gz") if source_file in p.name])
    if not candidate_files:
        candidate_files = [p for p in input_directory.glob("*.json.gz") if source_file in p.name]

    if candidate_files:
        candidate_files = sorted(candidate_files)
        found_file = candidate_files[0]
        found_group = found_file.parent
        LOG.info(f"Found {len(candidate_files)} matching file(s); using {found_file.name} for schema lookup")
    
    if not found_file or not found_group:
        raise FileNotFoundError(
            f"Source file not found: {source_file} in {input_directory}"
        )
    
    # Find schema file in the same directory as the file.
    # Convention: {group_prefix}__schema.json where group_prefix is before "__" in source_file.
    if "__" not in found_file.name:
        raise FileNotFoundError(
            f"Matched file does not include '__' group prefix: {found_file.name}"
        )
    group_prefix = found_file.name.split("__", 1)[0]
    schema_path = found_group / f"{group_prefix}__schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(
            f"Schema file not found: {schema_path}. Required for processing."
        )
    
    LOG.info(f"Found schema: {schema_path}")
    
    return found_file, schema_path


def find_all_file_parts(input_directory: Path, source_file: str) -> List[Path]:
    """
    Find all files that match a substring of source_file.
    
    Args:
        input_directory: Base directory containing group subdirectories
        source_file: Substring to match against file names
        
    Returns:
        List of paths to all matching files, sorted by name
    """
    # Use the same matching logic as find_source_file
    group_dirs = [d for d in input_directory.iterdir() if d.is_dir() and d.name.startswith("group_")]
    candidate_files = []
    for group_dir in sorted(group_dirs):
        candidate_files.extend([p for p in group_dir.glob("*.json.gz") if source_file in p.name])
    if not candidate_files:
        candidate_files = [p for p in input_directory.glob("*.json.gz") if source_file in p.name]
    
    return sorted(candidate_files)


def collect_structure_directories_for_files(
    structure_roots: Iterable[Path],
    file_names: Iterable[str],
) -> List[Path]:
    """
    Find directories that contain structure files matching given file names.

    Args:
        structure_roots: Iterable of root directories to scan
        file_names: Iterable of file names to match exactly

    Returns:
        List of directories that contain matching structure files
    """
    names = {n for n in file_names if n}
    if not names:
        return []
    dirs: List[Path] = []
    for root in structure_roots:
        if not root or not root.exists() or not root.is_dir():
            continue
        for name in names:
            for match in root.rglob(name):
                if match.is_file():
                    dirs.append(match.parent)
    return sorted(set(dirs))


def find_source_file_in_paths(search_paths: Iterable[Path], source_file: str) -> Tuple[Path, Path]:
    """
    Find source file in multiple search paths (and their group subdirectories).
    """
    for base in search_paths:
        if not base or not base.exists():
            continue
        try:
            return find_source_file(base, source_file)
        except FileNotFoundError:
            continue
    raise FileNotFoundError(f"Source file not found: {source_file} in any search path")


def find_all_file_parts_in_paths(search_paths: Iterable[Path], source_file: str) -> List[Path]:
    """
    Find all file parts across multiple search paths.
    """
    all_parts: List[Path] = []
    for base in search_paths:
        if not base or not base.exists():
            continue
        all_parts.extend(find_all_file_parts(base, source_file))
    return sorted(set(all_parts))


def find_group_dirs_with_files(search_paths: Iterable[Path], source_file: str) -> List[Path]:
    """
    Find group directories that contain matching source files.
    """
    files = find_all_file_parts_in_paths(search_paths, source_file)
    group_dirs = sorted({p.parent for p in files})
    return group_dirs
