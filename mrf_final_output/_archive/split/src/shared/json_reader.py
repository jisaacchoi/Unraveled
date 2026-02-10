"""Utility functions for reading JSON files (both compressed and uncompressed)."""
from __future__ import annotations

import gzip
from pathlib import Path
from typing import BinaryIO, Iterator, ContextManager


def open_json_file(file_path: Path) -> ContextManager[BinaryIO]:
    """
    Open a JSON file (compressed or uncompressed) for reading.
    
    Automatically detects file type based on extension:
    - .json.gz files are opened with gzip.open()
    - .json files are opened with open() in binary mode
    
    Args:
        file_path: Path to the JSON file (.json or .json.gz)
        
    Returns:
        Context manager that yields a binary file handle
        
    Raises:
        ValueError: If file extension is not .json or .json.gz
    """
    if file_path.suffixes == [".json", ".gz"]:
        # Compressed JSON file
        return gzip.open(file_path, "rb")
    elif file_path.suffix == ".json":
        # Uncompressed JSON file
        return open(file_path, "rb")
    elif file_path.suffixes == [".json", ".gz", ".part"]:
        return gzip.open(file_path, "rb")
    elif file_path.suffixes == [".json", ".part"]:
        return open(file_path, "rb")
    else:
        raise ValueError(
            f"File must be .json or .json.gz, got: {file_path.suffixes}"
        )


def is_json_file(file_path: Path) -> bool:
    """
    Check if a file path is a JSON file (.json or .json.gz).
    
    Args:
        file_path: Path to check
        
    Returns:
        True if file is .json or .json.gz, False otherwise
    """
    return (
        file_path.suffixes == [".json", ".gz"]
        or file_path.suffix == ".json"
        or file_path.suffixes == [".json", ".gz", ".part"]
        or file_path.suffixes == [".json", ".part"]
    )

