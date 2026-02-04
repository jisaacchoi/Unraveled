"""Utility functions."""
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

LOG = logging.getLogger("app.utils")


def parse_filter_string(filter_str: Optional[str]) -> Optional[List[str]]:
    """
    Parse comma-separated filter string into list.
    
    Args:
        filter_str: Comma-separated string (e.g., "123,456,789")
        
    Returns:
        List of strings, or None if input is None/empty
    """
    if not filter_str:
        return None
    
    # Split by comma and strip whitespace
    items = [item.strip() for item in filter_str.split(",") if item.strip()]
    return items if items else None


def generate_run_id() -> str:
    """
    Generate a unique run ID based on timestamp.
    
    Returns:
        Run ID string (e.g., "run_20260123_103000")
    """
    now = datetime.utcnow()
    return f"run_{now.strftime('%Y%m%d_%H%M%S')}"


def ensure_directory(path: Path) -> Path:
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object
    """
    path.mkdir(parents=True, exist_ok=True)
    return path
