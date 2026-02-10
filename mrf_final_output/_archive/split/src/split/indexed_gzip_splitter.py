"""Split large JSON.gz files using indexed_gzip for efficient random access."""
from __future__ import annotations

import gzip
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import indexed_gzip
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.ingest.indexed_gzip_ingester import (
    open_with_existing_index,
    read_span_between_offsets,
    scan_unique_keys_with_offsets,
)
from src.split.json_splitter import validate_gzip_file

LOG = logging.getLogger("src.split.indexed_gzip_splitter")


def convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def get_scalars_from_mrf_analysis(
    cursor: "psycopg2.extensions.cursor",
    file_name: str,
) -> Dict[str, Any]:
    """
    Query mrf_analysis for all top-level scalar values.
    
    Args:
        cursor: PostgreSQL cursor (connection should already be established)
        file_name: File name to query
        
    Returns:
        Dictionary mapping scalar key names to their values
    """
    try:
        # Query for level 1 records with value_dtype = 'scalar' (top-level scalars)
        # Use top_level_key as the dictionary key
        LOG.debug("Querying mrf_analysis for top-level scalars: file_name='%s'", file_name)
        cursor.execute("""
            SELECT top_level_key, value
            FROM mrf_analysis
            WHERE file_name = %s
              AND level = 1
              AND value_dtype = 'scalar'
            ORDER BY top_level_key
        """, (file_name,))
        
        rows = cursor.fetchall()
        scalars = {}
        
        for top_level_key, value_jsonb in rows:
            if value_jsonb is not None:
                # psycopg2 automatically converts JSONB to Python objects
                # For scalar values, JSONB stores them as-is (string, number, boolean, null)
                # So we can use the value directly
                scalars[top_level_key] = value_jsonb
            else:
                scalars[top_level_key] = None
        
        LOG.info("Found %d scalar value(s) from mrf_analysis", len(scalars))
        return scalars
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying scalars from mrf_analysis: %s", exc)
        return {}


def get_top_level_arrays_from_mrf_analysis(
    cursor: "psycopg2.extensions.cursor",
    file_name: str,
) -> List[str]:
    """
    Query mrf_analysis for all top-level array keys.
    
    Args:
        cursor: PostgreSQL cursor (connection should already be established)
        file_name: File name to query
        
    Returns:
        List of top-level array key names
    """
    try:
        # Query for level 0 records with value_dtype = 'list'
        cursor.execute("""
            SELECT DISTINCT key
            FROM mrf_analysis
            WHERE file_name = %s
              AND level = 0
              AND value_dtype = 'list'
              AND path = key
            ORDER BY key
        """, (file_name,))
        
        rows = cursor.fetchall()
        array_keys = [row[0] for row in rows]
        
        LOG.info("Found %d top-level array(s) from mrf_analysis: %s", len(array_keys), array_keys)
        return array_keys
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying arrays from mrf_analysis: %s", exc)
        return []


def get_array_start_offsets_from_mrf_landing(
    cursor: "psycopg2.extensions.cursor",
    file_name: str,
) -> Dict[str, int]:
    """
    Query mrf_landing for array start offsets for all top-level arrays.
    
    Args:
        cursor: PostgreSQL cursor (connection should already be established)
        file_name: File name to query
        
    Returns:
        Dictionary mapping array key names (record_type) to their start offsets (byte offset of opening '[')
    """
    try:
        # Query for distinct record_type and array_start_offset from mrf_landing
        cursor.execute("""
            SELECT DISTINCT record_type, array_start_offset
            FROM mrf_landing
            WHERE file_name = %s
              AND array_start_offset IS NOT NULL
            ORDER BY record_type
        """, (file_name,))
        
        rows = cursor.fetchall()
        offsets = {row[0]: row[1] for row in rows}
        
        LOG.info("Found %d array start offset(s) from mrf_landing for file '%s'", len(offsets), file_name)
        return offsets
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying array start offsets from mrf_landing: %s", exc)
        return {}


def get_array_next_level_keys(
    cursor: "psycopg2.extensions.cursor",
    file_name: str,
    array_key: str,
) -> Set[str]:
    """
    Query mrf_analysis for all next-level keys (level 2) for a given top-level array.
    These are the keys that should appear in each array item for completeness checking.
    
    Note: Top-level array is at level 0, array items are at level 1, and keys within items are at level 2.
    
    Args:
        cursor: PostgreSQL cursor (connection should already be established)
        file_name: File name to query
        array_key: Top-level array key name (e.g., 'in_network')
        
    Returns:
        Set of next-level key names that should appear in each array item
    """
    try:
        # Query for level 2 records where top_level_key matches the array key
        # Level 0 = top-level array key (e.g., 'in_network')
        # Level 1 = array items (e.g., 'in_network[0]._listvalue_')
        # Level 2 = keys within array items (e.g., 'in_network[0]._listvalue_[0].name')
        cursor.execute("""
            SELECT DISTINCT key
            FROM mrf_analysis
            WHERE file_name = %s
              AND top_level_key = %s
              AND level = 2
              AND path LIKE %s
            ORDER BY key
        """, (file_name, array_key, f"{array_key}[%"))
        
        rows = cursor.fetchall()
        next_level_keys = {row[0] for row in rows}
        
        LOG.debug("Found %d next-level key(s) for array '%s': %s", len(next_level_keys), array_key, sorted(next_level_keys))
        return next_level_keys
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying next-level keys from mrf_analysis: %s", exc)
        return set()


def check_item_completeness(
    item_bytes: bytes,
    next_level_keys: Set[str],
    min_key_threshold: float = 0.5,  # Minimum ratio of expected keys that must be present (50% default)
) -> bool:
    """
    Check if an array item is complete by verifying structural completeness and key presence.
    
    An item is complete if:
    1. It is structurally complete (balanced braces, valid JSON) - REQUIRED
    2. It contains at least min_key_threshold ratio of expected keys - ADVISORY (logged if below threshold)
    
    Args:
        item_bytes: Bytes containing the array item (should be a JSON object)
        next_level_keys: Set of key names that should appear in the item (empty set means skip key checking)
        min_key_threshold: Minimum ratio (0.0-1.0) of expected keys that should be present (default: 0.5 = 50%)
        
    Returns:
        True if item is structurally complete, False otherwise
    """
    if not item_bytes:
        return False
    
    try:
        # Decode bytes to string
        item_text = item_bytes.decode("utf-8", errors="replace").strip()
        # Remove trailing comma if present (common in array contexts)
        if item_text.endswith(","):
            item_text = item_text[:-1].strip()
        
        # PHASE 1: Structural completeness check (REQUIRED)
        # Check that the item is properly closed
        open_braces = item_text.count("{")
        close_braces = item_text.count("}")
        
        # For a complete item, we should have balanced braces
        if not item_text.endswith("}"):
            LOG.debug("Item incomplete: does not end with '}'")
            return False
        
        # Check that braces are balanced
        if open_braces != close_braces:
            LOG.debug("Item incomplete: unbalanced braces (open: %d, close: %d)", open_braces, close_braces)
            return False
        
        # Verify the item can be parsed as valid JSON
        try:
            json.loads(item_text)
        except json.JSONDecodeError:
            LOG.debug("Item incomplete: cannot parse as valid JSON")
            return False
        
        # PHASE 2: Key presence check (ADVISORY - logs warning if below threshold)
        if next_level_keys:
            # Count how many expected keys are present
            keys_found = 0
            missing_keys = []
            
            for key in next_level_keys:
                # Look for the key pattern: "key":
                key_pattern = f'"{key}"'
                if key_pattern in item_text:
                    keys_found += 1
                else:
                    missing_keys.append(key)
            
            # Calculate ratio of keys found
            key_ratio = keys_found / len(next_level_keys)
            
            # Log warning if below threshold
            if key_ratio < min_key_threshold:
                LOG.warning(
                    "Item has low key coverage: %d/%d expected keys found (%.1f%% < %.1f%% threshold). Missing: %s",
                    keys_found,
                    len(next_level_keys),
                    key_ratio * 100,
                    min_key_threshold * 100,
                    missing_keys[:5] if len(missing_keys) > 5 else missing_keys,  # Show first 5 missing keys
                )
            elif keys_found < len(next_level_keys):
                # Some keys missing but above threshold - log at debug level
                LOG.debug(
                    "Item missing some expected keys: %d/%d found. Missing: %s",
                    keys_found,
                    len(next_level_keys),
                    missing_keys[:3] if len(missing_keys) > 3 else missing_keys,
                )
        
        # Item is structurally complete (and key check passed or was advisory)
        return True
        
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Error checking item completeness: %s", exc)
        return False


def extract_complete_array_items(
    span_bytes: bytes,
    next_level_keys: Set[str],
    start_offset: int = 0,
) -> Tuple[List[bytes], int]:
    """
    Extract complete array items from a byte span.
    
    This function reads from the current position in the array and extracts
    complete items one by one, stopping when it encounters an incomplete item.
    
    Args:
        span_bytes: Bytes containing part of an array
        next_level_keys: Set of key names that should appear in each item
        start_offset: Starting offset within span_bytes (for tracking position)
        
    Returns:
        Tuple of (list of complete item byte strings, end_offset)
        - end_offset: Position after the last complete item (relative to start_offset)
    """
    if not span_bytes:
        return [], 0
    
    complete_items = []
    decoder = json.JSONDecoder()
    span_text = span_bytes.decode("utf-8", errors="replace")
    current_pos = 0
    
    def skip_whitespace(pos: int) -> int:
        """Skip whitespace characters."""
        while pos < len(span_text) and span_text[pos].isspace():
            pos += 1
        return pos
    
    # Skip leading whitespace
    current_pos = skip_whitespace(current_pos)
    
    # If we're at the start of an array, skip the opening bracket
    if current_pos < len(span_text) and span_text[current_pos] == "[":
        current_pos += 1
        current_pos = skip_whitespace(current_pos)
    
    # Extract items one by one
    while current_pos < len(span_text):
        current_pos = skip_whitespace(current_pos)
        
        # Check if we've reached the end of the array
        if current_pos >= len(span_text):
            break
        if span_text[current_pos] == "]":
            # Reached end of array, but we might want to include this position
            # for tracking purposes
            break
        
        # Try to parse the next JSON value
        item_start = current_pos
        try:
            # Use raw_decode to get the value and how many characters it consumed
            value, consumed = decoder.raw_decode(span_text[current_pos:])
            
            # Extract the bytes for this item
            item_end = current_pos + consumed
            item_bytes = span_bytes[current_pos:item_end]
            
            # Check if this item is complete
            if check_item_completeness(item_bytes, next_level_keys):
                complete_items.append(item_bytes)
                current_pos = item_end
                
                # Skip whitespace after the item
                current_pos = skip_whitespace(current_pos)
                
                # Check for comma separator
                if current_pos < len(span_text) and span_text[current_pos] == ",":
                    current_pos += 1
                    current_pos = skip_whitespace(current_pos)
                elif current_pos < len(span_text) and span_text[current_pos] == "]":
                    # End of array, but item is complete
                    break
            else:
                # Item is incomplete, stop here
                LOG.debug("Incomplete item found at position %d, stopping extraction", current_pos)
                break
                
        except json.JSONDecodeError as e:
            # Can't parse this as JSON, might be incomplete
            LOG.debug("JSON decode error at position %d: %s, stopping extraction", current_pos, e)
            break
        except Exception as exc:  # noqa: BLE001
            LOG.debug("Error extracting item at position %d: %s", current_pos, exc)
            break
    
    # Return items and relative end offset (from start of span_bytes)
    return complete_items, current_pos


def process_single_array(
    array_key: str,
    array_start: int,
    next_level_keys: Set[str],
    input_path: Path,
    index_path: Path,
    output_dir: Path,
    stem: str,
    size_per_file_bytes: int,
    chunk_read_bytes: int,
    get_next_part_num: Callable[[], int],
    add_to_progress: Callable[[int], Tuple[int, float]],  # Returns (cumulative_size, progress_pct)
    original_file_size: int,
    log_chunk_progress: bool = False,  # If True, log detailed chunk-level progress
) -> int:
    """
    Process a single array in a thread-safe manner.
    
    This function processes one array independently, reading chunks, extracting items,
    and writing part files. It uses thread-safe functions for part numbering and progress tracking.
    
    Args:
        array_key: Name of the array key (e.g., 'in_network')
        array_start: Byte offset where the array starts in the file
        next_level_keys: Set of keys that should appear in array items
        input_path: Path to input JSON.gz file
        index_path: Path to .gzi index file
        output_dir: Directory to write output files
        stem: Base filename stem (without extension)
        size_per_file_bytes: Target size per part file in bytes
        chunk_read_bytes: Number of bytes to read per chunk
        get_next_part_num: Thread-safe function to get next part number
        add_to_progress: Thread-safe function to add to progress and get updated progress
        original_file_size: Original file size for progress calculation
        
    Returns:
        Number of part files created for this array
    """
    # Open own indexed_gzip handle (each thread needs its own)
    f = open_with_existing_index(str(input_path), str(index_path))
    files_created_for_array = 0
    
    try:
        LOG.info("Processing array '%s' starting at offset %d (size_per_file: %.1f MB)...", 
                array_key, array_start, size_per_file_bytes / (1024 * 1024))
        
        # Array end is not needed - we'll read until no more complete items are found
        array_end = None
        
        # Read array in chunks
        current_offset = array_start
        items_buffer = []  # List of (item_bytes, parsed_item) tuples
        buffer_cumulative_size = 0  # Cumulative size of items in buffer (bytes)
        
        iteration = 0
        while True:
            iteration += 1
            if log_chunk_progress:
                LOG.info("Iteration %d: Reading chunk from offset %d (chunk_size: %d) for array '%s'", 
                        iteration, current_offset, chunk_read_bytes, array_key)
            
            # Determine how much to read
            if array_end is not None:
                remaining = array_end - current_offset
                read_size = min(chunk_read_bytes, remaining)
            else:
                read_size = chunk_read_bytes
            
            if read_size <= 0:
                if log_chunk_progress:
                    LOG.info("Read size is 0 or negative, breaking loop for array '%s'", array_key)
                break
            
            # Read chunk
            span = read_span_between_offsets(
                f,
                current_offset,
                array_end if array_end is not None and (array_end - current_offset) <= chunk_read_bytes else None,
                extra=512,
                max_bytes=chunk_read_bytes,
            )
            
            if not span:
                if log_chunk_progress:
                    LOG.info("No span read (empty or None), breaking loop for array '%s'", array_key)
                break
            
            if log_chunk_progress:
                LOG.info("Read %d bytes from offset %d for array '%s'", len(span), current_offset, array_key)
            
            # Extract complete items from this chunk
            complete_items, relative_end = extract_complete_array_items(
                span,
                next_level_keys,
            )
            
            if log_chunk_progress:
                LOG.info("Extracted %d complete item(s) from chunk for array '%s' (relative_end: %d, total in buffer: %d)", 
                        len(complete_items), array_key, relative_end, len(items_buffer) + len(complete_items))
            
            if not complete_items:
                # No complete items found, might be at the end or incomplete
                span_text = span.decode("utf-8", errors="replace")
                if "]" in span_text:
                    # We've reached the end of the array
                    if log_chunk_progress:
                        LOG.info("Found closing bracket ']' in span, reached end of array '%s' (buffer has %d items)", 
                                array_key, len(items_buffer))
                    if items_buffer:
                        if log_chunk_progress:
                            LOG.info("Breaking loop but have %d items in buffer for array '%s', will write them at end", 
                                    len(items_buffer), array_key)
                    break
                
                # If there's more data available, continue reading
                if array_end is None or (array_end - current_offset) > len(span):
                    if log_chunk_progress:
                        LOG.info("No complete items found but more data available for array '%s', advancing offset by %d (buffer has %d items)", 
                                array_key, len(span) // 2, len(items_buffer))
                    current_offset += len(span) // 2
                    continue
                else:
                    if log_chunk_progress:
                        LOG.info("No complete items found and at end of data for array '%s', breaking loop (buffer has %d items)", 
                                array_key, len(items_buffer))
                    break
            
            # Add items to buffer and track cumulative size
            for item_bytes in complete_items:
                try:
                    item_text = item_bytes.decode("utf-8", errors="replace")
                    parsed_item = json.loads(item_text)
                    items_buffer.append((item_bytes, parsed_item))
                    buffer_cumulative_size += len(item_bytes)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Failed to parse item during extraction for array '%s': %s", array_key, exc)
                    items_buffer.append((item_bytes, None))
                    buffer_cumulative_size += len(item_bytes)
            
            if log_chunk_progress:
                LOG.info("After adding items: buffer now has %d items (%.2f MB / %.2f MB threshold) for array '%s'",
                        len(items_buffer), buffer_cumulative_size / (1024 * 1024), size_per_file_bytes / (1024 * 1024), array_key)
            
            # Update current offset
            current_offset = current_offset + relative_end
            
            # Write files when buffer cumulative size reaches threshold
            while buffer_cumulative_size >= size_per_file_bytes:
                # Find how many items we can write without exceeding threshold
                wrapper_overhead = 1024
                available_size = size_per_file_bytes - wrapper_overhead
                
                items_to_write_count = 0
                cumulative_size_for_file = 0
                
                for item_bytes, parsed_item in items_buffer:
                    item_size = len(item_bytes)
                    if cumulative_size_for_file + item_size > available_size:
                        break
                    cumulative_size_for_file += item_size
                    items_to_write_count += 1
                
                # Ensure we write at least one item
                if items_to_write_count == 0 and items_buffer:
                    items_to_write_count = 1
                    cumulative_size_for_file = len(items_buffer[0][0])
                
                if items_to_write_count == 0:
                    break
                
                LOG.info("Buffer reached threshold (%.2f MB), writing file with %d items for array '%s'...", 
                        cumulative_size_for_file / (1024 * 1024), items_to_write_count, array_key)
                
                # Get next part number (thread-safe)
                part_num = get_next_part_num()
                part_path = output_dir / f"{stem}_part{part_num:04d}.json.gz"
                
                # Use pre-parsed items if available
                items_to_write = []
                for item_bytes, parsed_item in items_buffer[:items_to_write_count]:
                    if parsed_item is not None:
                        items_to_write.append(convert_decimals(parsed_item))
                    else:
                        try:
                            item_text = item_bytes.decode("utf-8", errors="replace")
                            item = json.loads(item_text)
                            items_to_write.append(convert_decimals(item))
                        except Exception as exc:  # noqa: BLE001
                            LOG.warning("Failed to parse item for array '%s': %s", array_key, exc)
                            continue
                
                # Write array chunk
                output_data = {array_key: items_to_write}
                
                with gzip.open(part_path, "wt", encoding="utf-8") as f_out:
                    json.dump(output_data, f_out)
                    f_out.flush()  # Ensure data is written to disk
                    if hasattr(f_out, 'fileobj') and hasattr(f_out.fileobj, 'flush'):
                        f_out.fileobj.flush()  # Flush underlying file object
                
                files_created_for_array += 1
                # Read file size after ensuring it's flushed
                part_file_size = part_path.stat().st_size
                if part_file_size == 0:
                    LOG.warning("Part file %s has size 0, this may indicate a write issue", part_path.name)
                cumulative_part_size, progress_pct = add_to_progress(part_file_size)
                
                LOG.info(
                    "Created %s with %d items (%.2f MB uncompressed, %.2f MB compressed) from array '%s' | Progress: %.1f%% (%.2f GB / %.2f GB)",
                    part_path.name,
                    len(items_to_write),
                    cumulative_size_for_file / (1024 * 1024),
                    part_file_size / (1024 * 1024),
                    array_key,
                    progress_pct,
                    cumulative_part_size / (1024**3),
                    original_file_size / (1024**3),
                )
                
                # Remove written items from buffer
                written_items = items_buffer[:items_to_write_count]
                items_buffer = items_buffer[items_to_write_count:]
                for item_bytes, _ in written_items:
                    buffer_cumulative_size -= len(item_bytes)
            
            # Check if we've reached the end of the array
            if array_end is not None and current_offset >= array_end:
                break
        
        LOG.info("Finished reading array '%s' (buffer has %d items, %d files created so far)", 
                array_key, len(items_buffer), files_created_for_array)
        
        # Write remaining items in buffer
        if items_buffer:
            part_num = get_next_part_num()
            part_path = output_dir / f"{stem}_part{part_num:04d}.json.gz"
            
            items_to_write = []
            for item_bytes, parsed_item in items_buffer:
                if parsed_item is not None:
                    items_to_write.append(convert_decimals(parsed_item))
                else:
                    try:
                        item_text = item_bytes.decode("utf-8", errors="replace")
                        item = json.loads(item_text)
                        items_to_write.append(convert_decimals(item))
                    except Exception as exc:  # noqa: BLE001
                        LOG.warning("Failed to parse item for array '%s': %s", array_key, exc)
                        continue
            
            if items_to_write:
                output_data = {array_key: items_to_write}
                
                # Calculate uncompressed size before writing
                part_file_size_uncompressed = sum(len(item_bytes) for item_bytes, _ in items_buffer)
                
                with gzip.open(part_path, "wt", encoding="utf-8") as f_out:
                    json.dump(output_data, f_out)
                    f_out.flush()  # Ensure data is written to disk
                    if hasattr(f_out, 'fileobj') and hasattr(f_out.fileobj, 'flush'):
                        f_out.fileobj.flush()  # Flush underlying file object
                
                files_created_for_array += 1
                # Read file size after ensuring it's flushed
                part_file_size = part_path.stat().st_size
                if part_file_size == 0:
                    LOG.warning("Part file %s has size 0, this may indicate a write issue", part_path.name)
                cumulative_part_size, progress_pct = add_to_progress(part_file_size)
                
                LOG.info(
                    "Created %s with %d items (%.2f MB uncompressed, %.2f MB compressed) from array '%s' | Progress: %.1f%% (%.2f GB / %.2f GB)",
                    part_path.name,
                    len(items_to_write),
                    part_file_size_uncompressed / (1024 * 1024),
                    part_file_size / (1024 * 1024),
                    array_key,
                    progress_pct,
                    cumulative_part_size / (1024**3),
                    original_file_size / (1024**3),
                )
            else:
                LOG.warning("No valid items to write from buffer for array '%s'", array_key)
        else:
            LOG.warning("No items in buffer for array '%s' after reading loop", array_key)
        
        LOG.info("Finished processing array '%s' (created %d part file(s))", array_key, files_created_for_array)
        return files_created_for_array
        
    finally:
        f.close()


def split_json_gz_with_indexed_gzip(
    input_path: Path,
    output_dir: Path,
    connection_string: str,
    index_path: Optional[Path] = None,
    chunk_read_bytes: int = 16 * 1024 * 1024,  # 16MB default (increased for better performance)
    size_per_file_bytes: int = 100 * 1024 * 1024,  # 100 MB default
    file_name: Optional[str] = None,
    num_array_threads: int = 1,  # Number of threads for processing arrays in parallel (1 = sequential)
    log_chunk_progress: bool = False,  # If True, log detailed chunk-level progress (iteration, bytes read, items extracted)
) -> int:
    """
    Split a large JSON.gz file using indexed_gzip for efficient random access.
    
    This function:
    1. Gets scalar values from mrf_analysis → writes to part 000
    2. For each top-level array:
       - Finds array start using indexed_gzip
       - Reads chunk_read_bytes at a time
       - Extracts complete items (validated against mrf_analysis)
       - Writes part files when cumulative item size reaches size_per_file_bytes
       - Continues until array end
    
    Args:
        input_path: Path to input JSON.gz file
        output_dir: Directory to write output files
        connection_string: Database connection string
        index_path: Optional path to .gzi index file
        chunk_read_bytes: Number of bytes to read per chunk (x)
        items_per_file: Number of items to write per part file (y)
        file_name: Optional file name for database queries (defaults to input_path.name)
        
    Returns:
        Number of part files created (including part 000)
    """
    if not input_path.exists():
        LOG.error("Input file not found: %s", input_path)
        return 0
    
    if not input_path.name.endswith(".gz"):
        LOG.error("File must be .json.gz: %s", input_path)
        return 0
    
    if file_name is None:
        file_name = input_path.name
    
    # Strip any prefixes from file_name for database queries
    from src.shared.file_prefix import remove_prefix, PREFIX_INGESTED, PREFIX_ANALYZED
    
    db_file_name = file_name
    if file_name.startswith("_ingested_analyzed_"):
        temp_path = remove_prefix(Path(file_name), PREFIX_INGESTED)
        temp_path = remove_prefix(temp_path, PREFIX_ANALYZED)
        db_file_name = temp_path.name
    
    LOG.info("Splitting %s using indexed_gzip...", input_path.name)
    
    # Get original file size for progress tracking
    original_file_size = input_path.stat().st_size
    
    # Determine index path
    if index_path is None:
        # Look for index in same directory or in a configured index directory
        index_path = input_path.with_suffix(input_path.suffix + ".gzi")
    
    if not index_path.exists():
        LOG.error("Index file not found: %s (required for indexed_gzip splitting)", index_path)
        return 0
    
    # Create single database connection for all queries (optimization #1)
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Step 1: Get scalars from mrf_analysis and write part 000
        LOG.info("Step 1: Getting scalar values from mrf_analysis...")
        scalars = get_scalars_from_mrf_analysis(cursor, db_file_name)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename pattern
        stem = input_path.stem
        if stem.endswith(".json"):
            stem = stem[:-5]
        
        part_num = 0
        files_created = 0
        
        # Write part 000 with scalars
        if scalars:
            part_000_path = output_dir / f"{stem}_part{part_num:04d}.json.gz"
            LOG.info("Writing part 000 with %d scalar value(s)...", len(scalars))
            
            scalars_converted = convert_decimals(scalars)
            with gzip.open(part_000_path, "wt", encoding="utf-8") as f:
                json.dump(scalars_converted, f)
            
            # Skip validation for part 000 (optimization #7) - assume write succeeded
            files_created += 1
            part_num += 1
            LOG.info("Created %s", part_000_path.name)
        
        # Step 2: Get top-level arrays from mrf_analysis
        LOG.info("Step 2: Getting top-level arrays from mrf_analysis...")
        array_keys = get_top_level_arrays_from_mrf_analysis(cursor, db_file_name)
        
        if not array_keys:
            LOG.info("No arrays found, only created part 000")
            return files_created
    
        # Step 3: Get array start offsets from mrf_landing (no need to scan file!)
        LOG.info("Step 3: Getting array start offsets from mrf_landing...")
        array_start_offsets = get_array_start_offsets_from_mrf_landing(cursor, db_file_name)
        LOG.info("Step 3: Retrieved %d array start offset(s)", len(array_start_offsets))
        
        if not array_start_offsets:
            LOG.warning("No array start offsets found in mrf_landing. This may indicate arrays were not ingested with indexed_gzip.")
            LOG.info("Falling back to scanning file to find array start positions...")
            # Fallback: scan file if offsets not available
            f = open_with_existing_index(str(input_path), str(index_path))
            try:
                unique_hits = scan_unique_keys_with_offsets(
                    gz_path=str(input_path),
                    index_path=str(index_path),
                    progress_every_chunks=2,
                )
                # Convert to offsets dict (we'll still need to find array start positions)
                array_start_offsets = {}
                for array_key in array_keys:
                    if array_key in unique_hits:
                        # Find array start position
                        array_key_offset = unique_hits[array_key].abs_offset
                        key_span = read_span_between_offsets(
                            f,
                            array_key_offset,
                            array_key_offset + 1024,
                            extra=0,
                            max_bytes=1024,
                        )
                        if key_span:
                            key_text = key_span.decode("utf-8", errors="replace")
                            key_pattern = f'"{array_key}"'
                            key_pos = key_text.find(key_pattern)
                            if key_pos >= 0:
                                colon_pos = key_text.find(":", key_pos + len(key_pattern))
                                if colon_pos >= 0:
                                    bracket_pos = key_text.find("[", colon_pos)
                                    if bracket_pos >= 0:
                                        array_start_offsets[array_key] = array_key_offset + bracket_pos + 1
            finally:
                f.close()
        
        # Step 4: Prepare for array processing (no need to open file here - each thread will open its own)
        LOG.info("Step 4: Preparing to process %d array(s)...", len(array_keys))
        
        # Thread-safe part number counter (starts at 1, after part 000)
        part_num_lock = threading.Lock()
        part_num_counter = 1
        
        def get_next_part_num() -> int:
            """Thread-safe function to get next part number."""
            nonlocal part_num_counter
            with part_num_lock:
                num = part_num_counter
                part_num_counter += 1
                return num
        
        # Thread-safe progress tracking
        cumulative_part_size = 0  # Track cumulative size of part files for progress (across all arrays)
        progress_lock = threading.Lock()
        
        def add_to_progress(part_file_size: int) -> Tuple[int, float]:
            """Thread-safe function to add to progress and return updated values."""
            nonlocal cumulative_part_size
            with progress_lock:
                old_cumulative = cumulative_part_size
                cumulative_part_size += part_file_size
                progress_pct = (cumulative_part_size / original_file_size) * 100 if original_file_size > 0 else 0
                # Debug logging to verify cumulative tracking
                LOG.debug("Progress update: part_size=%.2f MB, old_cumulative=%.2f GB, new_cumulative=%.2f GB, progress=%.1f%%",
                         part_file_size / (1024 * 1024), old_cumulative / (1024**3), cumulative_part_size / (1024**3), progress_pct)
                return cumulative_part_size, progress_pct
        
        # Cache for next-level keys (optimization #4) - keyed by (file_name, array_key)
        next_level_keys_cache: Dict[Tuple[str, str], Set[str]] = {}
        
        # Prepare array processing tasks
        array_tasks = []
        for array_key in array_keys:
            if array_key not in array_start_offsets:
                LOG.warning("Array key '%s' not found in array_start_offsets, skipping", array_key)
                continue
            
            # Get next-level keys for completeness checking (with caching)
            cache_key = (db_file_name, array_key)
            if cache_key not in next_level_keys_cache:
                LOG.info("Getting next-level keys for array '%s'...", array_key)
                next_level_keys_cache[cache_key] = get_array_next_level_keys(cursor, db_file_name, array_key)
            next_level_keys = next_level_keys_cache[cache_key]
            LOG.info("Found %d next-level key(s) for array '%s': %s", len(next_level_keys), array_key, sorted(next_level_keys) if next_level_keys else "none")
            
            # Get array start from stored offset
            array_start = array_start_offsets[array_key]
            
            # Add task to list
            array_tasks.append((array_key, array_start, next_level_keys))
        
        # Step 5: Process arrays (sequentially or in parallel based on num_array_threads)
        LOG.info("Step 5: Processing %d array(s) with %d thread(s)...", len(array_tasks), num_array_threads)
        
        if num_array_threads == 1 or len(array_tasks) == 1:
            # Sequential processing (backward compatible)
            for array_key, array_start, next_level_keys in array_tasks:
                files_created += process_single_array(
                    array_key=array_key,
                    array_start=array_start,
                    next_level_keys=next_level_keys,
                    input_path=input_path,
                    index_path=index_path,
                    output_dir=output_dir,
                    stem=stem,
                    size_per_file_bytes=size_per_file_bytes,
                    chunk_read_bytes=chunk_read_bytes,
                    get_next_part_num=get_next_part_num,
                    add_to_progress=add_to_progress,
                    original_file_size=original_file_size,
                    log_chunk_progress=log_chunk_progress,
                )
        else:
            # Parallel processing with ThreadPoolExecutor
            max_workers = min(num_array_threads, len(array_tasks))
            LOG.info("Using ThreadPoolExecutor with %d worker(s) to process %d array(s) in parallel", max_workers, len(array_tasks))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_array = {
                    executor.submit(
                        process_single_array,
                        array_key=array_key,
                        array_start=array_start,
                        next_level_keys=next_level_keys,
                        input_path=input_path,
                        index_path=index_path,
                        output_dir=output_dir,
                        stem=stem,
                        size_per_file_bytes=size_per_file_bytes,
                        chunk_read_bytes=chunk_read_bytes,
                        get_next_part_num=get_next_part_num,
                        add_to_progress=add_to_progress,
                        original_file_size=original_file_size,
                        log_chunk_progress=log_chunk_progress,
                    ): array_key
                    for array_key, array_start, next_level_keys in array_tasks
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_array):
                    array_key = future_to_array[future]
                    try:
                        array_files_created = future.result()
                        files_created += array_files_created
                        LOG.info("Array '%s' completed: created %d part file(s)", array_key, array_files_created)
                    except Exception as exc:  # noqa: BLE001
                        LOG.error("Array '%s' failed with exception: %s", array_key, exc, exc_info=True)
    finally:
        # Close database connection (optimization #1)
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    LOG.info("Split complete: created %d part file(s)", files_created)
    return files_created
