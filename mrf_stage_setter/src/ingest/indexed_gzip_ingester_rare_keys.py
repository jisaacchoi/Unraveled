"""Indexed gzip ingestion logic with rare key detection and extraction."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

# Try to import orjson for faster JSON serialization
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

import indexed_gzip
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from tqdm import tqdm

from src.shared.database import ensure_table_exists
from src.shared.json_reader import is_json_file

LOG = logging.getLogger("src.indexed_gzip_ingester_rare_keys")

# Regex pattern to match JSON object keys: "key":
KEY_PATTERN = re.compile(rb'"((?:\\.|[^"\\]){1,512})"\s*:')


def _configure_csv_field_size_limit() -> int:
    """Raise CSV parser field limit to the largest value supported on this platform."""
    limit = sys.maxsize
    while limit > 131072:
        try:
            csv.field_size_limit(limit)
            return limit
        except OverflowError:
            limit //= 10
    csv.field_size_limit(131072)
    return 131072


CSV_FIELD_SIZE_LIMIT = _configure_csv_field_size_limit()
LOG.info("Configured csv.field_size_limit=%d", CSV_FIELD_SIZE_LIMIT)


@dataclass
class KeyHit:
    """Represents a unique key occurrence with its position."""
    key: str
    abs_offset: int  # decompressed absolute byte offset
    chunk_index: int
    offset_in_chunk: int


@dataclass
class ScanResult:
    """Result of scanning for keys with occurrence tracking."""
    unique_hits: Dict[str, KeyHit]  # Top-level keys that appear once
    key_occurrences: Dict[str, Tuple[int, int]]  # Count and first occurrence: {key: (count, first_offset)}
    array_starts: Dict[str, int]  # Array start offsets: {array_key: start_offset}
    forced_key_offsets: Dict[str, Tuple[int, int]]  # Forced keys: {key: (first_offset, last_offset)}
    forced_key_array_starts: Dict[str, Tuple[Optional[int], Optional[int]]]  # Forced keys: {key: (first_array_start, last_array_start)}


class RareKeyExtractionExhaustedError(RuntimeError):
    """Raised when rare-key item extraction fails after all span-size attempts."""


def _decode_json_string_bytes(b: bytes) -> str:
    """
    Decode JSON string bytes from regex capture group.
    The regex already captured the key content, so we just need UTF-8 decode.
    Most keys don't have escape sequences, so we optimize for the common case.
    """
    # Fast path: decode UTF-8 directly (works for 99%+ of keys)
    try:
        decoded = b.decode("utf-8", errors="replace")
        # If no backslash, no escaping needed - return immediately
        if b'\\' not in b:
            return decoded
    except Exception:
        # Fallback: if decode fails, return as-is (shouldn't happen)
        return b.decode("utf-8", errors="replace")
    
    # Slow path: handle escape sequences (rare case)
    # Use JSON parsing only when escapes are present
    try:
        return json.loads('"' + decoded.replace('"', '\\"') + '"')
    except Exception:
        # If JSON parsing fails, return decoded string (better than nothing)
        return decoded


def open_indexed_gzip(gz_path: str) -> indexed_gzip.IndexedGzipFile:
    """Open an indexed gzip file."""
    return indexed_gzip.IndexedGzipFile(gz_path)


def build_and_export_index(gz_path: str, index_path: str) -> str:
    """
    Build a full index and export it to a file.
    
    Args:
        gz_path: Path to .json.gz file
        index_path: Path where to save the index file
        
    Returns:
        Path to the saved index file
    """
    LOG.info("Building indexed_gzip index for %s...", Path(gz_path).name)
    f = open_indexed_gzip(gz_path)
    try:
        f.build_full_index()
        index_path_str = str(index_path)
        f.export_index(index_path_str)
        LOG.info("Index saved to %s", index_path_str)
        return index_path_str
    finally:
        f.close()


def open_with_existing_index(gz_path: str, index_path: str) -> indexed_gzip.IndexedGzipFile:
    """Open an indexed gzip using a pre-built exported index."""
    f = indexed_gzip.IndexedGzipFile(gz_path)
    f.import_index(index_path)
    return f


def scan_keys_with_occurrences(
    gz_path: str,
    index_path: Optional[str] = None,
    chunk_size: int = 8 * 1024 * 1024,  # 8MB default
    overlap_bytes: int = 2048,  # 2KB default
    progress_every_chunks: int = 5,
    forced_top_level_arrays: Optional[List[str]] = None,
) -> ScanResult:
    """
    Scan for all JSON keys and track all occurrences (not just unique ones).
    
    Also detects arrays and tracks their start offsets.
    
    Args:
        gz_path: Path to .json.gz file
        index_path: Optional path to pre-built index file
        chunk_size: Size of chunks to read (default: 8MB)
        overlap_bytes: Overlap between chunks (default: 2KB)
        progress_every_chunks: Log progress every N chunks
        
    Returns:
        ScanResult with unique_hits, key_occurrences, and array_starts
    """
    total_compressed = os.path.getsize(gz_path)
    start_time = time.time()
    
    if index_path and os.path.exists(index_path):
        LOG.info("Using existing index: %s", index_path)
        f = open_with_existing_index(gz_path, index_path)
    else:
        LOG.info("Building index on-the-fly (no pre-built index found)")
        f = open_indexed_gzip(gz_path)
    
    # Get uncompressed size by seeking to the end of the file
    # This works for files of any size (no 4GB limitation like gzip footer)
    uncompressed_size = None
    try:
        # Save current position
        current_pos = f.tell()
        # Seek to end
        f.seek(0, 2)  # SEEK_END
        uncompressed_size = f.tell()
        # Restore position
        f.seek(current_pos)
        LOG.debug("Uncompressed size from IndexedGzipFile: %d bytes (%.2f MB)", 
                 uncompressed_size, uncompressed_size / (1024 * 1024))
    except Exception as e:
        LOG.warning("Could not get uncompressed size from IndexedGzipFile: %s. Using compressed size estimate.", e)
        uncompressed_size = None
    
    forced_keys = {k for k in (forced_top_level_arrays or []) if isinstance(k, str) and k}

    seen_once: Dict[str, int] = {}
    seen_multiple: set[str] = set()
    key_occurrences: Dict[str, Tuple[int, int]] = {}  # Track count and first occurrence: {key: (count, first_offset)}
    array_starts: Dict[str, int] = {}  # Track array start offsets
    forced_key_offsets: Dict[str, Tuple[int, int]] = {}
    forced_key_array_starts: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
    
    tail = b""
    abs_read = 0
    chunk_index = 0
    
    try:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            
            chunk_start_abs = abs_read
            abs_read += len(chunk)
            
            combined = tail + chunk
            tail_len = len(tail)
            
            # Track keys processed in this chunk for diagnostics
            keys_in_chunk = 0
            
            for m in KEY_PATTERN.finditer(combined):
                if m.end() <= tail_len:
                    continue
                
                key = _decode_json_string_bytes(m.group(1))
                abs_offset = chunk_start_abs - tail_len + m.start()
                if abs_offset < 0:
                    abs_offset = m.start()
                
                keys_in_chunk += 1
                
                # Track count and first occurrence only (not all occurrences)
                # Use dict.get() to avoid double lookup
                existing = key_occurrences.get(key)
                if existing is None:
                    key_occurrences[key] = (1, abs_offset)  # (count, first_offset)
                else:
                    count, first_offset = existing
                    key_occurrences[key] = (count + 1, first_offset)  # Increment count, keep first_offset

                # Detect if this key's value starts with an array marker.
                key_end = m.end()
                value_start = key_end
                while value_start < len(combined) and combined[value_start] in b" \t\r\n:":
                    value_start += 1
                current_array_start_offset: Optional[int] = None
                if value_start < len(combined) and combined[value_start] == ord(b'['):
                    current_array_start_offset = abs_offset + (value_start - m.start())

                # Track first/last offsets for forced keys even when they are non-unique.
                if key in forced_keys:
                    forced_existing = forced_key_offsets.get(key)
                    if forced_existing is None:
                        forced_key_offsets[key] = (abs_offset, abs_offset)
                    else:
                        forced_key_offsets[key] = (forced_existing[0], abs_offset)

                    arr_existing = forced_key_array_starts.get(key)
                    if arr_existing is None:
                        forced_key_array_starts[key] = (current_array_start_offset, current_array_start_offset)
                    else:
                        first_arr_start, last_arr_start = arr_existing
                        if first_arr_start is None and current_array_start_offset is not None:
                            first_arr_start = current_array_start_offset
                        if current_array_start_offset is not None:
                            last_arr_start = current_array_start_offset
                        forced_key_array_starts[key] = (first_arr_start, last_arr_start)
                
                # Track unique vs multiple (for top-level detection)
                # Check seen_multiple first to short-circuit early (most keys will be here)
                if key in seen_multiple:
                    # Already know it's not unique, skip array detection
                    continue
                
                # Check if this is the second occurrence (move from seen_once to seen_multiple)
                if key in seen_once:
                    del seen_once[key]
                    seen_multiple.add(key)
                    # Skip array detection for non-unique keys
                    continue
                
                # First occurrence - add to seen_once
                seen_once[key] = abs_offset
                
                # Track array starts discovered for unique keys.
                if current_array_start_offset is not None:
                    array_starts[key] = current_array_start_offset
            
            tail = combined[-overlap_bytes:] if overlap_bytes > 0 else b""
            chunk_index += 1
            
            if progress_every_chunks and (chunk_index % progress_every_chunks == 0):
                elapsed = time.time() - start_time
                msg = f"Scanned {chunk_index:,} chunks | "
                
                # Diagnostic info: dictionary sizes and keys per chunk
                num_unique_keys = len(key_occurrences)
                num_seen_once = len(seen_once)
                num_seen_multiple = len(seen_multiple)
                msg += f"Keys: {keys_in_chunk:,} in chunk | Unique: {num_unique_keys:,} | "
                
                # Calculate progress using uncompressed size from IndexedGzipFile
                if uncompressed_size is not None and uncompressed_size > 0:
                    # abs_read is the decompressed bytes read so far
                    pct = 100.0 * abs_read / uncompressed_size
                    decompressed_mb = abs_read / (1024 * 1024)
                    total_decompressed_mb = uncompressed_size / (1024 * 1024)
                    msg += f"{pct:6.2f}% ({decompressed_mb:,.1f}/{total_decompressed_mb:,.1f} MB decompressed) | "
                else:
                    # Fallback to compressed size estimation if we couldn't read uncompressed size
                    compressed_pos = None
                    try:
                        compressed_pos = f.fileobj().tell()
                    except Exception:
                        try:
                            if hasattr(f, '_fileobj') and hasattr(f._fileobj, 'tell'):
                                compressed_pos = f._fileobj.tell()
                        except Exception:
                            pass
                    
                    if compressed_pos is not None:
                        pct = 100.0 * compressed_pos / total_compressed
                        compressed_mb = compressed_pos / (1024 * 1024)
                        total_compressed_mb = total_compressed / (1024 * 1024)
                        msg += f"{pct:6.2f}% ({compressed_mb:,.1f}/{total_compressed_mb:,.1f} MB compressed) | "
                    else:
                        estimated_compressed = abs_read / 5
                        estimated_pct = 100.0 * estimated_compressed / total_compressed if total_compressed > 0 else 0
                        msg += f"~{estimated_pct:6.2f}% compressed (estimated) | "
                
                msg += f"{elapsed:,.1f}s elapsed"
                LOG.info(msg)
        
        unique_hits = {
            k: KeyHit(
                key=k,
                abs_offset=o,
                chunk_index=o // chunk_size,
                offset_in_chunk=o % chunk_size,
            )
            for k, o in seen_once.items()
        }

        # Force configured keys as top-level targets using their first offset,
        # even if they are not unique in the full document.
        for forced_key, (first_offset, last_offset) in forced_key_offsets.items():
            if forced_key not in unique_hits:
                unique_hits[forced_key] = KeyHit(
                    key=forced_key,
                    abs_offset=first_offset,
                    chunk_index=first_offset // chunk_size,
                    offset_in_chunk=first_offset % chunk_size,
                )
            forced_first_arr_start, _ = forced_key_array_starts.get(forced_key, (None, None))
            if forced_first_arr_start is not None:
                array_starts[forced_key] = forced_first_arr_start
            LOG.info(
                "Forced key tracked: '%s' first_offset=%d, last_offset=%d",
                forced_key,
                first_offset,
                last_offset,
            )
        
        LOG.info("Found %d unique (top-level) key(s)", len(unique_hits))
        total_occurrences = sum(count for count, _ in key_occurrences.values())
        LOG.info("Tracked %d total key occurrences across all keys", total_occurrences)
        LOG.info("Found %d array(s)", len(array_starts))
        
        return ScanResult(
            unique_hits=unique_hits,
            key_occurrences=key_occurrences,
            array_starts=array_starts,
            forced_key_offsets=forced_key_offsets,
            forced_key_array_starts=forced_key_array_starts,
        )
    
    finally:
        f.close()


def find_array_for_key_offset(key_offset: int, array_starts: Dict[str, int]) -> Optional[str]:
    """
    Find which array contains a key at the given offset.
    
    Args:
        key_offset: Byte offset where key was found
        array_starts: Dict of {array_key: start_offset}
        
    Returns:
        Array key name, or None if not in any array
    """
    best_array = None
    best_offset = -1
    
    for array_key, array_start in array_starts.items():
        if array_start < key_offset:
            if array_start > best_offset:
                best_offset = array_start
                best_array = array_key
    
    return best_array


def estimate_array_size(array_key: str, array_start_offset: int, f: indexed_gzip.IndexedGzipFile, sample_size: int = 1000) -> int:
    """
    Estimate array size by sampling items.
    
    Args:
        array_key: Name of the array
        array_start_offset: Byte offset where array starts
        f: IndexedGzipFile (already opened)
        sample_size: Number of items to sample
        
    Returns:
        Estimated array size
    """
    try:
        f.seek(array_start_offset + 1)  # Skip opening '['
        decoder = json.JSONDecoder()
        item_count = 0
        bytes_read = 0
        max_sample_bytes = 10 * 1024 * 1024  # Sample up to 10MB
        
        chunk = f.read(max_sample_bytes)
        pos = 0
        
        while pos < len(chunk) and item_count < sample_size:
            # Skip whitespace
            while pos < len(chunk) and chunk[pos] in b" \t\r\n":
                pos += 1
            
            if pos >= len(chunk) or chunk[pos] == ord(b']'):
                break
            
            if chunk[pos] == ord(b','):
                pos += 1
                continue
            
            # Try to parse one item
            try:
                chunk_text = chunk[pos:].decode('utf-8', errors='replace')
                _, consumed = decoder.raw_decode(chunk_text)
                item_count += 1
                pos += consumed
            except Exception:
                break
        
        if item_count == 0:
            return 1  # At least 1 item
        
        # Estimate: if we sampled N items in X bytes, estimate total
        # This is rough, but good enough for frequency calculation
        bytes_per_item = bytes_read / item_count if item_count > 0 else 1000
        
        # Estimate total array size (very rough)
        # We'll use a conservative estimate
        estimated_size = max(item_count * 10, 100)  # At least 100, or 10x sampled
        
        return estimated_size
    
    except Exception:
        LOG.warning("Failed to estimate array size for %s, using default 1000", array_key)
        return 1000


def identify_rare_keys_by_array(
    key_occurrences: Dict[str, Tuple[int, int]],
    array_starts: Dict[str, int],
    f: indexed_gzip.IndexedGzipFile,
    num_rare_keys: int = 10,
    unique_hits: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, List[int]]]:
    """
    Identify rare keys grouped by which array they belong to.
    Uses comparative approach: selects the N keys with lowest occurrence counts.
    Only considers top-level arrays (arrays that are values of top-level keys).
    
    Args:
        key_occurrences: Count and first occurrence: {key: (count, first_offset)}
        array_starts: Array start offsets: {array_key: start_offset}
        f: IndexedGzipFile (not used, kept for compatibility)
        num_rare_keys: Number of least common keys to mark as rare per array (default: 10)
        unique_hits: Top-level keys to identify which arrays are top-level (optional)
        
    Returns:
        Dict of {array_key: {rare_key: [offsets]}}
    """
    rare_keys_by_array: Dict[str, Dict[str, List[int]]] = {}
    
    # Identify top-level arrays (arrays that are values of top-level keys)
    top_level_array_names = set()
    top_level_key_names = set(unique_hits.keys()) if unique_hits else set()
    if unique_hits:
        for top_level_key in unique_hits.keys():
            if top_level_key in array_starts:
                top_level_array_names.add(top_level_key)
    
    # Pre-sort top-level arrays by offset for efficient lookup
    top_level_arrays_sorted = sorted(
        [(name, array_starts[name]) for name in top_level_array_names],
        key=lambda x: x[1],
        reverse=True  # Descending order for efficient lookup
    )
    
    # Group keys by top-level array using only first occurrence and count
    # We don't need all offsets - just count and first occurrence location
    occurrences_by_array: Dict[str, Dict[str, tuple[int, int]]] = {}  # {array: {key: (count, first_offset)}}
    
    for key, (count, first_offset) in key_occurrences.items():
        # Top-level keys are section anchors, not rare nested keys.
        if key in top_level_key_names:
            LOG.debug("Skipping top-level key '%s' from rare-key candidates", key)
            continue

        # Use only the first occurrence to determine which array this key belongs to
        
        # Find the top-level array that contains this first occurrence
        top_level_array = None
        for tl_name, tl_start in top_level_arrays_sorted:
            if tl_start < first_offset:
                top_level_array = tl_name
                break  # Found the best match (arrays are sorted descending)
        
        if top_level_array:
            if top_level_array not in occurrences_by_array:
                occurrences_by_array[top_level_array] = {}
            # Store count and first offset (we don't need all offsets)
            occurrences_by_array[top_level_array][key] = (count, first_offset)
    
    # Log grouping summary for debugging
    for array_key, keys_in_array in occurrences_by_array.items():
        total_keys = len(keys_in_array)
        total_occurrences = sum(count for count, _ in keys_in_array.values())
        LOG.debug("Array '%s': %d unique keys, %d total occurrences", array_key, total_keys, total_occurrences)
    
    # Identify rare keys in each array using comparative approach
    for array_key, keys_in_array in occurrences_by_array.items():
        if array_key not in array_starts:
            continue
        
        # Sort keys by occurrence count (ascending - lowest counts first)
        # keys_in_array is {key: (count, first_offset)}
        sorted_keys = sorted(
            keys_in_array.items(),
            key=lambda x: x[1][0]  # Sort by count (first element of tuple)
        )
        
        # Log all keys with their counts for debugging
        LOG.debug("Array '%s': Key counts (sorted ascending):", array_key)
        for idx, (key, (count, _)) in enumerate(sorted_keys):
            LOG.debug("  [%d] '%s': %d occurrences", idx + 1, key, count)
        
        # Select the N keys with lowest counts (comparative approach)
        num_keys_to_consider = min(num_rare_keys, len(sorted_keys))
        candidate_keys = sorted_keys[:num_keys_to_consider]
        
        # Filter to only include keys with count < 3 (absolute constraint)
        rare_keys_for_array = []
        for key, (count, first_offset) in candidate_keys:
            if count < 3:
                rare_keys_for_array.append((key, count, first_offset))
            else:
                LOG.debug("Key '%s' in array '%s' excluded from rare keys: %d occurrences (>= 3, not truly rare)",
                         key, array_key, count)
        
        rare_keys_by_array[array_key] = {}
        for key, count, first_offset in rare_keys_for_array:
            # Store only the first occurrence offset (we only need one location per rare key)
            rare_keys_by_array[array_key][key] = [first_offset]  # Store as list for compatibility
            rank = next(i for i, (k, _) in enumerate(sorted_keys, 1) if k == key)
            LOG.info("Rare key '%s' in array '%s': %d total occurrences, using first at offset %d (ranked %d/%d by count)",
                     key, array_key, count, first_offset, rank, len(sorted_keys))
    
    total_rare_keys = sum(len(keys) for keys in rare_keys_by_array.values())
    LOG.info("Identified %d rare key(s) across %d array(s) (comparative approach: %d least common keys per array, filtered to count < 3)",
             total_rare_keys, len(rare_keys_by_array), num_rare_keys)
    
    return rare_keys_by_array


def log_key_statistics(
    key_occurrences: Dict[str, Tuple[int, int]],
    array_starts: Dict[str, int],
    rare_keys_by_array: Dict[str, Dict[str, List[int]]],
    num_rare_keys: int,
    unique_hits: Dict[str, Any],  # Top-level keys to identify which arrays are top-level
) -> None:
    """
    Log statistics for top-level sections only: count of key occurrences per top-level array.
    Only shows top-level arrays (not nested arrays).
    
    Args:
        key_occurrences: Count and first occurrence: {key: (count, first_offset)}
        array_starts: Array start offsets: {array_key: start_offset}
        rare_keys_by_array: Dict of {array_key: {rare_key: [offsets]}}
        threshold: Frequency threshold used for rare key detection
        unique_hits: Top-level keys (to identify which arrays are top-level)
    """
    LOG.info("=" * 80)
    LOG.info("KEY STATISTICS SUMMARY (Top-Level Sections Only)")
    LOG.info("=" * 80)
    
    # Identify top-level arrays (arrays that are values of top-level keys)
    top_level_array_names = set()
    for top_level_key in unique_hits.keys():
        if top_level_key in array_starts:
            top_level_array_names.add(top_level_key)
    
    # Pre-sort top-level arrays by offset for efficient lookup
    top_level_arrays_sorted = sorted(
        [(name, array_starts[name]) for name in top_level_array_names],
        key=lambda x: x[1],
        reverse=True  # Descending order for efficient lookup
    )
    
    # Group keys by top-level array only (not nested arrays)
    # Only store first occurrence and count for logging - use first occurrence to determine array
    top_level_keys: Dict[Optional[str], Dict[str, Tuple[int, int]]] = {}  # {array_key: {key: (first_offset, total_count)}}
    
    for key, (total_count, first_offset) in key_occurrences.items():
        # Use only the first occurrence to determine which top-level array this key belongs to
        
        # Find the top-level array that contains this first occurrence
        top_level_array = None
        for tl_name, tl_start in top_level_arrays_sorted:
            if tl_start < first_offset:
                top_level_array = tl_name
                break  # Found the best match (arrays are sorted descending)
        
        # Store first occurrence and count for this top-level array
        if top_level_array not in top_level_keys:
            top_level_keys[top_level_array] = {}
        # Store (first_offset, total_count)
        top_level_keys[top_level_array][key] = (first_offset, total_count)
    
    # Log statistics for each top-level section
    # Sort with None values first (non-array keys), then array keys alphabetically
    sorted_items = sorted(
        top_level_keys.items(),
        key=lambda x: (x[0] is not None, x[0] if x[0] is not None else "")
    )
    
    for top_level_array, keys_in_section in sorted_items:
        if top_level_array is None:
            LOG.info("")
            LOG.info("Non-array keys (top-level scalars/objects):")
        else:
            LOG.info("")
            LOG.info("Top-level array: '%s' (start offset: %d)", top_level_array, array_starts.get(top_level_array, 0))
        
        # Sort keys by occurrence count (descending)
        sorted_keys = sorted(keys_in_section.items(), key=lambda x: x[1][1], reverse=True)
        
        for key, (first_offset, count) in sorted_keys:
            is_rare = False
            
            # Check if this key is rare in this top-level array
            if top_level_array and top_level_array in rare_keys_by_array:
                is_rare = key in rare_keys_by_array[top_level_array]
            
            status = "RARE" if is_rare else "common"
            LOG.info("  Key: '%s' | Count: %d | Status: %s | First location: %d", 
                    key, count, status, first_offset)
    
    # Summary statistics - only count top-level arrays
    all_unique_keys = set()
    rare_key_names = set()
    
    for top_level_array, keys_in_section in top_level_keys.items():
        for key in keys_in_section.keys():
            all_unique_keys.add(key)
            # Check if this key is rare in this top-level array
            if top_level_array and top_level_array in rare_keys_by_array:
                if key in rare_keys_by_array[top_level_array]:
                    rare_key_names.add(key)
    
    total_keys = len(all_unique_keys)
    total_rare_keys = len(rare_key_names)
    total_common_keys = total_keys - total_rare_keys
    
    # Count total rare key occurrences across top-level arrays only
    # rare_keys_by_array stores {array_key: {rare_key: [first_offset]}}, so each has 1 occurrence
    total_rare_occurrences = sum(
        len(keys) 
        for array_key, keys in rare_keys_by_array.items() 
        if array_key in top_level_array_names
    )
    
    LOG.info("")
    LOG.info("SUMMARY:")
    LOG.info("  Total unique keys found: %d", total_keys)
    LOG.info("  Unique keys that are rare in at least one top-level array: %d", total_rare_keys)
    LOG.info("  Unique keys that are common in all top-level arrays: %d", total_common_keys)
    LOG.info("  Total rare key occurrences in top-level arrays: %d", total_rare_occurrences)
    LOG.info("  Top-level arrays found: %d", len(top_level_array_names))
    LOG.info("  Rare key selection: %d least common keys per array, filtered to count < 3 (comparative + absolute constraint)", num_rare_keys)
    LOG.info("=" * 80)


def count_keys_in_item(item: Any) -> int:
    """
    Count unique keys in a parsed JSON item using regex.
    
    Args:
        item: Parsed JSON item (dict, list, or scalar)
        
    Returns:
        Number of unique keys found
    """
    if not isinstance(item, dict):
        return 0
    
    # Convert item to JSON string
    item_json_str = json.dumps(item)
    item_json_bytes = item_json_str.encode('utf-8')
    
    # Use KEY_PATTERN to find all keys
    keys_found = set()
    for match in KEY_PATTERN.finditer(item_json_bytes):
        key = _decode_json_string_bytes(match.group(1))
        keys_found.add(key)
    
    return len(keys_found)


def find_item_start_in_span(span_bytes: bytes, key_pos: int) -> int:
    """
    Find the opening '{' of the item containing the key, searching backwards.
    Tracks brace depth to find the item-level brace.
    
    Args:
        span_bytes: Bytes containing the item
        key_pos: Position of key within span_bytes
        
    Returns:
        Position of opening '{', or -1 if not found
    """
    depth = 0
    in_string = False
    escape_next = False
    
    # Search backwards from key position
    for i in range(key_pos, -1, -1):
        if escape_next:
            escape_next = False
            continue
        
        byte = span_bytes[i]
        
        if byte == ord('\\'):
            escape_next = True
            continue
        
        if byte == ord('"') and not escape_next:
            in_string = not in_string
            continue
        
        if in_string:
            continue
        
        if byte == ord('}'):
            depth += 1
        elif byte == ord('{'):
            if depth == 0:
                return i  # Found item-level opening brace
            depth -= 1
    
    # If we didn't find it, the item might start before the span
    # Try searching from the beginning of the span instead
    depth = 0
    in_string = False
    escape_next = False
    
    # Find the first '{' before the key that has matching depth
    for i in range(key_pos - 1, -1, -1):
        if escape_next:
            escape_next = False
            continue
        
        byte = span_bytes[i]
        
        if byte == ord('\\'):
            escape_next = True
            continue
        
        if byte == ord('"') and not escape_next:
            in_string = not in_string
            continue
        
        if in_string:
            continue
        
        if byte == ord('}'):
            depth += 1
        elif byte == ord('{'):
            if depth == 0:
                return i
            depth -= 1
    
    return -1


def find_item_end_in_span(span_bytes: bytes, key_pos: int) -> int:
    """
    Find the closing '}' of the item containing the key, searching forwards.
    Tracks brace depth to find the item-level brace.
    
    Args:
        span_bytes: Bytes containing the item
        key_pos: Position of key within span_bytes
        
    Returns:
        Position of closing '}', or -1 if not found
    """
    depth = 0
    in_string = False
    escape_next = False
    
    for i in range(key_pos, len(span_bytes)):
        if escape_next:
            escape_next = False
            continue
        
        byte = span_bytes[i]
        
        if byte == ord('\\'):
            escape_next = True
            continue
        
        if byte == ord('"') and not escape_next:
            in_string = not in_string
            continue
        
        if in_string:
            continue
        
        if byte == ord('{'):
            depth += 1
        elif byte == ord('}'):
            if depth == 0:
                return i  # Found item-level closing brace
            depth -= 1
    
    return -1


def read_span_between_offsets(
    f: indexed_gzip.IndexedGzipFile,
    start_offset: int,
    end_offset: Optional[int],
    extra: int = 256,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB cap
) -> bytes:
    """
    Read a span of bytes between two offsets.
    
    Args:
        f: IndexedGzipFile object
        start_offset: Start offset (decompressed)
        end_offset: End offset (decompressed), or None for unbounded read
        extra: Extra bytes to read beyond end_offset
        max_bytes: Maximum bytes to read if end_offset is None
        
    Returns:
        Bytes read from the span
    """
    if start_offset < 0:
        raise ValueError("start_offset must be >= 0")
    
    f.seek(start_offset)
    
    if end_offset is not None:
        if end_offset <= start_offset:
            raise ValueError("end_offset must be greater than start_offset")
        n = (end_offset - start_offset) + extra
        return f.read(n)
    
    return f.read(max_bytes)


def build_span_attempt_sizes(max_span_size: int) -> List[int]:
    """
    Build escalating span sizes up to max_span_size.

    Example: max=16MB -> [2MB, 4MB, 8MB, 16MB]
    """
    if max_span_size <= 0:
        raise ValueError("max_span_size must be > 0")

    base_span = min(2 * 1024 * 1024, max_span_size)  # 2MB fast path
    attempts: List[int] = []
    span = base_span

    while span < max_span_size:
        attempts.append(span)
        span *= 2

    if not attempts or attempts[-1] != max_span_size:
        attempts.append(max_span_size)

    return attempts


def extract_item_from_span_around_key(
    span_bytes: bytes,
    key_name: str,
    key_offset_in_span: int,
    absolute_key_offset: Optional[int] = None,
) -> Optional[dict]:
    """
    Extract complete array item from span containing a rare key.
    
    Args:
        span_bytes: Bytes containing the item (with padding)
        key_name: Name of the rare key
        key_offset_in_span: Position of rare key within span_bytes (expected position, but we'll search the whole span)
        absolute_key_offset: Absolute offset in file (for logging)
    
    Returns:
        Parsed item dict, or None if extraction failed
    """
    LOG.info("Extracting item for rare key '%s' (span size: %d bytes, expected offset in span: %d%s)",
             key_name, len(span_bytes), key_offset_in_span,
             f", absolute offset: {absolute_key_offset}" if absolute_key_offset else "")
    
    # Find the key position in span - search the entire span since the exact position might be off
    key_bytes = b'"' + key_name.encode('utf-8') + b'"'
    # First try near the expected position (within 500 bytes)
    search_start = max(0, key_offset_in_span - 500)
    search_end = min(len(span_bytes), key_offset_in_span + 500)
    key_pos = span_bytes.find(key_bytes, search_start, search_end)
    
    LOG.debug("  Searched for key '%s' in span range [%d, %d): %s", 
             key_name, search_start, search_end, "found" if key_pos >= 0 else "not found")
    
    # If not found near expected position, search the entire span
    if key_pos < 0:
        key_pos = span_bytes.find(key_bytes)
        if key_pos >= 0:
            LOG.debug("  Found key '%s' in full span search at position %d", key_name, key_pos)
    
    if key_pos < 0:
        # Log a sample of the span to help debug
        sample_start = max(0, key_offset_in_span - 500)
        sample_end = min(len(span_bytes), key_offset_in_span + 500)
        sample = span_bytes[sample_start:sample_end]
        try:
            sample_text = sample.decode('utf-8', errors='replace')
            LOG.warning("Key '%s' not found in span (expected at offset %d, span size: %d). Sample around expected position:\n%s",
                       key_name, key_offset_in_span, len(span_bytes), repr(sample_text))
        except Exception:
            LOG.warning("Key '%s' not found in span (expected at offset %d, span size: %d). Sample (hex): %s",
                       key_name, key_offset_in_span, len(span_bytes), sample.hex()[:400])
        
        # Also try to find the key anywhere in the span
        all_positions = []
        search_pos = 0
        while True:
            pos = span_bytes.find(key_bytes, search_pos)
            if pos < 0:
                break
            all_positions.append(pos)
            search_pos = pos + 1
        
        if all_positions:
            LOG.warning("  Key '%s' found at %d position(s) in span: %s", key_name, len(all_positions), all_positions[:10])
        else:
            LOG.warning("  Key '%s' not found anywhere in span", key_name)
        
        return None
    
    LOG.debug("  Key '%s' found at position %d in span", key_name, key_pos)
    
    # Find item boundaries
    item_start = find_item_start_in_span(span_bytes, key_pos)
    item_end = find_item_end_in_span(span_bytes, key_pos)
    
    LOG.info("  Item boundaries for key '%s': start=%d, end=%d (span size: %d)", 
             key_name, item_start, item_end, len(span_bytes))
    
    if item_start < 0:
        # Log context around key position to debug
        context_start = max(0, key_pos - 200)
        context_end = min(len(span_bytes), key_pos + 200)
        context = span_bytes[context_start:context_end]
        try:
            context_text = context.decode('utf-8', errors='replace')
            LOG.warning("  Failed to find item start for key '%s' at position %d. Context:\n%s",
                       key_name, key_pos, repr(context_text))
        except Exception:
            LOG.warning("  Failed to find item start for key '%s' at position %d. Context (hex): %s",
                       key_name, key_pos, context.hex()[:400])
        return None
    if item_end < 0:
        # Log context around key position to debug
        context_start = max(0, key_pos - 200)
        context_end = min(len(span_bytes), key_pos + 200)
        context = span_bytes[context_start:context_end]
        try:
            context_text = context.decode('utf-8', errors='replace')
            LOG.warning("  Failed to find item end for key '%s' at position %d. Context:\n%s",
                       key_name, key_pos, repr(context_text))
        except Exception:
            LOG.warning("  Failed to find item end for key '%s' at position %d. Context (hex): %s",
                       key_name, key_pos, context.hex()[:400])
        return None
    if item_end <= item_start:
        LOG.warning("  Invalid item boundaries for key '%s': start=%d, end=%d", key_name, item_start, item_end)
        return None
    
    # Extract and parse item
    item_bytes = span_bytes[item_start:item_end + 1]
    item_size = len(item_bytes)
    
    LOG.info("  Extracted item bytes for key '%s': %d bytes (from span positions %d-%d)", 
             key_name, item_size, item_start, item_end)
    
    # Log a sample of the item
    sample_size = min(200, item_size)
    try:
        sample_text = item_bytes[:sample_size].decode('utf-8', errors='replace')
        LOG.debug("  Item sample (first %d bytes): %s", sample_size, repr(sample_text))
    except Exception:
        LOG.debug("  Item sample (first %d bytes, hex): %s", sample_size, item_bytes[:sample_size].hex())
    
    try:
        decoder = json.JSONDecoder()
        item_text = item_bytes.decode('utf-8', errors='replace')
        item, consumed = decoder.raw_decode(item_text)
        
        LOG.info("  Successfully parsed item for key '%s': type=%s, consumed=%d bytes", 
                 key_name, type(item).__name__, consumed)
        
        if isinstance(item, dict):
            LOG.info("  Item for key '%s' contains %d keys: %s", 
                    key_name, len(item), list(item.keys())[:10])
            return item
        else:
            LOG.warning("  Parsed item for key '%s' is not a dict (type: %s)", key_name, type(item).__name__)
    except json.JSONDecodeError as e:
        LOG.warning("  JSON decode error for key '%s': %s (at position %d in item text)", 
                   key_name, str(e), e.pos if hasattr(e, 'pos') else 'unknown')
        # Log more context around the error
        if hasattr(e, 'pos') and e.pos is not None:
            error_start = max(0, e.pos - 50)
            error_end = min(len(item_text), e.pos + 50)
            LOG.debug("  Context around error: %s", repr(item_text[error_start:error_end]))
    except Exception as e:
        LOG.warning("  Exception parsing item for key '%s': %s", key_name, str(e))
        LOG.exception("  Full traceback:")
    
    return None


def extract_items_with_rare_keys(
    f: indexed_gzip.IndexedGzipFile,
    array_key: str,
    rare_keys: Dict[str, List[int]],
    max_array_items: int,
    padding: int = 204800,  # Default: 200KB (will try increasing sizes)
) -> List[Tuple[dict, bool]]:
    """
    Extract items from array that contain rare keys.
    Tries increasing span sizes (200KB, 500KB, 1MB, 15MB, 20MB) until item is found.
    If found, extracts only one item. If not found, raises to fail the file.
    
    Args:
        f: IndexedGzipFile (already opened)
        array_key: Name of the array
        rare_keys: Dict mapping rare key names to their offsets
        max_array_items: Maximum items to extract (used only if rare key extraction fails)
        padding: Initial padding size (will try increasing sizes: 200KB, 500KB, 1MB, 15MB, 20MB)
        
    Returns:
        List of (item_dict, has_rare_keys) tuples.
    """
    items = []
    seen_item_starts = set()  # Track item start offsets to avoid duplicates
    
    LOG.info("Extracting items with rare keys from array '%s' (will try increasing span sizes)", array_key)
    
    # For each rare key, use the first occurrence location (which is what we track)
    # We don't need to process all occurrences - just get one item per rare key
    rare_key_samples = []
    for rare_key, offsets in rare_keys.items():
        # Use the first occurrence (offsets[0] is the first occurrence we found during scanning)
        if offsets:
            first_offset = offsets[0]  # First occurrence location
            rare_key_samples.append((first_offset, rare_key))
            LOG.info("  Rare key '%s': %d total occurrence(s), using first occurrence at offset %d", 
                    rare_key, len(offsets), first_offset)
    
    # Sort by offset for consistent processing order
    rare_key_samples.sort(key=lambda x: x[0])
    
    LOG.info("  Processing %d rare key(s) (one item per rare key)", len(rare_key_samples))
    
    # Get file size (uncompressed) to ensure we don't read beyond the end
    current_pos = f.tell()
    f.seek(0, 2)  # SEEK_END
    file_size = f.tell()
    f.seek(current_pos)  # Restore position
    
    # Try increasing padding sizes from fast path to large fallback.
    padding_sizes = [
        200 * 1024,        # 200KB
        500 * 1024,        # 500KB
        1 * 1024 * 1024,   # 1MB
        15 * 1024 * 1024,  # 15MB
        20 * 1024 * 1024,  # 20MB
    ]
    
    for idx, (rare_key_offset, rare_key_name) in enumerate(rare_key_samples, 1):
        LOG.info("  [%d/%d] Processing rare key '%s' at absolute offset %d", 
                idx, len(rare_key_samples), rare_key_name, rare_key_offset)
        
        item = None
        successful_padding = None
        
        # Try increasing span sizes until we find the item
        for padding_size in padding_sizes:
            LOG.info("    Trying padding: %d KB", padding_size // 1024)
            
            # Read span around rare key, clamping to file boundaries
            span_start = max(0, rare_key_offset - padding_size)
            span_end = min(file_size, rare_key_offset + padding_size)
            
            # Adjust key offset in span if we had to clamp the start
            key_offset_in_span = rare_key_offset - span_start
            
            span = read_span_between_offsets(
                f,
                span_start,
                span_end,
                extra=0,
                max_bytes=span_end - span_start,
            )
            
            actual_span_size = len(span)
            LOG.info("    Read span: %d bytes (expected: %d bytes, start: %d, end: %d)", 
                    actual_span_size, span_end - span_start, span_start, span_end)
            
            # Extract item
            item = extract_item_from_span_around_key(
                span,
                rare_key_name,
                key_offset_in_span,
                absolute_key_offset=rare_key_offset,
            )
            
            if item:
                successful_padding = padding_size
                LOG.info("    ✓ Successfully extracted item with padding %d KB", padding_size // 1024)
                break
            else:
                LOG.warning("    ✗ Failed to extract item with padding %d KB, trying larger span...", padding_size // 1024)
        
        if item:
            # Item found - extract only one item (not max_array_items)
            item_start_approx = rare_key_offset - (successful_padding or padding_sizes[0])
            
            if item_start_approx not in seen_item_starts:
                items.append((item, True))  # True = has rare keys
                seen_item_starts.add(item_start_approx)
                LOG.info("    ✓ Added item for rare key '%s' (total items: %d)", rare_key_name, len(items))
            else:
                LOG.info("    ⊗ Skipped duplicate item (start offset %d already seen)", item_start_approx)
        else:
            LOG.warning("    ✗ Failed to extract item for rare key '%s' after trying all span sizes", rare_key_name)
            raise RareKeyExtractionExhaustedError(
                f"Failed rare-key extraction for array '{array_key}', key '{rare_key_name}' "
                "after trying all span sizes"
            )
    
    # If we found items with rare keys, return them (only one per rare key)
    if items:
        LOG.info("Extracted %d unique item(s) with rare keys from array '%s'", len(items), array_key)
        return items
    
    # No items were extracted despite rare key candidates being present.
    raise RareKeyExtractionExhaustedError(
        f"No items extracted for rare-key array '{array_key}' despite rare key candidates"
    )


def extract_scalar_or_first_n_from_span(
    key: str,
    span_bytes: bytes,
    n: int = 5,
    key_offset: int = 0,
) -> Tuple[Optional[Any], Optional[int]]:
    """
    Best-effort extraction from span_bytes containing '"key": <value>'.
    Same as original function - extracts first N items sequentially.
    
    Args:
        key: Key name to extract
        span_bytes: Bytes containing the key-value pair
        n: Maximum number of array elements to extract
        key_offset: Absolute offset of the key in the file
        
    Returns:
        Tuple of (extracted_value, array_start_offset)
    """
    if not span_bytes:
        return None, None
    
    key_bytes = b'"' + key.encode("utf-8") + b'"'
    p = span_bytes.find(key_bytes)
    if p < 0:
        return None, None
    
    c = span_bytes.find(b":", p + len(key_bytes))
    if c < 0:
        return None, None
    
    i = c + 1
    L = len(span_bytes)
    while i < L and span_bytes[i] in b" \t\r\n":
        i += 1
    if i >= L:
        return None, None
    
    first = span_bytes[i:i+1]
    array_start_offset = None
    
    if first == b"[":
        array_start_offset = key_offset + i
    
    tail_bytes = span_bytes[i:]
    
    try:
        tail_text = tail_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        tail_text = tail_bytes.decode("utf-8", errors="replace")
    
    decoder = json.JSONDecoder()
    
    if first == b"[":
        t = tail_text
        if not t or t[0] != "[":
            return [], array_start_offset
        
        out = []
        pos = 1
        
        def skip_ws(s, j):
            while j < len(s) and s[j].isspace():
                j += 1
            return j
        
        pos = skip_ws(t, pos)
        
        if pos < len(t) and t[pos] == "]":
            return [], array_start_offset
        
        while len(out) < n:
            pos = skip_ws(t, pos)
            if pos >= len(t):
                break
            if t[pos] == "]":
                break
            
            try:
                val, consumed = decoder.raw_decode(t[pos:])
            except Exception:
                break
            
            out.append(val)
            pos += consumed
            pos = skip_ws(t, pos)
            
            if pos >= len(t):
                break
            if t[pos] == ",":
                pos += 1
                continue
            if t[pos] == "]":
                break
            
            break
        
        return out, array_start_offset
    
    try:
        value, _ = decoder.raw_decode(tail_text)
    except Exception:
        return None, None
    
    return value, None


def convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal objects to float for JSON serialization."""
    from decimal import Decimal
    
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def process_single_key_with_rare_keys(
    key_name: str,
    key_offset: int,
    next_key_offset: Optional[int],
    input_path: Path,
    index_path: Path,
    source_name: str,
    file_name: str,
    file_size: int,
    max_array_items: int,
    max_span_size: int,
    rare_keys_by_array: Dict[str, Dict[str, List[int]]],
    array_starts: Dict[str, int],
    write_batch_func: Callable[[io.StringIO], None],
    batch_size: int,
    get_record_count: Callable[[], int],
    increment_record_count: Callable[[], None],
) -> int:
    """
    Process a single key, extracting items with rare keys if available.
    
    Args:
        key_name: Name of the key to process
        key_offset: Byte offset where the key starts
        next_key_offset: Byte offset of next key (None if last key)
        input_path: Path to input JSON.gz file
        index_path: Path to .gzi index file
        source_name: Source/payer identifier
        file_name: File name for database
        file_size: File size in bytes
        max_array_items: Maximum number of array items to extract
        max_span_size: Maximum span size to read
        rare_keys_by_array: Dict of {array_key: {rare_key: [offsets]}}
        array_starts: Dict of {array_key: start_offset}
        write_batch_func: Thread-safe function to write batch to database
        batch_size: Batch size for database writes
        get_record_count: Thread-safe function to get current record count
        increment_record_count: Thread-safe function to increment record count
        
    Returns:
        Number of records created for this key
    """
    LOG.info(
        "Starting key processing: key='%s', key_offset=%d, next_key_offset=%s",
        key_name,
        key_offset,
        str(next_key_offset) if next_key_offset is not None else "None",
    )
    f = open_with_existing_index(str(input_path), str(index_path))
    
    try:
        span_attempt_sizes = build_span_attempt_sizes(max_span_size)
        val: Optional[Any] = None
        array_start_offset: Optional[int] = None

        for attempt_idx, attempt_size in enumerate(span_attempt_sizes, 1):
            end = next_key_offset
            if end is not None:
                span_to_next_key = end - key_offset
                if span_to_next_key > attempt_size:
                    end = key_offset + attempt_size

            LOG.info(
                "Reading span for key '%s' (attempt %d/%d): start=%d, end=%s, span_size=%d",
                key_name,
                attempt_idx,
                len(span_attempt_sizes),
                key_offset,
                str(end) if end is not None else "None",
                attempt_size,
            )
            span = read_span_between_offsets(f, key_offset, end, extra=512, max_bytes=attempt_size)
            LOG.info("Read span for key '%s' (attempt %d): %d bytes", key_name, attempt_idx, len(span))
            LOG.info("Extracting value for key '%s' from span (attempt %d)...", key_name, attempt_idx)
            val, array_start_offset = extract_scalar_or_first_n_from_span(
                key_name, span, n=max_array_items, key_offset=key_offset
            )

            if val is not None:
                LOG.info(
                    "Extracted value for key '%s' on attempt %d: type=%s, array_start_offset=%s",
                    key_name,
                    attempt_idx,
                    type(val).__name__,
                    str(array_start_offset) if array_start_offset is not None else "None",
                )
                break

            # If current span already reaches the next key boundary, larger spans won't help.
            if next_key_offset is not None and (next_key_offset - key_offset) <= attempt_size:
                LOG.warning(
                    "Extraction failed for key '%s' at full key span on attempt %d; stopping retries",
                    key_name,
                    attempt_idx,
                )
                break

            LOG.warning(
                "Extraction failed for key '%s' on attempt %d with span_size=%d; escalating",
                key_name,
                attempt_idx,
                attempt_size,
            )
        
        if val is None:
            LOG.warning("Could not extract value for key '%s'", key_name)
            return 0
        
        records_created = 0
        
        # Check if this is an array with rare keys
        if isinstance(val, list) and key_name in rare_keys_by_array and rare_keys_by_array[key_name]:
            # Extract items with rare keys
            rare_key_items = extract_items_with_rare_keys(
                f,
                key_name,
                rare_keys_by_array[key_name],
                max_array_items,
            )
            
            # Store rare key items (or fallback items)
            for item_idx, (item, has_rare_keys) in enumerate(rare_key_items):
                payload_converted = convert_decimals(item)
                
                if HAS_ORJSON:
                    record_json = orjson.dumps(payload_converted, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
                    payload_size = len(record_json.encode('utf-8'))
                else:
                    record_json = json.dumps(payload_converted, ensure_ascii=False)
                    payload_size = len(record_json.encode('utf-8'))
                
                record_buffer = io.StringIO()
                record_writer = csv.writer(
                    record_buffer,
                    delimiter='\t',
                    quoting=csv.QUOTE_ALL,
                    doublequote=True,
                    lineterminator='\n',
                )
                
                row_data = [
                    source_name,
                    file_name,
                    str(file_size),
                    key_name,
                    str(item_idx),  # Item sequence, not array index
                    record_json,
                    str(payload_size),
                ]
                record_writer.writerow(row_data)
                
                # Remove trailing newline and manually append array_start_offset and has_rare_keys
                # This ensures NULL values are written as unquoted \N (not "\N")
                current_pos = record_buffer.tell()
                record_buffer.seek(current_pos - 1)
                record_buffer.truncate()
                
                if array_start_offset is not None:
                    record_buffer.write(f'\t"{array_start_offset}"')
                else:
                    record_buffer.write('\t\\N')
                
                # Append has_rare_keys (boolean as string)
                record_buffer.write(f'\t{"TRUE" if has_rare_keys else "FALSE"}\n')
                
                write_batch_func(record_buffer)
                increment_record_count()
                records_created += 1
            
            # If rare key items were found, we're done - don't extract additional items
            # (The whole point is to get items with rare keys, not fill up to max_array_items)
        elif isinstance(val, list):
            # Top-level arrays without rare-key hits: emit only the first item.
            rare_key_map = rare_keys_by_array.get(key_name, {})
            if rare_key_map:
                LOG.warning(
                    "Array '%s' has rare key map outside rare-key branch; emitting first item only",
                    key_name,
                )
            if val:
                LOG.info("Array '%s' has no rare keys; extracting first item only", key_name)
                regular_items = [val[0]]
            else:
                LOG.info("Array '%s' is empty; writing empty array payload", key_name)
                regular_items = [val]

            for item_idx, item in enumerate(regular_items):
                payload_converted = convert_decimals(item)
                
                if HAS_ORJSON:
                    record_json = orjson.dumps(payload_converted, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
                    payload_size = len(record_json.encode('utf-8'))
                else:
                    record_json = json.dumps(payload_converted, ensure_ascii=False)
                    payload_size = len(record_json.encode('utf-8'))
                
                record_buffer = io.StringIO()
                record_writer = csv.writer(
                    record_buffer,
                    delimiter='\t',
                    quoting=csv.QUOTE_ALL,
                    doublequote=True,
                    lineterminator='\n',
                )
                
                row_data = [
                    source_name,
                    file_name,
                    str(file_size),
                    key_name,
                    str(item_idx),
                    record_json,
                    str(payload_size),
                ]
                record_writer.writerow(row_data)
                
                # Remove trailing newline and manually append array_start_offset and has_rare_keys
                current_pos = record_buffer.tell()
                record_buffer.seek(current_pos - 1)
                record_buffer.truncate()
                
                if array_start_offset is not None:
                    record_buffer.write(f'\t"{array_start_offset}"')
                else:
                    record_buffer.write('\t\\N')
                
                # Append has_rare_keys (FALSE for regular items)
                record_buffer.write('\tFALSE\n')
                
                write_batch_func(record_buffer)
                increment_record_count()
                records_created += 1
        else:
            # Non-array payloads use original logic.
            if isinstance(val, (dict, str, int, float, bool)) or val is None:
                if isinstance(val, dict):
                    payload = val
                else:
                    payload = {"value": val}
            else:
                LOG.warning("Unexpected value type for key '%s': %s", key_name, type(val))
                return 0
            
            payload_converted = convert_decimals(payload)
            
            if HAS_ORJSON:
                record_json = orjson.dumps(payload_converted, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
                payload_size = len(record_json.encode('utf-8'))
            else:
                record_json = json.dumps(payload_converted, ensure_ascii=False)
                payload_size = len(record_json.encode('utf-8'))
            
            record_buffer = io.StringIO()
            record_writer = csv.writer(
                record_buffer,
                delimiter='\t',
                quoting=csv.QUOTE_ALL,
                doublequote=True,
                lineterminator='\n',
            )
            
            row_data = [
                source_name,
                file_name,
                str(file_size),
                key_name,
                "0",
                record_json,
                str(payload_size),
            ]
            record_writer.writerow(row_data)
            
            # Remove trailing newline and manually append array_start_offset and has_rare_keys
            current_pos = record_buffer.tell()
            record_buffer.seek(current_pos - 1)
            record_buffer.truncate()
            
            if array_start_offset is not None:
                record_buffer.write(f'\t"{array_start_offset}"')
            else:
                record_buffer.write('\t\\N')
            
            # Append has_rare_keys (FALSE for non-arrays)
            record_buffer.write('\tFALSE\n')
            
            write_batch_func(record_buffer)
            increment_record_count()
            records_created += 1
        
        LOG.info("Finished key processing: key='%s', records_created=%d", key_name, records_created)
        return records_created
        
    finally:
        f.close()


def ingest_file_indexed_gzip_rare_keys(
    connection_string: str,
    file_path: Path,
    source_name: str,
    batch_size: int = 1000,
    show_progress: bool = True,
    max_array_items: int = 20,
    index_path: Optional[Path] = None,
    chunk_size: int = 16 * 1024 * 1024,
    overlap_bytes: int = 2048,
    max_span_size: int = 50 * 1024 * 1024,
    num_key_threads: int = 1,
    progress_every_chunks: int = 5,
    num_rare_keys: int = 10,
    forced_top_level_arrays: Optional[List[str]] = None,
) -> int:
    """
    Ingest a large .json.gz file using indexed_gzip with rare key detection and extraction.
    
    Args:
        connection_string: PostgreSQL connection string
        file_path: Path to .json.gz file
        source_name: Source/payer identifier
        batch_size: Number of records to batch before writing to database
        show_progress: Whether to show progress bars
        max_array_items: Maximum number of items to extract from each array
        index_path: Optional path to index file
        chunk_size: Size of chunks for scanning
        overlap_bytes: Overlap between chunks
        max_span_size: Maximum size of span to read per key
        num_key_threads: Number of threads for processing keys in parallel
        progress_every_chunks: Log progress every N chunks during scanning
        num_rare_keys: Number of least common keys to mark as rare per array (default: 10)
        
    Returns:
        0 on success, 1 on error
    """
    if num_key_threads != 1:
        LOG.info(
            "num_key_threads=%d requested, overriding to 1 to avoid key-processing thread contention",
            num_key_threads,
        )
    num_key_threads = 1

    if not file_path.exists():
        LOG.error("File not found: %s", file_path)
        return 1
    
    if not is_json_file(file_path):
        LOG.error("File must be .json.gz: %s", file_path)
        return 1
    
    if not (file_path.name.endswith('.json.gz') or file_path.name.endswith('.json.gz.part')):
        LOG.error("indexed_gzip only works with .json.gz files: %s", file_path)
        return 1
    
    file_name = file_path.name
    if file_name.endswith(".part"):
        file_name = file_name[:-5]
    file_size = file_path.stat().st_size
    
    if index_path is None:
        index_path = file_path.with_suffix(file_path.suffix + '.gzi')
    else:
        index_path = Path(index_path)
    
    conn = None
    cursor = None
    
    try:
        overall_start = time.perf_counter()
        build_index_time = 0.0
        scan_time = 0.0
        process_time = 0.0
        copy_time = 0.0

        LOG.info("Connecting to database...")
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Ensure mrf_landing table exists (with has_rare_keys column)
        try:
            ensure_table_exists(
                connection_string,
                "mrf_landing",
                ddl_path=Path(__file__).parent.parent.parent / "sql" / "create_mrf_landing_table_rare_keys.sql",
                drop_if_exists=False,
            )
        except Exception as table_exc:  # noqa: BLE001
            LOG.error("Failed to ensure mrf_landing table exists: %s", table_exc)
            return 1
        
        # Step 1: Build or use existing index
        # If index exists, try to use it, but rebuild if it's corrupted
        index_valid = False
        if index_path.exists():
            try:
                # Try to open and validate the index
                test_f = open_with_existing_index(str(file_path), str(index_path))
                test_f.close()
                index_valid = True
                LOG.info("Using existing index: %s", index_path)
            except Exception as index_exc:  # noqa: BLE001
                # Index is corrupted or doesn't match, rebuild it
                LOG.warning("Existing index is corrupted or invalid (%s), rebuilding: %s", 
                           type(index_exc).__name__, index_path)
                try:
                    index_path.unlink()
                except Exception:  # noqa: BLE001
                    pass  # Ignore if we can't delete it
        
        if not index_path.exists() or not index_valid:
            LOG.info("Building index...")
            t0 = time.perf_counter()
            build_and_export_index(str(file_path), str(index_path))
            build_index_time += time.perf_counter() - t0
        
        # Step 2: Scan for keys with occurrence tracking
        LOG.info("Scanning for keys and tracking occurrences...")
        t0 = time.perf_counter()
        scan_result = scan_keys_with_occurrences(
            gz_path=str(file_path),
            index_path=str(index_path),
            chunk_size=chunk_size,
            overlap_bytes=overlap_bytes,
            progress_every_chunks=progress_every_chunks,
            forced_top_level_arrays=forced_top_level_arrays,
        )
        scan_time += time.perf_counter() - t0
        
        if not scan_result.unique_hits:
            LOG.warning("No unique keys found in %s", file_name)
            return 1
        
        # Step 3: Identify rare keys
        LOG.info("Identifying rare keys...")
        f = open_with_existing_index(str(file_path), str(index_path))
        try:
            rare_keys_by_array = identify_rare_keys_by_array(
                scan_result.key_occurrences,
                scan_result.array_starts,
                f,
                num_rare_keys=num_rare_keys,
                unique_hits=scan_result.unique_hits,
            )
        finally:
            f.close()
        
        # Step 3.5: Log comprehensive key statistics
        log_key_statistics(
            scan_result.key_occurrences,
            scan_result.array_starts,
            rare_keys_by_array,
            num_rare_keys,
            scan_result.unique_hits,
        )
        
        # Step 4: Extract values and write to database
        num_keys = len(scan_result.unique_hits)
        LOG.info("Extracting values for %d top-level key(s)...", num_keys)
        
        if HAS_ORJSON:
            LOG.info("Using orjson for fast JSON serialization")
        else:
            LOG.info("Using standard json library (install 'orjson' for 3-5x faster serialization)")
        
        # Build extraction targets:
        # - default: one target per unique top-level key
        # - forced keys: add both first and last offsets (if they differ)
        target_rows: List[Tuple[str, int]] = [
            (key_name, hit.abs_offset)
            for key_name, hit in scan_result.unique_hits.items()
        ]

        for forced_key in (forced_top_level_arrays or []):
            forced_offsets = scan_result.forced_key_offsets.get(forced_key)
            if not forced_offsets:
                continue
            first_offset, last_offset = forced_offsets
            target_rows.append((forced_key, first_offset))
            if last_offset != first_offset:
                target_rows.append((forced_key, last_offset))

        # De-duplicate and sort by offset so span boundaries remain well-ordered.
        seen_target_pairs: set[Tuple[str, int]] = set()
        deduped_targets: List[Tuple[str, int]] = []
        for key_name, key_offset in sorted(target_rows, key=lambda t: t[1]):
            pair = (key_name, key_offset)
            if pair in seen_target_pairs:
                continue
            seen_target_pairs.add(pair)
            deduped_targets.append(pair)

        targets = deduped_targets
        LOG.info("Prepared %d extraction target(s) after forced key expansion", len(targets))
        
        # Thread-safe dedupe buffer and record counting
        batch_lock = threading.Lock()
        # record_type -> (payload_size, raw_copy_line)
        best_rows_by_record_type: Dict[str, Tuple[int, str]] = {}
        record_count = 0
        record_count_lock = threading.Lock()

        def write_batch_to_db(batch_data: io.StringIO) -> None:
            """Thread-safe function to dedupe rows by record_type using max payload_size."""
            batch_data.seek(0)
            content = batch_data.getvalue()
            if not content or len(content.strip()) == 0:
                return
            
            with batch_lock:
                try:
                    for raw_line in content.splitlines():
                        if not raw_line.strip():
                            continue
                        parsed_rows = list(
                            csv.reader(
                                [raw_line],
                                delimiter="\t",
                                quotechar='"',
                                doublequote=True,
                            )
                        )
                        if not parsed_rows:
                            continue
                        parsed = parsed_rows[0]
                        if len(parsed) < 7:
                            continue
                        record_type = parsed[3]
                        try:
                            payload_size = int(parsed[6])
                        except Exception:  # noqa: BLE001
                            payload_size = -1

                        existing = best_rows_by_record_type.get(record_type)
                        if existing is None or payload_size > existing[0]:
                            best_rows_by_record_type[record_type] = (payload_size, raw_line)
                except Exception as exc:  # noqa: BLE001
                    LOG.exception("Error writing batch to database: %s", exc)
                    raise
                finally:
                    batch_data.seek(0)
                    batch_data.truncate(0)
        
        def get_record_count() -> int:
            """Thread-safe function to get current record count."""
            with record_count_lock:
                return record_count
        
        def increment_record_count() -> None:
            """Thread-safe function to increment record count."""
            nonlocal record_count
            with record_count_lock:
                record_count += 1
        
        if show_progress:
            pbar = tqdm(
                desc=f"Ingesting {file_name}",
                total=len(targets),
                unit=" keys",
                file=sys.stderr,
                mininterval=1.0,
                maxinterval=5.0,
            )
        
        # Process keys
        LOG.info("Processing %d key(s) with %d thread(s)...", len(targets), num_key_threads)
        
        if num_key_threads == 1:
            process_start = time.perf_counter()
            for i, (key_name, start) in enumerate(targets):
                next_key_offset = targets[i + 1][1] if i + 1 < len(targets) else None
                
                process_single_key_with_rare_keys(
                    key_name=key_name,
                    key_offset=start,
                    next_key_offset=next_key_offset,
                    input_path=file_path,
                    index_path=index_path,
                    source_name=source_name,
                    file_name=file_name,
                    file_size=file_size,
                    max_array_items=max_array_items,
                    max_span_size=max_span_size,
                    rare_keys_by_array=rare_keys_by_array,
                    array_starts=scan_result.array_starts,
                    write_batch_func=write_batch_to_db,
                    batch_size=batch_size,
                    get_record_count=get_record_count,
                    increment_record_count=increment_record_count,
                )
                if show_progress:
                    pbar.update(1)
            process_time += time.perf_counter() - process_start
        else:
            max_workers = min(num_key_threads, len(targets))
            LOG.info("Using ThreadPoolExecutor with %d worker(s) to process %d key(s) in parallel", max_workers, len(targets))
            
            process_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_key = {
                    executor.submit(
                        process_single_key_with_rare_keys,
                        key_name=key_name,
                        key_offset=key_offset,
                        next_key_offset=targets[i + 1][1] if i + 1 < len(targets) else None,
                        input_path=file_path,
                        index_path=index_path,
                        source_name=source_name,
                        file_name=file_name,
                        file_size=file_size,
                        max_array_items=max_array_items,
                        max_span_size=max_span_size,
                        rare_keys_by_array=rare_keys_by_array,
                        array_starts=scan_result.array_starts,
                        write_batch_func=write_batch_to_db,
                        batch_size=batch_size,
                        get_record_count=get_record_count,
                        increment_record_count=increment_record_count,
                    ): key_name
                    for i, (key_name, key_offset) in enumerate(targets)
                }
                
                for future in as_completed(future_to_key):
                    key_name = future_to_key[future]
                    try:
                        records_created = future.result()
                        if show_progress:
                            pbar.update(1)
                        if records_created == 0:
                            LOG.warning("Key '%s' processed but no records created", key_name)
                    except Exception as exc:  # noqa: BLE001
                        LOG.error("Key '%s' failed with exception: %s", key_name, exc, exc_info=True)
            process_time += time.perf_counter() - process_start

        # Finalize deduped rows and write once to mrf_landing.
        with batch_lock:
            deduped_lines = [raw_line for _, raw_line in best_rows_by_record_type.values()]

        if deduped_lines:
            final_buffer = io.StringIO("\n".join(deduped_lines) + "\n")
            try:
                copy_start = time.perf_counter()
                cursor.copy_expert(
                    """
                    COPY mrf_landing(source_name, file_name, file_size, record_type, record_index, payload, payload_size, array_start_offset, has_rare_keys)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '"', NULL '\\N')
                    """,
                    final_buffer,
                )
                copy_time += time.perf_counter() - copy_start
            except Exception as exc:  # noqa: BLE001
                LOG.exception("Error writing deduped rows to database: %s", exc)
                raise

        with record_count_lock:
            record_count = len(best_rows_by_record_type)
        
        if show_progress:
            pbar.close()
        
        LOG.info("Successfully ingested %d record(s) from %s", record_count, file_name)
        total_elapsed = time.perf_counter() - overall_start
        if total_elapsed > 0:
            LOG.info(
                "Timing summary: total=%.2fs, build_index=%.2fs (%.1f%%), scan=%.2fs (%.1f%%), process=%.2fs (%.1f%%), copy=%.2fs (%.1f%%)",
                total_elapsed,
                build_index_time, 100.0 * build_index_time / total_elapsed,
                scan_time, 100.0 * scan_time / total_elapsed,
                process_time, 100.0 * process_time / total_elapsed,
                copy_time, 100.0 * copy_time / total_elapsed,
            )
        return 0
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error ingesting file %s: %s", file_name, exc)
        return 1
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
