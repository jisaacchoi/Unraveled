"""Indexed gzip ingestion logic for large .json.gz files."""
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
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

# Try to import orjson for faster JSON serialization (optimization #5)
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

LOG = logging.getLogger("src.indexed_gzip_ingester")

# Regex pattern to match JSON object keys: "key":
KEY_PATTERN = re.compile(rb'"((?:\\.|[^"\\]){1,512})"\s*:')


@dataclass
class KeyHit:
    """Represents a unique key occurrence with its position."""
    key: str
    abs_offset: int  # decompressed absolute byte offset
    chunk_index: int
    offset_in_chunk: int


def _decode_json_string_bytes(b: bytes) -> str:
    """Decode JSON string bytes, handling escape sequences."""
    s = b.decode("utf-8", errors="replace")
    try:
        return json.loads('"' + s.replace('"', '\\"') + '"')
    except Exception:
        return s


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
        # Build full index over the compressed file
        f.build_full_index()
        # Export index to disk
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


def scan_unique_keys_with_offsets(
    gz_path: str,
    index_path: Optional[str] = None,
    chunk_size: int = 8 * 1024 * 1024,  # 8MB default
    overlap_bytes: int = 2048,  # 2KB default
    progress_every_chunks: int = 5,
) -> Dict[str, KeyHit]:
    """
    Scan for unique (top-level) JSON keys and record their offsets.
    
    Args:
        gz_path: Path to .json.gz file
        index_path: Optional path to pre-built index file
        chunk_size: Size of chunks to read (default: 8MB)
        overlap_bytes: Overlap between chunks to handle boundary cases (default: 2KB)
        progress_every_chunks: Log progress every N chunks (default: 20)
        
    Returns:
        Dictionary mapping key names to KeyHit objects (only keys that appear once)
    """
    total_compressed = os.path.getsize(gz_path)
    start_time = time.time()
    
    if index_path and os.path.exists(index_path):
        LOG.info("Using existing index: %s", index_path)
        f = open_with_existing_index(gz_path, index_path)
    else:
        LOG.info("Building index on-the-fly (no pre-built index found)")
        f = open_indexed_gzip(gz_path)
    
    seen_once: Dict[str, int] = {}
    seen_multiple: set[str] = set()
    
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
            
            for m in KEY_PATTERN.finditer(combined):
                if m.end() <= tail_len:
                    continue
                
                key = _decode_json_string_bytes(m.group(1))
                abs_offset = chunk_start_abs - tail_len + m.start()
                if abs_offset < 0:
                    abs_offset = m.start()
                
                if key in seen_multiple:
                    continue
                
                if key in seen_once:
                    del seen_once[key]
                    seen_multiple.add(key)
                else:
                    seen_once[key] = abs_offset
            
            tail = combined[-overlap_bytes:] if overlap_bytes > 0 else b""
            chunk_index += 1
            
            if progress_every_chunks and (chunk_index % progress_every_chunks == 0):
                elapsed = time.time() - start_time
                msg = f"Scanned {chunk_index:,} chunks | "
                
                # Compressed % progress - try multiple methods to get compressed position
                compressed_pos = None
                try:
                    # Try method 1: fileobj().tell()
                    compressed_pos = f.fileobj().tell()
                except Exception:
                    try:
                        # Try method 2: direct attribute access if available
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
                    # Estimate compression ratio if we can't get exact position
                    # Typical gzip compression ratio is 2-10x, use 5x as estimate
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
        
        LOG.info("Found %d unique (top-level) key(s)", len(unique_hits))
        return unique_hits
    
    finally:
        f.close()


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
        max_bytes: Maximum bytes to read if end_offset is None (default: 10MB)
        
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
    
    # end_offset is None → bounded read
    return f.read(max_bytes)


def find_array_start_offset(
    f: "IndexedGzipFile",
    key_offset: int,
    key_name: str,
) -> Optional[int]:
    """
    Find the byte offset of the opening '[' for an array value.
    
    Args:
        f: IndexedGzipFile object (already opened)
        key_offset: Byte offset of the key (from scan_unique_keys_with_offsets)
        key_name: Name of the key
        
    Returns:
        Byte offset of the opening '[' (after "key": [), or None if not found
    """
    # Read a small span to find the array start (after the colon and opening bracket)
    key_span = read_span_between_offsets(
        f,
        key_offset,
        key_offset + 1024,  # Read 1KB to find the array start
        extra=0,
        max_bytes=1024,
    )
    
    if not key_span:
        return None
    
    key_text = key_span.decode("utf-8", errors="replace")
    # Look for pattern: "key": [
    key_pattern = f'"{key_name}"'
    key_pos = key_text.find(key_pattern)
    if key_pos >= 0:
        # Find colon after key
        colon_pos = key_text.find(":", key_pos + len(key_pattern))
        if colon_pos >= 0:
            # Find opening bracket after colon (skip whitespace)
            bracket_pos = colon_pos + 1
            while bracket_pos < len(key_text) and key_text[bracket_pos] in (' ', '\t', '\n', '\r'):
                bracket_pos += 1
            if bracket_pos < len(key_text) and key_text[bracket_pos] == '[':
                # Return offset of the '[' character
                return key_offset + bracket_pos
    
    return None


def extract_scalar_or_first_n_from_span(
    key: str, 
    span_bytes: bytes, 
    n: int = 5,
    key_offset: int = 0,
) -> Tuple[Optional[Any], Optional[int]]:
    """
    Best-effort extraction from span_bytes containing '"key": <value>'.
    
    Behavior:
      - If key not found in span_bytes: return (None, None)
      - If value is scalar and fully present: return (scalar, None)
      - If scalar is truncated/unparseable: return (None, None)
      - If value is array: return (list, array_start_offset) where list has up to first n elements
        If array elements are truncated: returns what it can (possibly [])
    
    Args:
        key: Key name to extract
        span_bytes: Bytes containing the key-value pair
        n: Maximum number of array elements to extract (default: 5)
        key_offset: Absolute offset of the key in the file (for calculating array_start_offset)
        
    Returns:
        Tuple of (extracted_value, array_start_offset):
        - extracted_value: Extracted value (scalar, list, dict, or None if not found/unparseable)
        - array_start_offset: Byte offset of opening '[' for arrays, None for non-arrays
    """
    if not span_bytes:
        return None, None
    
    key_bytes = b'"' + key.encode("utf-8") + b'"'
    
    # Find the key in raw bytes
    p = span_bytes.find(key_bytes)
    if p < 0:
        return None, None
    
    # Find colon after the key
    c = span_bytes.find(b":", p + len(key_bytes))
    if c < 0:
        return None, None
    
    # Move to first non-whitespace after colon
    i = c + 1
    L = len(span_bytes)
    while i < L and span_bytes[i] in b" \t\r\n":
        i += 1
    if i >= L:
        return None, None
    
    first = span_bytes[i:i+1]
    
    # Calculate array_start_offset if this is an array (optimization #2, #7)
    array_start_offset = None
    if first == b"[":
        # Array start is at position i (relative to span_bytes start)
        # Absolute offset = key_offset + (position of key in span_bytes) + (offset to '[')
        # Since we found key at position p, and '[' at position i, the absolute offset is:
        array_start_offset = key_offset + i
    
    # Decode a working text view from value-start onward
    tail_bytes = span_bytes[i:]
    
    try:
        tail_text = tail_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        tail_text = tail_bytes.decode("utf-8", errors="replace")
    
    decoder = json.JSONDecoder()
    
    # Array case: parse up to n elements best-effort
    if first == b"[":
        t = tail_text
        if not t or t[0] != "[":
            return [], array_start_offset
        
        out = []
        pos = 1  # after '['
        
        def skip_ws(s, j):
            while j < len(s) and s[j].isspace():
                j += 1
            return j
        
        pos = skip_ws(t, pos)
        
        # Empty array
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
                # truncated or invalid element
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
            
            # Unexpected delimiter → stop best-effort
            break
        
        return out, array_start_offset
    
    # Scalar/object case: parse one JSON value best-effort
    try:
        value, _ = decoder.raw_decode(tail_text)
    except Exception:
        return None, None
    
    return value, None


def process_single_key(
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
    write_batch_func: Callable[[io.StringIO], None],
    batch_size: int,
    get_record_count: Callable[[], int],
    increment_record_count: Callable[[], None],
) -> int:
    """
    Process a single key in a thread-safe manner (optimization #4).
    
    This function processes one key independently, reading span, extracting value,
    and writing to database. It uses thread-safe functions for batch writing and record counting.
    
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
        write_batch_func: Thread-safe function to write batch to database
        batch_size: Batch size for database writes
        get_record_count: Thread-safe function to get current record count
        increment_record_count: Thread-safe function to increment record count
        
    Returns:
        Number of records created for this key (always 1 for indexed_gzip approach)
    """
    # Open own indexed_gzip handle (each thread needs its own)
    f = open_with_existing_index(str(input_path), str(index_path))
    
    try:
        # Determine end offset
        end = next_key_offset
        
        # Cap end to start + max_span_size if span is larger
        if end is not None:
            span_size = end - key_offset
            if span_size > max_span_size:
                end = key_offset + max_span_size
                LOG.debug("Capped read size for key '%s': %d bytes -> %d bytes (%d MB limit)", 
                         key_name, span_size, max_span_size, max_span_size // (1024 * 1024))
        
        # Read span and extract value (optimization #2, #7: find array_start_offset during extraction)
        span = read_span_between_offsets(f, key_offset, end, extra=512, max_bytes=max_span_size)
        val, array_start_offset = extract_scalar_or_first_n_from_span(
            key_name, span, n=max_array_items, key_offset=key_offset
        )
        
        if val is None:
            LOG.warning("Could not extract value for key '%s'", key_name)
            return 0
        
        # Prepare payload
        if isinstance(val, list):
            payload = val
        elif isinstance(val, (dict, str, int, float, bool)) or val is None:
            if isinstance(val, dict):
                payload = val
            else:
                payload = {"value": val}
        else:
            LOG.warning("Unexpected value type for key '%s': %s", key_name, type(val))
            return 0
        
        # Convert Decimals to floats
        payload_converted = convert_decimals(payload)
        
        # Convert to JSON string (optimization #5: use orjson if available)
        if HAS_ORJSON:
            record_json = orjson.dumps(payload_converted, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
            payload_size = len(record_json.encode('utf-8'))
        else:
            record_json = json.dumps(payload_converted, ensure_ascii=False)
            payload_size = len(record_json.encode('utf-8'))
        
        # Create thread-local buffer for this record
        record_buffer = io.StringIO()
        record_writer = csv.writer(
            record_buffer,
            delimiter='\t',
            quoting=csv.QUOTE_ALL,
            doublequote=True,
            lineterminator='\n',
        )
        
        # Write row data
        row_data = [
            source_name,
            file_name,
            str(file_size),
            key_name,  # record_type
            "0",  # record_index (always 0 for this approach)
            record_json,
            str(payload_size),
        ]
        record_writer.writerow(row_data)
        
        # Remove newline and append array_start_offset
        current_pos = record_buffer.tell()
        record_buffer.seek(current_pos - 1)
        record_buffer.truncate()
        
        if array_start_offset is not None:
            record_buffer.write(f'\t"{array_start_offset}"\n')
        else:
            record_buffer.write('\t\\N\n')
        
        # Write to database (thread-safe)
        write_batch_func(record_buffer)
        increment_record_count()
        
        return 1
        
    finally:
        f.close()


def convert_decimals(obj: Any) -> Any:
    """
    Recursively convert Decimal objects to float for JSON serialization.
    
    Args:
        obj: Object that may contain Decimal values
        
    Returns:
        Object with Decimals converted to float
    """
    from decimal import Decimal
    
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def ingest_file_indexed_gzip(
    connection_string: str,
    file_path: Path,
    source_name: str,
    batch_size: int = 1000,
    show_progress: bool = True,
    max_array_items: int = 20,
    index_path: Optional[Path] = None,
    chunk_size: int = 16 * 1024 * 1024,  # 16MB default (optimization #3)
    overlap_bytes: int = 2048,  # 2KB
    max_span_size: int = 50 * 1024 * 1024,  # 50MB default (optimization #8)
    num_key_threads: int = 1,  # Number of threads for processing keys in parallel (optimization #4)
    progress_every_chunks: int = 5,  # Log progress every N chunks during scanning (0 = disable)
) -> int:
    """
    Ingest a large .json.gz file using indexed_gzip for efficient random access.
    
    This function:
    1. Builds or uses an existing indexed_gzip index
    2. Scans for unique (top-level) keys using regex pattern matching
    3. Extracts values by reading spans between consecutive key offsets
    4. Stores each top-level key-value pair as a row in mrf_landing
    
    Args:
        connection_string: PostgreSQL connection string
        file_path: Path to .json.gz file
        source_name: Source/payer identifier
        batch_size: Number of records to batch before writing to database
        show_progress: Whether to show progress bars
        max_array_items: Maximum number of array elements to extract (default: 20)
        index_path: Optional path to index file (if None, auto-generates next to file)
        chunk_size: Size of chunks for scanning (default: 16MB)
        overlap_bytes: Overlap between chunks (default: 2KB)
        max_span_size: Maximum size of span to read per key (default: 50MB)
        num_key_threads: Number of threads for processing keys in parallel (1 = sequential, 2+ = parallel)
                        Only beneficial for files with 5+ top-level keys (default: 1)
        progress_every_chunks: Log progress every N chunks during scanning (0 = disable, default: 5)
        
    Returns:
        0 on success, 1 on error
    """
    if not file_path.exists():
        LOG.error("File not found: %s", file_path)
        return 1
    
    if not is_json_file(file_path):
        LOG.error("File must be .json.gz: %s", file_path)
        return 1
    
    if not file_path.name.endswith('.gz'):
        LOG.error("indexed_gzip only works with .json.gz files: %s", file_path)
        return 1
    
    file_name = file_path.name
    file_size = file_path.stat().st_size
    
    # Determine index path
    if index_path is None:
        index_path = file_path.with_suffix(file_path.suffix + '.gzi')
    else:
        index_path = Path(index_path)
    
    conn = None
    cursor = None
    
    try:
        LOG.info("Connecting to database...")
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Ensure mrf_landing table exists
        try:
            ensure_table_exists(connection_string, "mrf_landing", drop_if_exists=False)
        except Exception as table_exc:  # noqa: BLE001
            LOG.error("Failed to ensure mrf_landing table exists: %s", table_exc)
            return 1
        
        # Step 1: Build or use existing index
        if not index_path.exists():
            LOG.info("Index file not found, building index...")
            build_and_export_index(str(file_path), str(index_path))
        else:
            LOG.info("Using existing index: %s", index_path)
        
        # Step 2: Scan for unique keys
        LOG.info("Scanning for unique (top-level) keys...")
        unique_hits = scan_unique_keys_with_offsets(
            gz_path=str(file_path),
            index_path=str(index_path),
            chunk_size=chunk_size,
            overlap_bytes=overlap_bytes,
            progress_every_chunks=progress_every_chunks,
        )
        
        if not unique_hits:
            LOG.warning("No unique keys found in %s", file_name)
            return 1
        
        # Step 3: Extract values and write to database
        num_keys = len(unique_hits)
        LOG.info("Extracting values for %d top-level key(s)...", num_keys)
        
        # Log JSON serialization method (optimization #5)
        if HAS_ORJSON:
            LOG.info("Using orjson for fast JSON serialization (optimization #5)")
        else:
            LOG.info("Using standard json library (install 'orjson' for 3-5x faster serialization)")
        
        # Convert to list for indexing
        targets = list(unique_hits.items())
        
        # Thread-safe batch writing and record counting (optimization #4)
        batch_lock = threading.Lock()
        record_count = 0
        record_count_lock = threading.Lock()
        
        def write_batch_to_db(batch_data: io.StringIO) -> None:
            """Thread-safe function to write a batch of records to database using COPY FROM."""
            batch_data.seek(0)
            content = batch_data.getvalue()
            if not content or len(content.strip()) == 0:
                return
            
            batch_data.seek(0)
            with batch_lock:  # Thread-safe database write
                try:
                    cursor.copy_expert(
                        """
                        COPY mrf_landing(source_name, file_name, file_size, record_type, record_index, payload, payload_size, array_start_offset)
                        FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '"', NULL '\\N')
                        """,
                        batch_data,
                    )
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
        
        # Process keys (sequentially or in parallel based on num_key_threads) (optimization #4)
        LOG.info("Processing %d key(s) with %d thread(s)...", len(targets), num_key_threads)
        
        if num_key_threads == 1:
            # Sequential processing (backward compatible)
            for i, (key_name, hit) in enumerate(targets):
                start = hit.abs_offset
                next_key_offset = targets[i + 1][1].abs_offset if i + 1 < len(targets) else None
                
                process_single_key(
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
                    write_batch_func=write_batch_to_db,
                    batch_size=batch_size,
                    get_record_count=get_record_count,
                    increment_record_count=increment_record_count,
                )
                if show_progress:
                    pbar.update(1)
        else:
            # Parallel processing with ThreadPoolExecutor (optimization #4)
            max_workers = min(num_key_threads, len(targets))
            LOG.info("Using ThreadPoolExecutor with %d worker(s) to process %d key(s) in parallel", max_workers, len(targets))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_key = {
                    executor.submit(
                        process_single_key,
                        key_name=key_name,
                        key_offset=hit.abs_offset,
                        next_key_offset=targets[i + 1][1].abs_offset if i + 1 < len(targets) else None,
                        input_path=file_path,
                        index_path=index_path,
                        source_name=source_name,
                        file_name=file_name,
                        file_size=file_size,
                        max_array_items=max_array_items,
                        max_span_size=max_span_size,
                        write_batch_func=write_batch_to_db,
                        batch_size=batch_size,
                        get_record_count=get_record_count,
                        increment_record_count=increment_record_count,
                    ): key_name
                    for i, (key_name, hit) in enumerate(targets)
                }
                
                # Collect results as they complete
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
        
        if show_progress:
            pbar.close()
        
        LOG.info("Successfully ingested %d top-level key(s) from %s", record_count, file_name)
        return 0
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error ingesting file %s: %s", file_name, exc)
        return 1
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
