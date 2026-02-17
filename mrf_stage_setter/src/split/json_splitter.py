"""Split large JSON.gz files by chunking top-level arrays."""
from __future__ import annotations

import gzip
import json
import logging
import multiprocessing
import shutil
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
import time
import os

import ijson
import psycopg2

LOG = logging.getLogger("src.split.json_splitter")


def convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def validate_gzip_file(file_path: Path) -> bool:
    """
    Validate that a gzip file is not corrupted with minimal decompression.
    
    This function:
    1. Checks gzip header (fast, no decompression)
    2. Attempts minimal decompression (only first few bytes, not entire file)
    3. Verifies JSON structure (first character only)
    
    Note: This does NOT decompress or read the entire file. It only checks:
    - The gzip header is valid
    - The first compressed block can be decompressed
    - The decompressed content starts with valid JSON
    
    Args:
        file_path: Path to the gzip file to validate
        
    Returns:
        True if file appears valid, False if corrupted
    """
    try:
        # First, quick check: verify gzip header (magic bytes: 1f 8b)
        with open(file_path, "rb") as f:
            header = f.read(2)
            if len(header) < 2 or header != b"\x1f\x8b":
                LOG.error(f"File {file_path.name} does not have valid gzip header")
                return False
        
        # Second, minimal decompression check: try to read just a few bytes
        # This will only decompress the first block(s) needed for those bytes
        with gzip.open(file_path, "rb") as f:
            # Read just enough to verify decompression works (first 10 bytes is enough)
            first_bytes = f.read(10)
            if not first_bytes:
                LOG.error(f"File {file_path.name} is empty after decompression")
                return False
            
            # Check if it starts with JSON structure (first non-whitespace char should be { or [)
            first_char = first_bytes.lstrip()[0:1] if first_bytes.lstrip() else b""
            if first_char not in (b"{", b"["):
                LOG.error(f"File {file_path.name} does not appear to be valid JSON (first char: {first_char})")
                return False
        
        return True
    except (gzip.BadGzipFile, OSError, IOError) as e:
        LOG.error(f"Gzip validation failed for {file_path.name}: {e}")
        return False
    except Exception as e:
        LOG.error(f"Unexpected error validating {file_path.name}: {e}")
        return False


def open_json_file(file_path: Path):
    """Open a JSON file (compressed or uncompressed)."""
    if file_path.suffix == ".gz" or str(file_path).endswith(".json.gz"):
        return gzip.open(file_path, "rt", encoding="utf-8")
    else:
        return open(file_path, "r", encoding="utf-8")


def get_structure_from_mrf_landing(
    connection_string: str,
    file_name: str,
) -> Tuple[List[str], List[str]]:
    """
    Query mrf_landing to get structure info for a file.
    
    Returns:
        Tuple of (scalar_keys, array_keys) where:
        - scalar_keys: list of record_types that are scalars (max record_index = 0)
        - array_keys: list of record_types that are arrays (max record_index > 0)
    """
    LOG.debug(f"Querying mrf_landing for structure of '{file_name}'...")
    
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT record_type, MAX(record_index) as max_index
            FROM mrf_landing
            WHERE file_name = %s
            GROUP BY record_type
            ORDER BY record_type
        """, (file_name,))
        
        rows = cursor.fetchall()
        
        scalar_keys = []
        array_keys = []
        
        for record_type, max_index in rows:
            if max_index == 0:
                scalar_keys.append(record_type)
            else:
                array_keys.append(record_type)
        
        LOG.debug(f"Found {len(scalar_keys)} scalar(s): {scalar_keys}")
        LOG.debug(f"Found {len(array_keys)} array(s): {array_keys}")
        
        return scalar_keys, array_keys
        
    finally:
        cursor.close()
        conn.close()


def detect_structure(file_path: Path) -> Tuple[Dict[str, Any], List[str]]:
    """
    Detect top-level structure by parsing the file: identify scalars/objects vs arrays.
    (Fallback when mrf_landing data is not available)
    
    Returns:
        Tuple of (scalars_dict, array_keys) where:
        - scalars_dict: dict of scalar/object values to repeat in every output
        - array_keys: list of keys that are arrays (to be chunked)
    """
    scalars = {}
    array_keys = []
    
    LOG.debug("Detecting top-level structure from file...")
    
    with open_json_file(file_path) as fh:
        parser = ijson.parse(fh)
        current_key = None
        depth = 0
        
        for prefix, event, value in parser:
            if event == "start_map":
                depth += 1
            elif event == "end_map":
                depth -= 1
                if depth == 0:
                    break
            elif event == "start_array" and depth == 1 and current_key:
                array_keys.append(current_key)
                current_key = None
            elif event == "map_key" and depth == 1:
                current_key = value
            elif event in ("string", "number", "boolean", "null") and depth == 1 and current_key:
                scalars[current_key] = value
                current_key = None
    
    LOG.debug(f"Found {len(scalars)} scalar(s): {list(scalars.keys())}")
    LOG.debug(f"Found {len(array_keys)} array(s): {array_keys}")
    
    return scalars, array_keys


def extract_scalars(file_path: Path, scalar_keys: List[str]) -> Dict[str, Any]:
    """Extract scalar values from file for the given keys."""
    scalars = {}
    
    LOG.debug(f"Extracting {len(scalar_keys)} scalar values from file...")
    
    with open_json_file(file_path) as fh:
        parser = ijson.parse(fh)
        current_key = None
        depth = 0
        
        for prefix, event, value in parser:
            if event == "start_map":
                depth += 1
            elif event == "end_map":
                depth -= 1
                if depth == 0:
                    break
            elif event == "map_key" and depth == 1:
                current_key = value
            elif event in ("string", "number", "boolean", "null") and depth == 1 and current_key:
                if current_key in scalar_keys:
                    scalars[current_key] = value
                current_key = None
    
    return scalars


def stream_all_arrays(file_path: Path, array_keys: List[str], chunk_size: int, file_name: Optional[str] = None) -> Iterator[Dict[str, List[Any]]]:
    """
    Stream all arrays from file, yielding chunks of up to chunk_size items per array.
    
    Each yield is a dict of {array_key: [items...]} for that chunk.
    Arrays that run out of items simply won't appear in subsequent chunks.
    
    Args:
        file_path: Path to the JSON file
        array_keys: List of array keys to stream
        chunk_size: Number of items per chunk
        file_name: Optional filename for logging (if not provided, uses file_path.name)
    """
    if file_name is None:
        file_name = file_path.name
    
    LOG.info(f"[{file_name}] Opening {len(array_keys)} file handle(s) for streaming arrays: {array_keys}")
    iterators = {}
    file_handles = []
    
    for key in array_keys:
        LOG.info(f"[{file_name}] Creating iterator for array '{key}'...")
        fh = open_json_file(file_path)
        file_handles.append(fh)
        ijson_path = f"{key}.item"
        iterators[key] = ijson.items(fh, ijson_path)
        LOG.debug(f"[{file_name}] Iterator created for '{key}' (parsing JSON structure to find array start...)")
    
    LOG.info(f"[{file_name}] Starting to read chunks from arrays (parsing JSON structure - this may take a moment for large files)...")
    
    # Track when we started and when we got first items
    start_time = time.time()
    first_item_received = {key: False for key in array_keys}
    last_progress_log_time = start_time
    
    try:
        chunk_num = 0
        while True:
            chunk_num += 1
            
            # Log progress periodically if we haven't received first items yet (every 10 seconds)
            elapsed = time.time() - start_time
            if not all(first_item_received.values()):
                if elapsed > 5 and (time.time() - last_progress_log_time) >= 10:
                    waiting_for = [k for k, v in first_item_received.items() if not v]
                    LOG.info(f"[{file_name}] Still parsing JSON structure to locate arrays: {waiting_for} (elapsed: {elapsed:.1f}s, this is normal for large files)...")
                    last_progress_log_time = time.time()
            
            # Log every 10th chunk to avoid spam
            if chunk_num % 10 == 0:
                LOG.info(f"[{file_name}] Reading chunk {chunk_num} (up to {chunk_size} items per array)...")
            
            chunk = {}
            any_items = False
            
            for key, iterator in iterators.items():
                items = []
                items_read = 0
                for _ in range(chunk_size):
                    try:
                        item = next(iterator)
                        items.append(item)
                        items_read += 1
                        any_items = True
                        
                        # Mark that we've received first item for this array
                        if not first_item_received[key]:
                            first_item_received[key] = True
                            elapsed = time.time() - start_time
                            LOG.info(f"[{file_name}] Array '{key}': found first item after {elapsed:.1f}s, starting to read chunks...")
                    except StopIteration:
                        break
                
                if items:
                    chunk[key] = items
                    if chunk_num <= 3:  # Log first few chunks
                        LOG.info(f"[{file_name}] Array '{key}': read {items_read} items in chunk {chunk_num}")
            
            if not any_items:
                LOG.info(f"[{file_name}] No more items in any array, stopping after {chunk_num - 1} chunks")
                break
                
            yield chunk
            
    finally:
        LOG.info(f"[{file_name}] Closing {len(file_handles)} file handle(s)...")
        for fh in file_handles:
            fh.close()


def split_json_gz(
    input_path: Path,
    output_dir: Path,
    chunk_size: int = 100000,
    min_file_size_mb: float = 0,
    connection_string: Optional[str] = None,
    original_files_dir: Optional[Path] = None,
    process_id: Optional[int] = None,
) -> int:
    """
    Split a JSON.gz file by chunking all top-level arrays.
    
    Args:
        input_path: Path to input JSON.gz file
        output_dir: Directory to write output files (split files stay here)
        chunk_size: Number of items per chunk for each array
        min_file_size_mb: Minimum file size (MB) to trigger splitting (0 to disable)
        connection_string: Database connection string to query mrf_landing (optional)
        original_files_dir: Directory to move original files to after splitting (optional)
        
    Returns:
        Number of output files created (0 if file was skipped or not split)
    """
    if not input_path.exists():
        LOG.error(f"Input file not found: {input_path}")
        return 0
    
    file_size_mb = input_path.stat().st_size / (1024 * 1024)
    pid_prefix = f"[PID:{process_id}] " if process_id else ""
    LOG.info(f"{pid_prefix}Processing: {input_path.name} ({file_size_mb:.1f} MB)")
    
    if min_file_size_mb > 0 and file_size_mb < min_file_size_mb:
        LOG.debug(f"File size ({file_size_mb:.1f} MB) is below threshold ({min_file_size_mb} MB), skipping split")
        return 0
    
    file_name = input_path.name
    
    # Try to get structure from mrf_landing first
    # Note: mrf_landing stores file names without prefixes, so we need to strip prefixes
    scalar_keys = None
    array_keys = None
    
    if connection_string:
        try:
            # Strip prefixes from file name to match what's stored in mrf_landing
            # mrf_landing stores file names as they were at ingestion time
            # Files may have been renamed with _ingested_analyzed_ prefix
            from src.shared.file_prefix import remove_prefix, PREFIX_INGESTED, PREFIX_ANALYZED
            
            # Try multiple variations of the file name
            # 1. Original name (with all prefixes)
            # 2. Without _ingested_analyzed_ prefixes
            # 3. With just _ingested_analyzed_ prefix (in case original was ingested with that)
            
            name_variations = [file_name]  # Start with original
            
            # Remove _ingested_analyzed_ prefix if present
            if file_name.startswith("_ingested_analyzed_"):
                temp_path = remove_prefix(Path(file_name), PREFIX_INGESTED)
                temp_path = remove_prefix(temp_path, PREFIX_ANALYZED)
                name_variations.append(temp_path.name)
            
            # Also try with _ingested_analyzed_ prefix added (in case original was ingested with that)
            if not any("_ingested_analyzed_" in name for name in name_variations):
                # Get base name (without any prefixes)
                base_name = name_variations[-1] if name_variations else file_name
                name_variations.append(f"_ingested_analyzed_{base_name}")
            
            # Try each variation until we find a match
            scalar_keys = None
            array_keys = None
            for name_variant in name_variations:
                LOG.debug(f"Trying to query mrf_landing with file_name: {name_variant}")
                try:
                    scalar_keys, array_keys = get_structure_from_mrf_landing(connection_string, name_variant)
                    if scalar_keys or array_keys:
                        LOG.info(f"Found structure in mrf_landing using file_name: {name_variant}")
                        break
                except Exception as query_exc:  # noqa: BLE001
                    LOG.debug(f"Query with '{name_variant}' failed: {query_exc}, trying next variation...")
                    continue
            
            if not scalar_keys and not array_keys:
                LOG.warning(f"Could not find file '{file_name}' in mrf_landing with any name variation. Tried: {name_variations}")
        except Exception as e:
            LOG.warning(f"Could not query mrf_landing: {e}. Falling back to file parsing.")
    
    # If we got structure from mrf_landing, extract scalar values from file
    if scalar_keys is not None and array_keys is not None:
        LOG.info(f"Extracting {len(scalar_keys)} scalar values from {file_name}...")
        scalars = extract_scalars(input_path, scalar_keys)
        LOG.info(f"Extracted scalars: {list(scalars.keys())}")
    else:
        # Fallback: detect structure from file
        LOG.info(f"Detecting structure from file {file_name}...")
        scalars, array_keys = detect_structure(input_path)
        LOG.info(f"Detected structure: {len(scalars)} scalars, {len(array_keys)} arrays")
    
    if not array_keys:
        LOG.warning(f"No arrays found in {file_name}, nothing to split")
        return 0
    
    LOG.info(f"Will chunk {len(array_keys)} array(s): {array_keys}")
    LOG.info(f"Chunk size: {chunk_size:,} items per array per file")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filename pattern
    stem = input_path.name
    if stem.endswith(".json.gz"):
        stem = stem[:-8]
    elif stem.endswith(".json"):
        stem = stem[:-5]
    
    # Stream all arrays and write chunks
    LOG.info(f"{pid_prefix}Starting to stream arrays from {file_name} (arrays: {array_keys})...")
    files_created = 0
    
    for chunk_data in stream_all_arrays(input_path, array_keys, chunk_size, file_name=file_name):
        files_created += 1
        output_path = output_dir / f"{stem}_part{files_created:05d}.json.gz"
        
        # Build output JSON: scalars + array chunks
        output_data = dict(scalars)
        output_data.update(chunk_data)
        
        # Convert Decimals to floats for JSON serialization
        output_data = convert_decimals(output_data)
        
        # Log what's in this chunk
        chunk_info = ", ".join([f"{k}: {len(v):,}" for k, v in chunk_data.items()])
        LOG.info(f"{pid_prefix}Writing {output_path.name} ({chunk_info})...")
        
        # Write the file
        with gzip.open(output_path, "wt", encoding="utf-8") as f:
            json.dump(output_data, f)
        
        # Validate the written file to ensure it's not corrupted
        if not validate_gzip_file(output_path):
            LOG.error(f"VALIDATION FAILED: {output_path.name} is corrupted. Deleting invalid file...")
            try:
                output_path.unlink()
                LOG.error(f"Deleted corrupted file: {output_path.name}")
            except Exception as e:
                LOG.error(f"Failed to delete corrupted file {output_path.name}: {e}")
            # Don't count this as a successfully created file
            files_created -= 1
            continue
        
        LOG.debug(f"Validated {output_path.name} - file is valid")
    
    # Move original file to original_files_dir if specified and files were created
    if files_created > 0 and original_files_dir:
        original_files_dir.mkdir(parents=True, exist_ok=True)
        dest_path = original_files_dir / input_path.name
        try:
            shutil.move(str(input_path), str(dest_path))
            LOG.info(f"Moved original file to: {dest_path}")
        except Exception as e:
            LOG.warning(f"Failed to move original file {input_path.name}: {e}")
    
    LOG.info(f"Split {file_name} into {files_created} part(s)")
    return files_created


def _split_single_file_worker(
    file_info: Tuple[str, str, str, Optional[str], int, float, Optional[str], int, int, Optional[str]]
) -> Tuple[int, int, str, str]:
    """
    Worker function for multiprocessing - splits a single file.
    
    Args:
        file_info: Tuple of (file_path_str, group_name, input_dir_str, original_files_dir_str, chunk_size, min_file_size_mb, connection_string, file_index, total_files, log_file_path)
        Note: Paths are passed as strings for pickling compatibility
        
    Returns:
        Tuple of (parts_created, success_flag, group_name, file_name)
        - parts_created: Number of parts created (0 if skipped or failed)
        - success_flag: 1 if file was processed successfully, 0 otherwise
        - group_name: Group name for logging
        - file_name: File name for logging
    """
    file_path_str, group_name, input_dir_str, original_files_dir_str, chunk_size, min_file_size_mb, connection_string, file_index, total_files, log_file_path = file_info
    
    # Get process ID for logging identification
    process_id = os.getpid()
    
    # Set up logging for this worker process
    # On Windows multiprocessing, workers don't inherit logging configuration, so we need to set it up
    worker_log = logging.getLogger("src.split.json_splitter")
    
    # Configure logging if not already configured (worker process case)
    if not worker_log.handlers:
        if log_file_path:
            # Use the same log file as the main process
            log_file = Path(log_file_path)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")  # Append mode so all workers write to same file
        else:
            # Fall back to console logging
            handler = logging.StreamHandler()
        
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        handler.setLevel(logging.INFO)
        worker_log.addHandler(handler)
        worker_log.setLevel(logging.INFO)
        # Prevent propagation to root logger to avoid duplicate messages
        worker_log.propagate = False
    
    # Convert string paths back to Path objects
    file_path = Path(file_path_str)
    input_dir = Path(input_dir_str)
    original_files_dir = Path(original_files_dir_str) if original_files_dir_str else None
    
    try:
        # Output stays in the same directory as the input file
        output_dir = file_path.parent
        
        # If original_files_dir is specified, preserve the relative directory structure
        if original_files_dir:
            try:
                rel_path = file_path.parent.relative_to(input_dir)
                dest_original_dir = original_files_dir / rel_path
            except ValueError:
                dest_original_dir = original_files_dir
        else:
            dest_original_dir = None
        
        worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Processing file in '{group_name}': {file_path.name}")
        
        parts_created = split_json_gz(
            input_path=file_path,
            output_dir=output_dir,
            chunk_size=chunk_size,
            min_file_size_mb=min_file_size_mb,
            connection_string=connection_string,
            original_files_dir=dest_original_dir,
            process_id=process_id,  # Pass process ID for logging identification
        )
        
        if parts_created > 0:
            worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] ✓ Completed '{group_name}/{file_path.name}' → {parts_created} part(s)")
            return (parts_created, 1, group_name, file_path.name)
        else:
            worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] - Skipped '{group_name}/{file_path.name}' (no split needed)")
            return (0, 0, group_name, file_path.name)
    except Exception as e:
        worker_log.error(f"[PID:{process_id}] [{file_index}/{total_files}] ✗ Failed '{group_name}/{file_path.name}': {e}", exc_info=True)
        return (0, 0, group_name, file_path.name)


def split_files_in_directory(
    input_dir: Path,
    chunk_size: int = 100000,
    min_file_size_mb: float = 0,
    connection_string: Optional[str] = None,
    original_files_dir: Optional[Path] = None,
    recursive: bool = True,
    num_workers: Optional[int] = None,
    log_file_path: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Split all JSON.gz files in a directory.
    
    Args:
        input_dir: Directory containing JSON.gz files
        chunk_size: Number of items per chunk for each array
        min_file_size_mb: Minimum file size (MB) to trigger splitting
        connection_string: Database connection string to query mrf_landing
        original_files_dir: Directory to move original files to after splitting (preserves subfolder structure)
        recursive: If True, process subdirectories as well
        
    Returns:
        Tuple of (total_files_processed, total_parts_created)
    """
    if not input_dir.exists():
        LOG.error(f"Input directory not found: {input_dir}")
        return (0, 0)
    
    # First pass: Find all JSON.gz files and filter by size
    LOG.info("Scanning for JSON.gz files to split...")
    if recursive:
        all_json_gz_files = list(input_dir.rglob("*.json.gz"))
    else:
        all_json_gz_files = list(input_dir.glob("*.json.gz"))
    
    if not all_json_gz_files:
        LOG.warning(f"No JSON.gz files found in {input_dir}")
        return (0, 0)
    
    # Filter files by size and collect with their relative paths for progress tracking
    files_to_split = []
    for file_path in all_json_gz_files:
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if min_file_size_mb > 0 and file_size_mb < min_file_size_mb:
            continue  # Skip files below threshold
        
        # Calculate relative path for progress display
        try:
            rel_path = file_path.relative_to(input_dir)
            group_name = str(rel_path.parent) if rel_path.parent != Path(".") else "root"
        except ValueError:
            group_name = "unknown"
        
        files_to_split.append((file_path, group_name))
    
    total_files_to_split = len(files_to_split)
    if total_files_to_split == 0:
        LOG.info(f"Found {len(all_json_gz_files)} JSON.gz file(s), but none meet the size threshold ({min_file_size_mb} MB)")
        return (0, 0)
    
    LOG.info(f"Found {len(all_json_gz_files)} JSON.gz file(s) total")
    LOG.info(f"Will split {total_files_to_split} file(s) that meet size threshold ({min_file_size_mb} MB)")
    
    # Determine number of workers
    if num_workers is None:
        num_workers = max(1, multiprocessing.cpu_count() - 1)  # Leave one core free
    elif num_workers <= 0:
        num_workers = 1
    
    # Use multiprocessing if we have multiple files and workers > 1
    use_multiprocessing = total_files_to_split > 1 and num_workers > 1
    
    if use_multiprocessing:
        LOG.info(f"Using {num_workers} worker process(es) to split {total_files_to_split} file(s) in parallel")
        
        # Prepare arguments for worker function (convert Path objects to strings for better pickling compatibility)
        worker_args = []
        for file_index, (file_path, group_name) in enumerate(files_to_split, 1):
            worker_args.append((
                str(file_path),  # Convert to string for pickling
                group_name,
                str(input_dir),  # Convert to string for pickling
                str(original_files_dir) if original_files_dir else None,  # Convert to string for pickling
                chunk_size,
                min_file_size_mb,
                connection_string,
                file_index,
                total_files_to_split,
                log_file_path,  # Pass log file path so workers can configure logging
            ))
        
        # Process files in parallel
        total_files_processed = 0
        total_parts_created = 0
        
        try:
            LOG.info("Starting worker processes using ProcessPoolExecutor...")
            executor = None
            try:
                # Use ProcessPoolExecutor which is more reliable on Windows
                executor = ProcessPoolExecutor(max_workers=num_workers)
                LOG.info(f"ProcessPoolExecutor created with {num_workers} workers")
                
                # Submit all tasks
                LOG.info(f"Submitting {len(worker_args)} task(s) to workers...")
                future_to_args = {
                    executor.submit(_split_single_file_worker, args): args 
                    for args in worker_args
                }
                LOG.info(f"All {len(worker_args)} task(s) submitted, waiting for results...")
                
                # Collect results as they complete
                results = []
                completed = 0
                timeout_per_file = 3600  # 1 hour per file
                
                for future in as_completed(future_to_args, timeout=timeout_per_file * len(worker_args)):
                    completed += 1
                    args = future_to_args[future]
                    file_name = Path(args[0]).name
                    group_name = args[1]
                    
                    try:
                        result = future.result(timeout=1)  # Should already be done since as_completed returned it
                        parts_created, success_flag, result_group, result_file = result
                        results.append(result)
                        
                        # Log each completed task immediately
                        if success_flag > 0:
                            LOG.info(f"[{completed}/{len(worker_args)}] ✓ Completed '{group_name}/{file_name}' → {parts_created} part(s)")
                        else:
                            LOG.info(f"[{completed}/{len(worker_args)}] - Skipped '{group_name}/{file_name}' (no split needed)")
                    except Exception as e:
                        LOG.error(f"[{completed}/{len(worker_args)}] ✗ Failed '{group_name}/{file_name}': {e}", exc_info=True)
                        # Return error result
                        results.append((0, 0, group_name, file_name))
                
                LOG.info(f"All {len(results)} task(s) completed")
                
            except FuturesTimeoutError:
                if executor:
                    executor.shutdown(wait=False, cancel_futures=True)
                raise
            except Exception as executor_error:
                LOG.error(f"Failed to use ProcessPoolExecutor: {executor_error}", exc_info=True)
                if executor:
                    executor.shutdown(wait=False, cancel_futures=True)
                raise
            finally:
                if executor:
                    LOG.info("Shutting down ProcessPoolExecutor...")
                    executor.shutdown(wait=True)
                    LOG.info("ProcessPoolExecutor shut down")
            
            # Aggregate results
            for parts_created, success_flag, group_name, file_name in results:
                if success_flag > 0:
                    total_files_processed += 1
                total_parts_created += parts_created
            
            LOG.info(f"Split complete: processed {total_files_processed}/{total_files_to_split} file(s), created {total_parts_created} part(s)")
        except Exception as e:
            LOG.error(f"Multiprocessing failed: {e}. Falling back to sequential processing...", exc_info=True)
            # Fall back to sequential processing
            use_multiprocessing = False
    
    if not use_multiprocessing:
        # Sequential processing (original logic)
        if num_workers == 1:
            LOG.info("Using sequential processing (1 worker)")
        
        total_files_processed = 0
        total_parts_created = 0
        
        for file_index, (file_path, group_name) in enumerate(files_to_split, 1):
            # Output stays in the same directory as the input file
            output_dir = file_path.parent
            
            # If original_files_dir is specified, preserve the relative directory structure
            if original_files_dir:
                try:
                    rel_path = file_path.parent.relative_to(input_dir)
                    # Create subfolder structure in original_files_dir
                    dest_original_dir = original_files_dir / rel_path
                except ValueError:
                    # File is not under input_dir (shouldn't happen with rglob)
                    dest_original_dir = original_files_dir
            else:
                dest_original_dir = None
            
            LOG.info(f"[{file_index}/{total_files_to_split}] Processing file in '{group_name}': {file_path.name}")
            
            parts_created = split_json_gz(
                input_path=file_path,
                output_dir=output_dir,
                chunk_size=chunk_size,
                min_file_size_mb=min_file_size_mb,
                connection_string=connection_string,
                original_files_dir=dest_original_dir,
            )
            
            if parts_created > 0:
                total_files_processed += 1
                total_parts_created += parts_created
                LOG.info(f"[{file_index}/{total_files_to_split}] ✓ Completed '{group_name}/{file_path.name}' → {parts_created} part(s)")
            else:
                LOG.info(f"[{file_index}/{total_files_to_split}] - Skipped '{group_name}/{file_path.name}' (no split needed)")
        
        LOG.info(f"Split complete: processed {total_files_processed}/{total_files_to_split} file(s), created {total_parts_created} part(s)")
    
    return (total_files_processed, total_parts_created)
