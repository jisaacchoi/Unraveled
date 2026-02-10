"""Core ingestion logic for streaming MRF files into PostgreSQL."""
from __future__ import annotations

import csv
import io
import json
import logging
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

import ijson
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from tqdm import tqdm

from src.shared.json_reader import is_json_file, open_json_file


LOG = logging.getLogger("src.mrf_ingester")


def convert_decimals(obj: Any) -> Any:
    """
    Recursively convert Decimal objects to float for JSON serialization.
    
    Args:
        obj: Object that may contain Decimal values
        
    Returns:
        Object with Decimals converted to float
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def extract_records_from_json(
    file_path: Path,
    record_path: str,
) -> Iterator[dict]:
    """
    Stream records from a JSON file (compressed or uncompressed) using ijson.
    
    Args:
        file_path: Path to .json or .json.gz file
        record_path: JSONPath to extract records (e.g., ".in_network[]")
        
    Yields:
        Individual JSON records as dictionaries
    """
    # Normalize record_path for ijson
    # ijson uses paths like "in_network.item" for arrays
    ijson_path = record_path.strip(".").replace("[]", ".item")
    LOG.debug("extract_records_from_json: Opening file '%s' with path '%s' (ijson path: '%s')", 
             file_path.name, record_path, ijson_path)
    
    with open_json_file(file_path) as fh:
        # Parse JSON stream and extract records
        LOG.debug("extract_records_from_json: Creating ijson parser for path '%s'...", ijson_path)
        parser = ijson.items(fh, ijson_path)
        LOG.debug("extract_records_from_json: Starting to iterate over items...")
        item_count = 0
        for record in parser:
            item_count += 1
            if item_count == 1:
                LOG.debug("extract_records_from_json: Yielding first item from path '%s'", ijson_path)
            elif item_count % 100 == 0:
                LOG.debug("extract_records_from_json: Yielded %d items from path '%s' so far...", item_count, ijson_path)
            yield record
        LOG.debug("extract_records_from_json: Completed extraction from path '%s': %d item(s) total", 
                 ijson_path, item_count)


def extract_all_records(
    file_path: Path,
    max_array_items: int = 100,
) -> Iterator[Tuple[str, int, dict]]:
    """
    Dynamically extract all top-level keys (arrays, objects, and scalars).
    
    This function uses a two-phase approach optimized for efficiency:
    1. Quick detection pass: Scans only the structure (not data) to find all top-level keys
    2. Extraction pass: Extracts values from each detected key
    
    For arrays: Extracts up to max_array_items from the array, each as a separate record (top-level only)
    For objects: Extracts the object as a single record
    For scalars: Extracts the scalar value wrapped in a dict
    
    The detection pass is very lightweight and stops early, so the overhead is minimal.
    This ensures we don't miss any keys while maintaining good performance.
    
    Args:
        file_path: Path to .json or .json.gz file
        max_array_items: Maximum number of items to extract from each array (default: 100)
        
    Yields:
        Tuples of (record_type, record_index, record_dict) where:
        - record_type is the top-level key name (e.g., 'in_network', 'provider_references', 'reporting_entity_name')
        - record_index is the zero-based index (0 for non-arrays, index within array for arrays)
        - record_dict is the actual record data (array item, object, or scalar wrapped in dict)
    """
    # Phase 1: Quick detection pass - identify all top-level keys and their types
    # Also handle case where root is an array (not an object)
    top_level_keys = []  # List of (key_name, key_type) where type is 'array', 'object', or 'scalar'
    current_key = None
    key_start_depth = 0
    depth = 0
    root_is_array = False
    
    LOG.info("Phase 1: Detecting top-level structure in %s...", file_path.name)
    with open_json_file(file_path) as fh:
        parser = ijson.parse(fh)
        parse_count = 0
        for prefix, event, value in parser:
            parse_count += 1
            if parse_count % 10000 == 0:
                LOG.debug("Structure detection: processed %d parse events (current depth: %d, current_key: %s)", 
                         parse_count, depth, current_key)
            if event == "start_map":
                depth += 1
                if depth == 1 and current_key:
                    # Top-level object found - the value of current_key is an object
                    top_level_keys.append((current_key, "object"))
                    current_key = None
                elif depth > 1:
                    # Nested structure, reset current_key
                    current_key = None
            elif event == "end_map":
                depth -= 1
                if depth == 0:
                    # Reached end of root object - we've seen all top-level keys
                    break
            elif event == "start_array":
                if depth == 0:
                    # Root is an array (not an object with keys)
                    root_is_array = True
                    top_level_keys.append(("root_array", "array"))
                    break  # We've detected root array, can stop detection
                elif depth == 1 and current_key:
                    # Top-level key's value is an array
                    top_level_keys.append((current_key, "array"))
                    current_key = None
            elif event == "map_key" and depth == 1:
                current_key = value
                key_start_depth = depth
            elif event in ("string", "number", "boolean", "null") and depth == 1 and current_key:
                top_level_keys.append((current_key, "scalar"))
                current_key = None
    
    if not top_level_keys:
        LOG.warning("No top-level structure found in %s (expected object with keys or root array)", file_path.name)
        return
    
    LOG.info("Phase 1 complete: Detected %d top-level key(s) in %s: %s", 
             len(top_level_keys), file_path.name, [(k, t) for k, t in top_level_keys])
    
    # Phase 2: Extract values from each key based on type
    LOG.info("Phase 2: Starting extraction from %d key(s)...", len(top_level_keys))
    for key_idx, (key_name, key_type) in enumerate(top_level_keys, 1):
        LOG.info("Extracting from key %d/%d: '%s' (type: %s)...", key_idx, len(top_level_keys), key_name, key_type)
        try:
            # Use key name directly as record_type (no normalization)
            record_type = key_name
            
            if key_type == "array":
                # Extract up to max_array_items from the array - each item becomes a separate record
                # This stores top-level array items only (nested structures are preserved in payload)
                if key_name == "root_array":
                    # Root is an array, use empty path
                    record_path = "item"
                    LOG.info("Root is an array - extracting up to %d items...", max_array_items)
                else:
                    # Array is a value of a top-level key
                    record_path = f".{key_name}[]"
                    LOG.info("Extracting up to %d items from '%s' array...", max_array_items, key_name)
                try:
                    # Extract each array item as a separate record, up to max_array_items
                    item_count = 0
                    LOG.info("Starting to extract items from '%s' array (path: %s)...", key_name, record_path)
                    for record_index, item in enumerate(extract_records_from_json(file_path, record_path)):
                        item_count += 1
                        if item_count % 10 == 0:
                            LOG.debug("Extracted %d items from '%s' array so far...", item_count, key_name)
                        if item_count >= max_array_items:
                            LOG.info("Reached limit of %d items for '%s' array (stopping extraction)", max_array_items, key_name)
                            break
                        # Each array item becomes a separate record in mrf_landing
                        # Wrap the item in a list to preserve the fact that it came from an array
                        # This helps with dtype detection later (the payload will be a list, not just the item)
                        if isinstance(item, dict):
                            payload = [item]  # Wrap dict item in list
                        else:
                            payload = [{"value": item}]  # Wrap non-dict item in list
                        yield (record_type, record_index, payload)
                    LOG.info("Completed extraction from '%s' array: %d item(s) extracted", key_name, item_count)
                    
                    LOG.debug("Extracted %d item(s) from '%s' array", item_count, key_name)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Error extracting items from '%s' array: %s", key_name, exc)
            elif key_type == "object":
                # Extract the entire object as a single record
                LOG.debug("Extracting object '%s'...", key_name)
                try:
                    with open_json_file(file_path) as fh:
                        # Use ijson.items to extract the object at the top level
                        parser = ijson.items(fh, key_name)
                        obj = next(parser, None)
                        if obj is not None:
                            yield (record_type, 0, obj)
                            LOG.debug("Extracted object '%s'", key_name)
                        else:
                            LOG.warning("Object '%s' was detected but could not be extracted", key_name)
                except StopIteration:
                    LOG.warning("Object '%s' was detected but extraction returned no data", key_name)
            elif key_type == "scalar":
                # Extract scalar value and wrap it in a dict
                LOG.debug("Extracting scalar '%s'...", key_name)
                scalar_value = None
                with open_json_file(file_path) as fh:
                    parser = ijson.parse(fh)
                    for prefix, event, value in parser:
                        if prefix == key_name and event in ("string", "number", "boolean", "null"):
                            scalar_value = value
                            break
                if scalar_value is not None:
                    # Wrap scalar in a dict for consistency
                    yield (record_type, 0, {key_name: scalar_value})
                    LOG.debug("Extracted scalar '%s'", key_name)
                else:
                    LOG.warning("Scalar '%s' was detected but could not be extracted", key_name)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Error extracting from key '%s' (type: %s) in %s: %s", key_name, key_type, file_path.name, exc)


def ingest_file_python(
    connection_string: str,
    file_path: Path,
    source_name: str,
    batch_size: int = 1000,
    show_progress: bool = True,
    max_array_items: int = 100,
) -> int:
    """
    Ingest a single JSON file (compressed or uncompressed) into mrf_landing table using COPY FROM with streaming batches.
    
    This function:
    1. Streams the JSON file (compressed or uncompressed) using ijson
    2. Extracts records from all top-level keys (arrays, objects, scalars)
    3. Batches records and writes them directly to PostgreSQL using COPY FROM
    4. Marks each record with its source type and index
    
        Args:
        connection_string: PostgreSQL connection string
        file_path: Path to .json or .json.gz file
        source_name: Source/payer identifier
        batch_size: Number of records to batch before writing to database
        show_progress: Whether to show progress bars
        max_array_items: Maximum number of items to extract from each array (default: 100)
        
        Returns:
        0 on success, 1 on error
    """
    if not file_path.exists():
        LOG.error("File not found: %s", file_path)
        return 1
    
    if not is_json_file(file_path):
        LOG.error("File must be .json or .json.gz: %s", file_path)
        return 1
    
    file_name = file_path.name
    if file_name.endswith(".part"):
        file_name = file_name[:-5]
    # Get file size in bytes
    file_size = file_path.stat().st_size
    LOG.debug("File size: %d bytes (%.2f MB)", file_size, file_size / (1024 * 1024))
    
    conn = None
    cursor = None
    
    try:
        overall_start = time.perf_counter()
        extract_time = 0.0
        json_time = 0.0
        csv_time = 0.0
        copy_time = 0.0

        LOG.info("Connecting to database...")
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Ensure mrf_landing table exists before attempting to write
        from src.shared.database import ensure_table_exists
        try:
            ensure_table_exists(connection_string, "mrf_landing", drop_if_exists=False)
        except Exception as table_exc:  # noqa: BLE001
            LOG.error("Failed to ensure mrf_landing table exists: %s", table_exc)
            return 1
        
        # Stream records and batch write using COPY FROM
        LOG.info("Streaming records from %s (batch size: %d, file size: %.2f MB)", file_name, batch_size, file_size / (1024 * 1024))
        record_count = 0
        batch_buffer = io.StringIO()
        batch_writer = None
        
        # Create progress bar for record processing
        # Configure tqdm to write to stderr and update less frequently to avoid log spam
        if show_progress:
            pbar = tqdm(
                desc=f"Ingesting {file_name}",
                unit=" records",
                file=sys.stderr,  # Write to stderr to avoid interfering with logging
                mininterval=1.0,  # Update at most once per second
                maxinterval=5.0,  # Force update every 5 seconds even if no change
            )
        
        def write_batch_to_db(batch_data: io.StringIO) -> None:
            """Write a batch of records to database using COPY FROM."""
            nonlocal copy_time
            LOG.debug("write_batch_to_db: Starting...")
            # Get buffer size by checking current position
            current_pos = batch_data.tell()
            LOG.debug("write_batch_to_db: Current buffer position: %d", current_pos)
            
            # If position is 0, check if buffer actually has content
            if current_pos == 0:
                batch_data.seek(0)
                content = batch_data.read()
                batch_data.seek(0)
                if not content or len(content.strip()) == 0:
                    LOG.debug("Skipping empty batch")
                    return  # Empty batch, nothing to write
            
            # Seek to beginning for copy_expert
            batch_data.seek(0)
            
            # Get buffer content size for logging
            content_size = len(batch_data.getvalue()) if hasattr(batch_data, 'getvalue') else 0
            LOG.debug("write_batch_to_db: Buffer content size: %d bytes", content_size)
            
            try:
                LOG.info("Executing COPY FROM to write batch to database (buffer size: %d bytes)...", content_size)
                copy_start = time.perf_counter()
                cursor.copy_expert(
                    """
                    COPY mrf_landing(source_name, file_name, file_size, record_type, record_index, payload, payload_size)
                    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '"')
                    """,
                    batch_data,
                )
                copy_time += time.perf_counter() - copy_start
                LOG.info("COPY FROM completed successfully")
            except Exception as exc:  # noqa: BLE001
                LOG.exception("Error writing batch to database (buffer size: %d bytes): %s", content_size, exc)
                raise
            finally:
                # Reset buffer for next batch
                batch_data.seek(0)
                batch_data.truncate(0)
                LOG.debug("write_batch_to_db: Buffer reset, function complete")
        
        try:
            LOG.info("Starting to iterate over records from extract_all_records()...")
            records_extracted = 0
            record_iter = extract_all_records(file_path, max_array_items=max_array_items)
            while True:
                extract_start = time.perf_counter()
                try:
                    record_type, record_index, record = next(record_iter)
                except StopIteration:
                    break
                extract_time += time.perf_counter() - extract_start
                records_extracted += 1
                
                if records_extracted == 1:
                    LOG.info("Extracted first record: type=%s, index=%d", record_type, record_index)
                elif records_extracted % 10 == 0:
                    LOG.debug("Extracted %d record(s) so far (latest: type=%s, index=%d)", 
                             records_extracted, record_type, record_index)
                
                # Convert Decimal objects to float for JSON serialization
                LOG.debug("Converting record %d (type=%s, index=%d) to JSON...", records_extracted, record_type, record_index)
                json_start = time.perf_counter()
                record_converted = convert_decimals(record)
                # Convert record to JSON string for jsonb storage
                record_json = json.dumps(record_converted, ensure_ascii=False)
                json_time += time.perf_counter() - json_start
                # Calculate payload size (JSON string length in bytes)
                payload_size = len(record_json.encode('utf-8'))
                LOG.debug("Record %d converted: payload_size=%d bytes", records_extracted, payload_size)
                
                # Write to CSV buffer (tab-delimited for COPY FROM)
                # Use QUOTE_ALL and doublequote=True to properly escape quotes in JSON
                if batch_writer is None:
                    batch_writer = csv.writer(
                        batch_buffer,
                        delimiter='\t',
                        quoting=csv.QUOTE_ALL,
                        doublequote=True,  # Escape quotes by doubling them ("")
                        lineterminator='\n',
                    )
                
                # Write row: source_name, file_name, file_size, record_type, record_index, payload, payload_size
                # The CSV writer will properly escape quotes and special characters
                csv_start = time.perf_counter()
                batch_writer.writerow([
                    source_name,
                    file_name,
                    str(file_size),  # File size in bytes
                    record_type,
                    str(record_index),
                    record_json,
                    str(payload_size),  # Payload size in bytes (integer)
                ])
                csv_time += time.perf_counter() - csv_start
                record_count += 1
                
                # Update progress bar less frequently (every 100 records) to reduce output
                if show_progress and record_count % 100 == 0:
                    pbar.update(100)
                
                # Write batch to database when it reaches batch_size
                if record_count % batch_size == 0:
                    LOG.info("Batch full (%d records), writing to database...", batch_size)
                    write_batch_to_db(batch_buffer)
                    LOG.info("Batch written successfully (total: %d records)", record_count)
                    # Only log every 10 batches to reduce log noise
                    if (record_count // batch_size) % 10 == 0:
                        LOG.info("Wrote %d batch(es) (total: %d records)", record_count // batch_size, record_count)
            
            # Write remaining records
            LOG.info("Finished extracting records (total extracted: %d, total written: %d)", 
                    records_extracted, record_count)
            if record_count % batch_size != 0:
                LOG.info("Writing final batch with %d remaining records...", record_count % batch_size)
                write_batch_to_db(batch_buffer)
                LOG.info("Final batch written successfully")
                LOG.info("Wrote final batch (%d total records)", record_count)
            
            if show_progress:
                # Update progress bar to final count and close
                remaining = record_count % 100
                if remaining > 0:
                    pbar.update(remaining)
                pbar.close()
            
            if record_count == 0:
                LOG.warning("No records extracted from %s (records_extracted from iterator: %d)", file_name, records_extracted)
                return 1
            
            # Ensure final batch is written even if we didn't hit batch_size boundary
            if batch_buffer.tell() > 0:
                LOG.debug("Writing any remaining records in buffer")
                write_batch_to_db(batch_buffer)
            
            total_elapsed = time.perf_counter() - overall_start
            LOG.info(
                "Successfully ingested %s: %d records (source: %s)",
                file_name,
                record_count,
                source_name,
            )
            if total_elapsed > 0:
                LOG.info(
                    "Timing summary: total=%.2fs, extract=%.2fs (%.1f%%), json=%.2fs (%.1f%%), csv=%.2fs (%.1f%%), copy=%.2fs (%.1f%%)",
                    total_elapsed,
                    extract_time, 100.0 * extract_time / total_elapsed,
                    json_time, 100.0 * json_time / total_elapsed,
                    csv_time, 100.0 * csv_time / total_elapsed,
                    copy_time, 100.0 * copy_time / total_elapsed,
                )
            return 0
            
        except Exception as exc:  # noqa: BLE001
            LOG.exception("Error processing records from %s: %s", file_name, exc)
            if show_progress:
                pbar.close()
            return 1
            
    except psycopg2.Error as exc:
        LOG.exception("Database error during ingestion: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Unexpected error during ingestion: %s", exc)
        return 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def ingest_file(
    connection_string: str,
    file_path: Path,
    source_name: str,
    batch_size: int = 1000,
    max_array_items: int = 100,
) -> int:
    """
    Ingest a single JSON file (compressed or uncompressed) into mrf_landing table by extracting individual records.
    
    Extracts records from both .in_network[] and .provider_references[] arrays,
    marking each record with its source array type.
    
    Args:
        connection_string: PostgreSQL connection string
        file_path: Path to .json or .json.gz file
        source_name: Source/payer identifier
        batch_size: Batch size for streaming records to database
        max_array_items: Maximum number of items to extract from each array (default: 100)
        
    Returns:
        0 on success, 1 on error
    """
    if not file_path.exists():
        LOG.error("File not found: %s", file_path)
        return 1
    
    if not is_json_file(file_path):
        LOG.error("File must be .json or .json.gz: %s", file_path)
        return 1
    
    # Use Python implementation for ingestion
    LOG.info("Ingesting %s -> mrf_landing (source: %s)", file_path.name, source_name)
    
    return ingest_file_python(
        connection_string=connection_string,
        file_path=file_path,
        source_name=source_name,
        batch_size=batch_size,
        show_progress=True,
        max_array_items=max_array_items,
    )


