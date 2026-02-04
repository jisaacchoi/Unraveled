"""Core ingestion logic for streaming MRF files into PostgreSQL."""
from __future__ import annotations

import csv
import io
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple

import ijson
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from tqdm import tqdm

from src.shared.json_reader import is_json_file, open_json_file


LOG = logging.getLogger("src.data_ingester")


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
    
    with open_json_file(file_path) as fh:
        # Parse JSON stream and extract records
        parser = ijson.items(fh, ijson_path)
        
        for record in parser:
            yield record


def extract_all_records(
    file_path: Path,
) -> Iterator[Tuple[str, int, dict]]:
    """
    Dynamically extract all top-level keys (arrays, objects, and scalars).
    
    This function uses a two-phase approach optimized for efficiency:
    1. Quick detection pass: Scans only the structure (not data) to find all top-level keys
    2. Extraction pass: Extracts values from each detected key
    
    For arrays: Extracts each item as a separate record
    For objects: Extracts the object as a single record
    For scalars: Extracts the scalar value wrapped in a dict
    
    The detection pass is very lightweight and stops early, so the overhead is minimal.
    This ensures we don't miss any keys while maintaining good performance.
    
    Args:
        file_path: Path to .json or .json.gz file
        
    Yields:
        Tuples of (record_type, record_index, record_dict) where:
        - record_type is the top-level key name (e.g., 'in_network', 'provider_references', 'reporting_entity_name')
        - record_index is the zero-based index (0 for non-arrays, index within array for arrays)
        - record_dict is the actual record data (array item, object, or scalar wrapped in dict)
    """
    # Phase 1: Quick detection pass - identify all top-level keys and their types
    top_level_keys = []  # List of (key_name, key_type) where type is 'array', 'object', or 'scalar'
    current_key = None
    key_start_depth = 0
    depth = 0
    
    LOG.debug("Detecting top-level keys in %s...", file_path.name)
    with open_json_file(file_path) as fh:
        parser = ijson.parse(fh)
        for prefix, event, value in parser:
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
            elif event == "map_key" and depth == 1:
                current_key = value
                key_start_depth = depth
            elif event == "start_array" and depth == 1 and current_key:
                top_level_keys.append((current_key, "array"))
                current_key = None
            elif event in ("string", "number", "boolean", "null") and depth == 1 and current_key:
                top_level_keys.append((current_key, "scalar"))
                current_key = None
    
    if not top_level_keys:
        LOG.warning("No top-level keys found in %s", file_path.name)
        return
    
    LOG.info("Detected top-level keys in %s: %s", file_path.name, [(k, t) for k, t in top_level_keys])
    
    # Phase 2: Extract values from each key based on type
    for key_name, key_type in top_level_keys:
        try:
            # Use key name directly as record_type (no normalization)
            record_type = key_name
            
            if key_type == "array":
                # Extract only the first item from the array for memory efficiency
                # Wrap it in a list so the payload represents an array (for correct dtype detection)
                record_path = f".{key_name}[]"
                LOG.debug("Extracting first item from '%s' array...", key_name)
                try:
                    # Get only the first item
                    first_record = next(extract_records_from_json(file_path, record_path), None)
                    if first_record is not None:
                        # Wrap in list so payload represents an array (for correct dtype in mrf_analysis)
                        payload = [first_record]
                        yield (record_type, 0, payload)
                        LOG.debug("Extracted first item from '%s' array (wrapped in list)", key_name)
                    else:
                        LOG.warning("Array '%s' was detected but is empty", key_name)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Error extracting first item from '%s' array: %s", key_name, exc)
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
    # Get file size in bytes
    file_size = file_path.stat().st_size
    LOG.debug("File size: %d bytes (%.2f MB)", file_size, file_size / (1024 * 1024))
    
    conn = None
    cursor = None
    
    try:
        LOG.info("Connecting to database...")
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
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
            if batch_data.tell() == 0:
                return  # Empty batch, nothing to write
            batch_data.seek(0)
            cursor.copy_expert(
                """
                COPY mrf_landing(source_name, file_name, file_size, record_type, record_index, payload)
                FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '"')
                """,
                batch_data,
            )
            batch_data.seek(0)
            batch_data.truncate(0)
        
        try:
            for record_type, record_index, record in extract_all_records(file_path):
                # Convert Decimal objects to float for JSON serialization
                record_converted = convert_decimals(record)
                # Convert record to JSON string for jsonb storage
                record_json = json.dumps(record_converted, ensure_ascii=False)
                
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
                
                # Write row: source_name, file_name, file_size, record_type, record_index, payload
                # The CSV writer will properly escape quotes and special characters
                batch_writer.writerow([
                    source_name,
                    file_name,
                    str(file_size),  # File size in bytes
                    record_type,
                    str(record_index),
                    record_json,
                ])
                record_count += 1
                
                # Update progress bar less frequently (every 100 records) to reduce output
                if show_progress and record_count % 100 == 0:
                    pbar.update(100)
                
                # Write batch to database when it reaches batch_size
                if record_count % batch_size == 0:
                    write_batch_to_db(batch_buffer)
                    # Only log every 10 batches to reduce log noise
                    if (record_count // batch_size) % 10 == 0:
                        LOG.debug("Wrote batch (total: %d records)", record_count)
            
            # Write remaining records
            if record_count % batch_size != 0:
                write_batch_to_db(batch_buffer)
                LOG.debug("Wrote final batch (%d total records)", record_count)
            
            if show_progress:
                # Update progress bar to final count and close
                remaining = record_count % 100
                if remaining > 0:
                    pbar.update(remaining)
                pbar.close()
            
            if record_count == 0:
                LOG.warning("No records extracted from %s", file_name)
                return 1
            
            LOG.info(
                "Successfully ingested %s: %d records (source: %s)",
                file_name,
                record_count,
                source_name,
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
    )


def move_ingested_file(file_path: Path, output_dir: Path) -> None:
    """
    Move an ingested file into the configured output directory.
    
    Args:
        file_path: Path to the file to move
        output_dir: Directory to move the file to
    """
    import shutil
    
    # Directory creation handled by ensure_directories_from_config()
    destination = output_dir / file_path.name
    counter = 1
    while destination.exists():
        destination = output_dir / f"{file_path.stem}_{counter}{''.join(file_path.suffixes)}"
        counter += 1

    shutil.move(str(file_path), destination)
    LOG.info("Moved %s -> %s", file_path, destination)