"""Core logic for converting .json.gz files to .ndjson format."""
from __future__ import annotations

import gzip
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Optional

import ijson
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

LOG = logging.getLogger("src.json_to_ndjson")


def load_sql_file(filename: str) -> str:
    """
    Load SQL query from a file in the sql/ directory.
    
    Args:
        filename: Name of the SQL file (e.g., "select_top_level_array_keys.sql")
        
    Returns:
        SQL query as string
    """
    sql_dir = Path(__file__).parent.parent.parent / "sql"
    sql_path = sql_dir / filename
    
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(sql_path, "r", encoding="utf-8") as fh:
        return fh.read().strip()


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


def extract_array_items(file_path: Path, array_path: str) -> Iterator[dict]:
    """
    Stream items from a JSON array in a gzipped file.
    
    Args:
        file_path: Path to .json.gz file
        array_path: JSONPath to array (e.g., ".in_network[]")
        
    Yields:
        Individual items from the array as dictionaries
    """
    # Normalize array_path for ijson
    # ijson uses paths like "in_network.item" for arrays
    ijson_path = array_path.strip(".").replace("[]", ".item")
    
    with gzip.open(file_path, "rb") as fh:
        parser = ijson.items(fh, ijson_path)
        for item in parser:
            yield item


def detect_top_level_arrays_from_db(
    connection_string: str,
    file_name: str,
) -> Optional[list[tuple[str, str]]]:
    """
    Detect top-level arrays from mrf_analysis table.
    
    Queries the database to find which top-level keys are arrays (value_dtype = 'list').
    This is faster than scanning the file.
    
    Args:
        connection_string: PostgreSQL connection string
        file_name: Name of the file to query
        
    Returns:
        List of tuples (key_name, array_path) for each top-level array found, or None if not in DB
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if mrf_analysis table exists first
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'mrf_analysis'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            LOG.debug("mrf_analysis table does not exist, falling back to file scanning")
            return None
        
        # Query mrf_analysis for top-level arrays (level = 0, value_dtype = 'list')
        sql_query = load_sql_file("select_top_level_array_keys.sql")
        cursor.execute(sql_query, (file_name,))
        
        rows = cursor.fetchall()
        if not rows:
            # File not in database, return None to fall back to file scanning
            LOG.debug("File %s not found in mrf_analysis table, falling back to file scanning", file_name)
            return None
        
        arrays = [(row[0], f".{row[0]}[]") for row in rows]
        LOG.debug("Found %d top-level array(s) from database: %s", len(arrays), [name for name, _ in arrays])
        return arrays
        
    except psycopg2.Error as exc:
        LOG.debug("Database query failed, falling back to file scanning: %s", exc)
        return None
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Error querying database, falling back to file scanning: %s", exc)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def detect_top_level_arrays(file_path: Path) -> list[tuple[str, str]]:
    """
    Detect top-level arrays in a JSON file by scanning the file.
    
    This is a fallback method when database information is not available.
    
    Args:
        file_path: Path to .json.gz file
        
    Returns:
        List of tuples (key_name, array_path) for each top-level array found
    """
    arrays = []
    current_key = None
    depth = 0
    
    with gzip.open(file_path, "rb") as fh:
        parser = ijson.parse(fh)
        for prefix, event, value in parser:
            if event == "start_map":
                depth += 1
            elif event == "end_map":
                depth -= 1
                if depth == 0:
                    # Reached end of root object
                    break
            elif event == "map_key" and depth == 1:
                current_key = value
            elif event == "start_array" and depth == 1 and current_key:
                arrays.append((current_key, f".{current_key}[]"))
                current_key = None
    
    return arrays


def convert_json_gz_to_ndjson(
    input_path: Path,
    output_path: Path,
    connection_string: Optional[str] = None,
    skip_if_not_in_db: bool = False,
) -> int:
    """
    Convert a .json.gz file to .ndjson format.
    
    This function handles JSON objects with:
    - Top-level arrays: extracts items from arrays and writes each as a line
    - No arrays: writes the entire object as one line
    
    Args:
        input_path: Path to input .json.gz file
        output_path: Path to output .ndjson file
        connection_string: Optional PostgreSQL connection string to query mrf_analysis for array detection
        skip_if_not_in_db: If True, skip files that are not in the database instead of falling back to file scanning
        
    Returns:
        0 on success, 1 on error (2 if skipped because not in DB)
    """
    if not input_path.exists():
        LOG.error("Input file not found: %s", input_path)
        return 1
    
    if not input_path.suffixes == [".json", ".gz"]:
        LOG.error("Input file must be .json.gz: %s", input_path)
        return 1
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        LOG.info("Converting %s -> %s", input_path.name, output_path.name)
        
        # Root is always a JSON object - detect top-level arrays
        LOG.debug("Root is an object, detecting top-level arrays...")
        # Try database first (faster), fall back to file scanning if not available
        arrays = None
        if connection_string:
            file_name = input_path.name
            LOG.debug("Querying database for array information...")
            arrays = detect_top_level_arrays_from_db(connection_string, file_name)
            
            if arrays is None and skip_if_not_in_db:
                # File not in database and we're configured to skip
                LOG.warning("Skipping %s: file not found in database (skip_files_not_in_db=true)", file_name)
                return 2
        
        if arrays is None:
            # Fall back to file scanning (only if skip_if_not_in_db is False or no connection_string)
            if skip_if_not_in_db and connection_string:
                # This shouldn't happen due to check above, but just in case
                LOG.warning("Skipping %s: file not found in database", input_path.name)
                return 2
            
            LOG.info("Using file scanning to detect arrays (database not available or file not in DB)")
            arrays = detect_top_level_arrays(input_path)
        
        if arrays:
            # Extract items from arrays
            LOG.info("Found %d top-level array(s): %s", len(arrays), [name for name, _ in arrays])
            record_count = 0
            arrays_processed = 0
            with open(output_path, "w", encoding="utf-8") as out_fh:
                for array_name, array_path in arrays:
                    try:
                        LOG.info("Extracting items from '%s' array (path: %s)...", array_name, array_path)
                        array_item_count = 0
                        for item in extract_array_items(input_path, array_path):
                            item_converted = convert_decimals(item)
                            json_line = json.dumps(item_converted, ensure_ascii=False)
                            out_fh.write(json_line + "\n")
                            record_count += 1
                            array_item_count += 1
                            
                            # Log progress every 10000 items
                            if array_item_count % 10000 == 0:
                                LOG.info("Extracted %d items from '%s' array so far...", array_item_count, array_name)
                        
                        arrays_processed += 1
                        LOG.info("Extracted %d items from '%s' array (completed %d/%d arrays)", 
                                array_item_count, array_name, arrays_processed, len(arrays))
                    except Exception as exc:  # noqa: BLE001
                        LOG.exception("Error extracting items from '%s' array: %s", array_name, exc)
                        # Continue with next array instead of failing completely
                        continue
            
            LOG.info("Converted %d items from %d/%d array(s) to %s", record_count, arrays_processed, len(arrays), output_path.name)
            return 0
        else:
            # No arrays found - write entire object as one line
            LOG.info("No arrays found, writing entire object as single line...")
            LOG.warning("Loading entire file into memory - this may be slow for large files")
            with gzip.open(input_path, "rb") as fh:
                obj = json.load(fh)
                LOG.debug("File loaded, converting and writing...")
                obj_converted = convert_decimals(obj)
                json_line = json.dumps(obj_converted, ensure_ascii=False)
                with open(output_path, "w", encoding="utf-8") as out_fh:
                    out_fh.write(json_line + "\n")
            
            LOG.info("Converted entire object to %s", output_path.name)
            return 0
            
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error converting %s to NDJSON: %s", input_path, exc)
        return 1


def convert_file(
    input_path: Path,
    output_dir: Path,
    connection_string: Optional[str] = None,
) -> int:
    """
    Convert a single .json.gz file to .ndjson format.
    
    Args:
        input_path: Path to input .json.gz file
        output_dir: Directory to write output .ndjson file
        connection_string: Optional PostgreSQL connection string for database-based array detection
        
    Returns:
        0 on success, 1 on error
    """
    if not input_path.exists():
        LOG.error("Input file not found: %s", input_path)
        return 1
    
    if not input_path.suffixes == [".json", ".gz"]:
        LOG.error("Input file must be .json.gz: %s", input_path)
        return 1
    
    # Generate output filename: same name but with .ndjson extension
    # e.g., "file.json.gz" -> "file.ndjson"
    output_filename = input_path.stem.replace(".json", "") + ".ndjson"
    output_path = output_dir / output_filename
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    result = convert_json_gz_to_ndjson(input_path, output_path, connection_string=connection_string)
    
    if result == 0:
        LOG.info("Successfully converted %s -> %s", input_path.name, output_path.name)
    else:
        LOG.error("Failed to convert %s", input_path.name)
    
    return result
