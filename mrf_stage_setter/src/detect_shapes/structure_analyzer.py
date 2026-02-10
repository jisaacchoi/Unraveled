"""MRF JSON structure analyzer - recursively traverses JSON and stores flattened structure."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import psycopg2
import requests
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_batch
from requests.adapters import HTTPAdapter, Retry

from src.detect_shapes.enhanced_structure_analyzer import analyze_json_structure_enhanced

LOG = logging.getLogger("src.structure_analyzer")

# Pattern to detect URLs in text
URL_PATTERN = re.compile(
    r'https?://[^\s<>"{}|\\^`\[\]]+|www\.[^\s<>"{}|\\^`\[\]]+',
    re.IGNORECASE,
)

# Note: Path normalization (replacing numeric array indices with []) is now handled
# by SQL when creating schema_groups table, so Python normalization is no longer needed.


def load_sql_file(filename: str) -> str:
    """
    Load SQL query from a file in the sql/ directory.
    
    Args:
        filename: Name of the SQL file (e.g., "select_unanalyzed_payloads.sql")
        
    Returns:
        SQL query as a string
    """
    sql_path = Path(__file__).parent.parent.parent / "sql" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    with open(sql_path, "r", encoding="utf-8") as fh:
        return fh.read().strip()


def build_empty_marker_path(source_path: Path) -> Path:
    """
    Build the empty marker filename for a processed file.

    Uses the analyzed-prefix convention and inserts "empty_" after the analyzed prefix.
    Example:
      _ingested_foo.json.gz -> _ingested_analyzed_empty_foo.json.gz
    """
    from src.shared.file_prefix import add_prefix, PREFIX_ANALYZED, PREFIX_INGESTED

    analyzed_path = add_prefix(source_path, PREFIX_ANALYZED)
    filename = analyzed_path.name

    combined_prefix = PREFIX_INGESTED + PREFIX_ANALYZED[1:]  # _ingested_analyzed_
    if filename.startswith(combined_prefix):
        remainder = filename[len(combined_prefix):]
        marker_name = f"{combined_prefix}empty_{remainder}"
    elif filename.startswith(PREFIX_ANALYZED):
        remainder = filename[len(PREFIX_ANALYZED):]
        marker_name = f"{PREFIX_ANALYZED}empty_{remainder}"
    else:
        marker_name = f"empty_{filename}"

    return analyzed_path.with_name(marker_name)


def _normalize_file_name(file_name: str) -> str:
    """Normalize DB/file names to avoid .part suffix leaking into mrf_analysis."""
    return file_name[:-5] if file_name.endswith(".part") else file_name


def extract_top_level_key(level: int, path: str, key: str) -> str:
    """
    Extract the top-level key from a record.
    
    Args:
        level: Depth level (0 = top level)
        path: Full JSONPath to this key
        key: Current key name
        
    Returns:
        Top-level key name
    """
    if level == 0:
        return key
    # For nested records, extract the first part of the path (before [ or .)
    # e.g., "in_network[0].negotiated_rates" -> "in_network"
    # e.g., "in_network[0]" -> "in_network"
    top_level = path.split('[')[0].split('.')[0]
    return top_level


def has_url(value: Any) -> bool:
    """
    Check if a value contains a URL pattern.
    
    Args:
        value: Value to check (can be any JSON-serializable type)
        
    Returns:
        True if value contains a URL pattern
    """
    if value is None:
        return False
    
    # Convert to string and check for URL pattern
    value_str = json.dumps(value) if not isinstance(value, str) else value
    return bool(URL_PATTERN.search(value_str))


def get_value_dtype(value: Any) -> str:
    """
    Get the data type of a value.
    
    Args:
        value: Value to check
        
    Returns:
        'scalar', 'list', or 'dict'
    """
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return "scalar"


def calculate_dict_complexity(item: dict) -> tuple[int, int]:
    """
    Calculate a complexity score for a dict based on key count and nested list sizes.
    
    Returns:
        Tuple of (key_count, total_nested_list_items) where:
        - key_count: Number of top-level keys
        - total_nested_list_items: Sum of lengths of all nested lists (recursive)
    """
    key_count = len(item)
    total_nested_list_items = 0
    
    def count_list_items(value: Any) -> int:
        """Recursively count items in nested lists."""
        if isinstance(value, list):
            count = len(value)
            # Also count items in nested lists within this list
            for sub_item in value:
                count += count_list_items(sub_item)
            return count
        elif isinstance(value, dict):
            # Count items in lists within nested dicts
            count = 0
            for sub_value in value.values():
                count += count_list_items(sub_value)
            return count
        return 0
    
    # Count items in all nested lists
    for value in item.values():
        total_nested_list_items += count_list_items(value)
    
    return (key_count, total_nested_list_items)


def find_largest_dict_in_list(items: list) -> tuple[Any, int]:
    """
    Find the dictionary with the most keys in a list.
    If multiple dicts have the same number of keys, prefer the one with more items in nested lists.
    If no dicts are found, returns the first item in the list.
    
    Args:
        items: List of items (may contain dicts, lists, scalars, etc.)
        
    Returns:
        Tuple of (item, index) where item is the largest dict found, or (first_item, 0) if no dicts found
    """
    if not items or len(items) == 0:
        LOG.info("find_largest_dict_in_list: Empty list, returning (None, 0)")
        return (None, 0)
    
    largest_dict = None
    largest_index = 0
    max_keys = -1
    max_nested_items = -1
    dict_counts = []  # Track all dict key counts for logging
    
    LOG.info("find_largest_dict_in_list: Scanning %d items to find largest dict", len(items))
    
    for idx, item in enumerate(items):
        if isinstance(item, dict):
            key_count, nested_list_items = calculate_dict_complexity(item)
            dict_counts.append((idx, key_count, nested_list_items))
            
            # Prefer dict with more keys, or if same key count, prefer one with more nested list items
            if key_count > max_keys or (key_count == max_keys and nested_list_items > max_nested_items):
                max_keys = key_count
                max_nested_items = nested_list_items
                largest_dict = item
                largest_index = idx
    
    # Fallback: if no dicts found, return first item
    if largest_dict is None:
        LOG.info("find_largest_dict_in_list: No dictionaries found in list (out of %d items), using first item at index 0", len(items))
        return (items[0], 0)
    
    # Log summary about what was found (avoid logging all items for large lists)
    if dict_counts:
        # Calculate statistics instead of logging all items
        unique_key_counts = {}
        for _, keys, nested in dict_counts:
            key = (keys, nested)
            unique_key_counts[key] = unique_key_counts.get(key, 0) + 1
        
        # Log summary statistics
        if len(dict_counts) <= 10:
            # For small lists, show all details
            details = ", ".join([f"idx[{idx}]={keys}keys/{nested}items" for idx, keys, nested in dict_counts])
            LOG.info("find_largest_dict_in_list: Found %d dict(s) in list of %d items. Details: %s", 
                    len(dict_counts), len(items), details)
        else:
            # For large lists, show summary statistics
            LOG.info("find_largest_dict_in_list: Found %d dict(s) in list of %d items. Unique (keys/nested) combinations: %d", 
                    len(dict_counts), len(items), len(unique_key_counts))
            LOG.debug("find_largest_dict_in_list: Distribution: %s", 
                     ", ".join([f"({k[0]}keys/{k[1]}items)={v}x" for k, v in sorted(unique_key_counts.items(), key=lambda x: x[1], reverse=True)[:5]]))
        
        LOG.info("find_largest_dict_in_list: Selected dict at index %d with %d keys and %d nested list items (largest)", 
                largest_index, max_keys, max_nested_items)
    else:
        LOG.info("find_largest_dict_in_list: Selected dict at index %d with %d keys (out of %d items)", 
                largest_index, max_keys, len(items))
    
    return (largest_dict, largest_index)


def analyze_json_structure(
    data: Dict[str, Any],
    file_name: str,
    source_name: str,
    file_size: Optional[int] = None,
    level: int = 0,
    path: str = "",
    use_largest_dict: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Recursively analyze JSON structure and yield analysis records.
    
    Rules:
    - For scalars: yield and stop
    - For lists: yield, then process the largest dict item (by number of keys) if use_largest_dict=True,
                 or the first item if use_largest_dict=False
    - For dicts: yield, then process all key-value pairs
    
    Args:
        data: JSON data to analyze (dict at top level)
        file_name: Name of the source file
        source_name: Name of the source/payer
        file_size: File size in bytes (from mrf_landing)
        level: Current depth level (0 = top level)
        path: Full JSONPath to current location (e.g., "provider_references[5].provider_group_id")
        use_largest_dict: If True, find largest dict in lists; if False, use first item (for URL content)
        
    Yields:
        Dict with keys: file_name, source_name, file_size, level, path, key, value, value_dtype, url_in_value
    """
    if not isinstance(data, dict):
        LOG.warning("Top-level data is not a dict, skipping")
        return
    
    for key, value in data.items():
        value_dtype = get_value_dtype(value)
        url_in_value = has_url(value)
        
        # Build full path to this key
        if path:
            current_path = f"{path}.{key}"
        else:
            current_path = key
        
        # Convert value to jsonb-compatible format
        value_jsonb = json.dumps(value) if value is not None else None
        
        # Yield current level record
        yield {
            "file_name": file_name,
            "source_name": source_name,
            "file_size": file_size,
            "level": level,
            "path": current_path,
            "key": key,
            "value": value_jsonb,
            "value_dtype": value_dtype,
            "url_in_value": url_in_value,
        }
        
        # Recursive processing based on type
        if value_dtype == "scalar":
            # Stop at scalars
            continue
        elif value_dtype == "list":
            # For lists: process largest dict item or first item based on use_largest_dict flag
            if value and len(value) > 0:
                if use_largest_dict:
                    # Find largest dict in list
                    LOG.info("Processing list '%s' (path: %s): finding largest dict in list of %d items", 
                            key, current_path, len(value))
                    selected_item, selected_index = find_largest_dict_in_list(value)
                    LOG.info("Processing list '%s' (path: %s): selected item at index %d (largest dict with %d keys)", 
                             key, current_path, selected_index, len(selected_item) if isinstance(selected_item, dict) else 0)
                else:
                    # Use first item (for URL content analysis)
                    selected_item = value[0]
                    selected_index = 0
                    LOG.info("Processing list '%s' (path: %s): using first item at index 0 (URL content mode, list has %d items)", 
                            key, current_path, len(value))
                
                # Build path for list item: use the selected index
                list_item_path = f"{current_path}[{selected_index}]"
                
                if isinstance(selected_item, (dict, list)):
                    # Recursively process selected item (pass use_largest_dict flag down)
                    if isinstance(selected_item, dict):
                        yield from analyze_json_structure(
                            selected_item,
                            file_name,
                            source_name,
                            file_size=file_size,
                            level=level + 1,
                            path=list_item_path,
                            use_largest_dict=use_largest_dict,
                        )
                    elif isinstance(selected_item, list):
                        # Nested list: process based on use_largest_dict flag
                        if selected_item and len(selected_item) > 0:
                            if use_largest_dict:
                                nested_selected, nested_selected_index = find_largest_dict_in_list(selected_item)
                            else:
                                nested_selected = selected_item[0]
                                nested_selected_index = 0
                            nested_list_path = f"{list_item_path}[{nested_selected_index}]"
                            if isinstance(nested_selected, dict):
                                yield from analyze_json_structure(
                                    nested_selected,
                                    file_name,
                                    source_name,
                                    file_size=file_size,
                                    level=level + 1,
                                    path=nested_list_path,
                                    use_largest_dict=use_largest_dict,
                                )
        elif value_dtype == "dict":
            # For dicts: process all key-value pairs
            # Use current_path as the base for nested keys
            # Pass use_largest_dict flag down
            yield from analyze_json_structure(
                value,
                file_name,
                source_name,
                file_size=file_size,
                level=level + 1,
                path=current_path,
                use_largest_dict=use_largest_dict,
            )


def _analyze_files_from_directory(
    cursor: Any,
    batch_size: int,
    fetch_batch_size: int,
    input_directory: Path,
    analyzed_directory: Optional[Path] = None,
    max_list_items: int = 10,
    end_delete_after_analyze: bool = False,
) -> tuple[int, list[str]]:
    """
    Process files one at a time from input_directory for better performance on large tables.
    Moves each file to analyzed_directory immediately after successful analysis.
    If end_delete_after_analyze is True, deletes the analyzed file and creates an empty marker file instead.
    
    Args:
        cursor: Database cursor
        batch_size: Batch size for inserts
        fetch_batch_size: Batch size for fetching rows from database
        input_directory: Directory containing files to process
        analyzed_directory: Optional directory to move files to after analysis (if None, files are not moved)
        end_delete_after_analyze: If True, delete analyzed files and create empty marker files
        
    Returns:
        Tuple of (number of analysis records inserted, list of processed file names)
    """
    # Get list of .json.gz and .json files from input directory
    # Only process files with exactly _ingested_ prefix (not _ingested_analyzed_)
    from src.shared.file_prefix import has_exactly_prefix, PREFIX_INGESTED
    json_gz_files = [
        f for f in input_directory.glob("*.json.gz")
        if has_exactly_prefix(f, PREFIX_INGESTED)
    ]
    json_files = [
        f for f in input_directory.glob("*.json")
        if not f.name.endswith(".json.gz")  # Filter out .json files that are actually .json.gz (avoid duplicates)
        and has_exactly_prefix(f, PREFIX_INGESTED)
    ]
    json_files = json_gz_files + json_files
    
    if not json_files:
        LOG.warning("No .json.gz or .json files found in input directory: %s", input_directory)
        return 0, []
    
    LOG.info("Found %d file(s) in input directory. Processing one file at a time...", len(json_files))
    
    # Load SQL query for single file
    query_sql = load_sql_file("select_payloads_by_file_name.sql")
    
    total_records = 0
    files_processed = 0
    processed_files = []  # Track files that were actually processed (had records inserted)
    
    for json_file in json_files:
        file_name_with_prefix = json_file.name
        LOG.info("=== Processing file: %s ===", file_name_with_prefix)
        
        # Strip _ingested_ prefix to get original filename for database query
        # Database stores original filename without prefix
        from src.shared.file_prefix import remove_prefix, PREFIX_INGESTED
        original_file_name = remove_prefix(json_file, PREFIX_INGESTED).name
        LOG.debug("Original filename (for DB query): %s", original_file_name)
        
        # Execute query for this specific file using original filename and .part fallback
        file_names = [original_file_name, f"{original_file_name}.part"]
        cursor.execute(query_sql, (file_names,))
        
        # Fetch the first batch to confirm there are rows
        try:
            rows = cursor.fetchmany(fetch_batch_size)
        except psycopg2.ProgrammingError as e:
            if "no results to fetch" in str(e).lower():
                rows = []
            else:
                raise
        
        if not rows:
            LOG.info("No records found in mrf_landing for file: %s (original: %s, may already be analyzed), skipping", 
                    file_name_with_prefix, original_file_name)
            continue
        
        LOG.info("Found records in mrf_landing for file: %s (original: %s), starting analysis...", 
                file_name_with_prefix, original_file_name)
        
        # Process rows for this file
        file_record_count = 0
        batch = []
        pending_rows = rows
        
        while True:
            if not pending_rows:
                try:
                    pending_rows = cursor.fetchmany(fetch_batch_size)
                    if not pending_rows:
                        break
                except psycopg2.ProgrammingError as e:
                    if "no results to fetch" in str(e).lower():
                        break
                    raise
            
            for row in pending_rows:
                fn, source_name, file_size, record_type, record_index, payload = row
                fn_norm = _normalize_file_name(fn)
                
                LOG.debug("Analyzing structure for file: %s, record_type: %s, record_index: %d", 
                         fn, record_type, record_index)
                
                # Parse payload JSON
                if isinstance(payload, str):
                    try:
                        payload_dict = json.loads(payload)
                    except json.JSONDecodeError:
                        LOG.warning("Invalid JSON in payload for %s (record_type: %s), skipping", fn, record_type)
                        continue
                else:
                    payload_dict = payload
                
                # Record the top-level key (record_type) at level 0
                # Check if payload is a list (from array items) - this indicates it came from an array
                is_array_payload = isinstance(payload_dict, list)
                
                # If payload is a list, unwrap it to get the actual item for analysis
                if is_array_payload and len(payload_dict) == 1:
                    # Payload is wrapped in a list (from array item) - unwrap it
                    payload_dict = payload_dict[0]
                
                if isinstance(payload_dict, dict) and len(payload_dict) == 1 and list(payload_dict.keys())[0] == record_type:
                    top_level_actual_value = list(payload_dict.values())[0]
                else:
                    top_level_actual_value = payload_dict
                
                # Determine top-level dtype: arrays produce multiple rows in mrf_landing (one per item)
                # Array items are stored as [item] in the payload, so check if payload is a list first
                if is_array_payload:
                    # Payload is a list - this came from an array
                    top_level_value_dtype = "list"
                else:
                    # Check count of rows for this record_type
                    cursor.execute(
                        "SELECT COUNT(*) FROM mrf_landing WHERE file_name = %s AND record_type = %s",
                        (fn, record_type)
                    )
                    total_rows = cursor.fetchone()[0]
                    if total_rows > 1:
                        # Multiple rows = array (each array item becomes a separate row)
                        top_level_value_dtype = "list"
                    else:
                        # Single row: could be array with 1 item, object, or scalar - determine from payload
                        top_level_value_dtype = get_value_dtype(top_level_actual_value)
                
                top_level_url_in_value = has_url(top_level_actual_value)
                top_level_value_jsonb = json.dumps(top_level_actual_value) if top_level_actual_value is not None else None
                
                # For level 0, path is just record_type (no array indices), so path_group is the same
                top_level_path = record_type
                # Path normalization is handled by SQL when creating schema_groups
                top_level_path_group = top_level_path
                
                batch.append((
                    fn_norm,
                    source_name,
                    file_size,
                    0,  # Level 0
                    top_level_path,
                    top_level_path_group,
                    record_type,
                    extract_top_level_key(0, top_level_path, record_type),  # Top-level key
                    top_level_value_jsonb,
                    top_level_value_dtype,
                    top_level_url_in_value,
                ))
                file_record_count += 1
                
                # Build initial path
                initial_path = f"{record_type}[{record_index}]"
                
                # Ensure payload is a dict for analysis
                if not isinstance(payload_dict, dict):
                    if isinstance(payload_dict, list):
                        payload_dict = {"_listvalue_": payload_dict}
                    else:
                        payload_dict = {"_value_": payload_dict}
                
                # Analyze contents starting at level 1 using enhanced analyzer
                # Create new memoization cache for this file
                memo = {}
                for analysis_record in analyze_json_structure_enhanced(
                    payload_dict, fn_norm, source_name, file_size=file_size, level=1, path=initial_path,
                    max_list_items=max_list_items,                     memo=memo
                ):
                    # Path normalization is handled by SQL when creating schema_groups
                    path_group = analysis_record["path"]
                    
                    batch.append((
                        analysis_record["file_name"],
                        analysis_record["source_name"],
                        analysis_record["file_size"],
                        analysis_record["level"],
                        analysis_record["path"],
                        path_group,
                        analysis_record["key"],
                        extract_top_level_key(
                            analysis_record["level"],
                            analysis_record["path"],
                            analysis_record["key"]
                        ),
                        analysis_record["value"],
                        analysis_record["value_dtype"],
                        analysis_record["url_in_value"],
                    ))
                    file_record_count += 1
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
                        execute_batch(
                            cursor,
                            insert_sql,
                            batch,
                            page_size=batch_size,
                        )
                        total_records += len(batch)
                        LOG.debug("Inserted batch of %d analysis records (total: %d)", len(batch), total_records)
                        batch = []
            pending_rows = []
        
        # Insert remaining records for this file
        if batch:
            insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
            execute_batch(
                cursor,
                insert_sql,
                batch,
                page_size=len(batch),
            )
            total_records += len(batch)
        
        if file_record_count > 0:
            files_processed += 1
            processed_files.append(file_name_with_prefix)  # Track this file as processed
            LOG.info("Completed analysis for %s (original: %s): %d analysis records inserted", 
                    file_name_with_prefix, original_file_name, file_record_count)
            
            # Rename file with _analyzed_ prefix or delete and create an empty marker file
            source_path = input_directory / file_name_with_prefix
            if source_path.exists():
                try:
                    if end_delete_after_analyze:
                        marker_path = build_empty_marker_path(source_path)
                        LOG.info("Deleting analyzed file and creating empty marker: %s -> %s", source_path, marker_path)
                        source_path.unlink()
                        marker_path.touch(exist_ok=True)
                        LOG.info("✓ Deleted analyzed file and created empty marker: %s -> %s", file_name_with_prefix, marker_path.name)
                    else:
                        from src.shared.file_prefix import rename_with_prefix, PREFIX_ANALYZED
                        LOG.info("Renaming analyzed file with _analyzed_ prefix: %s", source_path)
                        new_path = rename_with_prefix(source_path, PREFIX_ANALYZED)
                        LOG.info("✓ Successfully renamed analyzed file: %s -> %s", file_name_with_prefix, new_path.name)
                except Exception as rename_exc:  # noqa: BLE001
                    LOG.error("✗ Failed to finalize analyzed file %s (continuing): %s", 
                             file_name_with_prefix, rename_exc)
                    LOG.exception("Full error details for finalization failure:")
            else:
                LOG.warning("File %s not found in input directory (may have already been processed): %s", 
                           file_name_with_prefix, source_path)
            LOG.info("=== Finished processing file: %s ===", file_name_with_prefix)
        else:
            LOG.info("No new records to analyze for %s (original: %s, may already be analyzed), skipping", 
                    file_name_with_prefix, original_file_name)
    
    if files_processed == 0:
        LOG.warning("No files were processed. All files may already be analyzed.")
    else:
        LOG.info("Total analysis records inserted: %d (from %d files)", total_records, files_processed)
        if end_delete_after_analyze:
            LOG.info("Deleted %d file(s) and created empty markers during processing", files_processed)
        else:
            LOG.info("Renamed %d file(s) with _analyzed_ prefix during processing", files_processed)
    
    return total_records, processed_files


def analyze_mrf_landing_records(
    connection_string: str,
    batch_size: int = 1000,
    fetch_batch_size: int = 100,
    input_directory: Optional[Path] = None,
    analyzed_directory: Optional[Path] = None,
    max_list_items: int = 10,
    end_delete_after_analyze: bool = False,
) -> tuple[int, list[str]]:
    """
    Analyze JSON structures from mrf_landing and insert into mrf_analysis table.
    
    Gets one payload per (file_name, record_type) combination for analysis,
    using the record with the largest payload_size as representative of that record type's structure.
    
    If input_directory is provided, processes files one at a time from that directory
    (more efficient for large mrf_landing tables). Otherwise, queries all unanalyzed records.
    
    Args:
        connection_string: PostgreSQL connection string
        batch_size: Batch size for inserts
        fetch_batch_size: Batch size for fetching rows from database
        input_directory: Optional directory path to get file list from (processes files one at a time)
        
    Returns:
        Tuple of (number of analysis records inserted, list of processed file names)
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # If input_directory is provided, process files one at a time
        if input_directory and input_directory.exists() and input_directory.is_dir():
            total_records, processed_files = _analyze_files_from_directory(
                cursor,
                batch_size,
                fetch_batch_size,
                input_directory,
                analyzed_directory,
                max_list_items,
                end_delete_after_analyze,
            )
            return total_records, processed_files
        
        # Otherwise, use the original approach (query all unanalyzed records)
        # Load SQL query from file
        query_sql = load_sql_file("get_payloads_for_analysis.sql")
        LOG.info("Executing query to get payloads from mrf_landing table (all unanalyzed records)...")
        LOG.debug("Query: %s", query_sql[:200] + "..." if len(query_sql) > 200 else query_sql)
        cursor.execute(query_sql)
        
        # Check if cursor has results before trying to fetch
        if cursor.description is None:
            LOG.warning("Query returned no result set. This might indicate an empty mrf_landing table or a query issue.")
            # Try to check if table exists and has data
            try:
                cursor.execute("SELECT COUNT(*) FROM mrf_landing")
                count = cursor.fetchone()[0]
                LOG.info("Total records in mrf_landing table: %d", count)
                if count == 0:
                    LOG.warning("mrf_landing table is empty. No records to analyze.")
                else:
                    LOG.warning("mrf_landing table has %d records, but query returned no results. Check query logic.", count)
            except Exception as check_exc:  # noqa: BLE001
                LOG.warning("Could not check mrf_landing table count: %s", check_exc)
            return 0, []
        
        total_records = 0
        processed_files_set = set()  # Track unique files that were processed
        
        # Process rows in batches to avoid loading everything into memory
        LOG.info("Starting to fetch and process records in batches of %d...", fetch_batch_size)
        while True:
            try:
                rows = cursor.fetchmany(fetch_batch_size)
                if not rows:
                    LOG.debug("No more rows to fetch. Processed %d files, %d analysis records", 
                             len(processed_files_set), total_records)
                    break
            except psycopg2.ProgrammingError as e:
                if "no results to fetch" in str(e).lower():
                    LOG.info("Cursor exhausted. Processed %d files, %d analysis records", 
                            len(processed_files_set), total_records)
                    break
                # Re-raise if it's a different ProgrammingError
                LOG.error("Unexpected ProgrammingError: %s", e)
                raise
            
            for row in rows:
                fn, source_name, file_size, record_type, record_index, payload = row
                
                fn_norm = _normalize_file_name(fn)
                processed_files_set.add(fn_norm)  # Track this file
                LOG.info("Analyzing structure for file: %s, record_type: %s, record_index: %d, file_size: %d bytes", fn_norm, record_type, record_index, file_size or 0)
                
                # Parse payload JSON
                if isinstance(payload, str):
                    try:
                        payload_dict = json.loads(payload)
                    except json.JSONDecodeError:
                        LOG.warning("Invalid JSON in payload for %s (record_type: %s), skipping", fn, record_type)
                        continue
                else:
                    payload_dict = payload
                
                # First, record the top-level key (record_type) itself as level 0
                # This represents the top-level key in the JSON file (e.g., "in_network", "provider_references")
                batch = []
                record_count = 0
                
                # Determine the value and dtype for the top-level key
                # The payload is the value of this top-level key
                # For arrays: payload is the largest dict item (ingestion extracts largest dict)
                # For objects: payload is the object itself
                # For scalars: payload is wrapped in a dict like {key: value}
                
                # Get the actual value for the top-level key
                # Check if payload is a list (from array items) - this indicates it came from an array
                is_array_payload = isinstance(payload_dict, list)
                
                # If payload is a list, unwrap it to get the actual item for analysis
                if is_array_payload and len(payload_dict) == 1:
                    # Payload is wrapped in a list (from array item) - unwrap it
                    payload_dict = payload_dict[0]
                
                # If payload is wrapped (scalar case), unwrap it
                if isinstance(payload_dict, dict) and len(payload_dict) == 1 and list(payload_dict.keys())[0] == record_type:
                    # Scalar wrapped in dict - unwrap it
                    top_level_actual_value = list(payload_dict.values())[0]
                else:
                    # Array item or object - use payload as-is
                    top_level_actual_value = payload_dict
                
                # Determine top-level dtype: arrays produce multiple rows in mrf_landing (one per item)
                # Array items are stored as [item] in the payload, so check if payload is a list first
                if is_array_payload:
                    # Payload is a list - this came from an array
                    top_level_value_dtype = "list"
                else:
                    # Check count of rows for this record_type
                    cursor.execute(
                        "SELECT COUNT(*) FROM mrf_landing WHERE file_name = %s AND record_type = %s",
                        (fn, record_type)
                    )
                    total_rows = cursor.fetchone()[0]
                    if total_rows > 1:
                        # Multiple rows = array (each array item becomes a separate row)
                        top_level_value_dtype = "list"
                    else:
                        # Single row: could be array with 1 item, object, or scalar - determine from payload
                        top_level_value_dtype = get_value_dtype(top_level_actual_value)
                
                top_level_url_in_value = has_url(top_level_actual_value)
                top_level_value_jsonb = json.dumps(top_level_actual_value) if top_level_actual_value is not None else None
                
                # For level 0, path is just record_type (no array indices), so path_group is the same
                top_level_path = record_type
                # Path normalization is handled by SQL when creating schema_groups
                top_level_path_group = top_level_path
                
                # Record the top-level key at level 0
                batch.append((
                    fn_norm,
                    source_name,
                    file_size,  # File size in bytes
                    0,  # Level 0 for top-level key
                    top_level_path,  # Path is just the key name for top-level
                    top_level_path_group,  # Path group (same as path for level 0)
                    record_type,  # Key name
                    extract_top_level_key(0, top_level_path, record_type),  # Top-level key
                    top_level_value_jsonb,
                    top_level_value_dtype,
                    top_level_url_in_value,
                ))
                record_count += 1
                
                # Now analyze the contents of the payload starting at level 1
                # Keys inside the payload are one level deeper than the top-level key
                # Build initial path: for arrays, include [record_index], for objects/scalars just the key name
                if record_index > 0:
                    initial_path = f"{record_type}[{record_index}]"
                else:
                    initial_path = f"{record_type}[{record_index}]"
                
                # Ensure payload is a dict for analysis (wrap if needed)
                if not isinstance(payload_dict, dict):
                    LOG.warning("Payload for %s (record_type: %s) is not a dict (type: %s), wrapping in dict", fn, record_type, type(payload_dict))
                    # Use _listvalue_ for lists, _value_ for other types
                    if isinstance(payload_dict, list):
                        payload_dict = {"_listvalue_": payload_dict}
                    else:
                        payload_dict = {"_value_": payload_dict}
                
                # Analyze contents starting at level 1 (one level down from top-level key) using enhanced analyzer
                # Create new memoization cache for this file
                memo = {}
                for analysis_record in analyze_json_structure_enhanced(
                    payload_dict, fn_norm, source_name, file_size=file_size, level=1, path=initial_path,
                    max_list_items=max_list_items,                     memo=memo
                ):
                    # Path normalization is handled by SQL when creating schema_groups
                    path_group = analysis_record["path"]
                    
                    batch.append((
                        analysis_record["file_name"],
                        analysis_record["source_name"],
                        analysis_record["file_size"],
                        analysis_record["level"],
                        analysis_record["path"],
                        path_group,
                        analysis_record["key"],
                        extract_top_level_key(
                            analysis_record["level"],
                            analysis_record["path"],
                            analysis_record["key"]
                        ),
                        analysis_record["value"],
                        analysis_record["value_dtype"],
                        analysis_record["url_in_value"],
                    ))
                    record_count += 1
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
                        execute_batch(
                            cursor,
                            insert_sql,
                            batch,
                            page_size=batch_size,
                        )
                        total_records += len(batch)
                        LOG.debug("Inserted batch of %d analysis records (total: %d)", len(batch), total_records)
                        batch = []
                
                # Insert remaining records for this file
                if batch:
                    insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
                    execute_batch(
                        cursor,
                        insert_sql,
                        batch,
                        page_size=len(batch),
                    )
                    total_records += len(batch)
                
                LOG.info("Analyzed %s (record_type: %s): %d analysis records", fn, record_type, record_count)
        
        processed_files = sorted(list(processed_files_set))
        if len(processed_files) == 0:
            LOG.warning("No files were processed. Check if mrf_landing table has data.")
        else:
            LOG.info("Total analysis records inserted: %d (from %d files)", total_records, len(processed_files))
        return total_records, processed_files
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error analyzing mrf_landing records: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def create_mrf_analysis_table(connection_string: str, drop_if_exists: bool = False) -> None:
    """
    Create the mrf_analysis table if it doesn't exist.
    
    Args:
        connection_string: PostgreSQL connection string
        drop_if_exists: If True, drop the table before creating (useful for schema changes)
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Drop table if it exists and drop_if_exists is True
        if drop_if_exists:
            cursor.execute("DROP TABLE IF EXISTS mrf_analysis CASCADE;")
            LOG.info("Dropped existing mrf_analysis table")
        
        # Load and execute DDL
        ddl_path = Path(__file__).parent.parent.parent / "sql" / "create_mrf_analysis_table.sql"
        
        if not ddl_path.exists():
            LOG.warning("DDL file not found at %s, skipping table creation", ddl_path)
            return
        
        with open(ddl_path, "r", encoding="utf-8") as fh:
            ddl_sql = fh.read()
        
        cursor.execute(ddl_sql)
        LOG.info("Ensured mrf_analysis table exists")

    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error creating mrf_analysis table: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def extract_url_from_value(value: Any) -> Optional[str]:
    """
    Extract the first URL from a JSON value.
    
    Args:
        value: JSON value (can be string, dict, list, etc.)
        
    Returns:
        First URL found, or None if no URL found
    """
    if value is None:
        return None
    
    if isinstance(value, str):
        # Direct string - check if it's a URL
        match = URL_PATTERN.search(value)
        if match:
            return match.group(0)
        return None
    
    # For complex types, convert to string and search
    value_str = json.dumps(value)
    match = URL_PATTERN.search(value_str)
    if match:
        return match.group(0)
    
    return None


def get_urls_to_download(
    connection_string: str,
) -> List[Tuple[str, str, Optional[int], str, str, str]]:
    """
    Query mrf_analysis for records with URLs and return one URL per (file_name, path) group.
    
    Groups by path to distinguish between keys with the same name at different locations in the JSON structure.
    Path provides full context (e.g., "provider_references[0].provider_group_id").
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        List of tuples: (file_name, source_name, file_size, path, key, url) where:
        - file_size is the file size in bytes (from mrf_landing)
        - path is the full path where the URL was found (e.g., "provider_references[0].location")
        - key is the key name that contains the URL (e.g., "location")
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Load SQL query from file
        query_sql = load_sql_file("select_urls_from_analysis.sql")
        cursor.execute(query_sql)
        
        urls_to_download = []
        for row in cursor.fetchall():
            fn, source_name, file_size, path, key, level, value = row
            url = extract_url_from_value(value)
            if url:
                # path is the full path where URL was found (e.g., "provider_references[0].location")
                # key is the key name containing the URL (e.g., "location")
                urls_to_download.append((fn, source_name, file_size, path, key, url))
        
        return urls_to_download
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying URLs from mrf_analysis: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def download_and_analyze_url(
    connection_string: str,
    file_name: str,
    source_name: str,
    file_size: Optional[int],
    parent_path: str,
    parent_key: str,
    url: str,
    download_path: Optional[Path] = None,  # Deprecated: kept for compatibility but not used
    batch_size: int = 1000,
) -> int:
    """
    Download a URL, parse its content in memory, and analyze the structure.
    Downloaded content is processed in memory and discarded after analysis (not saved to disk).
    
    Treats the downloaded content as a regular nested dictionary, continuing the path
    from where the URL was found. For example, if URL was at "provider_references[0].location",
    the content will be analyzed with paths like "provider_references[0].location.provider_group_id".
    
    Args:
        connection_string: PostgreSQL connection string
        file_name: Original file name (to link back)
        source_name: Source/payer identifier
        parent_path: Full path where the URL was found (e.g., "provider_references[0].location")
        parent_key: The key name that contained the URL (e.g., "location")
        url: URL to download and analyze
        download_path: Deprecated - kept for backward compatibility but not used (content is not saved to disk)
        batch_size: Batch size for inserts
        
    Returns:
        Number of analysis records inserted
    """
    conn = None
    cursor = None
    sess = None
    
    try:
        # Create session with retries
        sess = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        sess.mount("https://", HTTPAdapter(max_retries=retry))
        sess.mount("http://", HTTPAdapter(max_retries=retry))
        
        LOG.info("Downloading URL for %s (path: %s): %s", file_name, parent_path, url)
        
        # Download content to memory (no disk storage)
        resp = sess.get(url, timeout=30)
        resp.raise_for_status()
        
        LOG.debug("Downloaded URL content (%d bytes), processing in memory", len(resp.content))
        
        # Try to parse as JSON
        try:
            content = resp.json()
        except json.JSONDecodeError:
            # If not JSON, try to parse as text and wrap in dict
            content_text = resp.text
            LOG.warning("URL content is not JSON, storing as text: %s", url)
            content = {"_raw_content": content_text}
        
        # Ensure content is a dict for analysis
        if not isinstance(content, dict):
            # Use _listvalue_ for lists, _value_ for other types
            if isinstance(content, list):
                content = {"_listvalue_": content}
            else:
                content = {"_value_": content}
        
        # Analyze the downloaded content as a regular nested dictionary
        # Continue the path from where the URL was found
        # parent_path is the full path to the URL (e.g., "provider_references[0].location")
        # So content will be analyzed with paths like "provider_references[0].location.provider_group_id"
        # Use use_largest_dict=False to just use first items in lists (don't search for largest)
        analysis_records = []
        for record in analyze_json_structure(
            content,
            file_name=file_name,  # Link back to original file
            source_name=source_name,
            file_size=file_size,  # File size from original file
            level=0,
            path=parent_path,  # Continue from the path where URL was found
            use_largest_dict=False,  # For URL content, use first item instead of finding largest
        ):
            # Path normalization is handled by SQL when creating schema_groups
            path_group = record["path"]
            
            analysis_records.append((
                record["file_name"],
                record["source_name"],
                record["file_size"],
                record["level"],
                record["path"],
                path_group,
                record["key"],
                extract_top_level_key(
                    record["level"],
                    record["path"],
                    record["key"]
                ),
                record["value"],
                record["value_dtype"],
                record["url_in_value"],
            ))
        
        if not analysis_records:
            LOG.warning("No analysis records generated from URL: %s", url)
            return 0
        
        # Insert into database
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Batch insert
        insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
        total_inserted = 0
        for i in range(0, len(analysis_records), batch_size):
            batch = analysis_records[i:i + batch_size]
            execute_batch(
                cursor,
                insert_sql,
                batch,
                page_size=batch_size,
            )
            total_inserted += len(batch)
            LOG.debug("Inserted batch of %d analysis records from URL (total: %d)", len(batch), total_inserted)
        
        LOG.info("Analyzed URL content: %d records inserted for %s (path: %s)", total_inserted, file_name, parent_path)
        return total_inserted
        
    except requests.RequestException as exc:
        LOG.warning("Failed to download URL %s for %s (path: %s): %s", url, file_name, parent_path, exc)
        return 0
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error downloading/analyzing URL %s for %s (path: %s): %s", url, file_name, parent_path, exc)
        return 0
    finally:
        if sess:
            sess.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def analyze_urls_from_analysis(
    connection_string: str,
    download_path: Optional[Path] = None,
    batch_size: int = 1000,
    max_url_downloads: Optional[int] = None,
) -> int:
    """
    Find URLs in mrf_analysis, download one per (file_name, path) group, and analyze their structure.
    
    Groups by path to distinguish between keys with the same name at different locations in the JSON structure.
    Path provides full context (e.g., "provider_references[0].provider_group_id").
    
    Args:
        connection_string: PostgreSQL connection string
        download_path: Optional directory path to save downloaded URL content files
        batch_size: Batch size for inserts
        max_url_downloads: Maximum number of successful URL downloads to process (None = no limit)
        
    Returns:
        Total number of analysis records inserted from URLs
    """
    LOG.info("Finding URLs in mrf_analysis table...")
    urls_to_download = get_urls_to_download(connection_string)
    
    if not urls_to_download:
        LOG.info("No URLs found in mrf_analysis table")
        return 0
    
    LOG.info("Found %d unique (file_name, path) combinations with URLs", len(urls_to_download))
    if max_url_downloads is not None:
        LOG.info("Limiting to %d successful URL download(s) for structure analysis", max_url_downloads)
    
    total_inserted = 0
    successful_downloads = 0
    for fn, source_name, file_size, path, key, url in urls_to_download:
        # Check if we've reached the limit of successful downloads
        if max_url_downloads is not None and successful_downloads >= max_url_downloads:
            LOG.info("Reached max_url_downloads limit (%d successful downloads), stopping URL analysis", max_url_downloads)
            break
        
        try:
            # path is the full path where URL was found (e.g., "provider_references[0].location")
            # key is the key name containing the URL (e.g., "location")
            # Pass both to continue the path naturally
            inserted = download_and_analyze_url(
                connection_string=connection_string,
                file_name=fn,
                source_name=source_name,
                file_size=file_size,  # File size from original file
                parent_path=path,  # Full path where URL was found
                parent_key=key,  # Key name containing the URL
                url=url,
                download_path=download_path,
                batch_size=batch_size,
            )
            total_inserted += inserted
            successful_downloads += 1
            LOG.info("Successfully processed URL %d/%s for %s (path: %s): %d records inserted", 
                    successful_downloads, 
                    max_url_downloads if max_url_downloads else "?", 
                    fn, path, inserted)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed to process URL for %s (path: %s): %s (continuing with next URL)", fn, path, exc)
            continue
    
    LOG.info("Completed URL analysis: %d total records inserted from %d successful download(s) (out of %d attempted)", 
            total_inserted, successful_downloads, len(urls_to_download))
    return total_inserted


def get_table_signature(connection_string: str) -> tuple[int, int]:
    """
    Get a signature of the mrf_analysis table state: (total_rows, distinct_files).
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        Tuple of (total_rows, distinct_files)
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM mrf_analysis")
        total_rows = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT file_name) FROM mrf_analysis")
        distinct_files = cursor.fetchone()[0]
        
        return (total_rows, distinct_files)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_shape_analysis(
    connection_string: str,
    batch_size: int = 100,
    fetch_batch_size: int = 10000,
    download_path: Optional[Path] = None,
    input_directory: Optional[Path] = None,
    analyzed_directory: Optional[Path] = None,
    max_list_items: int = 10,
    max_url_downloads: Optional[int] = None,
    end_delete_after_analyze: bool = False,
) -> tuple[int, int, list[str]]:
    """
    Run full shape analysis: analyze JSON structures and URLs.
    
    Args:
        connection_string: PostgreSQL connection string
        batch_size: Batch size for inserts
        fetch_batch_size: Batch size for fetching rows from database
        download_path: Optional directory path (deprecated, kept for compatibility)
        input_directory: Optional directory path to get file list from (processes files one at a time for better performance)
        analyzed_directory: Optional directory to move files to after analysis (files moved one at a time)
        max_list_items: Maximum number of items to sample from lists when analyzing structure (default: 10)
        max_url_downloads: Maximum number of successful URL downloads to process (None = no limit, recommended: 10 for testing)
        end_delete_after_analyze: If True, delete analyzed files and create empty marker files
        
    Returns:
        Tuple of (total_records, url_records, processed_files) where:
        - total_records: Number of analysis records from JSON structures
        - url_records: Number of analysis records from URLs
        - processed_files: List of file names that were analyzed in this run
    """
    # Analyze structures from mrf_landing
    LOG.info("Analyzing JSON structures from mrf_landing table...")
    total_records, processed_files = analyze_mrf_landing_records(
        connection_string=connection_string,
        batch_size=batch_size,
        fetch_batch_size=fetch_batch_size,
        input_directory=input_directory,
        analyzed_directory=analyzed_directory,
        max_list_items=max_list_items,
        end_delete_after_analyze=end_delete_after_analyze,
    )

    LOG.info("Successfully analyzed structures: %d analysis records inserted from %d files", total_records, len(processed_files))
    
    # Analyze URLs found in the analysis
    LOG.info("Analyzing URLs found in mrf_analysis...")
    url_records = analyze_urls_from_analysis(
        connection_string=connection_string,
        download_path=download_path,
        batch_size=batch_size,
        max_url_downloads=max_url_downloads,
    )
    
    LOG.info("Total analysis complete: %d records from JSON structures, %d records from URLs, %d files processed", 
             total_records, url_records, len(processed_files))
    return total_records, url_records, processed_files
