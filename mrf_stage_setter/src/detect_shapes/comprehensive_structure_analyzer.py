"""Comprehensive structure analyzer that processes all items to build complete structure."""
from __future__ import annotations

import json
import logging
import psycopg2
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from psycopg2.extras import execute_batch

from src.detect_shapes.enhanced_structure_analyzer import (
    analyze_json_structure_enhanced,
    count_unique_keys_in_structure,
    get_value_dtype,
    has_url,
)

LOG = logging.getLogger("src.comprehensive_structure_analyzer")


def load_sql_file(filename: str) -> str:
    """Load SQL file from sql directory."""
    sql_path = Path(__file__).parent.parent.parent / "sql" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def extract_top_level_key(level: int, path: str, key: str) -> str:
    """
    Extract top-level key from path.
    
    For level 0, returns the key itself.
    For level > 0, extracts the top-level key from the path.
    """
    if level == 0:
        return key
    
    # Path format: "record_type[index].key1.key2..."
    # Extract record_type
    if "." in path:
        return path.split(".")[0].split("[")[0]
    return path.split("[")[0]


def _normalize_file_name(file_name: str) -> str:
    """Normalize file name by removing .part suffix if present."""
    if file_name.endswith(".part"):
        return file_name[:-5]
    return file_name


def analyze_all_items_comprehensively(
    cursor: Any,
    file_name: str,
    record_type: str,
    all_items: List[Tuple[str, str, int, str, int, Any, bool]],  # (file_name, source_name, file_size, record_type, record_index, payload, has_rare_keys)
    file_size: int,
    source_name: str,
    max_list_items: int = 10,
    top_level_is_array: bool = True,
) -> Iterator[Dict[str, Any]]:
    """
    Analyze all items for a record_type to build comprehensive structure.
    
    Processes items with rare keys first, then common items, combining
    all discovered keys into a comprehensive structure.
    
    Args:
        cursor: Database cursor
        file_name: Normalized file name
        record_type: Record type (top-level key name)
        all_items: List of all items for this record_type
        file_size: File size in bytes
        source_name: Source/payer identifier
        max_list_items: Maximum items to sample from nested lists
        
    Yields:
        Analysis records (dicts with file_name, source_name, etc.)
    """
    # Separate items with rare keys vs common items
    rare_key_items = [(idx, item) for idx, item in enumerate(all_items) if item[6]]  # has_rare_keys is index 6
    common_items = [(idx, item) for idx, item in enumerate(all_items) if not item[6]]
    
    LOG.info("Analyzing %d item(s) for record_type '%s': %d with rare keys, %d common",
             len(all_items), record_type, len(rare_key_items), len(common_items))
    
    # Track all keys discovered across all items
    comprehensive_keys: Set[str] = set()
    memo = {}  # Memoization cache shared across all items
    
    def _yield_item_analysis_records(payload_item: Any, item_idx: int) -> Iterator[Dict[str, Any]]:
        """
        Yield analysis records for a single top-level array item.

        Emits an explicit container row for the array element path
        (e.g., "in_network[0]") so top-level array type and element type
        are represented separately in mrf_analysis.
        """
        initial_path = f"{record_type}[{item_idx}]" if top_level_is_array else record_type
        item_value_dtype = get_value_dtype(payload_item)

        if top_level_is_array:
            # Explicit array-element container row (e.g., in_network[0]).
            # This keeps "in_network" (list) separate from "in_network[0]" (dict/scalar/list).
            yield {
                "file_name": file_name,
                "source_name": source_name,
                "file_size": file_size,
                "level": 1,
                "path": initial_path,
                "key": record_type,
                "value": json.dumps(payload_item) if payload_item is not None else None,
                "value_dtype": item_value_dtype,
                "url_in_value": has_url(payload_item),
            }

        # Nested traversal starts one level below the explicit element row.
        if item_value_dtype == "dict":
            yield from analyze_json_structure_enhanced(
                payload_item,
                file_name,
                source_name,
                file_size=file_size,
                level=2 if top_level_is_array else 1,
                path=initial_path,
                max_list_items=max_list_items,
                memo=memo,
            )
        elif item_value_dtype == "list":
            # Keep list elements traversable even when an array item itself is a list.
            yield from analyze_json_structure_enhanced(
                {"_listvalue_": payload_item},
                file_name,
                source_name,
                file_size=file_size,
                level=2 if top_level_is_array else 1,
                path=initial_path,
                max_list_items=max_list_items,
                memo=memo,
            )

    # Process rare key items first (they're comprehensive)
    for item_idx, (fn, src, fs, rt, rec_idx, payload, has_rare) in rare_key_items:
        LOG.debug("Processing rare key item %d/%d for record_type '%s'", item_idx + 1, len(rare_key_items), record_type)
        
        # Parse payload
        if isinstance(payload, str):
            try:
                payload_dict = json.loads(payload)
            except json.JSONDecodeError:
                LOG.warning("Invalid JSON in payload for %s (record_type: %s, item: %d), skipping", fn, rt, item_idx)
                continue
        else:
            payload_dict = payload
        
        # Unwrap if payload is wrapped in list
        if isinstance(payload_dict, list) and len(payload_dict) == 1:
            payload_dict = payload_dict[0]
        
        # Count keys in this item
        item_keys, _ = count_unique_keys_in_structure(payload_dict, f"{record_type}[{item_idx}]", memo)
        comprehensive_keys.update(item_keys)
        
        # Analyze this top-level array item with explicit item container row.
        yield from _yield_item_analysis_records(payload_dict, item_idx)
    
    # Process common items, but only if they have keys we haven't seen
    for item_idx, (fn, src, fs, rt, rec_idx, payload, has_rare) in common_items:
        # Parse payload
        if isinstance(payload, str):
            try:
                payload_dict = json.loads(payload)
            except json.JSONDecodeError:
                continue
        else:
            payload_dict = payload
        
        # Unwrap if payload is wrapped in list
        if isinstance(payload_dict, list) and len(payload_dict) == 1:
            payload_dict = payload_dict[0]
        
        # Check if this item has new keys
        item_keys, _ = count_unique_keys_in_structure(payload_dict, f"{record_type}[{item_idx}]", memo)
        new_keys = item_keys - comprehensive_keys
        
        if new_keys:
            LOG.debug("Common item %d has %d new key(s): %s", item_idx, len(new_keys), sorted(list(new_keys))[:5])
            comprehensive_keys.update(new_keys)
            
            # Analyze this top-level array item with explicit item container row.
            yield from _yield_item_analysis_records(payload_dict, item_idx)
    
    LOG.info("Comprehensive analysis complete for record_type '%s': discovered %d unique keys across all items",
             record_type, len(comprehensive_keys))


def _analyze_files_from_directory_comprehensive(
    cursor: Any,
    batch_size: int,
    fetch_batch_size: int,
    input_directory: Path,
    max_list_items: int = 10,
    end_delete_after_analyze: bool = False,
) -> tuple[int, list[str]]:
    """
    Process files one at a time, analyzing ALL items comprehensively.
    
    Files are renamed in-place with _analyzed_ prefix after successful analysis.
    
    Args:
        cursor: Database cursor
        batch_size: Batch size for inserts
        fetch_batch_size: Batch size for fetching rows from database
        input_directory: Directory containing files to process
        max_list_items: Maximum items to sample from nested lists
        end_delete_after_analyze: If True, delete analyzed files and create empty marker files
        
    Returns:
        Tuple of (number of analysis records inserted, list of processed file names)
    """
    from src.shared.file_prefix import has_exactly_prefix, PREFIX_INGESTED
    
    json_gz_files = [
        f for f in input_directory.glob("*.json.gz")
        if has_exactly_prefix(f, PREFIX_INGESTED)
    ]
    json_files = [
        f for f in input_directory.glob("*.json")
        if not f.name.endswith(".json.gz")
        and has_exactly_prefix(f, PREFIX_INGESTED)
    ]
    json_files = json_gz_files + json_files
    
    if not json_files:
        LOG.warning("No .json.gz or .json files found in input directory: %s", input_directory)
        return 0, []
    
    LOG.info("Found %d file(s) in input directory. Processing one file at a time...", len(json_files))
    
    # Load SQL query for all items
    query_sql = load_sql_file("select_all_payloads_by_file_name.sql")
    
    total_records = 0
    files_processed = 0
    processed_files = []
    
    for json_file in json_files:
        file_name_with_prefix = json_file.name
        LOG.info("=== Processing file: %s ===", file_name_with_prefix)
        
        # Strip _ingested_ prefix to get original filename
        from src.shared.file_prefix import remove_prefix, PREFIX_INGESTED
        original_file_name = remove_prefix(json_file, PREFIX_INGESTED).name
        LOG.debug("Original filename (for DB query): %s", original_file_name)
        
        # Execute query for this specific file
        file_names = [original_file_name, f"{original_file_name}.part"]
        
        # First, check if file exists in mrf_landing at all
        check_landing_sql = "SELECT COUNT(*) FROM mrf_landing WHERE file_name = ANY(%s)"
        cursor.execute(check_landing_sql, (file_names,))
        landing_count = cursor.fetchone()[0]
        
        # Check if file is already analyzed
        check_analyzed_sql = """
            SELECT COUNT(DISTINCT key) 
            FROM mrf_analysis 
            WHERE file_name = ANY(%s) AND level = 0
        """
        cursor.execute(check_analyzed_sql, (file_names,))
        analyzed_record_types = cursor.fetchone()[0]
        
        # Now execute the main query (excludes already analyzed)
        cursor.execute(query_sql, (file_names,))
        
        # Fetch all rows
        try:
            rows = cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            if "no results to fetch" in str(e).lower():
                rows = []
            else:
                raise
        
        if not rows:
            if landing_count == 0:
                LOG.warning("File not found in mrf_landing: %s (original: %s) - file may not have been ingested yet", 
                        file_name_with_prefix, original_file_name)
            elif analyzed_record_types > 0:
                LOG.info("File already analyzed in mrf_analysis: %s (original: %s, %d record type(s) analyzed), skipping",
                        file_name_with_prefix, original_file_name, analyzed_record_types)
                # Rename file with _analyzed_ prefix if it doesn't already have it
                from src.shared.file_prefix import add_prefix, PREFIX_ANALYZED, has_prefix
                if not has_prefix(json_file, PREFIX_ANALYZED):
                    try:
                        analyzed_path = add_prefix(json_file, PREFIX_ANALYZED)
                        if analyzed_path != json_file and not analyzed_path.exists():
                            json_file.rename(analyzed_path)
                            LOG.info("Renamed already-analyzed file: %s -> %s", json_file.name, analyzed_path.name)
                        elif analyzed_path.exists():
                            LOG.debug("File %s already has _analyzed_ prefix or target exists, skipping rename", json_file.name)
                    except Exception as rename_exc:  # noqa: BLE001
                        LOG.warning("Failed to rename already-analyzed file %s: %s", json_file.name, rename_exc)
            else:
                LOG.warning("File exists in mrf_landing (%d records) but no unanalyzed records found for: %s (original: %s)", 
                        landing_count, file_name_with_prefix, original_file_name)
            continue
        
        LOG.info("Found %d record(s) in mrf_landing for file: %s (original: %s), starting comprehensive analysis...",
                len(rows), file_name_with_prefix, original_file_name)

        # Group rows by record_type
        rows_by_record_type: Dict[str, List[Tuple]] = {}
        for row in rows:
            record_type = row[3]  # record_type is index 3
            if record_type not in rows_by_record_type:
                rows_by_record_type[record_type] = []
            rows_by_record_type[record_type].append(row)
        
        LOG.info("Grouped into %d record_type(s): %s", len(rows_by_record_type), list(rows_by_record_type.keys()))
        
        # Process each record_type comprehensively
        file_record_count = 0
        batch = []
        
        for record_type, type_rows in rows_by_record_type.items():
            LOG.info("Processing record_type '%s' with %d item(s)...", record_type, len(type_rows))
            
            # Get file metadata from first row
            fn, source_name, file_size, rt, rec_idx, payload, has_rare_keys = type_rows[0]
            # Normalize file name: remove .part suffix and ensure no prefixes
            # Use original_file_name from outer scope to ensure consistency (database stores original names)
            fn_norm = _normalize_file_name(original_file_name)
            
            # Record top-level structure (level 0).
            # Do NOT infer arrays from row count: duplicate ingestions can create
            # multiple rows for scalar keys and incorrectly force list dtype.
            
            # Get representative payload for top-level analysis
            # Use first rare key item if available, otherwise first item
            representative_row = None
            for row in type_rows:
                if row[6]:  # has_rare_keys
                    representative_row = row
                    break
            if not representative_row:
                representative_row = type_rows[0]
            
            rep_fn, rep_src, rep_fs, rep_rt, rep_rec_idx, rep_payload, rep_has_rare = representative_row
            
            # Parse representative payload
            if isinstance(rep_payload, str):
                try:
                    rep_payload_dict = json.loads(rep_payload)
                except json.JSONDecodeError:
                    LOG.warning("Invalid JSON in representative payload for %s (record_type: %s), skipping", fn, record_type)
                    continue
            else:
                rep_payload_dict = rep_payload
            
            # Track whether payload was originally a list before optional unwrapping.
            rep_payload_was_list = isinstance(rep_payload_dict, list)

            # Unwrap if wrapped in list
            if isinstance(rep_payload_dict, list) and len(rep_payload_dict) == 1:
                rep_payload_dict = rep_payload_dict[0]
            
            # Check mrf_landing to determine if this record_type is actually an array
            # This is necessary because during ingestion, only specific items are extracted,
            # so we might only have 1 row even though it's an array
            is_array_from_landing = False
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM mrf_landing 
                    WHERE file_name = %s 
                      AND record_type = %s 
                      AND array_start_offset IS NOT NULL
                    LIMIT 1
                """, (fn_norm, record_type))
                has_array_offset = cursor.fetchone()[0] > 0
                if has_array_offset:
                    is_array_from_landing = True
                    LOG.debug("Record_type '%s' is an array (array_start_offset found in mrf_landing)", record_type)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Error checking array_start_offset for %s/%s: %s (continuing)", fn_norm, record_type, exc)
            
            # Determine top-level value_dtype
            # If mrf_landing says it's an array, treat it as array regardless of payload type
            if is_array_from_landing:
                top_level_value_dtype = "list"
                LOG.debug("Record_type '%s' forced to 'list' based on array_start_offset in mrf_landing", record_type)
            elif rep_payload_was_list:
                # Single-row files can still represent arrays (e.g., payload wrapped as [item]).
                top_level_value_dtype = "list"
                LOG.debug("Record_type '%s' forced to 'list' because representative payload was a list", record_type)
            else:
                # Single row: determine from payload
                top_level_value_dtype = get_value_dtype(rep_payload_dict)
            
            top_level_url_in_value = has_url(rep_payload_dict)
            top_level_value_jsonb = json.dumps(rep_payload_dict) if rep_payload_dict is not None else None
            
            # Level 0 record
            top_level_path = record_type
            top_level_path_group = top_level_path
            
            batch.append((
                fn_norm,
                source_name,
                file_size,
                0,  # Level 0
                top_level_path,
                top_level_path_group,
                record_type,
                extract_top_level_key(0, top_level_path, record_type),
                top_level_value_jsonb,
                top_level_value_dtype,
                top_level_url_in_value,
            ))
            file_record_count += 1
            
            # Analyze all items comprehensively
            for analysis_record in analyze_all_items_comprehensively(
                cursor,
                fn_norm,
                record_type,
                type_rows,
                file_size,
                source_name,
                max_list_items=max_list_items,
                top_level_is_array=(top_level_value_dtype == "list"),
            ):
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
                    execute_batch(cursor, insert_sql, batch)
                    total_records += len(batch)
                    LOG.debug("Inserted batch of %d analysis records (total: %d)", len(batch), total_records)
                    batch = []
        
        # Insert remaining batch
        if batch:
            insert_sql = load_sql_file("insert_mrf_analysis_record.sql")
            execute_batch(cursor, insert_sql, batch)
            total_records += len(batch)
            LOG.info("Inserted final batch of %d analysis records for file %s", len(batch), file_name_with_prefix)
        
        files_processed += 1
        processed_files.append(file_name_with_prefix)
        LOG.info("Completed comprehensive analysis for file %s: %d analysis records inserted",
                file_name_with_prefix, file_record_count)
        
        # Rename file in-place with _analyzed_ prefix
        from src.shared.file_prefix import add_prefix, PREFIX_ANALYZED, has_prefix
        if not has_prefix(json_file, PREFIX_ANALYZED):
            try:
                analyzed_path = add_prefix(json_file, PREFIX_ANALYZED)
                if analyzed_path != json_file and not analyzed_path.exists():
                    json_file.rename(analyzed_path)
                    LOG.info("Renamed analyzed file: %s -> %s", json_file.name, analyzed_path.name)
                elif analyzed_path.exists():
                    LOG.debug("File %s already has _analyzed_ prefix or target exists, skipping rename", json_file.name)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to rename analyzed file %s: %s", json_file.name, exc)
    
    if files_processed == 0:
        LOG.warning("No files were processed. All files may already be analyzed.")
    else:
        LOG.info("Total analysis records inserted: %d (from %d files)", total_records, files_processed)
    
    return total_records, processed_files


def run_comprehensive_shape_analysis(
    connection_string: str,
    batch_size: int = 1000,
    fetch_batch_size: int = 10000,
    download_path: Optional[Path] = None,
    input_directory: Optional[Path] = None,
    max_list_items: int = 10,
    max_url_downloads: Optional[int] = None,
) -> tuple[int, int, list[str]]:
    """
    Run comprehensive shape analysis on mrf_landing records.
    
    Analyzes ALL items for each record_type to build comprehensive structure,
    prioritizing items with rare keys but including common items for completeness.
    
    Files are renamed in-place with _analyzed_ prefix after successful analysis.
    
    Args:
        connection_string: PostgreSQL connection string
        batch_size: Batch size for inserts
        fetch_batch_size: Batch size for fetching rows
        download_path: Optional directory for URL downloads (deprecated)
        input_directory: Optional directory to get file list from
        max_list_items: Maximum items to sample from nested lists
        max_url_downloads: Maximum URL downloads (deprecated)
        
    Returns:
        Tuple of (total_records, url_records, processed_files)
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        
        if input_directory:
            total_records, processed_files = _analyze_files_from_directory_comprehensive(
                cursor,
                batch_size,
                fetch_batch_size,
                input_directory,
                max_list_items,
                end_delete_after_analyze=False,
            )
        else:
            LOG.warning("No input_directory specified. Comprehensive analysis requires input_directory.")
            return -1, 0, []
        
        conn.commit()
        return total_records, 0, processed_files
    
    except Exception as exc:  # noqa: BLE001
        if conn:
            conn.rollback()
        LOG.exception("Error running comprehensive shape analysis: %s", exc)
        return -1, 0, []
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
