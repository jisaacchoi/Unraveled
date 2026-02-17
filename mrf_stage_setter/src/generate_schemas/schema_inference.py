"""Schema inference and file operations for schema JSON."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from src.generate_schemas.schema_groups_db import (
    get_schema_groups,
    hash_schema_sig_to_folder_name,
)

LOG = logging.getLogger("src.schema_inference")

SchemaType = dict | str
SchemaField = dict


def _resolve_value_dtype(existing_dtype: str, incoming_dtype: str) -> str:
    """
    Resolve conflicting mrf_analysis value_dtype values for the same path/key.

    Prefer container shapes over scalar, and prefer list when both list/dict
    are observed so top-level arrays are not downgraded by stale rows.
    """
    if existing_dtype == incoming_dtype:
        return existing_dtype

    priority = {"scalar": 0, "dict": 1, "list": 2}
    existing_rank = priority.get(existing_dtype, -1)
    incoming_rank = priority.get(incoming_dtype, -1)
    return incoming_dtype if incoming_rank > existing_rank else existing_dtype


def _make_field(name: str, field_type: SchemaType, nullable: bool = True) -> SchemaField:
    return {
        "name": name,
        "type": field_type,
        "nullable": nullable,
        "metadata": {},
    }


def _make_struct(fields: List[SchemaField]) -> SchemaType:
    return {
        "type": "struct",
        "fields": fields,
    }


def _make_array(element_type: SchemaType, contains_null: bool = True) -> SchemaType:
    return {
        "type": "array",
        "elementType": element_type,
        "containsNull": contains_null,
    }


def _make_map(key_type: SchemaType, value_type: SchemaType, value_contains_null: bool = True) -> SchemaType:
    return {
        "type": "map",
        "keyType": key_type,
        "valueType": value_type,
        "valueContainsNull": value_contains_null,
    }


def array(element_type: SchemaType) -> SchemaType:
    """Compatibility helper for legacy calls."""
    return _make_array(element_type)


def field(name: str, field_type: SchemaType, nullable: bool = True) -> SchemaField:
    """Compatibility helper for legacy calls."""
    return _make_field(name, field_type, nullable)


def string() -> SchemaType:
    """Compatibility helper for legacy calls."""
    return "string"


def struct(fields: List[SchemaField]) -> SchemaType:
    """Compatibility helper for legacy calls."""
    return _make_struct(fields)


def _is_struct(schema_type: SchemaType) -> bool:
    return isinstance(schema_type, dict) and schema_type.get("type") == "struct"


def _get_struct_fields(schema_type: SchemaType) -> List[SchemaField]:
    if _is_struct(schema_type):
        return schema_type.get("fields", [])
    return []


def schema_to_json(schema: SchemaType) -> str:
    return json.dumps(schema, ensure_ascii=True)


def _merge_types(left: SchemaType, right: SchemaType) -> SchemaType:
    if left == right:
        return left

    # Prefer widening numeric types over strings when possible
    numeric = {"integer", "long", "double"}
    if isinstance(left, str) and isinstance(right, str):
        if left in numeric and right in numeric:
            if "double" in (left, right):
                return "double"
            if "long" in (left, right):
                return "long"
            return "integer"
        # Any other mismatch defaults to string
        return "string"

    if _is_struct(left) and _is_struct(right):
        left_fields = {f["name"]: f for f in _get_struct_fields(left)}
        right_fields = {f["name"]: f for f in _get_struct_fields(right)}
        merged_fields = []
        for name in sorted(set(left_fields) | set(right_fields)):
            if name in left_fields and name in right_fields:
                merged_type = _merge_types(left_fields[name]["type"], right_fields[name]["type"])
                merged_fields.append(_make_field(name, merged_type))
            elif name in left_fields:
                merged_fields.append(left_fields[name])
            else:
                merged_fields.append(right_fields[name])
        return _make_struct(merged_fields)

    if isinstance(left, dict) and left.get("type") == "array" and isinstance(right, dict) and right.get("type") == "array":
        merged_element = _merge_types(left.get("elementType", "string"), right.get("elementType", "string"))
        return _make_array(merged_element, contains_null=True)

    # Fallback when types are incompatible
    return "string"


def save_schema_to_file(
    schema: SchemaType,
    output_path: Path,
) -> None:
    """
    Save schema JSON to a file.
    
    Args:
        schema: Schema in Spark JSON format
        output_path: Path to save schema JSON file
    """
    # Convert schema to JSON-serializable format
    schema_json = schema_to_json(schema)
    
    # Directory creation handled by ensure_directories_from_config()
    
    # Write schema to file
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(schema_json)
    
    LOG.info("Saved schema to %s", output_path)


def load_schema_from_file(
    schema_path: Path,
) -> SchemaType:
    """
    Load schema JSON from a file.
    
    Args:
        schema_path: Path to schema JSON file
        
    Returns:
        Schema in Spark JSON format
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, "r", encoding="utf-8") as fh:
        schema_json = fh.read()
    
    # Parse schema from JSON
    schema = json.loads(schema_json)

    LOG.info("Loaded schema from %s", schema_path)
    return schema


def min_file_name_to_schema_filename(min_file_name: str) -> str:
    """
    Convert min_file_name to schema filename.
    
    Args:
        min_file_name: Name of the min file (e.g., "file.json.gz" or "file.ndjson")
        
    Returns:
        Schema filename with .schema.json extension (e.g., "file.json.gz.schema.json")
    """
    return f"{min_file_name}.schema.json"


def schema_sig_to_schema_filename(schema_sig: str) -> str:
    """
    Convert schema_sig to schema filename using the group hash prefix.
    
    Args:
        schema_sig: Schema signature string
        
    Returns:
        Schema filename in format: group_<hash>__schema.json
    """
    group_prefix = hash_schema_sig_to_folder_name(schema_sig)
    return f"{group_prefix}__schema.json"


def file_name_to_schema_filename(file_name: str) -> str:
    """
    Convert file_name to schema filename.
    
    Args:
        file_name: Original file name (e.g., "file.json.gz")
        
    Returns:
        Schema filename in format: "<file>_schema.json"
    """
    return f"{file_name}_schema.json"


def generate_and_save_schemas(
    connection_string: str,
    input_directory: Path,
    schema_output_directory: Path,
    refresh_schema_groups: bool = False,
    analyzed_directory: Path | None = None,
) -> tuple[Dict[str, Path], list[str]]:
    """
    Generate schemas for all schema groups and save them to files.
    
    For each schema group:
    1. Generates schema from database tables (mrf_analysis/mrf_landing)
    2. Saves schema to a JSON file named after min_file_name (e.g., "file.json.gz.schema.json")
    3. Moves the min_file to analyzed_directory immediately after schema is generated (if provided)
    
    Args:
        connection_string: PostgreSQL connection string (used for schema_groups query, optional for file conversion)
        input_directory: Directory containing .json.gz files (or .ndjson files)
        schema_output_directory: Directory to save schema JSON files
        refresh_schema_groups: If True, refresh schema_groups table before querying
        analyzed_directory: Optional directory to move min_files to immediately after schema generation
        
    Returns:
        Tuple of (dictionary mapping min_file_name to schema file path, list of processed min_file_names)
    """
    # Get schema groups from database
    schema_groups = get_schema_groups(connection_string, refresh=refresh_schema_groups)
    
    if not schema_groups:
        LOG.warning("No schema groups found in database")
        return {}, []
    
    # Directory creation handled by ensure_directories_from_config()
    
    schema_files = {}  # Maps min_file_name -> schema_path
    processed_file_names = []
    
    LOG.info("Generating schemas for %d schema group(s)...", len(schema_groups))
    
    for schema_sig, min_file_name, min_file_name_size, has_urls, file_count in schema_groups:
        if not min_file_name:
            LOG.warning("No minimum file found for schema group: %s", schema_sig[:100])
            continue
        
        LOG.info("Processing schema group (signature: %s...)", schema_sig[:100])
        LOG.info("  Min file: %s (size: %s bytes, %d files in group)", 
                min_file_name, 
                min_file_name_size if min_file_name_size is not None else "unknown",
                file_count)
        
        try:
            # Generate schema from database tables (mrf_analysis / mrf_landing)
            LOG.info("Generating schema for %s from database tables", min_file_name)
            schema = generate_schema_from_mrf_analysis(connection_string, min_file_name)
            if not schema:
                LOG.error("No schema generated from mrf_analysis for %s (no fallback)", min_file_name)
                continue
            
            # Generate filename from min_file_name
            schema_filename = min_file_name_to_schema_filename(min_file_name)
            schema_path = schema_output_directory / schema_filename
            
            # Skip if schema file already exists (but still track the file as processed and move it)
            if schema_path.exists():
                LOG.info("Schema file already exists, skipping: %s", schema_path)
                schema_files[min_file_name] = schema_path
                # Still track the min_file_name that was processed
                processed_file_names.append(min_file_name)
                
                # Move min_file to analyzed_directory even if schema already existed
                if analyzed_directory:
                    try:
                        from src.shared.file_mover import move_files_to_analyzed
                        moved_count, failed_count = move_files_to_analyzed(
                            input_directory, [min_file_name], analyzed_directory
                        )
                        if moved_count > 0:
                            LOG.info("Moved min file %s to analyzed directory (schema already existed)", min_file_name)
                        elif failed_count > 0:
                            LOG.warning("Failed to move min file %s to analyzed directory", min_file_name)
                    except Exception as exc:  # noqa: BLE001
                        LOG.warning("Error moving min file %s to analyzed directory (continuing): %s", min_file_name, exc)
                
                continue
            
            # Save schema to file
            save_schema_to_file(schema, schema_path)
            schema_files[min_file_name] = schema_path
            
            # Track the min_file_name that was processed (use original .json.gz name)
            processed_file_names.append(min_file_name)
            
            LOG.info("Saved schema for group to %s", schema_path)
            
            # Move min_file to analyzed_directory immediately after schema is generated
            if analyzed_directory:
                try:
                    from src.shared.file_mover import move_files_to_analyzed
                    moved_count, failed_count = move_files_to_analyzed(
                        input_directory, [min_file_name], analyzed_directory
                    )
                    if moved_count > 0:
                        LOG.info("Moved min file %s to analyzed directory", min_file_name)
                    elif failed_count > 0:
                        LOG.warning("Failed to move min file %s to analyzed directory", min_file_name)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Error moving min file %s to analyzed directory (continuing): %s", min_file_name, exc)
            
        except Exception as exc:  # noqa: BLE001
            LOG.exception("Error generating schema for group %s: %s", schema_sig[:100], exc)
            continue
        finally:
            pass
    
    LOG.info("Generated %d schema(s) out of %d schema group(s)", len(schema_files), len(schema_groups))
    return schema_files, processed_file_names


def load_schema_by_min_file_name(
    min_file_name: str,
    schema_directory: Path,
) -> Optional[SchemaType]:
    """
    Load a schema from a file by min_file_name.
    
    Args:
        min_file_name: Name of the min file (e.g., "file.json.gz" or "file.ndjson")
        schema_directory: Directory containing schema JSON files
        
    Returns:
        Schema in Spark JSON format, or None if schema file not found
    """
    schema_filename = min_file_name_to_schema_filename(min_file_name)
    schema_path = schema_directory / schema_filename
    
    if not schema_path.exists():
        LOG.warning(
            "Schema file not found for min_file_name %s (looked for %s)",
            min_file_name,
            schema_path,
        )
        # List available schema files for debugging
        if schema_directory.exists():
            available_schemas = list(schema_directory.glob("*.schema.json"))
            LOG.warning(
                "Available schema files in %s: %d files. First few: %s",
                schema_directory,
                len(available_schemas),
                [f.name for f in available_schemas[:5]],
            )
        return None
    
    try:
        return load_schema_from_file(schema_path)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error loading schema from %s: %s", schema_path, exc)
        return None


def infer_type_from_value(value: Any) -> SchemaType:
    """
    Infer schema type from a Python value.
    
    Args:
        value: Python value (dict, list, str, int, float, bool, None)
        
    Returns:
        Schema type in Spark JSON format
    """
    if value is None:
        return "string"  # Nullable, default to string
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        # Check if it fits in IntegerType range
        if -2147483648 <= value <= 2147483647:
            return "integer"
        else:
            return "long"
    elif isinstance(value, float):
        return "double"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        if not value:
            # Empty list - default to array(string)
            return _make_array("string")
        # Infer element type from first item
        element_type = infer_type_from_value(value[0])
        return _make_array(element_type)
    elif isinstance(value, dict):
        # Build struct from dict keys
        fields = []
        for key, val in value.items():
            field_type = infer_type_from_value(val)
            fields.append(_make_field(str(key), field_type))
        return _make_struct(fields)
    else:
        # Unknown type, default to string
        return "string"


def generate_schema_from_mrf_analysis(
    connection_string: str,
    file_name: str,
) -> Optional[SchemaType]:
    """
    Generate a schema from mrf_analysis table.
    
    Queries mrf_analysis table for the given file_name and builds schema recursively
    using the pre-analyzed structure information (path, level, value_dtype, value).
    
    This is more efficient than parsing JSONB payloads from mrf_landing because:
    - Structure is already analyzed and flattened
    - Type information (scalar/list/dict) is pre-determined
    - Nested paths are already extracted
    
    Args:
        connection_string: PostgreSQL connection string
        file_name: File name to query structure for
        
    Returns:
        Schema in Spark JSON format, or None if file not found
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Query mrf_analysis for this file_name, ordered by level and path
        # This gives us a hierarchical view of the structure
        # Use top_level_key to easily group nested records by their top-level key
        cursor.execute("""
            SELECT level, path, path_group, key, top_level_key, value, value_dtype
            FROM mrf_analysis
            WHERE file_name = %s
            ORDER BY top_level_key, level, path
        """, (file_name,))
        
        rows = cursor.fetchall()
        if not rows:
            LOG.warning("No records found in mrf_analysis for file: %s", file_name)
            return None
        
        LOG.info("Found %d analysis record(s) for file %s", len(rows), file_name)
        
        # Log record distribution by level
        level_counts = {}
        for level, path, path_group, key, top_level_key, value_jsonb, value_dtype in rows:
            level_counts[level] = level_counts.get(level, 0) + 1
        LOG.info("Record distribution by level: %s", level_counts)
        
        # Log sample records to understand the structure
        LOG.info("Sample records (first 15):")
        for i, (level, path, path_group, key, top_level_key, value_jsonb, value_dtype) in enumerate(rows[:15]):
            value_preview = str(value_jsonb)[:100] if value_jsonb else None
            LOG.info("  [%d] level=%d, top_level_key='%s', path='%s', key='%s', value_dtype='%s'", 
                     i, level, top_level_key, path, key, value_dtype)
        
        # Build schema from level 0 records (top-level keys)
        # Group records by top_level_key for easier processing
        top_level_records = {}  # key -> (value_dtype, value, nested_records)
        all_nested_records = []  # All nested records (level > 0) for recursive processing
        
        # Debug: show distribution of records by level
        level_counts = {}
        for level, path, path_group, key, top_level_key, value_jsonb, value_dtype in rows:
            level_counts[level] = level_counts.get(level, 0) + 1
        LOG.debug("Record distribution by level: %s", level_counts)
        
        for level, path, path_group, key, top_level_key, value_jsonb, value_dtype in rows:
            if level == 0:
                # Top-level key. Merge duplicates robustly across reruns/stale rows.
                if key not in top_level_records:
                    top_level_records[key] = {
                        'value_dtype': value_dtype,
                        'value': value_jsonb,
                        'nested': []
                    }
                else:
                    resolved_dtype = _resolve_value_dtype(
                        top_level_records[key]['value_dtype'],
                        value_dtype,
                    )
                    top_level_records[key]['value_dtype'] = resolved_dtype
                    # Keep a representative value aligned with resolved dtype when possible.
                    if resolved_dtype == value_dtype:
                        top_level_records[key]['value'] = value_jsonb
            else:
                # Nested record - collect all nested records for recursive processing
                nested_record = {
                    'level': level,
                    'path': path,
                    'path_group': path_group,
                    'key': key,
                    'top_level_key': top_level_key,
                    'value': value_jsonb,
                    'value_dtype': value_dtype
                }
                all_nested_records.append(nested_record)
                
                # Add to top-level key's nested list using top_level_key for grouping
                if top_level_key in top_level_records:
                    top_level_records[top_level_key]['nested'].append(nested_record)
        
        # Build Spark schema fields from top-level records
        fields = []
        
        for key, record_info in top_level_records.items():
            value_dtype = record_info['value_dtype']
            value = record_info['value']
            nested = record_info['nested']
            
            try:
                if value_dtype == 'scalar':
                    # Infer type from value
                    field_type = infer_type_from_value(value)
                    LOG.debug("Top-level key '%s' is scalar with type %s", key, field_type)
                
                elif value_dtype == 'list':
                    # Build array from nested records or first element
                    # Use top_level_key to find all nested records for this key
                    # Look for records at level 1 with matching top_level_key and path starting with "{key}[" (array elements)
                    element_records = [r for r in all_nested_records if r['top_level_key'] == key and r['level'] == 1 and r['path'].startswith(f"{key}[")]
                    LOG.info("Top-level key '%s' (list): found %d element record(s) at level 1", key, len(element_records))
                    if element_records:
                        # Log what we found
                        for er in element_records[:3]:  # Log first 3
                            LOG.info("  Element record: level=%d, path='%s', key='%s', value_dtype='%s'", 
                                    er['level'], er['path'], er['key'], er['value_dtype'])
                        # Build element type from nested structure (recursively handles all levels)
                        # Pass all_nested_records so recursion can find deeper levels
                        LOG.info("Building nested type for '%s' array elements (recursively)...", key)
                        element_type = _build_type_from_nested_records(element_records, all_nested_records, level=1)
                        field_type = array(element_type)
                        LOG.info("Top-level key '%s' (list): built element type %s", key, element_type)
                    elif nested:
                        # Fallback to nested list if all_nested_records didn't have matches
                        element_records = [r for r in nested if r['level'] == 1 and r['path'].startswith(f"{key}[")]
                        if element_records:
                            element_type = _build_type_from_nested_records(element_records, all_nested_records, level=1)
                            field_type = array(element_type)
                        elif isinstance(value, list) and value:
                            element_type = infer_type_from_value(value[0])
                            field_type = array(element_type)
                        else:
                            field_type = array(string())
                    elif isinstance(value, list) and value:
                        # Use value directly if available
                        element_type = infer_type_from_value(value[0])
                        field_type = array(element_type)
                    elif isinstance(value, dict):
                        # Defensive fallback: stale list dtype can exist when historical
                        # analysis misclassified duplicated scalar keys as arrays.
                        # If no element records are present, treat object payload as struct.
                        LOG.warning(
                            "Top-level key '%s' marked list but has dict payload and no element records; "
                            "treating as struct fallback",
                            key,
                        )
                        field_type = struct(
                            [_make_field(k, infer_type_from_value(v)) for k, v in value.items()]
                        )
                    else:
                        # Default to array(string)
                        field_type = array(string())
                    if isinstance(field_type, dict) and field_type.get("type") == "array":
                        LOG.debug(
                            "Top-level key '%s' is array with element type %s",
                            key,
                            field_type.get("elementType"),
                        )
                
                elif value_dtype == 'dict':
                    # Build struct from nested records (recursively handles all levels)
                    # Use top_level_key to find all nested records for this key
                    child_records = [r for r in all_nested_records if r['top_level_key'] == key and r['level'] == 1 and r['path'].startswith(f"{key}.")]
                    LOG.info("Top-level key '%s' (dict): found %d child record(s) at level 1", key, len(child_records))
                    if child_records:
                        # Log what we found
                        for cr in child_records[:5]:  # Log first 5
                            LOG.info("  Child record: level=%d, path='%s', key='%s', value_dtype='%s'", 
                                    cr['level'], cr['path'], cr['key'], cr['value_dtype'])
                        # Recursively build struct fields (handles all nested levels)
                        LOG.info("Building nested struct for '%s' (recursively)...", key)
                        struct_fields = _build_struct_fields_from_records(child_records, all_nested_records, parent_path=key, parent_level=1)
                        field_type = struct(struct_fields)
                        LOG.info("Top-level key '%s' (dict): built struct with %d fields", key, len(struct_fields))
                    elif nested:
                        # Fallback to nested list if all_nested_records didn't have matches
                        child_records = [r for r in nested if r['level'] == 1 and r['path'].startswith(f"{key}.")]
                        if child_records:
                            struct_fields = _build_struct_fields_from_records(child_records, all_nested_records, parent_path=key, parent_level=1)
                            field_type = struct(struct_fields)
                        elif isinstance(value, dict):
                            field_type = infer_type_from_value(value)
                        else:
                            field_type = struct([])
                    elif isinstance(value, dict):
                        # Use value directly if available
                        field_type = infer_type_from_value(value)
                    else:
                        # Default to empty struct
                        field_type = struct([])
                    LOG.debug(
                        "Top-level key '%s' is object with %d nested fields",
                        key,
                        len(_get_struct_fields(field_type)) if _is_struct(field_type) else 0,
                    )
                
                else:
                    # Unknown type, default to string
                    field_type = string()
                    LOG.warning("Unknown value_dtype '%s' for key '%s', defaulting to string", value_dtype, key)
                
                fields.append(field(key, field_type, nullable=True))
                LOG.debug("Added field '%s' with type %s", key, field_type)
                
            except Exception as exc: # noqa: BLE001
                LOG.warning("Failed to build schema for key '%s': %s", key, exc)
                # Default to string if building fails
                fields.append(field(key, string(), nullable=True))
        
        if not fields:
            LOG.warning("No valid fields generated from mrf_analysis for file: %s", file_name)
            return None
        
        schema = struct(fields)
        LOG.info("Generated schema from mrf_analysis for %s: %d top-level fields", file_name, len(fields))
        return schema
        
    except Exception as exc: # noqa: BLE001
        LOG.exception("Error generating schema from mrf_analysis for %s: %s", file_name, exc)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _build_type_from_nested_records(
    records: list,
    all_nested: list,
    level: int
) -> any:
    """
    Build Spark type from nested records.
    
    This function is called for array elements. It determines the type of a single array element
    by analyzing the records. If there are multiple records with different keys at the same level,
    it means the array element is a struct/object.
    
    Args:
        records: List of records at current level (e.g., children of an array element like "in_network[0].billing_code", "in_network[0].billing_code_type", etc.)
        all_nested: All nested records (for deeper levels)
        level: Current depth level
        
    Returns:
        Spark DataType
    """
    if not records:
        LOG.info("_build_type_from_nested_records: no records at level %d, returning string", level)
        return string()
    
    # Check if we have multiple records with different keys - this means it's a struct/object
    # Group records by their immediate parent path (e.g., "in_network[0]" for "in_network[0].billing_code")
    # If we have multiple different keys under the same parent, it's a struct
    parent_paths = {}
    for record in records:
        # Extract parent path: "in_network[0].billing_code" -> "in_network[0]"
        path = record['path']
        if '.' in path:
            parent_path = path.rsplit('.', 1)[0]
        elif '[' in path:
            # Handle case like "in_network[0][1]" -> "in_network[0]"
            parts = path.split('[')
            if len(parts) > 1:
                parent_path = parts[0] + '[' + parts[1].split(']')[0] + ']'
            else:
                parent_path = path
        else:
            parent_path = path
        
        if parent_path not in parent_paths:
            parent_paths[parent_path] = []
        parent_paths[parent_path].append(record)
    
    LOG.info("_build_type_from_nested_records: level %d, found %d unique parent path(s)", level, len(parent_paths))
    
    # If we have multiple records with different keys under the same parent, it's a struct
    # Use the first parent path to determine the structure
    first_parent_path = list(parent_paths.keys())[0]
    records_for_parent = parent_paths[first_parent_path]
    
    # Check if we have multiple different keys - if so, it's a struct
    unique_keys = set(r['key'] for r in records_for_parent)
    LOG.info("_build_type_from_nested_records: parent path '%s' has %d unique key(s): %s", 
            first_parent_path, len(unique_keys), sorted(unique_keys))
    
    if len(unique_keys) > 1:
        # Multiple different keys = it's a struct/object
        # Build struct from all records under this parent path
        LOG.info("_build_type_from_nested_records: multiple keys detected, building struct for '%s'", first_parent_path)
        struct_fields = _build_struct_fields_from_records(records_for_parent, all_nested, parent_path=first_parent_path, parent_level=level)
        result = _make_struct(struct_fields)
        LOG.info("_build_type_from_nested_records: built struct with %d fields for '%s'", len(struct_fields), first_parent_path)
        return result
    
    # Single key - determine type from the first record
    first_record = records_for_parent[0]
    record_path = first_record['path']
    value_dtype = first_record['value_dtype']
    value = first_record['value']
    
    LOG.info("_build_type_from_nested_records: single key '%s' at '%s', value_dtype '%s'", 
            first_record['key'], record_path, value_dtype)
    
    if value_dtype == 'scalar':
        result = infer_type_from_value(value)
        LOG.info("_build_type_from_nested_records: scalar at '%s', returning %s", record_path, result)
        return result
    elif value_dtype == 'list':
        # Array of arrays - get element type from nested records
        # Look for records at the next level that start with this path (e.g., "key[0][1]" for nested arrays)
        # Use top_level_key to ensure we're only looking at records from the same top-level key
        top_level_key = first_record.get('top_level_key')
        if top_level_key:
            next_level_records = [r for r in all_nested if r.get('top_level_key') == top_level_key and r['level'] == level + 1 and r['path'].startswith(record_path)]
        else:
            next_level_records = [r for r in all_nested if r['level'] == level + 1 and r['path'].startswith(record_path)]
        LOG.debug("_build_type_from_nested_records: list at '%s', found %d next-level record(s)", record_path, len(next_level_records))
        element_type = _build_type_from_nested_records(next_level_records, all_nested, level + 1)
        result = _make_array(element_type)
        LOG.debug("_build_type_from_nested_records: array at '%s', returning array(%s)", record_path, element_type)
        return result
    elif value_dtype == 'dict':
        # Array of objects - build struct from nested records
        # Look for records at the next level that are children of this path (e.g., "key[0].nested_key")
        # Use top_level_key to ensure we're only looking at records from the same top-level key
        top_level_key = first_record.get('top_level_key')
        if top_level_key:
            child_records = [r for r in all_nested if r.get('top_level_key') == top_level_key and r['level'] == level + 1 and r['path'].startswith(f"{record_path}.")]
        else:
            child_records = [r for r in all_nested if r['level'] == level + 1 and r['path'].startswith(f"{record_path}.")]
        LOG.info("_build_type_from_nested_records: dict at '%s' (level %d), found %d child record(s) at level %d", record_path, level, len(child_records), level + 1)
        if child_records:
            for cr in child_records[:3]:  # Log first 3
                LOG.info("    Child: path='%s', key='%s', value_dtype='%s'", cr['path'], cr['key'], cr['value_dtype'])
        if child_records:
            struct_fields = _build_struct_fields_from_records(child_records, all_nested, parent_path=record_path, parent_level=level + 1)
            result = _make_struct(struct_fields)
            LOG.debug("_build_type_from_nested_records: dict at '%s', returning struct with %d fields", record_path, len(struct_fields))
            return result
        else:
            # No nested records found, try to infer from value
            if isinstance(value, dict) and value:
                result = infer_type_from_value(value)
                LOG.debug("_build_type_from_nested_records: dict at '%s', no nested records, inferred from value: %s", record_path, result)
                return result
            else:
                LOG.debug("_build_type_from_nested_records: dict at '%s', no nested records and empty value, returning empty struct", record_path)
                return _make_struct([])
    else:
        LOG.debug("_build_type_from_nested_records: unknown value_dtype '%s' at '%s', returning string", value_dtype, record_path)
        return "string"


def _build_struct_fields_from_records(
    records: list,
    all_nested: list,
    parent_path: str,
    parent_level: int
) -> list:
    """
    Build list of field from records at a given level.
    
    Args:
        records: Records at current level (direct children of parent_path)
        all_nested: All nested records (for deeper levels)
        parent_path: Path of parent (e.g., "provider_references[0]" or "in_network[0]")
        parent_level: Level of parent
        
    Returns:
        List of field objects
    """
    fields = []
    seen_keys = set()
    
    LOG.debug("_build_struct_fields_from_records: parent_path='%s', parent_level=%d, processing %d record(s)", parent_path, parent_level, len(records))
    
    for record in records:
        key = record['key']
        if key in seen_keys:
            LOG.debug("_build_struct_fields_from_records: skipping duplicate key '%s' at path '%s'", key, record['path'])
            continue
        seen_keys.add(key)
        
        value_dtype = record['value_dtype']
        value = record['value']
        record_path = record['path']
        
        LOG.debug("_build_struct_fields_from_records: processing key '%s' at path '%s', value_dtype='%s'", key, record_path, value_dtype)
        
        try:
            if value_dtype == 'scalar':
                field_type = infer_type_from_value(value)
                LOG.debug("_build_struct_fields_from_records: key '%s' is scalar, type=%s", key, field_type)
            elif value_dtype == 'list':
                # Get nested records for this list
                # Look for records at the next level that start with this path followed by "[" (array elements)
                # Use top_level_key from parent_path or record to filter correctly
                top_level_key = record.get('top_level_key')
                if top_level_key:
                    child_records = [r for r in all_nested if r.get('top_level_key') == top_level_key and r['level'] == parent_level + 1 and r['path'].startswith(f"{record_path}[")]
                else:
                    child_records = [r for r in all_nested if r['level'] == parent_level + 1 and r['path'].startswith(f"{record_path}[")]
                LOG.debug("_build_struct_fields_from_records: key '%s' is list, found %d child record(s) at level %d", key, len(child_records), parent_level + 1)
                element_type = _build_type_from_nested_records(child_records, all_nested, parent_level + 1)
                field_type = _make_array(element_type)
                LOG.debug("_build_struct_fields_from_records: key '%s' is list, element_type=%s", key, element_type)
            elif value_dtype == 'dict':
                # Get nested records for this dict
                # Look for records at the next level that start with this path followed by "." (object properties)
                # Use top_level_key from record to filter correctly
                top_level_key = record.get('top_level_key')
                if top_level_key:
                    child_records = [r for r in all_nested if r.get('top_level_key') == top_level_key and r['level'] == parent_level + 1 and r['path'].startswith(f"{record_path}.")]
                else:
                    child_records = [r for r in all_nested if r['level'] == parent_level + 1 and r['path'].startswith(f"{record_path}.")]
                LOG.info("_build_struct_fields_from_records: key '%s' (path '%s', level %d) is dict, found %d child record(s) at level %d", 
                        key, record_path, parent_level, len(child_records), parent_level + 1)
                if child_records:
                    for cr in child_records[:5]:  # Log first 5
                        LOG.info("    Child: path='%s', key='%s', value_dtype='%s'", cr['path'], cr['key'], cr['value_dtype'])
                if child_records:
                    struct_fields = _build_struct_fields_from_records(child_records, all_nested, parent_path=record_path, parent_level=parent_level + 1)
                    field_type = _make_struct(struct_fields)
                    LOG.debug("_build_struct_fields_from_records: key '%s' is dict, built struct with %d fields", key, len(struct_fields))
                else:
                    # No nested records, try to infer from value
                    if isinstance(value, dict) and value:
                        field_type = infer_type_from_value(value)
                        LOG.debug("_build_struct_fields_from_records: key '%s' is dict, no nested records, inferred from value: %s", key, field_type)
                    else:
                        field_type = _make_struct([])
                        LOG.debug("_build_struct_fields_from_records: key '%s' is dict, no nested records and empty value, returning empty struct", key)
            else:
                field_type = "string"
                LOG.debug("_build_struct_fields_from_records: key '%s' has unknown value_dtype '%s', defaulting to string", key, value_dtype)
            
            fields.append(_make_field(key, field_type))
            LOG.debug("_build_struct_fields_from_records: added field '%s' with type %s", key, field_type)
        except Exception as exc: # noqa: BLE001
            LOG.warning("Failed to build field '%s' at path '%s': %s", key, record_path, exc)
            fields.append(_make_field(key, "string"))
    
    LOG.debug("_build_struct_fields_from_records: returning %d field(s) for parent_path='%s'", len(fields), parent_path)
    return fields


def generate_schema_from_mrf_landing(
    connection_string: str,
    file_name: str,
) -> Optional[SchemaType]:
    """
    Disabled. Schemas must be generated from mrf_analysis only.
    """
    LOG.error("generate_schema_from_mrf_landing is disabled; use mrf_analysis only (file: %s)", file_name)
    return None


def generate_schema_for_group(
    group_dir: Path,
    schema_filename: str = "schema.json",
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    """
    Generate a schema JSON file for a group directory using mrf_analysis table structure.
    
    Args:
        group_dir: Directory containing JSON.gz files (group subfolder)
        schema_filename: Name of the schema JSON file to create
        connection_string: PostgreSQL connection string (required)
        
    Returns:
        Path to created schema file, or None if no files found or connection_string missing
    """
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_analysis")
        return None
    
    # Find all JSON.gz files in the group directory
    json_gz_files = list(group_dir.glob("*.json.gz"))
    
    if not json_gz_files:
        LOG.warning("No JSON.gz files found in %s, skipping schema generation", group_dir.name)
        return None
    
    # Get the first file name (we'll use it to find the schema group)
    # Strip _part#### suffix if present
    first_file = json_gz_files[0]
    file_name = first_file.name
    file_name = re.sub(r'_part\d{4}(\.json(?:\.gz)?)$', r'\1', file_name)
    
    LOG.info("Generating schema from mrf_analysis for file: %s", file_name)
    
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=file_name,
    )
    
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", file_name)
        return None
    
    LOG.info(
        "Schema generated: %d top-level fields",
        len(_get_struct_fields(inferred_schema)) if _is_struct(inferred_schema) else 0,
    )
    
    schema_json = schema_to_json(inferred_schema)
    schema_path = group_dir / schema_filename
    
    LOG.info("Saving schema to %s...", schema_path)
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_json)
    
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path


def generate_schema_for_schema_sig(
    output_directory: Path,
    schema_sig: str,
    min_file_name: str,
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    """
    Generate a schema JSON file for a schema signature using mrf_analysis table structure.
    
    Args:
        output_directory: Directory to write schema JSON file
        schema_sig: Schema signature string
        min_file_name: Min file name for the schema group (used for DB lookup)
        connection_string: PostgreSQL connection string (required)
        
    Returns:
        Path to created schema file, or None on failure
    """
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_landing")
        return None
    
    if not min_file_name:
        LOG.warning("No min_file_name provided for schema_sig, skipping")
        return None
    
    LOG.info("Generating schema from mrf_analysis for min file: %s", min_file_name)
    
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=min_file_name,
    )
    
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", min_file_name)
        return None
    
    LOG.info(
        "Schema generated: %d top-level fields",
        len(_get_struct_fields(inferred_schema)) if _is_struct(inferred_schema) else 0,
    )
    
    schema_filename = schema_sig_to_schema_filename(schema_sig)
    schema_path = output_directory / schema_filename
    
    LOG.info("Saving schema to %s...", schema_path)
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_to_json(inferred_schema))
    
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path


def generate_schema_for_file(
    output_directory: Path,
    file_name: str,
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    """
    Generate a schema JSON file for a specific file using mrf_analysis table structure.
    
    Args:
        output_directory: Directory to write schema JSON file
        file_name: File name to generate schema for
        connection_string: PostgreSQL connection string (required)
        
    Returns:
        Path to created schema file, or None on failure
    """
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_landing")
        return None
    
    if not file_name:
        LOG.warning("No file_name provided, skipping")
        return None
    
    LOG.info("Generating schema from mrf_analysis for file: %s", file_name)
    
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=file_name,
    )
    
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", file_name)
        return None
    
    LOG.info(
        "Schema generated: %d top-level fields",
        len(_get_struct_fields(inferred_schema)) if _is_struct(inferred_schema) else 0,
    )
    
    schema_filename = file_name_to_schema_filename(file_name)
    schema_path = output_directory / schema_filename
    
    LOG.info("Saving schema to %s...", schema_path)
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_to_json(inferred_schema))
    
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path


# Override legacy implementations with mrf_analysis-only behavior
def generate_schema_for_group(
    group_dir: Path,
    schema_filename: str = "schema.json",
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_analysis")
        return None

    json_gz_files = list(group_dir.glob("*.json.gz"))
    if not json_gz_files:
        LOG.warning("No JSON.gz files found in %s, skipping schema generation", group_dir.name)
        return None

    first_file = json_gz_files[0]
    file_name = re.sub(r"_part\\d{4}(\\.json(?:\\.gz)?)$", r"\\1", first_file.name)

    LOG.info("Generating schema from mrf_analysis for file: %s", file_name)
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=file_name,
    )
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", file_name)
        return None

    schema_path = group_dir / schema_filename
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_to_json(inferred_schema))
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path


def generate_schema_for_schema_sig(
    output_directory: Path,
    schema_sig: str,
    min_file_name: str,
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_analysis")
        return None
    if not min_file_name:
        LOG.warning("No min_file_name provided for schema_sig, skipping")
        return None

    LOG.info("Generating schema from mrf_analysis for min file: %s", min_file_name)
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=min_file_name,
    )
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", min_file_name)
        return None

    schema_filename = schema_sig_to_schema_filename(schema_sig)
    schema_path = output_directory / schema_filename
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_to_json(inferred_schema))
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path


def generate_schema_for_file(
    output_directory: Path,
    file_name: str,
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_analysis")
        return None
    if not file_name:
        LOG.warning("No file_name provided, skipping")
        return None

    LOG.info("Generating schema from mrf_analysis for file: %s", file_name)
    inferred_schema = generate_schema_from_mrf_analysis(
        connection_string=connection_string,
        file_name=file_name,
    )
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_analysis for %s (no fallback)", file_name)
        return None

    schema_filename = file_name_to_schema_filename(file_name)
    schema_path = output_directory / schema_filename
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_to_json(inferred_schema))
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path
