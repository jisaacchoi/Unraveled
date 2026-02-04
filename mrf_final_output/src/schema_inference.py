"""Schema inference and file operations for PySpark schemas."""
from __future__ import annotations

import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, BooleanType, ArrayType, MapType, NullType
)

import psycopg2
from src.generate_schemas.schema_groups_db import get_schema_groups

LOG = logging.getLogger("src.schema_inference")


def infer_schema_from_file(
    spark: SparkSession,
    ndjson_path: Path,
) -> StructType:
    """
    Infer PySpark schema from an NDJSON file by reading through the entire file.
    
    This function reads the entire file to ensure all fields and types are captured
    in the schema inference. This is more accurate than reading a sample.
    
    Args:
        spark: SparkSession
        ndjson_path: Path to NDJSON file
        
    Returns:
        Inferred PySpark StructType schema
    """
    if not ndjson_path.exists():
        raise FileNotFoundError(f"NDJSON file not found: {ndjson_path}")
    
    LOG.info("Inferring schema from %s (reading entire file for accurate schema inference)...", ndjson_path.name)
    
    # Read entire file to infer schema (sampleRatio=1.0 means read all rows)
    # This ensures all fields and types are captured, especially important for schema generation
    # Since we're using the smallest file in each schema group, reading the entire file is feasible
    df = spark.read.option("samplingRatio", "1.0").json(str(ndjson_path))
    
    schema = df.schema
    LOG.info("Inferred schema with %d fields from %s", len(schema.fields), ndjson_path.name)
    
    return schema


def save_schema_to_file(
    schema: StructType,
    output_path: Path,
) -> None:
    """
    Save PySpark schema to a JSON file.
    
    Args:
        schema: PySpark StructType schema
        output_path: Path to save schema JSON file
    """
    # Convert schema to JSON-serializable format
    schema_json = schema.json()
    
    # Directory creation handled by ensure_directories_from_config()
    
    # Write schema to file
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(schema_json)
    
    LOG.info("Saved schema to %s", output_path)


def load_schema_from_file(
    schema_path: Path,
) -> StructType:
    """
    Load PySpark schema from a JSON file.
    
    Args:
        schema_path: Path to schema JSON file
        
    Returns:
        PySpark StructType schema
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with open(schema_path, "r", encoding="utf-8") as fh:
        schema_json = fh.read()
    
    # Parse schema from JSON
    schema = StructType.fromJson(json.loads(schema_json))
    
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


def generate_and_save_schemas(
    spark: SparkSession,
    connection_string: str,
    input_directory: Path,
    schema_output_directory: Path,
    refresh_schema_groups: bool = False,
    analyzed_directory: Path | None = None,
) -> tuple[Dict[str, Path], list[str]]:
    """
    Generate schemas for all schema groups and save them to files.
    
    For each schema group:
    1. Infers schema from the min_file_name file
    2. Saves schema to a JSON file named after min_file_name (e.g., "file.json.gz.schema.json")
    3. Moves the min_file to analyzed_directory immediately after schema is generated (if provided)
    
    Args:
        spark: SparkSession
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
        
        # Convert .json.gz to temporary .ndjson if needed
        min_file_path = None
        temp_ndjson_path = None
        is_temp_file = False
        
        if min_file_name.endswith(".json.gz"):
            # Input is .json.gz - need to convert to temporary NDJSON
            min_file_gz_path = input_directory / min_file_name
            if not min_file_gz_path.exists():
                LOG.warning("Min file not found: %s (tried: %s), skipping group", min_file_name, min_file_gz_path)
                continue
            
            # Create temporary NDJSON file
            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".ndjson",
                prefix=f"{min_file_gz_path.stem}_",
                delete=False,
                encoding="utf-8",
            )
            temp_ndjson_path = Path(temp_file.name)
            temp_file.close()
            is_temp_file = True
            
            # Convert .json.gz to temporary NDJSON
            LOG.info("Converting %s to temporary NDJSON format for schema inference...", min_file_name)
            from src.convert.json_to_ndjson import convert_json_gz_to_ndjson  # noqa: PLC0415
            result = convert_json_gz_to_ndjson(
                input_path=min_file_gz_path,
                output_path=temp_ndjson_path,
                connection_string=None,  # Don't need DB connection for conversion here
                skip_if_not_in_db=False,
            )
            
            if result != 0:
                LOG.error("Failed to convert %s to NDJSON for schema inference", min_file_name)
                # Clean up temp file on failure
                try:
                    if temp_ndjson_path.exists():
                        temp_ndjson_path.unlink()
                except Exception:  # noqa: BLE001
                    pass
                continue
            
            min_file_path = temp_ndjson_path
        else:
            # Input is already .ndjson
            min_file_ndjson = min_file_name
            min_file_path = input_directory / min_file_ndjson
            if not min_file_path.exists():
                LOG.warning("Min file not found: %s (tried: %s), skipping group", min_file_name, min_file_path)
                continue
        
        try:
            # Infer schema from file (either .ndjson or temp converted file)
            LOG.info("Inferring schema from %s (this may take a while for large files)...", min_file_path.name)
            schema = infer_schema_from_file(spark, min_file_path)
            
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
            # Clean up temporary NDJSON file if one was created
            if is_temp_file and temp_ndjson_path and temp_ndjson_path.exists():
                try:
                    temp_ndjson_path.unlink()
                    LOG.debug("Cleaned up temporary NDJSON file: %s", temp_ndjson_path)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Failed to clean up temporary file %s: %s", temp_ndjson_path, exc)
    
    LOG.info("Generated %d schema(s) out of %d schema group(s)", len(schema_files), len(schema_groups))
    return schema_files, processed_file_names


def load_schema_by_min_file_name(
    min_file_name: str,
    schema_directory: Path,
) -> Optional[StructType]:
    """
    Load a schema from a file by min_file_name.
    
    Args:
        min_file_name: Name of the min file (e.g., "file.json.gz" or "file.ndjson")
        schema_directory: Directory containing schema JSON files
        
    Returns:
        PySpark StructType schema, or None if schema file not found
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


def infer_type_from_value(value: any) -> any:
    """
    Infer PySpark type from a Python value.
    
    Args:
        value: Python value (dict, list, str, int, float, bool, None)
        
    Returns:
        PySpark DataType
    """
    if value is None:
        return StringType()  # Nullable, default to StringType
    elif isinstance(value, bool):
        return BooleanType()
    elif isinstance(value, int):
        # Check if it fits in IntegerType range
        if -2147483648 <= value <= 2147483647:
            return IntegerType()
        else:
            return LongType()
    elif isinstance(value, float):
        return DoubleType()
    elif isinstance(value, str):
        return StringType()
    elif isinstance(value, list):
        if not value:
            # Empty list - default to ArrayType(StringType)
            return ArrayType(StringType())
        # Infer element type from first item
        element_type = infer_type_from_value(value[0])
        return ArrayType(element_type)
    elif isinstance(value, dict):
        # Build StructType from dict keys
        fields = []
        for key, val in value.items():
            field_type = infer_type_from_value(val)
            fields.append(StructField(str(key), field_type, nullable=True))
        return StructType(fields)
    else:
        # Unknown type, default to StringType
        return StringType()


def generate_schema_from_mrf_landing(
    connection_string: str,
    file_name: str,
) -> Optional[StructType]:
    """
    Generate a PySpark schema from mrf_landing table by parsing JSONB payloads.
    
    Queries mrf_landing table for the given file_name, groups by record_type,
    gets the record with the largest payload_size for each record_type, then
    parses each JSONB payload to infer the structure and combines them into
    a single schema.
    
    Args:
        connection_string: PostgreSQL connection string
        file_name: File name to query structure for
        
    Returns:
        PySpark StructType schema, or None if file not found
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Query mrf_landing for this file_name, grouped by record_type
        # Get the record with the largest payload_size for each record_type
        cursor.execute("""
            SELECT DISTINCT ON (record_type)
                record_type,
                payload,
                payload_size
            FROM mrf_landing
            WHERE file_name = %s
            ORDER BY record_type, COALESCE(payload_size, 0) DESC, id
        """, (file_name,))
        
        rows = cursor.fetchall()
        if not rows:
            LOG.warning("No records found in mrf_landing for file: %s", file_name)
            return None
        
        LOG.info("Found %d record type(s) for file %s", len(rows), file_name)
        
        # Build schema fields from each record_type's payload
        fields = []
        seen_record_types = set()
        
        for record_type, payload_jsonb, payload_size in rows:
            if record_type in seen_record_types:
                continue
            seen_record_types.add(record_type)
            
            LOG.info("Processing record_type '%s' (payload_size: %s bytes)", 
                    record_type, payload_size if payload_size else "unknown")
            
            # Parse JSONB payload
            try:
                # psycopg2 returns jsonb as dict/list directly
                payload = payload_jsonb
                
                # Determine the original structure based on payload format:
                # - Arrays: payload is wrapped as [item] to preserve array nature
                # - Objects: payload is the object itself (dict)
                # - Scalars: payload is wrapped as {"value": scalar}
                
                if isinstance(payload, list):
                    # This came from an array - unwrap and infer element type
                    if payload:
                        element = payload[0]
                        element_type = infer_type_from_value(element)
                        field_type = ArrayType(element_type)
                        LOG.debug("Record_type '%s' is an array with element type %s", record_type, element_type)
                    else:
                        # Empty array - default to ArrayType(StringType)
                        field_type = ArrayType(StringType())
                        LOG.debug("Record_type '%s' is an empty array, defaulting to ArrayType(StringType)", record_type)
                    
                elif isinstance(payload, dict):
                    # Check if this is a scalar wrapped in {"value": ...}
                    if len(payload) == 1 and "value" in payload:
                        # This came from a scalar - unwrap and infer type
                        scalar_value = payload["value"]
                        field_type = infer_type_from_value(scalar_value)
                        LOG.debug("Record_type '%s' is a scalar with type %s", record_type, field_type)
                    else:
                        # This came from an object - infer struct type from dict
                        field_type = infer_type_from_value(payload)
                        LOG.debug("Record_type '%s' is an object with struct type", record_type)
                else:
                    # Unexpected payload format - default to StringType
                    field_type = StringType()
                    LOG.warning("Unexpected payload format for record_type '%s': %s", record_type, type(payload))
                
                fields.append(StructField(record_type, field_type, nullable=True))
                LOG.debug("Added field '%s' with type %s", record_type, field_type)
                
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to parse payload for record_type '%s': %s", record_type, exc)
                # Default to StringType if parsing fails
                fields.append(StructField(record_type, StringType(), nullable=True))
        
        if not fields:
            LOG.warning("No valid fields generated from mrf_landing for file: %s", file_name)
            return None
        
        schema = StructType(fields)
        LOG.info("Generated schema from mrf_landing for %s: %d top-level fields", file_name, len(fields))
        return schema
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error generating schema from mrf_landing for %s: %s", file_name, exc)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def generate_schema_for_group(
    spark: SparkSession,
    group_dir: Path,
    schema_filename: str = "schema.json",
    connection_string: Optional[str] = None,
) -> Optional[Path]:
    """
    Generate a schema JSON file for a group directory using mrf_landing table structure.
    
    This function queries the mrf_landing table to get the largest payload per record_type
    for files in this group, parses each JSONB payload to infer structure, and combines
    them into a PySpark-compatible schema.
    
    Args:
        spark: SparkSession (not used, kept for API compatibility)
        group_dir: Directory containing JSON.gz files (group subfolder)
        schema_filename: Name of the schema JSON file to create
        connection_string: PostgreSQL connection string (required)
        
    Returns:
        Path to created schema file, or None if no files found or connection_string missing
    """
    if not connection_string:
        LOG.error("connection_string is required for schema generation from mrf_landing")
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
    
    LOG.info("Generating schema from mrf_landing for file: %s", file_name)
    
    # Generate schema from mrf_landing
    inferred_schema = generate_schema_from_mrf_landing(
        connection_string=connection_string,
        file_name=file_name,
    )
    
    if not inferred_schema:
        LOG.error("Failed to generate schema from mrf_landing for %s", file_name)
        return None
    
    LOG.info("Schema generated from mrf_landing: %d top-level fields", len(inferred_schema.fields))
    
    # Convert schema to JSON and save to file
    schema_json = inferred_schema.json()
    schema_path = group_dir / schema_filename
    
    LOG.info("Saving schema to %s...", schema_path)
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(schema_json)
    
    LOG.info("Schema saved successfully: %s", schema_path)
    return schema_path

