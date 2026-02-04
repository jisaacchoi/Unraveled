#!/usr/bin/env python3
"""
Convert .json.gz files directly to Parquet/CSV files using PySpark with pre-generated schemas.

This command combines convert_to_ndjson and convert_to_parquet functionality:
1. Accepts .json.gz files as input (typically from analyzed_directory after schema generation)
2. Converts each file to temporary NDJSON format
3. Looks up schema from schema_groups table by file name
4. Processes each file individually to Parquet or CSV output

Usage:
    python commands/03_convert_json_gz_to_parquet.py --config config.yaml
    
Note: 
    - Run 04_group_schemas.py first to refresh schema groups (schema generation is no longer part of this command)
    - Input directory should be set to the analyzed_directory from schema_generation config
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tempfile
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

import psycopg2

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string
from src.convert.json_to_ndjson import convert_json_gz_to_ndjson
from src.convert.ndjson_to_csv import (
    convert_file_with_schema_to_csv,
    load_provider_data_npis,
)
from src.generate_schemas.schema_inference import load_schema_by_min_file_name
from src.shared.spark_session import create_spark_session

LOG = logging.getLogger("commands.json_gz_to_parquet")


def lookup_min_file_name_by_filename(file_name: str, connection_string: str) -> str | None:
    """
    Look up min_file_name for a given file name from schema_groups table.
    
    Args:
        file_name: Name of the file to look up
        connection_string: PostgreSQL connection string
        
    Returns:
        min_file_name if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Query schema_groups table for this file name
        # Handle both .json.gz and .ndjson file names
        file_name_ndjson = file_name
        if file_name.endswith(".json.gz"):
            file_name_ndjson = file_name[:-8] + ".ndjson"
        
        cursor.execute(
            "SELECT DISTINCT min_file_name FROM schema_groups WHERE file_name = %s OR file_name = %s LIMIT 1",
            (file_name, file_name_ndjson),
        )
        result = cursor.fetchone()
        
        if result and result[0]:
            return result[0]
        return None
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error looking up min_file_name for file %s: %s", file_name, exc)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def process_json_gz_file(
    spark: SparkSession,
    json_gz_path: Path,
    output_dir: Path,
    schema_directory: Path,
    connection_string: str,
    process_min_file_only: bool,
    target_partition_size_mb: float,
    billing_code_type: str | None,
    billing_codes: list[str] | None,
    download_dir: Path | None,
    delete_input_files: bool,
    processed_files_dir: Path | None = None,
    skip_if_schema_not_found: bool = False,
    provider_npi_set: set[str] | None = None,
) -> int:
    """
    Process a single .json.gz file: convert to NDJSON, then to Parquet/CSV.
    
    Args:
        spark: SparkSession
        json_gz_path: Path to input .json.gz file
        output_dir: Directory to write output files
        schema_directory: Directory containing pre-generated schema files
        connection_string: Database connection string
        process_min_file_only: If True, output CSV with top 10 rows; if False, output Parquet
        target_partition_size_mb: Target partition size for Parquet (when process_min_file_only=False)
        billing_code_type: Optional billing code type filter
        billing_codes: Optional list of billing codes to filter by
        download_dir: Optional directory for URL downloads
        delete_input_files: If True, create .done marker file and delete input file after successful processing
        processed_files_dir: If delete_input_files is False, move processed files to this directory
        
    Returns:
        Number of rows processed, or 0 on error
    """
    if not json_gz_path.exists():
        LOG.error("Input file not found: %s", json_gz_path)
        return 0
    
    if not json_gz_path.suffixes == [".json", ".gz"]:
        LOG.error("Input file must be .json.gz: %s", json_gz_path)
        return 0
    
    temp_ndjson_path = None
    try:
        # Step 1: Convert .json.gz to temporary NDJSON file
        LOG.info("Converting %s to temporary NDJSON format...", json_gz_path.name)
        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".ndjson",
            prefix=f"{json_gz_path.stem}_",
            delete=False,
            encoding="utf-8",
        )
        temp_ndjson_path = Path(temp_file.name)
        temp_file.close()
        
        result = convert_json_gz_to_ndjson(
            input_path=json_gz_path,
            output_path=temp_ndjson_path,
            connection_string=connection_string,
            skip_if_not_in_db=False,
        )
        
        if result != 0:
            LOG.error("Failed to convert %s to NDJSON", json_gz_path.name)
            return 0
        
        LOG.info("Successfully converted %s to temporary NDJSON", json_gz_path.name)
        
        # Step 2: Look up min_file_name for this file from schema_groups table
        LOG.info("Looking up min_file_name for file: %s", json_gz_path.name)
        min_file_name = lookup_min_file_name_by_filename(json_gz_path.name, connection_string)
        
        if not min_file_name:
            if skip_if_schema_not_found:
                LOG.warning("Could not find min_file_name for file: %s. Skipping (skip_if_schema_not_found=True)", json_gz_path.name)
                return -1  # Special return value to indicate skipped
            LOG.error("Could not find min_file_name for file: %s", json_gz_path.name)
            LOG.error("Make sure the file has been analyzed and schema_groups table has been populated.")
            return 0
        
        LOG.info("Found min_file_name for %s: %s", json_gz_path.name, min_file_name)
        
        # Step 3: Load schema from schema directory using min_file_name
        schema = load_schema_by_min_file_name(min_file_name, schema_directory)
        if not schema:
            if skip_if_schema_not_found:
                LOG.warning(
                    "Could not load schema file for min_file_name: %s. Skipping (skip_if_schema_not_found=True)",
                    min_file_name,
                )
                LOG.warning("Expected schema file: %s", schema_directory / f"{min_file_name}.schema.json")
                return -1  # Special return value to indicate skipped
            LOG.error(
                "Could not load schema file for min_file_name: %s",
                min_file_name,
            )
            LOG.error("Make sure schemas have been generated (schema generation is no longer part of 04_group_schemas.py)")
            LOG.error("Expected schema file: %s", schema_directory / f"{min_file_name}.schema.json")
            return 0
        
        LOG.info("Loaded schema for %s", json_gz_path.name)
        
        # Step 4: Process with existing conversion function
        # Extract original filename from json_gz_path
        original_filename = json_gz_path.name
        
        if process_min_file_only:
            # Output CSV with top 10 rows
            result = convert_file_with_schema_to_csv(
                spark=spark,
                ndjson_path=temp_ndjson_path,
                output_dir=output_dir,
                schema=schema,
                top_n_rows=10,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                download_dir=download_dir,
                original_filename=original_filename,
                provider_npi_set=provider_npi_set,
            )
        else:
            # Output CSV files (Parquet function archived due to Windows native library issues)
            result = convert_file_with_schema_to_csv(
                spark=spark,
                ndjson_path=temp_ndjson_path,
                output_dir=output_dir,
                schema=schema,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                download_dir=download_dir,
                original_filename=original_filename,
                provider_npi_set=provider_npi_set,
            )
        
        # Step 5: Mark input file as done (create .done marker file) and delete/move if configured
        if result > 0:
            try:
                # Create .done marker file first
                # For file.json.gz, create file.json.gz.done
                done_file_path = json_gz_path.with_name(json_gz_path.name + ".done")
                done_file_path.touch()
                LOG.info("Created .done marker file: %s", done_file_path)
                
                if delete_input_files:
                    # Delete the original file
                    json_gz_path.unlink()
                    LOG.info("Deleted input file after successful processing: %s", json_gz_path)
                elif processed_files_dir is not None:
                    # Move the file to processed_files_dir instead of deleting
                    # Create the directory if it doesn't exist
                    processed_files_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Move the file, preserving the filename
                    destination = processed_files_dir / json_gz_path.name
                    # If destination already exists, add a suffix to avoid overwriting
                    if destination.exists():
                        counter = 1
                        while destination.exists():
                            stem = json_gz_path.stem
                            suffix = json_gz_path.suffix
                            destination = processed_files_dir / f"{stem}_{counter}{suffix}"
                            counter += 1
                    
                    shutil.move(str(json_gz_path), str(destination))
                    LOG.info("Moved processed input file to: %s", destination)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to create .done marker file or handle input file %s: %s", json_gz_path, exc)
        
        return result
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error processing file %s: %s", json_gz_path, exc)
        return 0
    finally:
        # Clean up temporary NDJSON file
        if temp_ndjson_path and temp_ndjson_path.exists():
            try:
                temp_ndjson_path.unlink()
                LOG.debug("Cleaned up temporary NDJSON file: %s", temp_ndjson_path)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Failed to clean up temporary file %s: %s", temp_ndjson_path, exc)


def main() -> int:
    """Entrypoint for the json_gz_to_parquet command."""
    parser = argparse.ArgumentParser(description="Convert .json.gz files directly to Parquet/CSV files using PySpark")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "convert_json_gz_to_parquet")
    configure_logging(config, log_file=log_file)
    
    # Get database connection
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("No database configuration found in config.yaml")
        return 1
    
    try:
        connection_string = build_connection_string(db_config)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Could not build database connection string: %s", exc)
        return 1
    
    # Get conversion settings (use json_gz_to_parquet config)
    pipeline_config = config.get("pipeline", {})
    json_gz_cfg = pipeline_config.get("json_gz_to_parquet", {})
    input_path_cfg = json_gz_cfg.get("input_directory", json_gz_cfg.get("input_path"))
    output_path_cfg = json_gz_cfg.get("output_directory", json_gz_cfg.get("output_path"))
    schema_dir_cfg = json_gz_cfg.get("schema_directory")
    
    if not input_path_cfg:
        LOG.error("No input_directory in config.pipeline.json_gz_to_parquet")
        return 1
    
    if not output_path_cfg:
        LOG.error("No output_directory in config.pipeline.json_gz_to_parquet")
        return 1
    
    if not schema_dir_cfg:
        LOG.error("No schema_directory in config.pipeline.json_gz_to_parquet")
        LOG.error("Schema directory is required. Run generate_schemas.py first to create schema files.")
        return 1
    
    input_path = Path(input_path_cfg)
    output_dir = Path(output_path_cfg)
    schema_directory = Path(schema_dir_cfg)
    target_partition_size_mb = float(json_gz_cfg.get("target_partition_size_mb", 500.0))
    process_min_file_only = json_gz_cfg.get("process_min_file_only", True)
    
    # Get billing code filter settings
    filter_cfg = json_gz_cfg.get("filter_billing_codes", {})
    billing_code_type = filter_cfg.get("billing_code_type")
    billing_codes = filter_cfg.get("billing_codes", [])
    
    # Convert empty list to None to disable filtering
    if billing_codes is not None and len(billing_codes) == 0:
        billing_codes = None
    
    # Get URL download directory (optional)
    download_dir_cfg = json_gz_cfg.get("download_directory")
    download_dir = Path(download_dir_cfg) if download_dir_cfg else None
    
    # Get delete input files setting
    delete_input_files = json_gz_cfg.get("delete_input_files_after_conversion", False)
    
    # Get processed files directory (for moving files when delete_input_files is False)
    processed_files_dir_cfg = json_gz_cfg.get("processed_files_directory")
    processed_files_dir = Path(processed_files_dir_cfg) if processed_files_dir_cfg else None
    
    # Get skip if schema not found setting
    skip_if_schema_not_found = json_gz_cfg.get("skip_if_schema_not_found", False)
    
    if not schema_directory.exists():
        LOG.error("Schema directory does not exist: %s", schema_directory)
        LOG.error("Run generate_schemas.py first to create schema files.")
        return 1
    
    if not schema_directory.is_dir():
        LOG.error("Schema directory path is not a directory: %s", schema_directory)
        return 1
    
    # Create SparkSession
    LOG.info("Creating SparkSession...")
    spark = create_spark_session()
    
    # Load provider_data from CSV file to filter NPIs
    provider_csv_path = json_gz_cfg.get("provider_data_csv_path", "D:/payer_mrf/filename_filtered_Illinois_institutions.csv")
    LOG.info("Loading provider_data from CSV file to filter NPIs: %s", provider_csv_path)
    provider_npi_set = load_provider_data_npis(spark, provider_csv_path, npi_column="NPPES_NPI")
    if provider_npi_set is None:
        LOG.warning("Failed to load provider_data from CSV file. NPIs will not be filtered.")
        provider_npi_set = None
    else:
        LOG.info("Successfully loaded provider_data from CSV file for NPI filtering")
    
    try:
        # Process all .json.gz files in input directory
        if not input_path.exists():
            LOG.error("Input path does not exist: %s", input_path)
            return 1
        
        if input_path.is_file():
            files_to_process = [input_path]
        elif input_path.is_dir():
            files_to_process = list(input_path.glob("*.json.gz"))
            if not files_to_process:
                LOG.error("No .json.gz files found in %s", input_path)
                return 1
        else:
            LOG.error("Input path is neither a file nor a directory: %s", input_path)
            return 1
        
        LOG.info("Processing %d .json.gz file(s)...", len(files_to_process))
        if skip_if_schema_not_found:
            LOG.info("skip_if_schema_not_found is enabled - files without schemas will be skipped")
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        total_rows = 0
        
        for file_path in files_to_process:
            LOG.info("Processing file: %s", file_path.name)
            result = process_json_gz_file(
                spark=spark,
                json_gz_path=file_path,
                output_dir=output_dir,
                schema_directory=schema_directory,
                connection_string=connection_string,
                process_min_file_only=process_min_file_only,
                target_partition_size_mb=target_partition_size_mb,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                download_dir=download_dir,
                delete_input_files=delete_input_files,
                processed_files_dir=processed_files_dir,
                skip_if_schema_not_found=skip_if_schema_not_found,
                provider_npi_set=provider_npi_set,
            )
            
            if result > 0:
                success_count += 1
                total_rows += result
            elif result == -1:
                skipped_count += 1
            else:
                failed_count += 1
        
        LOG.info("Processing complete: %d succeeded, %d failed, %d skipped, %d total rows", 
                success_count, failed_count, skipped_count, total_rows)
        return 0 if failed_count == 0 else 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())

