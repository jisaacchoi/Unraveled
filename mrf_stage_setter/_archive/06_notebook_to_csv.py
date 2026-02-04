#!/usr/bin/env python3
"""
Convert JSON.gz files to CSV using the notebook processing logic.

This command processes JSON.gz files similar to the Jupyter notebook:
1. Reads JSON.gz files with schema inference
2. Processes provider references and in-network rates
3. Joins rates with providers
4. Applies filters (NPI, billing_code_type, billing_code ranges)
5. Writes to CSV

Usage:
    python commands/06_notebook_to_csv.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.spark_session import create_spark_session
from src.convert.notebook_processor import process_json_gz_to_csv

LOG = logging.getLogger("commands.notebook_to_csv")


def main() -> int:
    """Entrypoint for the notebook_to_csv command."""
    parser = argparse.ArgumentParser(description="Convert JSON.gz files to CSV using notebook processing logic")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "notebook_to_csv")
    configure_logging(config, log_file=log_file)
    
    # Get settings from json_gz_to_parquet config section
    pipeline_config = config.get("pipeline", {})
    parquet_cfg = pipeline_config.get("json_gz_to_parquet", {})
    
    input_dir_cfg = parquet_cfg.get("input_directory")
    output_dir_cfg = parquet_cfg.get("output_directory")
    provider_csv_path = parquet_cfg.get("provider_data_csv_path")
    sample_size = parquet_cfg.get("sample_size")  # Optional: for testing (not in json_gz_to_parquet, but can be added)
    explode_service_codes = parquet_cfg.get("explode_service_codes", False)  # Optional: defaults to False
    download_dir_cfg = parquet_cfg.get("download_directory")  # Directory for URL downloads (needed for NPI from URLs)
    skip_url_download = parquet_cfg.get("skip_url_download", False)  # If True, use existing files instead of downloading
    
    # Get filter settings - new format: filter1: (column_name, filter_value), filter2: (column_name, filter_value)
    filter_cfg = parquet_cfg.get("filter_billing_codes", {})
    filters = []
    
    print(f"DEBUG Raw filter_billing_codes config: {filter_cfg}")
    LOG.info(f"Raw filter_billing_codes config: {filter_cfg}")
    
    # Parse filter1, filter2, etc. from config
    # YAML may parse (a, b) as a list [a, b] or as a string "(a, b)"
    for key in sorted(filter_cfg.keys()):
        if key.startswith("filter"):
            filter_value = filter_cfg[key]
            LOG.info(f"Parsing {key}: type={type(filter_value).__name__}, value={filter_value}")
            
            # Handle list/tuple format: [column_name, filter_value] (YAML parses tuples as lists)
            if isinstance(filter_value, (list, tuple)) and len(filter_value) == 2:
                column_name, filter_val = filter_value
                filters.append((column_name, filter_val))
                LOG.info(f"  Parsed as list/tuple: column={column_name}, val={filter_val}")
            # Handle string format: "(column_name, filter_value)" - try to parse as Python literal
            elif isinstance(filter_value, str):
                try:
                    import ast
                    parsed = ast.literal_eval(filter_value)
                    LOG.info(f"  ast.literal_eval result: type={type(parsed).__name__}, value={parsed}")
                    if isinstance(parsed, (list, tuple)) and len(parsed) == 2:
                        column_name, filter_val = parsed
                        filters.append((column_name, filter_val))
                        LOG.info(f"  Parsed as string->tuple: column={column_name}, val={filter_val}")
                    else:
                        LOG.warning(f"Filter {key} did not parse to (column_name, value) tuple: {filter_value}")
                except (ValueError, SyntaxError) as e:
                    LOG.warning(f"Could not parse filter {key} as Python literal: {filter_value}, error: {e}")
            else:
                LOG.warning(f"Invalid filter format for {key}: {filter_value}. Expected (column_name, filter_value) or [column_name, filter_value]")
    
    print(f"DEBUG Final parsed filters: {filters}")
    LOG.info(f"Final parsed filters: {filters}")
    
    if not input_dir_cfg:
        LOG.error("No input_directory in config.pipeline.json_gz_to_parquet")
        return 1
    
    if not output_dir_cfg:
        LOG.error("No output_directory in config.pipeline.json_gz_to_parquet")
        return 1
    
    input_dir = Path(input_dir_cfg)
    output_dir = Path(output_dir_cfg)
    
    if not input_dir.exists():
        LOG.error("Input directory does not exist: %s", input_dir)
        return 1
    
    if not input_dir.is_dir():
        LOG.error("Input directory path is not a directory: %s", input_dir)
        return 1
    
    # Create SparkSession
    LOG.info("Creating SparkSession...")
    spark = create_spark_session()
    
    # Set download directory for URL content (needed when NPI data is behind URLs)
    download_dir = Path(download_dir_cfg) if download_dir_cfg else None
    if download_dir:
        download_dir.mkdir(parents=True, exist_ok=True)
        LOG.info("URL download directory: %s", download_dir)
    
    try:
        # Process files
        total_rows = process_json_gz_to_csv(
            spark=spark,
            input_dir=input_dir,
            output_dir=output_dir,
            provider_csv_path=provider_csv_path,
            filters=filters,
            sample_size=sample_size,
            explode_service_codes=explode_service_codes,
            download_dir=download_dir,
            skip_url_download=skip_url_download,
        )
        
        LOG.info("Processing complete: %d total rows written", total_rows)
        return 0 if total_rows > 0 else 1
        
    except Exception as exc:
        LOG.exception("Error processing files: %s", exc)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
