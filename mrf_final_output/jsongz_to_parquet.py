#!/usr/bin/env python3
"""
Convert JSON.gz files to CSV using the notebook processing logic.
Databricks-compatible version - can be run directly as a script.

Usage in Databricks:
    %run ./commands/jsongz_to_parquet
"""
from __future__ import annotations

import subprocess
import sys

# Install required packages
# subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pandas", "pyyaml", "requests", "ijson"])

import logging
from pathlib import Path

# Databricks: add repo root to path
# Adjust this path to match your Databricks workspace location
repo_root = "/Workspace/Repos/j.isaac.choi@gmail.com/test/notitle_lite2"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
    print(f"Added to sys.path: {repo_root}")

from pyspark.sql import SparkSession

from src.pipeline_config import configure_logging, load_config
from src.convert.processor import process_json_gz_to_csv

LOG = logging.getLogger("commands.jsongz_to_parquet")

# Load config from repo
config_path = Path(repo_root) / "config.yaml"
config = load_config(config_path)

# Configure logging to console only (no file logging for Databricks)
# Clear any existing handlers first
logging.root.handlers = []

# Set up console handler only
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s'))
logging.root.addHandler(console_handler)
logging.root.setLevel(logging.INFO)

# Get settings from json_gz_to_parquet config section
pipeline_config = config.get("pipeline", {})
parquet_cfg = pipeline_config.get("json_gz_to_parquet", {})

input_dir_cfg = parquet_cfg.get("input_directory")
output_dir_cfg = parquet_cfg.get("output_directory")
provider_csv_path = parquet_cfg.get("provider_data_csv_path")
sample_size = None  # Optional: for testing, can be set programmatically
explode_service_codes = False  # Whether to explode service_code arrays
download_dir_cfg = parquet_cfg.get("download_directory")
skip_url_download = parquet_cfg.get("skip_url_download", False)
enable_url_expansion = parquet_cfg.get("enable_url_expansion", True)  # Whether to enable URL provider expansion

# Serverless mode: Set to True for Databricks serverless compute (default)
# Set to False for dedicated clusters to enable caching and temp file optimizations
is_serverless = parquet_cfg.get("is_serverless", True)
temp_dir_cfg = parquet_cfg.get("temp_directory")  # Optional: temp directory for non-serverless mode


# Parse filters
filter_cfg = parquet_cfg.get("filter_billing_codes", {})
filters = []

for key in sorted(filter_cfg.keys()):
    if key.startswith("filter"):
        filter_value = filter_cfg[key]
        if isinstance(filter_value, (list, tuple)) and len(filter_value) == 2:
            column_name, filter_val = filter_value
            filters.append((column_name, filter_val))
        elif isinstance(filter_value, str):
            try:
                import ast
                parsed = ast.literal_eval(filter_value)
                if isinstance(parsed, (list, tuple)) and len(parsed) == 2:
                    column_name, filter_val = parsed
                    filters.append((column_name, filter_val))
            except (ValueError, SyntaxError):
                pass

LOG.info(f"Filters: {filters}")

# Resolve paths relative to repo root
input_dir = Path(repo_root) / input_dir_cfg if input_dir_cfg else None
output_dir = Path(repo_root) / output_dir_cfg if output_dir_cfg else None
download_dir = Path(repo_root) / download_dir_cfg if download_dir_cfg else None
temp_dir = str(Path(repo_root) / temp_dir_cfg) if temp_dir_cfg else None

if not input_dir or not input_dir.exists():
    raise ValueError(f"Input directory does not exist: {input_dir}")

if output_dir:
    output_dir.mkdir(parents=True, exist_ok=True)
if download_dir:
    download_dir.mkdir(parents=True, exist_ok=True)

# Get existing Spark session (Databricks provides one)
# Use getOrCreate() which will reuse existing session or create new one if expired
spark = SparkSession.builder.getOrCreate()
LOG.info("Spark session ready")

LOG.info(f"Input: {input_dir}")
LOG.info(f"Output: {output_dir}")
LOG.info(f"Download dir: {download_dir}")
LOG.info(f"Serverless mode: {is_serverless}")
if not is_serverless:
    LOG.info(f"Temp dir: {temp_dir}")

# Process files - output to Parquet (better for Databricks)
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
    output_format="parquet",
    is_serverless=is_serverless,
    temp_dir=temp_dir,
    enable_url_expansion=enable_url_expansion,
)

LOG.info(f"Processing complete: {total_rows} total rows written to Parquet")
