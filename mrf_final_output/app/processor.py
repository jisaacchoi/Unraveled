"""Core processing logic for converting JSON.gz to Parquet."""
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

# Import from src - now a sibling directory, no path manipulation needed
from src.schema_utils import simplify_column_names
from src.provider_processing import (
    process_provider_references,
    download_and_process_provider_urls,
)
from src.rates_processing import (
    process_in_network_rates,
    join_rates_with_providers,
)
from src.filtering import filter_rates_dataframe
from src.schema_inference import load_schema_from_file

LOG = logging.getLogger("app.processor")


def process_single_file(
    spark: SparkSession,
    file_paths: List[Path],
    schema_path: Path,
    output_dir: Path,
    npi_filter: Optional[List[str]] = None,
    billing_code_filter: Optional[List[str]] = None,
    explode_service_codes: bool = False,
    download_dir: Optional[Path] = None,
    skip_url_download: bool = False,
    is_serverless: bool = True,
    enable_url_expansion: bool = True,
) -> Dict:
    """
    Process a single JSON.gz file (or split files) to Parquet format.
    
    Args:
        spark: SparkSession
        file_paths: List of paths to JSON.gz files (single file or split parts)
        schema_path: Path to schema.json file
        output_dir: Output directory for Parquet files
        npi_filter: Optional list of NPI values to filter
        billing_code_filter: Optional list of billing codes to filter (can be ranges)
        explode_service_codes: Whether to explode service_code arrays
        download_dir: Directory for downloading provider URLs
        skip_url_download: Skip downloading URLs if True
        is_serverless: Serverless mode flag
        enable_url_expansion: Whether to enable URL provider expansion (default: True)
        
    Returns:
        Dictionary with processing metadata:
        - total_rows: Total number of rows processed
        - files_written: Number of Parquet files written
        - output_directory: Path to output directory
        - url_expansion_status: Status of URL expansion ("enabled", "disabled", "failed", "not_needed")
        Dictionary with processing metadata:
        - total_rows: Total number of rows processed
        - files_written: Number of Parquet files written
        - output_directory: Path to output directory
    """
    LOG.info(f"Processing {len(file_paths)} file(s) to Parquet")
    LOG.info(f"Output directory: {output_dir}")
    
    # Load schema
    LOG.info(f"Loading schema from: {schema_path}")
    schema = load_schema_from_file(schema_path)
    LOG.info(f"Schema loaded: {len(schema.fields)} top-level fields")
    
    # Read all file parts
    LOG.info("Reading files using text mode...")
    all_files = [str(f) for f in file_paths]
    
    # Calculate partitions based on file size
    MB_PER_PARTITION = 300
    MIN_PARTITIONS_PER_FILE = 2
    MAX_PARTITIONS_PER_FILE = 10
    
    raw_dfs = []
    
    for i, file_path in enumerate(all_files):
        file_path_obj = Path(file_path)
        file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
        
        calculated_partitions = max(MIN_PARTITIONS_PER_FILE, int(file_size_mb / MB_PER_PARTITION))
        partitions_per_file = min(calculated_partitions, MAX_PARTITIONS_PER_FILE)
        
        LOG.info(f"File {i+1}/{len(all_files)}: {file_path_obj.name} ({file_size_mb:.1f} MB, {partitions_per_file} partitions)")
        
        # Get original filename by stripping _part#### suffix
        original_filename = file_path_obj.name
        original_filename = re.sub(r'_part\d{4}(\.json(?:\.gz)?)$', r'\1', original_filename)
        
        # Read as text first, then parse JSON
        text_df = spark.read.text(file_path, wholetext=True)
        file_df = (
            text_df
            .withColumn("parsed", F.from_json(F.col("value"), schema))
            .select("parsed.*")
            .withColumn("source_file", F.lit(original_filename))
        )
        
        file_df = file_df.repartition(partitions_per_file)
        raw_dfs.append(file_df)
    
    # Union all DataFrames
    if len(raw_dfs) == 1:
        raw_full = raw_dfs[0]
    else:
        raw_full = raw_dfs[0]
        for df in raw_dfs[1:]:
            raw_full = raw_full.union(df)
    
    LOG.info(f"Unioned {len(raw_dfs)} DataFrames")
    
    # Select only needed columns
    available_columns = set(raw_full.columns)
    has_provider_references = "provider_references" in available_columns
    has_in_network = "in_network" in available_columns
    
    cols_to_select = ["source_file"]
    if has_in_network:
        cols_to_select.append("in_network")
    if has_provider_references:
        cols_to_select.append("provider_references")
    
    raw = raw_full.select(*cols_to_select)
    LOG.info(f"Selected columns: {cols_to_select}")
    
    # Process provider references
    provider_flat, provider_urls = process_provider_references(
        spark, raw, has_provider_references, download_dir
    )
    
    # Track URL expansion status for manifest
    url_expansion_status = "not_needed"
    
    # Download and process provider URLs if needed
    if provider_urls is not None and download_dir:
        LOG.info(f"Processing provider URLs (skip_download={skip_url_download}, enable_url_expansion={enable_url_expansion})...")
        url_provider_flat = download_and_process_provider_urls(
            spark, provider_urls, download_dir, skip_download=skip_url_download, enable_url_expansion=enable_url_expansion
        )
        
        if url_provider_flat is not None:
            url_expansion_status = "enabled"
            if provider_flat is not None:
                provider_flat = provider_flat.union(url_provider_flat)
            else:
                provider_flat = url_provider_flat
        else:
            # Determine why it failed
            if not enable_url_expansion:
                url_expansion_status = "disabled"
            else:
                url_expansion_status = "failed"
    elif provider_urls is not None and not download_dir:
        LOG.warning(
            "=" * 80 + "\n"
            "WARNING: Provider references contain URLs but download_dir not specified!\n"
            "  URL expansion will be skipped. NPIs from URLs will be missing.\n"
            "  Set processing.download_directory in config to enable URL expansion.\n"
            "=" * 80
        )
        url_expansion_status = "failed_no_download_dir"
    elif provider_urls is not None:
        url_expansion_status = "failed_no_download_dir"
    
    if provider_flat is not None:
        provider_flat = simplify_column_names(provider_flat)
    
    # Process in-network rates
    rates = process_in_network_rates(spark, raw, has_in_network, explode_service_codes)
    
    if rates is None:
        LOG.warning("No rates data to process")
        return {
            "total_rows": 0,
            "files_written": 0,
            "output_directory": str(output_dir),
        }
    
    # Join rates with providers
    rates_with_providers = join_rates_with_providers(rates, provider_flat)
    
    if rates_with_providers is None:
        LOG.warning("No rates data after join")
        return {
            "total_rows": 0,
            "files_written": 0,
            "output_directory": str(output_dir),
        }
    
    # Simplify column names
    rates_with_providers = simplify_column_names(rates_with_providers)
    
    # Prepare filters
    provider_npi_set = None
    if npi_filter:
        provider_npi_set = set(npi_filter)
        LOG.info(f"Using NPI filter: {len(provider_npi_set)} NPIs")
    
    filters = []
    if billing_code_filter:
        # Convert billing code filter to list format for filter_rates_dataframe
        # filter_rates_dataframe expects filters as list of (column_name, filter_value) tuples
        # where filter_value can be a list for ranges
        filters = [("billing_code", billing_code_filter)]
        LOG.info(f"Using billing code filter: {len(billing_code_filter)} codes/ranges")
    
    # Apply filters
    if provider_npi_set or filters:
        LOG.info("Applying filters...")
        rates_with_providers = filter_rates_dataframe(
            spark,
            rates_with_providers,
            provider_npi_set=provider_npi_set,
            filters=filters,
        )
    
    # Non-serverless optimization: write intermediate result
    if not is_serverless:
        LOG.info("Non-serverless mode: Writing intermediate result...")
        temp_dir = output_dir / "_temp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        intermediate_path = str(temp_dir / "intermediate_rates")
        rates_with_providers.write.mode("overwrite").parquet(intermediate_path)
        rates_with_providers = spark.read.parquet(intermediate_path)
    
    # Non-serverless: cache before writing
    if not is_serverless:
        LOG.info("Caching DataFrame before writing...")
        rates_with_providers.persist(StorageLevel.MEMORY_AND_DISK)
        cache_count = rates_with_providers.count()
        LOG.info(f"Cached {cache_count:,} rows")
    
    # Use rates_with_providers as fact table (no dimension normalization)
    fact_df = rates_with_providers
    
    # Unpersist if cached
    if not is_serverless:
        rates_with_providers.unpersist()
    
    # Write fact table
    files_written = 0
    fact_output_dir = output_dir / "fact"
    fact_output_dir.mkdir(parents=True, exist_ok=True)
    
    LOG.info("Writing fact table...")
    fact_df.write.mode("overwrite").option("compression", "zstd").parquet(str(fact_output_dir))
    
    # Count files written
    files_written += len(list(fact_output_dir.glob("*.parquet")))
    
    # Get row count (approximate or actual)
    try:
        total_rows = fact_df.count()
    except Exception as e:
        LOG.warning(f"Could not count rows: {e}. Using -1 as placeholder.")
        total_rows = -1
    
    LOG.info(f"Processing complete: {total_rows:,} rows, {files_written} files")
    
    return {
        "total_rows": total_rows,
        "files_written": files_written,
        "output_directory": str(output_dir),
        "url_expansion_status": url_expansion_status,
    }
