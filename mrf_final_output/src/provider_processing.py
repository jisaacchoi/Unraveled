"""Processing logic for provider references and NPI extraction."""
from __future__ import annotations

import gzip
import hashlib
import json as json_module
import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

LOG = logging.getLogger("src.convert.provider_processing")

# Import URL processing functions
try:
    from src.mrf_downloader import download_file, session_with_retries
    URL_PROCESSING_AVAILABLE = True
except ImportError as e:
    URL_PROCESSING_AVAILABLE = False
    LOG.warning(
        "=" * 80 + "\n"
        "WARNING: URL provider expansion is DISABLED due to import error!\n"
        f"  Import error: {e}\n"
        "  Provider URLs will NOT be downloaded or processed.\n"
        "  This may result in missing NPI data if providers are referenced via URLs.\n"
        "=" * 80
    )


def process_provider_references(
    spark: SparkSession,
    raw: DataFrame,
    has_provider_references: bool,
    download_dir: Optional[Path] = None,
    provider_array_column: str = "provider_references",
) -> tuple[Optional[DataFrame], Optional[DataFrame]]:
    """
    Process provider references from raw DataFrame.
    
    Handles two formats:
    1. Inline provider_groups with NPIs
    2. URL references (location field) that need to be downloaded
    
    Args:
        spark: SparkSession
        raw: Raw DataFrame
        has_provider_references: Whether provider_references field exists
        download_dir: Directory for downloading URL content
    
    Returns:
        Tuple of (provider_flat DataFrame, provider_urls DataFrame)
        - provider_flat: DataFrame with provider_group_id and npi (inline NPIs)
        - provider_urls: DataFrame with provider_group_id and location URL (for downloading)
    """
    if not has_provider_references:
        LOG.info("No %s field found. Skipping providers dataframe creation.", provider_array_column)
        return None, None
    
    LOG.info("Processing provider references from top-level array '%s'...", provider_array_column)
    
    # First, explode provider_references to get individual entries
    pr = raw.select(
        "source_file",
        F.explode_outer(F.col(provider_array_column)).alias("pr")
    )
    
    # Check schema to see what fields are available
    pr_schema = pr.select("pr.*").schema
    field_names = [f.name for f in pr_schema.fields]
    LOG.info(f"Provider reference fields: {field_names}")
    
    has_provider_groups = "provider_groups" in field_names
    has_location = "location" in field_names
    
    provider_flat = None
    provider_urls = None
    
    # Process inline provider_groups (format 1)
    if has_provider_groups:
        LOG.info("Processing inline provider_groups...")
        try:
            provider_flat = (
                pr.select(
                    "source_file",
                    F.col("pr.provider_group_id").cast("string").alias("provider_group_id"),
                    F.explode_outer("pr.provider_groups").alias("provider_group")
                )
                .select(
                    "source_file",
                    "provider_group_id",
                    F.explode_outer(F.col("provider_group.npi")).alias("npi")
                )
                .filter(F.col("npi").isNotNull())
            )
            LOG.info("Inline provider references DataFrame created (joins will handle empty DataFrames)")
        except Exception as e:
            LOG.warning(f"Could not process inline provider_groups: {e}")
            provider_flat = None
    
    # Extract URL references (format 2 - location field contains URLs)
    if has_location:
        LOG.info("Found 'location' field in provider_references (URLs to download)")
        try:
            provider_urls = pr.select(
                "source_file",
                F.col("pr.provider_group_id").cast("string").alias("provider_group_id"),
                F.col("pr.location").alias("location")
            ).filter(F.col("location").isNotNull())
            
            # Skip count() to avoid file re-read issues with Unity Catalog Volumes on serverless
            # The download function will handle empty DataFrames and will collect when needed
            LOG.info("Provider reference URLs DataFrame created (will process during download)")
            
            # Warn if URL processing is not available
            if not URL_PROCESSING_AVAILABLE:
                LOG.error(
                    "=" * 80 + "\n"
                    "ERROR: Provider references contain URLs but downloader module is not available!\n"
                    "  URL expansion will fail. NPIs from URLs will be missing.\n"
                    "  Check that src/mrf_downloader.py exists and is importable.\n"
                    "=" * 80
                )
        except Exception as e:
            LOG.warning(f"Could not extract provider reference URLs: {e}")
            provider_urls = None
    
    return provider_flat, provider_urls


def download_and_process_provider_urls(
    spark: SparkSession,
    provider_urls: DataFrame,
    download_dir: Path,
    skip_download: bool = False,
    enable_url_expansion: bool = True,
) -> Optional[DataFrame]:
    """
    Download provider reference URLs and extract NPIs from content.
    
    The provider_group_id from the original JSON links rates to provider URLs.
    Each URL contains provider_groups with NPIs. We keep the original provider_group_id
    for joining back to rates.
    
    Args:
        spark: SparkSession
        provider_urls: DataFrame with source_file, provider_group_id and location columns
        download_dir: Directory to save downloaded files
        skip_download: If True, skip downloading and just read existing files from download_dir
        enable_url_expansion: If False, skip URL expansion entirely (default: True)
        
    Returns:
        DataFrame with source_file, provider_group_id, npi, provider_url_file columns, or None if skipped
    """
    # Check if URL expansion is enabled
    if not enable_url_expansion:
        LOG.warning(
            "=" * 80 + "\n"
            "WARNING: URL provider expansion is DISABLED via config!\n"
            "  Provider URLs will NOT be downloaded or processed.\n"
            "  This may result in missing NPI data if providers are referenced via URLs.\n"
            "  Set processing.enable_url_expansion=true to enable.\n"
            "=" * 80
        )
        return None
    
    # Check if URL processing module is available
    if not URL_PROCESSING_AVAILABLE:
        LOG.error(
            "=" * 80 + "\n"
            "ERROR: URL provider expansion FAILED - downloader module not available!\n"
            "  Provider URLs will NOT be downloaded or processed.\n"
            "  This will result in missing NPI data if providers are referenced via URLs.\n"
            "  Check that src/mrf_downloader.py exists and is importable.\n"
            "=" * 80
        )
        return None
    # Collect URLs info - this is necessary but may be slow for large datasets
    # The provider_urls DataFrame should be relatively small (just 3 columns)
    LOG.info("Collecting provider URL rows (this may take a while)...")
    LOG.info("Note: This requires reading source files to extract URLs")
    
    try:
        url_df = provider_urls.select("source_file", "provider_group_id", "location")
        # Skip count() to avoid expensive materialization - collect() will work regardless
        
        url_rows = url_df.collect()
        LOG.info(f"Successfully collected {len(url_rows):,} URL rows")
    except Exception as collect_error:
        LOG.error(f"Failed to collect URL rows: {collect_error}")
        LOG.error("Consider using skip_url_download=true if URLs are already downloaded")
        return None
    
    if not url_rows:
        LOG.warning("No URL rows to process")
        return None
    
    unique_urls = list(set(row["location"] for row in url_rows if row["location"]))
    total_urls = len(unique_urls)
    LOG.info(f"Found {total_urls} unique URLs from {len(url_rows)} rows")
    
    url_to_path = {}
    
    if skip_download:
        # Skip downloading - build url_to_path from existing files in download_dir
        LOG.info(f"Skipping download - reading existing files from {download_dir}")
        
        for url in unique_urls:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
            expected_file = download_dir / f"url_{url_hash}.json"
            if expected_file.exists():
                url_to_path[url] = expected_file
        
        LOG.info(f"Found {len(url_to_path)}/{total_urls} files already downloaded")
        
        if not url_to_path:
            LOG.warning("No pre-downloaded files found matching URLs")
            return None
    else:
        # Download URLs with progress logging
        LOG.info("Downloading provider reference URLs...")
        
        # Download with progress tracking
        session = session_with_retries()
        downloaded_count = 0
        failed_count = 0
        
        for idx, url in enumerate(unique_urls, 1):
            # Generate filename based on URL hash
            url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
            dest_file = download_dir / f"url_{url_hash}.json"
            
            # Skip if already exists
            if dest_file.exists():
                url_to_path[url] = dest_file
                downloaded_count += 1
                continue
            
            try:
                download_file(session, url, dest_file)
                url_to_path[url] = dest_file
                downloaded_count += 1
                LOG.info(f"Downloaded {downloaded_count}/{total_urls}: {url.split('/')[-1]}")
            except Exception as e:
                failed_count += 1
                LOG.warning(f"Failed {idx}/{total_urls}: {url.split('/')[-1]} - {e}")
        
        LOG.info(f"Download complete: {downloaded_count}/{total_urls} succeeded, {failed_count} failed")
        
        if not url_to_path:
            LOG.warning("No URLs downloaded successfully")
            return None
    
    # Parse downloaded content and extract NPIs
    # Key: each provider_group_id in original JSON points to a URL
    # The URL contains provider_groups with NPIs - we flatten all NPIs under that provider_group_id
    all_provider_data = []
    total_url_rows = len(url_rows)
    
    LOG.info(f"Processing {total_url_rows} URL rows to extract NPIs...")
    
    for idx, row in enumerate(url_rows, 1):
        source_file = row["source_file"]
        provider_group_id = row["provider_group_id"]  # This is the link to rates
        location = row["location"]
        
        if location not in url_to_path:
            LOG.debug(f"Skipping {idx}/{total_url_rows}: {location} - file not found")
            continue
        
        file_path = url_to_path[location]
        # Extract filename from URL for tracking
        url_filename = location.split("/")[-1] if "/" in location else location
        
        try:
            # Read the downloaded file (may be gzipped)
            if str(file_path).endswith('.gz'):
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    content = json_module.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = json_module.load(f)
            
            # Extract provider_groups from content
            provider_groups = content.get("provider_groups", [])
            if not provider_groups and isinstance(content, list):
                provider_groups = content
            
            # Flatten all NPIs from all provider_groups in this URL
            # They all map to the same provider_group_id from the original file
            npis_extracted = 0
            for pg in provider_groups:
                if isinstance(pg, dict):
                    npis = pg.get("npi", [])
                    if isinstance(npis, list):
                        for npi in npis:
                            all_provider_data.append({
                                "source_file": source_file,
                                "provider_group_id": str(provider_group_id) if provider_group_id else "",
                                "npi": str(npi),
                                "provider_url_file": url_filename,
                            })
                            npis_extracted += 1
            
            LOG.info(f"Processed {idx}/{total_url_rows}: {url_filename} - extracted {npis_extracted} NPIs (total so far: {len(all_provider_data)})")
        except Exception as e:
            LOG.warning(f"Could not parse content from {location} ({idx}/{total_url_rows}): {e}")
            continue
    
    if not all_provider_data:
        LOG.warning("No NPI data extracted from downloaded URLs")
        return None
    
    LOG.info(f"Extracted {len(all_provider_data)} NPI records from {len(url_to_path)} URL files")
    
    # Create DataFrame from extracted data - use StringType for provider_group_id to preserve decimals
    schema = T.StructType([
        T.StructField("source_file", T.StringType()),
        T.StructField("provider_group_id", T.StringType()),
        T.StructField("npi", T.StringType()),
        T.StructField("provider_url_file", T.StringType()),
    ])
    
    return spark.createDataFrame(all_provider_data, schema)


def load_provider_npis(provider_csv_path: str) -> Optional[set[str]]:
    """
    Load NPIs from provider CSV file.
    
    Returns:
        Set of NPI strings, or None if loading fails
    """
    import pandas as pd
    
    try:
        provider_df_pd = pd.read_csv(provider_csv_path)
        if "NPPES_NPI" in provider_df_pd.columns:
            provider_npi_set = set(provider_df_pd["NPPES_NPI"].astype(str).dropna().unique())
            LOG.info(f"Loaded {len(provider_npi_set)} NPIs from provider CSV")
            return provider_npi_set
        else:
            LOG.warning(f"NPPES_NPI column not found in {provider_csv_path}")
            return None
    except Exception as e:
        LOG.warning(f"Could not load provider NPIs: {e}")
        return None
