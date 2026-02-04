"""Utility functions for downloading URLs from Parquet files and joining content back."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.download.mrf_downloader import download_file, session_with_retries

LOG = logging.getLogger("src.url_content_downloader")

# Pattern to detect URLs in text (same as in structure_analyzer.py)
URL_PATTERN = re.compile(
    r'https?://[^\s<>"{}|\\^`\[\]]+|www\.[^\s<>"{}|\\^`\[\]]+',
    re.IGNORECASE,
)


def extract_urls_from_value(value: Any) -> List[str]:
    """
    Extract all URLs from a value (string, number, or other JSON-serializable type).
    
    Args:
        value: Value to extract URLs from
        
    Returns:
        List of unique URLs found in the value
    """
    if value is None:
        return []
    
    # Convert to string
    if isinstance(value, str):
        value_str = value
    else:
        value_str = json.dumps(value)
    
    # Find all URLs
    urls = URL_PATTERN.findall(value_str)
    
    # Normalize URLs (add https:// if www. is found)
    normalized_urls = []
    for url in urls:
        if url.startswith("www."):
            url = "https://" + url
        normalized_urls.append(url)
    
    return list(set(normalized_urls))  # Return unique URLs


def detect_url_column(df: DataFrame) -> Optional[str]:
    """
    Detect which column in the DataFrame contains URLs.
    
    Checks column names first (looking for 'url' in name), then scans a sample
    of data to find columns containing URL patterns.
    
    Args:
        df: Spark DataFrame to check
        
    Returns:
        Name of column containing URLs, or None if not found
    """
    # First, check if any column name suggests it contains URLs
    url_like_columns = [col for col in df.columns if "url" in col.lower()]
    if url_like_columns:
        LOG.info("Found potential URL columns by name: %s", url_like_columns)
        # Return the first one found
        return url_like_columns[0]
    
    # If no obvious column names, sample data to find URLs
    LOG.info("No URL column names found, sampling data to detect URLs...")
    # Use a fixed sample size instead of calling count() to avoid triggering execution
    # that may cause serialization issues with UDFs/broadcast variables
    
    LOG.warning("No URL column detected in DataFrame")
    return None


def extract_unique_urls_with_row_ids(
    df: DataFrame,
    url_column: str,
) -> Tuple[DataFrame, Set[str]]:
    """
    Extract unique URLs from a DataFrame column and add row identifiers.
    
    Args:
        df: Spark DataFrame
        url_column: Name of column containing URLs
        
    Returns:
        Tuple of:
        - DataFrame with row_id and url columns (one row per URL found)
        - Set of unique URLs
    """
    # Add row ID to DataFrame
    df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
    
    # Extract URLs from the URL column
    # Use a UDF to extract URLs from each value
    from pyspark.sql.types import ArrayType, StringType
    
    def extract_urls_udf(value):
        """UDF to extract URLs from a value."""
        if value is None:
            return []
        urls = extract_urls_from_value(value)
        return urls
    
    extract_urls_func = F.udf(extract_urls_udf, ArrayType(StringType()))
    
    # Explode URLs into separate rows
    df_urls = df_with_id.select(
        "_row_id",
        F.explode_outer(extract_urls_func(F.col(url_column))).alias("url")
    ).filter(F.col("url").isNotNull())
    
    # Get unique URLs
    unique_urls = set(df_urls.select("url").distinct().rdd.map(lambda r: r[0]).collect())
    
    LOG.info("Extracted %d unique URLs from %d rows", len(unique_urls), df.count())
    
    return df_urls, unique_urls


def download_url_content(
    urls: Set[str],
    download_dir: Path,
    session: Optional[requests.Session] = None,
    max_consecutive_failures: int = 10,
) -> Dict[str, Path]:
    """
    Download URL content to files in download directory.
    
    If 10 consecutive URLs fail to download, assumes the rest won't work either
    and skips remaining URLs.
    
    Args:
        urls: Set of URLs to download
        download_dir: Directory to save downloaded files
        session: Optional requests session (creates new one if not provided)
        max_consecutive_failures: Maximum consecutive failures before stopping (default: 10)
        
    Returns:
        Dictionary mapping URL to local file path (only successfully downloaded URLs)
    """
    # Directory creation handled by ensure_directories_from_config()
    
    if session is None:
        session = session_with_retries()
    
    url_to_path: Dict[str, Path] = {}
    consecutive_failures = 0
    total_urls = len(urls)
    processed_count = 0
    
    for url in urls:
        processed_count += 1
        # Generate filename based on URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
        
        # Try to determine file extension from URL or content type
        # For now, use .json as default (MRF URLs are typically JSON)
        dest_file = download_dir / f"url_{url_hash}.json"
        
        try:
            download_file(session, url, dest_file)
            url_to_path[url] = dest_file
            consecutive_failures = 0  # Reset failure counter on success
            LOG.info("Downloaded URL %s -> %s", url, dest_file)
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1
            LOG.error("Failed to download URL %s: %s (consecutive failures: %d)", url, exc, consecutive_failures)
            
            # If we hit the max consecutive failures, stop trying
            if consecutive_failures >= max_consecutive_failures:
                remaining_urls = total_urls - processed_count
                LOG.warning(
                    "Reached %d consecutive URL download failures. "
                    "Assuming remaining %d URLs won't work and skipping them.",
                    max_consecutive_failures,
                    remaining_urls,
                )
                break
    
    LOG.info("Downloaded %d/%d URLs successfully (processed %d, skipped %d)", 
             len(url_to_path), total_urls, processed_count, total_urls - processed_count)
    return url_to_path


def load_url_content(url_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load JSON content from a downloaded URL file.
    
    Args:
        url_path: Path to downloaded JSON file
        
    Returns:
        Parsed JSON content as dict, or None if loading fails
    """
    try:
        with open(url_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:  # noqa: BLE001
        LOG.error("Failed to load URL content from %s: %s", url_path, exc)
        return None


def join_url_content_to_dataframe(
    spark: SparkSession,
    df: DataFrame,
    url_column: str,
    url_to_path: Dict[str, Path],
) -> DataFrame:
    """
    Join downloaded URL content back to the original DataFrame.
    
    Args:
        spark: SparkSession
        df: Original DataFrame
        url_column: Name of column containing URLs
        url_to_path: Dictionary mapping URLs to local file paths
        
    Returns:
        DataFrame with URL content joined as a new column
    """
    # Add row ID to original DataFrame
    df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
    
    # Load all URL content into memory (for small to medium datasets)
    # Create a mapping: url -> content
    url_to_content: Dict[str, Dict[str, Any]] = {}
    for url, path in url_to_path.items():
        content = load_url_content(path)
        if content is not None:
            url_to_content[url] = content
    
    LOG.info("Loaded content for %d/%d URLs", len(url_to_content), len(url_to_path))
    
    # Create a DataFrame with URL -> content mapping
    if not url_to_content:
        LOG.warning("No URL content loaded, returning original DataFrame")
        return df_with_id.drop("_row_id")
    
    # Create list of (url, content) tuples for DataFrame creation
    url_content_data = [(url, json.dumps(content)) for url, content in url_to_content.items()]
    
    from pyspark.sql.types import StructType, StructField, StringType
    
    url_content_schema = StructType([
        StructField("url", StringType(), False),
        StructField("url_content", StringType(), True),
    ])
    
    url_content_df = spark.createDataFrame(url_content_data, schema=url_content_schema)
    
    # Extract URLs from the URL column and create a joinable structure
    from pyspark.sql.types import ArrayType
    
    def extract_urls_udf(value):
        """UDF to extract URLs from a value."""
        if value is None:
            return []
        urls = extract_urls_from_value(value)
        return urls
    
    extract_urls_func = F.udf(extract_urls_udf, ArrayType(StringType()))
    
    # Create exploded DataFrame with row_id and url
    df_urls = df_with_id.select(
        "_row_id",
        F.explode_outer(extract_urls_func(F.col(url_column))).alias("url")
    ).filter(F.col("url").isNotNull())
    
    # Join with URL content
    df_urls_with_content = df_urls.join(
        url_content_df,
        on="url",
        how="left"
    )
    
    # Aggregate URL content back to rows (collect all URLs and their content for each row)
    # Since a row might have multiple URLs, we'll create a map or array of URL contents
    df_url_content_agg = df_urls_with_content.groupBy("_row_id").agg(
        F.map_from_arrays(
            F.collect_list("url"),
            F.collect_list("url_content")
        ).alias("url_content_map")
    )
    
    # Join back to original DataFrame
    result_df = df_with_id.join(
        df_url_content_agg,
        on="_row_id",
        how="left"
    ).drop("_row_id")
    
    return result_df

