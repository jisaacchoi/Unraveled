"""Main conversion function for converting NDJSON files to Parquet using PySpark schemas."""
from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import array_distinct, collect_list, flatten as spark_flatten, udf
from pyspark.sql.types import ArrayType, StringType, StructType

from src.generate_schemas.schema_groups_db import get_files_for_schema_group
from src.generate_schemas.schema_inference import load_schema_by_min_file_name
from src.convert.dataframe_transformer import (
    auto_flatten,
    restructure_negotiated_rate_df,
)
from src.shared.url_content_downloader import (
    detect_url_column,
    download_url_content,
    extract_unique_urls_with_row_ids,
    join_url_content_to_dataframe,
)

LOG = logging.getLogger("src.pyspark_converter_test")


def load_provider_data_npis(
    spark: SparkSession,
    csv_file_path: str | Path,
    npi_column: str = "NPI",
) -> Optional[set[str]]:
    """
    Load NPIs from a CSV file into a Python set.
    
    Collects NPIs into a Python set to avoid SparkSession lifecycle issues in long-running jobs.
    The set can be used with broadcast variables for efficient filtering.
    
    Args:
        spark: SparkSession
        csv_file_path: Path to CSV file containing provider data
        npi_column: Name of the NPI column in the CSV (default: "NPI")
        
    Returns:
        Python set of NPI strings, or None if loading fails
    """
    try:
        csv_path = Path(csv_file_path)
        
        if not csv_path.exists():
            LOG.error("CSV file not found: %s", csv_path)
            return None
        
        LOG.info("Loading NPIs from CSV file: %s (column: %s)...", csv_path, npi_column)
        
        # Read CSV using Spark
        provider_df = spark.read.option("header", "true").csv(str(csv_path))
        
        # Check if NPI column exists
        if npi_column not in provider_df.columns:
            LOG.error("Column '%s' not found in CSV file. Available columns: %s", 
                     npi_column, provider_df.columns)
            return None
        
        # Cast NPI column to string, get distinct values, and collect into Python set
        # This avoids SparkSession lifecycle issues by not keeping a DataFrame reference
        npi_list = provider_df.select(F.col(npi_column).cast("string").alias("NPPES_NPI")).distinct().collect()
        npi_set = {row["NPPES_NPI"] for row in npi_list if row["NPPES_NPI"] is not None}
        
        LOG.info("Loaded %d distinct NPIs from %s (collected into Python set)", len(npi_set), csv_path.name)
        
        return npi_set
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Failed to load provider_data from CSV file: %s", exc)
        return None


def filter_npis_by_provider_data(
    df: DataFrame,
    provider_npi_set: set[str] | None,
    spark: SparkSession | None = None,
) -> DataFrame:
    """
    Filter NPIs in the DataFrame to only include NPI 1043268410 (TEST VERSION - hardcoded).
    
    Uses a broadcast variable with a Python set to avoid SparkSession lifecycle issues.
    After clean_dataframe_columns, the DataFrame should have a "NPI_Final" column containing arrays of NPIs.
    This function filters those arrays to keep only NPI 1043268410.
    
    Args:
        df: Input Spark DataFrame with "NPI_Final" array column (after clean_dataframe_columns)
        provider_npi_set: Ignored in test version (always uses hardcoded NPI 1043268410)
        spark: SparkSession (required for broadcast variable)
        
    Returns:
        DataFrame with filtered NPI arrays (only 1043268410)
    """
    # Log all columns in the DataFrame at the start of the function
    all_columns = list(df.columns)
    LOG.info("DataFrame columns at start of filter_npis_by_provider_data: %s", all_columns)
    LOG.info("Total columns: %d", len(all_columns))
    LOG.info("Column headers list:")
    for i, col_name in enumerate(all_columns):
        LOG.info("  [%d] %s", i, col_name)
    
    if "NPI_Final" not in df.columns:
        LOG.debug("'NPI_Final' column not found in DataFrame, skipping NPI filtering")
        return df
    
    # TEST VERSION: Always filter to only NPI 1043268410
    test_npi_set = {"1043268410"}
    LOG.info("TEST VERSION: Filtering NPIs to only include: 1043268410")
    
    try:
        # Get SparkSession from DataFrame if not provided
        if spark is None:
            spark = df.sparkSession
        
        # Validate SparkSession is still active
        try:
            _ = spark.sparkContext
        except Exception as session_exc:  # noqa: BLE001
            LOG.error("SparkSession is invalid: %s", session_exc)
            raise ValueError("SparkSession is invalid or closed") from session_exc
        
        LOG.info("Filtering NPIs: 1 valid NPI (1043268410)")
        
        # Broadcast the NPI set for efficient filtering
        broadcast_npi_set = spark.sparkContext.broadcast(test_npi_set)
        
        # Use standard PySpark pattern: nested function with broadcast variable access
        # This pattern should work, but we'll add error handling to catch serialization issues
        def filter_npi_array(npi_array):
            """Filter NPI array to only include valid NPIs from broadcast set."""
            try:
                if npi_array is None:
                    return []
                valid_set = broadcast_npi_set.value
                return [npi for npi in npi_array if npi is not None and str(npi) in valid_set]
            except Exception as filter_exc:
                # If there's an error accessing the broadcast variable, return empty array
                # Logging in UDFs is limited, so we can't log here
                return []
        
        filter_udf = udf(filter_npi_array, ArrayType(StringType()))
        
        # 1. Cast NPI_Final column to array<string> first
        df_with_casted_npis = df.withColumn(
            "NPI_Final",
            F.coalesce(
                F.col("NPI_Final").cast("array<string>"),
                F.array().cast("array<string>")
            )
        )
        
        # 2. Filter the NPI arrays using the UDF
        df_filtered = df_with_casted_npis.withColumn(
            "NPI_Final",
            filter_udf(F.col("NPI_Final"))
        )
        
        # Note: We skip calling count() here to avoid triggering execution that may cause
        # "Python worker failed to connect back" errors. The filtering transformation is
        # still applied and will execute when the DataFrame is written. This is a lazy
        # transformation, so skipping count() doesn't affect the actual filtering logic.
        # Also, we don't unpersist the broadcast variable here - let Spark handle cleanup automatically.
        # Unpersisting too early can cause serialization issues when the DataFrame is executed later.
        LOG.info("NPI filtering by provider_data applied (lazy transformation, will execute on write)")
        
        return df_filtered
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Failed to filter NPIs by provider_data: %s", exc)
        return df


def group_by_billing_and_aggregate_npis(df: DataFrame) -> DataFrame:
    """
    Group DataFrame by billing fields and aggregate all NPIs into a single list with no duplicates.
    
    Groups by: billing_code, billing_code_type, billing_code_type_version, negotiated_rate, negotiated_type, billing_class
    Also includes optional columns if present: original_filename, provider_references
    Aggregates: All NPIs from the group into a single list with duplicates removed.
    All grouping columns (including billing_class and optional columns) are preserved in the output.
    
    Args:
        df: Input Spark DataFrame with billing and NPI columns
        
    Returns:
        Grouped and aggregated Spark DataFrame (includes all grouping columns in output)
    """
    # After clean_dataframe_columns, the DataFrame should have "NPI_Final" column
    if "NPI_Final" not in df.columns:
        LOG.warning("'NPI_Final' column not found. Skipping NPI aggregation.")
        return df
    
    # Check if required grouping columns exist
    group_by_cols = ["billing_code", "billing_code_type", "billing_code_type_version", "negotiated_rate", "negotiated_type", "billing_class"]
    missing_cols = [col for col in group_by_cols if col not in df.columns]
    if missing_cols:
        LOG.warning("Missing grouping columns: %s. Skipping grouping.", missing_cols)
        return df
    
    # Include optional columns in groupBy if they exist (to preserve them in output)
    optional_cols = []
    if "original_filename" in df.columns:
        optional_cols.append("original_filename")
    if "provider_references" in df.columns:
        optional_cols.append("provider_references")
    
    if optional_cols:
        group_by_cols = group_by_cols + optional_cols
        LOG.info("Grouping by: %s (including optional columns: %s) and aggregating NPIs...", group_by_cols, optional_cols)
    else:
        LOG.info("Grouping by: %s and aggregating NPIs...", group_by_cols)
    
    # Define aggregation: collect all NPI arrays, flatten them, and remove duplicates
    # Since NPIs are already in arrays, we need to:
    # 1. Collect all arrays using collect_list
    # 2. Flatten the array of arrays
    # 3. Remove duplicates using array_distinct
    
    # Aggregate NPIs: collect all arrays, flatten them, and remove duplicates
    # Since NPIs are already in arrays (each row has an array), we collect all arrays,
    # flatten the array of arrays into one array, and remove duplicates
    # Use repartition before groupBy to ensure better distribution and reduce memory pressure
    # This helps avoid OOM errors during aggregation
    num_partitions = df.rdd.getNumPartitions()
    if num_partitions < 50:
        # Repartition to more partitions for better distribution during groupBy
        LOG.info("Repartitioning DataFrame to 100 partitions before grouping (current: %d)", num_partitions)
        df = df.repartition(100)
    
    df_grouped = df.groupBy(*group_by_cols).agg(
        F.array_distinct(spark_flatten(F.collect_list(F.coalesce(F.col("NPI_Final"), F.array())))).alias("NPI_Final")
    )
    
    # Note: We skip count() calls here to avoid triggering execution that may cause
    # "Python worker failed to connect back" errors. The grouping will execute when DataFrame is written.
    LOG.info("Applied grouping and aggregation (lazy transformation, will execute on write)")
    
    return df_grouped


def clean_dataframe_columns(df: DataFrame) -> DataFrame:
    """
    Clean DataFrame columns: drop empty columns, handle duplicates, rename columns.
    
    Operations:
    1. Drop columns that are completely empty (all null values)
    2. Rename "all_npis" to "NPI_Final" if it exists
    3. Handle duplicate column names with "_1" suffix: keep "_1" version, drop original, rename back
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Cleaned Spark DataFrame
    """
    LOG.info("Cleaning DataFrame (drop empty columns, handle duplicates, rename columns)...")
    
    # Log columns before cleaning
    columns_before = list(df.columns)
    LOG.info("Columns before cleaning: %s", columns_before)
    
    # 1. Rename "all_npis" to "NPI_Final" if it exists (do this BEFORE dropping "npi")
    if "all_npis" in df.columns:
        df = df.withColumnRenamed("all_npis", "NPI_Final")
        LOG.info("Renamed 'all_npis' column to 'NPI_Final'")
    
    # 3. Drop lowercase "npi" column if it exists (keep only "NPI_Final")
    if "npi" in df.columns:
        if "NPI_Final" in df.columns:
            df = df.drop("npi")
            LOG.info("Dropped lowercase 'npi' column, keeping 'NPI_Final' column")
        else:
            # If no "NPI_Final" exists, rename "npi" to "NPI_Final" instead of dropping it
            df = df.withColumnRenamed("npi", "NPI_Final")
            LOG.info("Renamed lowercase 'npi' column to 'NPI_Final' (no 'all_npis' or 'NPI_Final' existed)")
    
    # Log columns after cleaning
    columns_after = list(df.columns)
    LOG.info("Columns after cleaning: %s", columns_after)
    if "NPI_Final" not in columns_after:
        LOG.warning("'NPI_Final' column not present after cleaning! This may cause issues in subsequent steps.")

    
    # 4. Handle duplicate column names with "_1" suffix
    # When Spark encounters duplicate column names, it adds "_1" suffix
    # Strategy: Find columns ending with "_1", check if base name exists,
    # drop the base column, and rename "_1" version back to original name
    columns_with_suffix = [col for col in df.columns if col.endswith("_1")]
    rename_mapping = {}
    columns_to_drop_duplicates = []
    
    for col_with_suffix in columns_with_suffix:
        base_name = col_with_suffix[:-2]  # Remove "_1" suffix (2 characters)
        if base_name in df.columns:
            # Both base and "_1" versions exist - keep "_1", drop base, rename back
            rename_mapping[col_with_suffix] = base_name
            columns_to_drop_duplicates.append(base_name)
            LOG.debug("Will rename '%s' to '%s' and drop original '%s'", col_with_suffix, base_name, base_name)
    
    if columns_to_drop_duplicates:
        # Drop original columns first
        df = df.drop(*columns_to_drop_duplicates)
        LOG.info("Dropped %d duplicate column(s): %s", len(columns_to_drop_duplicates), columns_to_drop_duplicates)
    
    if rename_mapping:
        # Rename "_1" columns back to original names
        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:  # Verify it still exists after dropping
                df = df.withColumnRenamed(old_name, new_name)
        LOG.info("Renamed %d column(s) from '_1' suffix to original names: %s", 
                len(rename_mapping), list(rename_mapping.values()))
    
    return df


def process_urls_in_dataframe(
    spark: SparkSession,
    df: DataFrame,
    download_dir: Path | None = None,
) -> DataFrame:
    """
    Process URLs in DataFrame: detect URL column, download content, and join back.
    
    If download_dir is None, URLs are detected but not downloaded/joined.
    If download_dir is provided, URLs are downloaded and joined back to DataFrame.
    
    Args:
        spark: SparkSession
        df: Input Spark DataFrame
        download_dir: Optional directory to download URL content to. If None, skips URL processing.
        
    Returns:
        DataFrame with URL content joined (if URLs were processed), or original DataFrame
    """
    if download_dir is None:
        LOG.debug("No download directory provided, skipping URL processing")
        return df
    
    try:
        # Detect URL column
        url_column = detect_url_column(df)
        if not url_column:
            LOG.debug("No URL column detected in DataFrame, skipping URL processing")
            return df
        
        LOG.info("Detected URL column: %s", url_column)
        
        # Extract unique URLs with row IDs
        df_urls, unique_urls = extract_unique_urls_with_row_ids(df, url_column)
        
        if not unique_urls:
            LOG.debug("No URLs found in DataFrame, skipping URL processing")
            return df
        
        # Download URL content
        LOG.info("Downloading %d unique URLs...", len(unique_urls))
        url_to_path = download_url_content(unique_urls, download_dir)
        
        if not url_to_path:
            LOG.warning("No URLs downloaded successfully, returning original DataFrame")
            return df
        
        # Join URL content back to DataFrame
        LOG.info("Joining URL content back to DataFrame...")
        df_with_urls = join_url_content_to_dataframe(spark, df, url_column, url_to_path)
        
        LOG.info("Successfully processed URLs and joined content to DataFrame")
        return df_with_urls
        
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Error processing URLs in DataFrame: %s, returning original DataFrame", exc)
        return df


def filter_billing_codes(df: DataFrame, billing_code_type: str | None = None, billing_codes: list[str] | None = None) -> DataFrame:
    """
    Filter DataFrame to only include records matching billing_code_type and billing_codes.
    
    Supports both exact codes and ranges (e.g., "70010–76499" or "70010-76499").
    Ranges can use en-dash (–), em-dash (—), or hyphen (-).
    
    Args:
        df: Input Spark DataFrame
        billing_code_type: Required billing_code_type value (e.g., "CPT"). If None, no filtering by type.
        billing_codes: List of billing_code values to include. Can contain:
            - Exact codes: "43242", "0813T"
            - Ranges: "70010–76499", "76506-76999" (supports en-dash, em-dash, or hyphen)
            If None or empty, no filtering by codes.
        
    Returns:
        Filtered Spark DataFrame
    """
    if billing_code_type is None and (billing_codes is None or len(billing_codes) == 0):
        # No filtering requested
        return df
    
    conditions = []
    
    # Filter by billing_code_type if specified
    if billing_code_type is not None:
        if "billing_code_type" in df.columns:
            conditions.append(F.col("billing_code_type") == billing_code_type)
            LOG.info("Filtering by billing_code_type='%s'", billing_code_type)
        else:
            LOG.warning("billing_code_type column not found in DataFrame, skipping type filter")
    
    # Filter by billing_codes if specified
    if billing_codes and len(billing_codes) > 0:
        if "billing_code" in df.columns:
            # Parse billing codes into exact matches and ranges
            exact_codes = []
            range_conditions = []
            
            import re
            # Pattern to match ranges: digits/digits or alphanumeric/alphanumeric separated by dash
            range_pattern = re.compile(r'^(.+?)[–—\-](.+)$')
            
            for code_entry in billing_codes:
                code_entry = code_entry.strip()
                if not code_entry:
                    continue
                
                # Check if it's a range
                range_match = range_pattern.match(code_entry)
                if range_match:
                    start_code = range_match.group(1).strip()
                    end_code = range_match.group(2).strip()
                    
                    # Determine if we should use numeric or string comparison
                    # If both start and end are numeric (can be converted to float), use numeric comparison
                    # Otherwise, use string comparison (handles alphanumeric codes like "0813T")
                    try:
                        start_num = float(start_code)
                        end_num = float(end_code)
                        # Both are numeric - use numeric comparison
                        # Use regex to check if billing_code is numeric before casting
                        # This safely handles non-numeric billing codes (e.g., '0134U')
                        # Pattern matches: optional sign, digits, optional decimal point and digits
                        numeric_pattern = r'^[+-]?\d+(\.\d+)?$'
                        is_numeric = F.col("billing_code").rlike(numeric_pattern)
                        # Cast to double only if numeric, otherwise NULL
                        billing_code_num = F.when(is_numeric, F.col("billing_code").cast("double")).otherwise(None)
                        # Only match if the value is numeric and within range
                        range_condition = (
                            billing_code_num.isNotNull() & 
                            (billing_code_num >= start_num) & 
                            (billing_code_num <= end_num)
                        )
                        range_conditions.append(range_condition)
                        LOG.debug("Added numeric range condition: %s <= billing_code <= %s (non-numeric codes will be excluded)", start_code, end_code)
                    except (ValueError, TypeError):
                        # At least one is not numeric - use string comparison
                        range_condition = (F.col("billing_code") >= start_code) & (F.col("billing_code") <= end_code)
                        range_conditions.append(range_condition)
                        LOG.debug("Added string range condition: %s <= billing_code <= %s", start_code, end_code)
                else:
                    # Exact match
                    exact_codes.append(code_entry)
            
            # Build combined condition for billing codes
            code_conditions = []
            
            # Add exact match conditions
            if exact_codes:
                code_conditions.append(F.col("billing_code").isin(exact_codes))
                LOG.info("Filtering by exact billing_code matches: %s", exact_codes)
            
            # Add range conditions (OR them together)
            if range_conditions:
                range_condition_combined = range_conditions[0]
                for range_cond in range_conditions[1:]:
                    range_condition_combined = range_condition_combined | range_cond
                code_conditions.append(range_condition_combined)
                LOG.info("Filtering by billing_code ranges: %d range(s)", len(range_conditions))
            
            # Combine exact and range conditions with OR
            if code_conditions:
                if len(code_conditions) == 1:
                    billing_code_condition = code_conditions[0]
                else:
                    billing_code_condition = code_conditions[0]
                    for cond in code_conditions[1:]:
                        billing_code_condition = billing_code_condition | cond
                
                conditions.append(billing_code_condition)
        else:
            LOG.warning("billing_code column not found in DataFrame, skipping code filter")
    
    if conditions:
        # Combine all conditions with AND
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition & condition
        
        # Note: We skip count() calls here to avoid triggering execution that may cause
        # "Python worker failed to connect back" errors. The filter will execute when DataFrame is written.
        df_filtered = df.filter(combined_condition)
        LOG.info("Applied billing code filter (lazy transformation, will execute on write)")
        return df_filtered
    
    return df


def convert_file_with_schema_to_parquet(
    spark: SparkSession,
    ndjson_path: Path,
    output_dir: Path,
    schema: StructType,
    target_partition_size_mb: float = 500.0,
    billing_code_type: str | None = None,
    billing_codes: list[str] | None = None,
    download_dir: Path | None = None,
    original_filename: str | None = None,
    provider_npi_set: Optional[set[str]] = None,
) -> int:
    """
    Convert a single NDJSON file to Parquet files using a provided schema.
    
    This function reads the NDJSON file, flattens nested structures,
    filters by billing_code_type and billing_codes (if specified),
    and writes Parquet files partitioned by target size (default 500MB).
    Uses ZSTD compression and ensures array columns (like NPIs) are stored as Parquet lists.
    
    Args:
        spark: SparkSession
        ndjson_path: Path to input NDJSON file
        output_dir: Directory to write output Parquet files
        schema: PySpark StructType schema to use
        target_partition_size_mb: Target size per Parquet partition in MB (default: 500MB)
        billing_code_type: Optional billing_code_type filter (e.g., "CPT")
        billing_codes: Optional list of billing_code values to filter by
        provider_npi_set: Optional set of valid NPI strings for filtering
        
    Returns:
        Number of rows written, or 0 on error
    """
    if not ndjson_path.exists():
        LOG.error("NDJSON file not found: %s", ndjson_path)
        return 0
    
    if not ndjson_path.suffix == ".ndjson":
        LOG.error("Input file must be .ndjson: %s", ndjson_path)
        return 0
    
    try:
        # Read NDJSON with provided schema
        df = spark.read.schema(schema).json(str(ndjson_path))
        
        # Apply auto_flatten to flatten nested structures
        LOG.info("Flattening DataFrame...")
        df = auto_flatten(df)
        
        # Restructure negotiated rate data if applicable (after flattening)
        LOG.info("Checking if restructure_negotiated_rate_df is applicable...")
        if "provider_group_id" in df.columns and "provider_references" in df.columns:
            LOG.info("Applying restructure_negotiated_rate_df...")
            df = restructure_negotiated_rate_df(df)
        else:
            LOG.debug("Skipping restructure_negotiated_rate_df (provider_group_id or provider_references not present)")
        
        # Clean DataFrame columns before filtering and grouping
        df = clean_dataframe_columns(df)
        
        # Filter NPIs by provider_data if provider_npi_set is provided
        if provider_npi_set is not None:
            LOG.info("Filtering NPIs to only include those in provider_data table...")
            df = filter_npis_by_provider_data(df, provider_npi_set, spark)
            # Filter to only keep rows that have at least one NPI after filtering
            # Note: We skip count() here to avoid triggering UDF execution that causes
            # "Python worker failed to connect back" errors. The filter will execute when DataFrame is written.
            df = df.filter(F.size(F.col("NPI_Final")) > 0)
            LOG.info("Applied filter to keep only rows with at least one NPI (lazy transformation)")
        else:
            LOG.debug("No provider_npi_set provided, skipping NPI filtering")
        
        # Add original filename as a column (before grouping so it can be included in groupBy)
        if original_filename is None:
            # Extract original filename from ndjson_path
            original_filename = ndjson_path.stem
            # If the filename contains .json.gz, keep that part; otherwise try to reconstruct
            if ".json.gz" in original_filename:
                # Extract the part before any added suffixes (e.g., "file.json.gz_1234" -> "file.json.gz")
                parts = original_filename.split(".json.gz")
                if len(parts) > 1:
                    original_filename = parts[0] + ".json.gz"
            else:
                # Try to reconstruct original .json.gz filename if it was converted
                # Many ndjson files are named like "file.json.ndjson" where original was "file.json.gz"
                if original_filename.endswith(".json"):
                    original_filename = original_filename[:-5] + ".json.gz"
        
        df = df.withColumn("original_filename", F.lit(original_filename))
        
        # Filter by billing_code_type and billing_codes if specified
        df = filter_billing_codes(df, billing_code_type=billing_code_type, billing_codes=billing_codes)
        
        # TEST VERSION: Skip group by (commented out)
        # df = group_by_billing_and_aggregate_npis(df)
        LOG.info("TEST VERSION: Skipping group_by_billing_and_aggregate_npis")
        
        # Process URLs: download content and join back (if download_dir is provided)
        df = process_urls_in_dataframe(spark, df, download_dir=download_dir)
        
        # Note: We skip count() here to avoid triggering UDF execution that causes
        # "Python worker failed to connect back" errors. Row count will be determined during write.
        LOG.info("DataFrame ready for output (lazy transformation, will execute on write)")
        
        # Generate output file name (based on input filename)
        file_name = ndjson_path.stem
        csv_output_path = output_dir / f"{file_name}.csv"
        
        # Directory creation handled by ensure_directories_from_config()
        
        # TEMPORARY: Convert to Pandas DataFrame and write to CSV (instead of Parquet)
        LOG.info("Converting cleaned DataFrame to Pandas for CSV output (TEMPORARY - will revert to Parquet)...")
        pandas_df = df.toPandas()
        
        # Write to CSV
        LOG.info("Writing CSV file to %s...", csv_output_path)
        pandas_df.to_csv(csv_output_path, index=False)
        
        row_count = len(pandas_df)
        LOG.info("Successfully wrote %d rows to CSV: %s", row_count, csv_output_path)
        
        return row_count
            
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error converting %s to Parquet: %s", ndjson_path, exc)
        return 0


def resolve_ndjson_path(file_name: str, input_path: Path) -> Path:
    """
    Convert file name (potentially .json.gz) to .ndjson path.
    
    Args:
        file_name: Original file name (may end with .json.gz)
        input_path: Base input directory path
        
    Returns:
        Resolved Path to .ndjson file
    """
    file_ndjson = file_name
    if file_name.endswith(".json.gz"):
        file_ndjson = file_name[:-8] + ".ndjson"
    return input_path / file_ndjson


def process_schema_group(
    spark: SparkSession,
    schema_sig: str,
    min_file_name: str,
    min_file_name_size: int | None,
    has_urls: bool,
    file_count: int,
    input_path: Path,
    output_dir: Path,
    schema_directory: Path,
    connection_string: str,
    process_min_file_only: bool,
    target_partition_size_mb: float,
    billing_code_type: str | None = None,
    billing_codes: list[str] | None = None,
    download_dir: Path | None = None,
    delete_input_files: bool = False,
) -> Tuple[int, int, int]:
    """
    Process a single schema group by converting its files.
    
    When process_min_file_only=true: Converts only the min_file_name, takes top 10 rows,
    converts to Pandas DataFrame, and exports as CSV.
    
    When process_min_file_only=false: Converts all files in the group to Parquet files.
    
    Args:
        spark: SparkSession
        schema_sig: Schema signature for the group
        min_file_name: Name of the smallest file in the group
        min_file_name_size: Size of the smallest file in bytes
        has_urls: Whether files in this group contain URLs
        file_count: Number of files in the group
        input_path: Directory containing input NDJSON files
        output_dir: Directory to write output files (CSV when process_min_file_only=true, Parquet when false)
        schema_directory: Directory containing pre-generated schema files
        connection_string: Database connection string
        process_min_file_only: If True, process only min_file_name and output CSV with top 10 rows; if False, process all files to Parquet
        target_partition_size_mb: Target size per Parquet partition in MB (only used when process_min_file_only=false)
        billing_code_type: Optional billing_code_type filter (e.g., "CPT")
        billing_codes: Optional list of billing_code values to filter by
        
    Returns:
        Tuple of (success_count, failed_count, total_rows)
    """
    if not min_file_name:
        LOG.warning("No minimum file found for schema group: %s", schema_sig[:100])
        return (0, 0, 0)
    
    LOG.info("Processing schema group (signature: %s...)", schema_sig[:100])
    LOG.info("  Min file: %s (size: %s bytes, %d files in group)", 
            min_file_name, 
            min_file_name_size if min_file_name_size is not None else "unknown",
            file_count)
    
    # Verify min file exists
    min_file_path = resolve_ndjson_path(min_file_name, input_path)
    if not min_file_path.exists():
        LOG.warning("Min file not found: %s (tried: %s), skipping group", min_file_name, min_file_path)
        return (0, 1, 0)
    
    # Load schema from pre-generated schema file using min_file_name
    LOG.info("Loading schema for min_file_name: %s", min_file_name)
    schema = load_schema_by_min_file_name(min_file_name, schema_directory)
    if schema is None:
        LOG.warning("Schema not found for min_file_name: %s (skipping group)", min_file_name)
        LOG.warning("Run 02_group_generate_schemas.py first to create schema files.")
        return (0, 1, 0)
    
    # Determine which files to process based on config
    if process_min_file_only:
        # Use min_file_name directly from get_schema_groups() query (same logic as generate_schemas.py)
        # This uses the smallest file per schema group as determined by SQL window functions in schema_groups table
        files_to_process = [(min_file_name, min_file_name_size, has_urls)]
        LOG.info("Processing only min_file_name: %s (process_min_file_only=true, same logic as generate_schemas.py)", min_file_name)
    else:
        # Process all files in the schema group (requires additional database query)
        LOG.info("Processing all files in schema group (process_min_file_only=false)...")
        all_files = get_files_for_schema_group(connection_string, schema_sig)
        if not all_files:
            LOG.warning("No files found for schema group %s, skipping", schema_sig[:100])
            return (0, 1, 0)
        files_to_process = all_files
        LOG.info("Found %d file(s) to process in this schema group", len(files_to_process))
    
    group_success = 0
    group_failed = 0
    group_rows = 0
    
    # Process each file
    for file_name, file_size, file_has_urls in files_to_process:
        file_path = resolve_ndjson_path(file_name, input_path)
        if not file_path.exists():
            LOG.warning("File not found: %s (tried: %s), skipping", file_name, file_path)
            group_failed += 1
            continue
        
        LOG.info("Processing file: %s (size: %s bytes)", 
                file_name, file_size if file_size is not None else "unknown")
        
        # Use different conversion logic based on process_min_file_only flag
        # Pass original filename (file_name from schema_groups)
        if process_min_file_only:
            # For min_file_only mode: convert top 10 rows to CSV
            result = convert_file_with_schema_to_csv(
                spark=spark,
                ndjson_path=file_path,
                output_dir=output_dir,
                schema=schema,
                top_n_rows=10,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                original_filename=file_name,
            )
        else:
            # For full processing mode: convert to Parquet files
            result = convert_file_with_schema_to_parquet(
                spark=spark,
                ndjson_path=file_path,
                output_dir=output_dir,
                schema=schema,
                target_partition_size_mb=target_partition_size_mb,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                original_filename=file_name,
            )
        
        if result > 0:
            group_success += 1
            group_rows += result
        else:
            group_failed += 1
    
    LOG.info(
        "Schema group complete: %d file(s) processed (%d failed, %d total rows)",
        group_success,
        group_failed,
        group_rows,
    )
    
    return (group_success, group_failed, group_rows)


def process_all_schema_groups(
    spark: SparkSession,
    schema_groups: list[Tuple[str, str | None, int | None, bool, int]],
    input_path: Path,
    output_dir: Path,
    schema_directory: Path,
    connection_string: str,
    process_min_file_only: bool,
    target_partition_size_mb: float,
    billing_code_type: str | None = None,
    billing_codes: list[str] | None = None,
    download_dir: Path | None = None,
    delete_input_files: bool = False,
) -> Tuple[int, int, int]:
    """
    Process all schema groups by converting their files to Parquet.
    
    Args:
        spark: SparkSession
        schema_groups: List of schema group tuples (schema_sig, min_file_name, min_file_name_size, has_urls, file_count)
        input_path: Directory containing input NDJSON files
        output_dir: Directory to write output Parquet files
        schema_directory: Directory containing pre-generated schema files
        connection_string: Database connection string
        process_min_file_only: If True, process only min_file_name; if False, process all files
        target_partition_size_mb: Target size per Parquet partition in MB
        billing_code_type: Optional billing_code_type filter (e.g., "CPT")
        billing_codes: Optional list of billing_code values to filter by
        
    Returns:
        Tuple of (success_count, failed_count, total_rows)
    """
    if not schema_groups:
        LOG.warning("No schema groups provided")
        return (0, 0, 0)
    
    LOG.info("Found %d schema group(s), processing each group...", len(schema_groups))
    
    success_count = 0
    failed_count = 0
    total_rows = 0
    
    # Process each schema group
    for schema_sig, min_file_name, min_file_name_size, has_urls, file_count in schema_groups:
        group_success, group_failed, group_rows = process_schema_group(
            spark=spark,
            schema_sig=schema_sig,
            min_file_name=min_file_name,
            min_file_name_size=min_file_name_size,
            has_urls=has_urls,
            file_count=file_count,
            input_path=input_path,
            output_dir=output_dir,
            schema_directory=schema_directory,
            connection_string=connection_string,
            process_min_file_only=process_min_file_only,
            target_partition_size_mb=target_partition_size_mb,
            billing_code_type=billing_code_type,
            billing_codes=billing_codes,
            download_dir=download_dir,
            delete_input_files=delete_input_files,
        )
        
        success_count += group_success
        failed_count += group_failed
        total_rows += group_rows
    
    LOG.info(
        "Conversion complete: %d files converted successfully (%d failed, %d total rows)",
        success_count,
        failed_count,
        total_rows,
    )
    
    return (success_count, failed_count, total_rows)
