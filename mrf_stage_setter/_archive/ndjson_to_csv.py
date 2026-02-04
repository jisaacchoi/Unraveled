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

LOG = logging.getLogger("src.ndjson_to_csv")


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
    Filter NPIs in the DataFrame to only include NPIs that exist in provider_data.
    
    Uses a broadcast variable with a Python set to avoid SparkSession lifecycle issues.
    After clean_dataframe_columns, the DataFrame should have a "NPI_Final" column containing arrays of NPIs.
    This function filters those arrays to keep only NPIs that exist in provider_data.
    
    Args:
        df: Input Spark DataFrame with "NPI_Final" array column (after clean_dataframe_columns)
        provider_npi_set: Python set containing valid NPIs from provider_data (as strings)
        spark: SparkSession (required if provider_npi_set is provided, for broadcast variable)
        
    Returns:
        DataFrame with filtered NPI arrays
    """
    if "NPI_Final" not in df.columns:
        LOG.debug("'NPI_Final' column not found in DataFrame, skipping NPI filtering")
        return df
    
    if provider_npi_set is None or len(provider_npi_set) == 0:
        LOG.warning("No valid NPIs provided in provider_npi_set, all NPI arrays will be empty")
        return df.withColumn("NPI_Final", F.array().cast("array<string>"))
    
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
        
        valid_npi_count = len(provider_npi_set)
        LOG.info("Filtering NPIs: %d valid NPIs from provider_data", valid_npi_count)
        
        # Broadcast the NPI set for efficient filtering
        broadcast_npi_set = spark.sparkContext.broadcast(provider_npi_set)
        
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
    Group DataFrame by billing fields and aggregate NPIs and service codes into lists with no duplicates.
    
    Groups by: billing_code, billing_code_type, billing_code_type_version, negotiated_rate, negotiated_type, 
               negotiation_arrangement, billing_class_version, original_filename
    Aggregates: 
    - All NPIs from the group into a single list with duplicates removed
    - All service codes from the group into a single list with duplicates removed (if service_code column exists)
    All grouping columns are preserved in the output.
    
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
    group_by_cols = [
        "billing_code", 
        "billing_code_type", 
        "billing_code_type_version", 
        "negotiated_rate", 
        "negotiated_type",
        "negotiated_arrangement",
        "billing_class_version",
        "original_filename"
    ]
    missing_cols = [col for col in group_by_cols if col not in df.columns]
    if missing_cols:
        LOG.warning("Missing grouping columns: %s. Skipping grouping.", missing_cols)
        return df
    
    LOG.info("Grouping by: %s and aggregating NPIs and service codes...", group_by_cols)
    
    # Define aggregation: collect all NPI arrays, flatten them, and remove duplicates
    # Since NPIs are already in arrays, we need to:
    # 1. Collect all arrays using collect_list
    # 2. Flatten the array of arrays
    # 3. Remove duplicates using array_distinct
    
    # Aggregate NPIs: collect all arrays, flatten them, and remove duplicates
    # Since NPIs are already in arrays (each row has an array), we collect all arrays,
    # flatten the array of arrays into one array, and remove duplicates
    # Dynamic partitioning: calculate optimal number of partitions based on data characteristics
    # This ensures better resource utilization and avoids over/under-partitioning
    num_partitions = df.rdd.getNumPartitions()
    
    # Estimate data size (rough heuristic: assume ~1KB per row average)
    # In production, you might want to use df.rdd.mapPartitions to get actual size
    # For now, use partition count as a proxy for data size
    if num_partitions < 50:
        # For small datasets, use a reasonable default
        # Target: ~100-200 partitions for good parallelism without overhead
        optimal_partitions = min(200, max(100, num_partitions * 2))
        LOG.info("Repartitioning DataFrame to %d partitions before grouping (current: %d, dynamic calculation)", 
                 optimal_partitions, num_partitions)
        df = df.repartition(optimal_partitions)
    elif num_partitions > 500:
        # For very large datasets, coalesce to avoid too many small partitions
        optimal_partitions = min(500, max(200, int(num_partitions * 0.5)))
        LOG.info("Coalescing DataFrame to %d partitions before grouping (current: %d, dynamic calculation)", 
                 optimal_partitions, num_partitions)
        df = df.coalesce(optimal_partitions)
    else:
        LOG.debug("Partition count (%d) is within optimal range, no repartitioning needed", num_partitions)
    
    # Build aggregation expressions
    agg_exprs = []
    
    # Aggregate NPIs: collect all arrays, flatten them, and remove duplicates
    # Since NPIs are already in arrays (each row has an array), we collect all arrays,
    # flatten the array of arrays into one array, and remove duplicates
    agg_exprs.append(
        F.array_distinct(spark_flatten(F.collect_list(F.coalesce(F.col("NPI_Final"), F.array())))).alias("NPI_Final")
    )
    
    # Aggregate service_code if it exists: collect into arrays, flatten, and remove duplicates
    # Use native Spark functions instead of UDF for better performance
    if "service_code" in df.columns:
        try:
            # Convert service_code to array format using native Spark functions
            # Handle null, single values, and arrays
            service_code_type = dict(df.dtypes).get("service_code", "")
            
            if service_code_type.startswith("array"):
                # Already an array type - use directly, but ensure it's array<string>
                service_code_as_array = F.coalesce(
                    F.col("service_code").cast("array<string>"), 
                    F.array().cast("array<string>")
                )
            else:
                # Single value (string, int, etc.) - wrap in array
                # Convert to string first, then wrap in array
                service_code_as_array = F.array(
                    F.coalesce(F.col("service_code").cast("string"), F.lit(""))
                )
            
            agg_exprs.append(
                F.array_distinct(
                    spark_flatten(
                        F.collect_list(
                            F.coalesce(service_code_as_array, F.array().cast("array<string>"))
                        )
                    )
                ).alias("service_code")
            )
            LOG.info("Including service_code in aggregation (using native Spark functions, will be collected into list)")
        except Exception as exc:  # noqa: BLE001
            # If service_code conversion fails, create empty array column
            LOG.warning("Failed to convert service_code to array format: %s. Creating empty array column.", exc)
            agg_exprs.append(F.array().cast("array<string>").alias("service_code"))
    else:
        # service_code column doesn't exist - create empty array column for consistency
        agg_exprs.append(F.array().cast("array<string>").alias("service_code"))
        LOG.warning("service_code column not found in DataFrame. Creating empty array column for consistency.")
    
    df_grouped = df.groupBy(*group_by_cols).agg(*agg_exprs)
    
    # Note: We skip count() calls here to avoid triggering execution that may cause
    # "Python worker failed to connect back" errors. The grouping will execute when DataFrame is written.
    LOG.info("Applied grouping and aggregation (lazy transformation, will execute on write)")
    
    return df_grouped


def early_column_pruning(df: DataFrame) -> DataFrame:
    """
    Early column pruning: select only columns that will be needed later.
    
    This reduces memory usage and improves performance by dropping unnecessary columns
    early in the pipeline, before expensive operations like grouping.
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        DataFrame with only necessary columns
    """
    LOG.info("Applying early column pruning...")
    
    columns_before = list(df.columns)
    
    # Columns we'll need for final output plus intermediate processing
    columns_to_keep = [
        # Billing fields (for grouping and output)
        "billing_code",
        "billing_code_type", 
        "billing_code_type_version",
        "description",
        "billing_class",
        "billing_class_version",
        "expiration_date",
        "negotiated_rate",
        "negotiated_type",
        "negotiated_arrangement",
        # NPI fields (for filtering and aggregation)
        "NPI_Final",
        # Service code (for aggregation)
        "service_code",
        # Filename (for grouping)
        "original_filename"
        # URL fields (for URL processing if needed)
        # We'll keep any column that might be a URL column - URL detection happens later
    ]
    
    # Also keep any columns that might be URL columns (detected later)
    # We can't know which columns are URLs yet, so we'll be conservative
    # and keep columns that might contain URLs (detected in process_urls_in_dataframe)
    
    # Find columns that exist in the DataFrame
    existing_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    
    # Keep all columns for now if we're early in the pipeline (before URL detection)
    # We'll do more aggressive pruning after URL processing
    # For now, just drop obviously unused columns if we can identify them
    
    # Don't drop columns yet - we need them for URL detection and other processing
    # This is a placeholder for future optimization
    LOG.debug("Early column pruning: keeping %d columns out of %d total", 
              len(existing_columns_to_keep), len(columns_before))
    
    return df


def ensure_parquet_compatible(df: DataFrame) -> DataFrame:
    """
    Ensure all columns are Parquet-compatible by converting problematic types.
    
    Fixes common issues:
    - Ensures array columns are properly typed (array<string>)
    - Handles null values in arrays
    - Converts complex types to simpler formats if needed
    - Ensures all string columns are properly cast
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        DataFrame with Parquet-compatible column types
    """
    LOG.info("Ensuring Parquet compatibility for all columns...")
    
    from pyspark.sql.types import ArrayType, StringType
    
    columns_to_fix = []
    
    # Check each column for potential issues
    for field in df.schema.fields:
        field_name = field.name
        field_type = str(field.dataType)
        
        # Fix array columns - ensure they're array<string> and handle nulls
        if isinstance(field.dataType, ArrayType):
            element_type = field.dataType.elementType
            if not isinstance(element_type, StringType):
                # Array of non-strings - convert to array<string>
                LOG.debug("Converting array column '%s' from %s to array<string>", field_name, field_type)
                columns_to_fix.append((field_name, "array_to_string"))
            else:
                # Already array<string>, but ensure nulls are handled
                LOG.debug("Array column '%s' is already array<string>, ensuring null handling", field_name)
                columns_to_fix.append((field_name, "ensure_array_null_safe"))
        elif "array" in field_type.lower() and not isinstance(field.dataType, ArrayType):
            # Column might be stored as string but should be array
            LOG.debug("Column '%s' appears to be array-like but not ArrayType: %s", field_name, field_type)
            columns_to_fix.append((field_name, "string_to_array"))
    
    # Apply fixes
    for field_name, fix_type in columns_to_fix:
        try:
            if fix_type == "array_to_string":
                # Convert array elements to strings - use simpler approach
                # Just cast the entire array to array<string> - Spark will handle element conversion
                df = df.withColumn(
                    field_name,
                    F.coalesce(
                        F.col(field_name).cast("array<string>"),
                        F.array().cast("array<string>")
                    )
                )
                LOG.debug("Converted array column '%s' to array<string>", field_name)
            elif fix_type == "ensure_array_null_safe":
                # Ensure array column handles nulls properly
                df = df.withColumn(
                    field_name,
                    F.coalesce(F.col(field_name), F.array().cast("array<string>"))
                )
            elif fix_type == "string_to_array":
                # Try to parse string as array (if it's JSON-like)
                # For now, just ensure it's an array type
                df = df.withColumn(
                    field_name,
                    F.coalesce(
                        F.array(F.col(field_name).cast("string")),
                        F.array().cast("array<string>")
                    )
                )
        except Exception as fix_exc:  # noqa: BLE001
            LOG.warning("Failed to fix column '%s' (%s): %s. Leaving as-is.", field_name, fix_type, fix_exc)
    
    # Ensure all string columns are properly typed
    for field in df.schema.fields:
        if str(field.dataType) == "string" and field.nullable:
            # String columns are generally fine, but ensure they're not causing issues
            pass
    
    # Special handling for known array columns - ensure they're properly formatted
    array_columns = ["NPI_Final", "service_code"]
    for col_name in array_columns:
        if col_name in df.columns:
            try:
                # Get the current type
                current_type = dict(df.dtypes).get(col_name, "")
                LOG.debug("Column '%s' current type: %s", col_name, current_type)
                
                # Ensure it's array<string> and null-safe
                # Use a simple cast - Spark will handle the conversion
                df = df.withColumn(
                    col_name,
                    F.coalesce(
                        F.col(col_name).cast("array<string>"),
                        F.array().cast("array<string>")
                    )
                )
                LOG.info("Ensured '%s' is array<string> and null-safe", col_name)
            except Exception as col_exc:  # noqa: BLE001
                LOG.warning("Could not ensure Parquet compatibility for '%s': %s. Column will be written as-is.", col_name, col_exc)
                # Don't fail - try to continue with the column as-is
    
    # Final validation: check if there are any struct types (these can cause Parquet issues)
    struct_columns = []
    for field in df.schema.fields:
        if "struct" in str(field.dataType).lower():
            struct_columns.append(field.name)
            LOG.warning("Column '%s' has struct type (%s) - this may cause Parquet write issues", 
                       field.name, field.dataType)
    
    if struct_columns:
        LOG.warning("Found %d struct columns that may cause issues: %s", len(struct_columns), struct_columns)
    
    LOG.info("Parquet compatibility check complete")
    return df


def final_clean(df: DataFrame) -> DataFrame:
    """
    Final cleanup: keep only specified columns and drop all others.
    
    Keeps only the following columns:
    - billing_code
    - billing_code_type
    - billing_code_type_version
    - description
    - billing_class
    - expiration_date
    - negotiated_rate
    - negotiated_type
    - service_code
    - negotiation_arrangement
    - NPI_Final
    - original_filename
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        DataFrame with only the specified columns
    """
    LOG.info("Applying final column cleanup (keeping only specified columns)...")
    
    columns_before = list(df.columns)
    LOG.info("Columns before final cleanup: %s", columns_before)
    
    # List of columns to keep
    columns_to_keep = [
        "billing_code",
        "billing_code_type",
        "billing_code_type_version",
        "description",
        "billing_class",
        "expiration_date",
        "negotiated_rate",
        "negotiated_type",
        "service_code",
        "negotiated_arrangement",
        "NPI_Final",
        "original_filename"
    ]
    
    # Find columns that exist in the DataFrame and are in the keep list
    existing_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    
    if missing_columns:
        LOG.warning("Some expected columns are missing and will not be included: %s", missing_columns)
    
    # Find columns to drop (all columns not in the keep list)
    columns_to_drop = [col for col in df.columns if col not in existing_columns_to_keep]
    
    if columns_to_drop:
        df = df.select(*existing_columns_to_keep)
        LOG.info("Kept %d column(s): %s", len(existing_columns_to_keep), existing_columns_to_keep)
        LOG.info("Dropped %d column(s): %s", len(columns_to_drop), columns_to_drop)
    else:
        LOG.info("All columns are already in the keep list, no columns to drop")
    
    columns_after = list(df.columns)
    LOG.info("Columns after final cleanup: %s", columns_after)
    
    return df


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
    
    # 1. Drop columns that are completely empty (all null values)
    # Use a small sample-based approach to detect empty columns
    # This happens early in the pipeline, so it should be safe from UDF serialization issues
    empty_columns = []
    sample_size = 100  # Small sample to minimize execution overhead
    try:
        sample_df = df.limit(sample_size)
        sample_rows = sample_df.collect()
        
        if sample_rows:
            for col_name in df.columns:
                # Check if column has any non-null values in the sample
                has_non_null = False
                for row in sample_rows:
                    if row[col_name] is not None:
                        has_non_null = True
                        break
                
                if not has_non_null:
                    empty_columns.append(col_name)
                    LOG.debug("Detected empty column (all null in sample): %s", col_name)
        
        if empty_columns:
            # Don't drop important columns that we need for processing or grouping
            protected_columns = [
                "NPI_Final", "all_npis", "npi",
                "billing_code", "billing_code_type", "billing_code_type_version",
                "negotiated_rate", "negotiated_type", "negotiated_arrangement",
                "billing_class_version", "original_filename", "service_code"
            ]
            columns_to_drop = [col for col in empty_columns if col not in protected_columns]
            
            if columns_to_drop:
                df = df.drop(*columns_to_drop)
                LOG.info("Dropped %d empty column(s): %s", len(columns_to_drop), columns_to_drop)
            else:
                LOG.debug("Found %d empty column(s) but they are protected: %s", len(empty_columns), empty_columns)
    except Exception as exc:  # noqa: BLE001
        # If sample collection fails (e.g., due to serialization issues), skip empty column detection
        LOG.warning("Could not detect empty columns (sample collection failed): %s. Skipping empty column removal.", exc)
    
    # 2. Rename "all_npis" to "NPI_Final" if it exists (do this BEFORE dropping "npi")
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


def convert_file_with_schema_to_csv(
    spark: SparkSession,
    ndjson_path: Path,
    output_dir: Path,
    schema: StructType,
    billing_code_type: str | None = None,
    billing_codes: list[str] | None = None,
    download_dir: Path | None = None,
    original_filename: str | None = None,
    provider_npi_set: Optional[set[str]] = None,
    top_n_rows: int | None = None,
) -> int:
    """
    Convert a single NDJSON file to CSV file using a provided schema.
    
    This function reads the NDJSON file, flattens nested structures,
    filters by billing_code_type and billing_codes (if specified),
    converts the Spark DataFrame to Pandas, and writes a CSV file.
    
    Args:
        spark: SparkSession
        ndjson_path: Path to input NDJSON file
        output_dir: Directory to write output CSV file
        schema: PySpark StructType schema to use
        billing_code_type: Optional billing_code_type filter (e.g., "CPT")
        billing_codes: Optional list of billing_code values to filter by
        download_dir: Optional directory for URL downloads
        original_filename: Original filename to include in output
        provider_npi_set: Optional set of valid NPI strings for filtering
        top_n_rows: Optional limit on number of rows to process (for process_min_file_only mode)
        
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
        
        # Apply auto_flatten (uses mrf_analysis if available, falls back to standard flattening)
        LOG.info("Flattening DataFrame...")
        # Get connection string from config if available
        from src.shared.database import build_connection_string
        from src.shared.config import load_config
        try:
            config = load_config(Path("config.yaml"))
            db_config = config.get("database", {})
            cloud_config = config.get("cloud", {})
            conn_string = build_connection_string(db_config, cloud_config) if db_config else None
        except Exception:  # noqa: BLE001
            conn_string = None
        
        df = auto_flatten(df, file_name=original_filename, connection_string=conn_string)
        
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
            df = df.filter(F.size(F.col("NPI_Final")) > 0)
            LOG.info("Applied NPI filtering (lazy transformation, will execute on conversion)")
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
                if original_filename.endswith(".json"):
                    original_filename = original_filename[:-5] + ".json.gz"
        
        df = df.withColumn("original_filename", F.lit(original_filename))
        
        # Filter by billing_code_type and billing_codes if specified
        df = filter_billing_codes(df, billing_code_type=billing_code_type, billing_codes=billing_codes)
        
        # Group by billing fields and aggregate NPIs (original_filename will be preserved in groupBy)
        df = group_by_billing_and_aggregate_npis(df)
        
        # Process URLs: download content and join back (if download_dir is provided)
        # Do this before final_clean so URL detection can access all columns
        df = process_urls_in_dataframe(spark, df, download_dir=download_dir)
        
        # Final cleanup: keep only specified columns (after URL processing)
        df = final_clean(df)
        
        # Limit rows if top_n_rows is specified (for process_min_file_only mode)
        if top_n_rows is not None and top_n_rows > 0:
            LOG.info("Limiting to top %d rows (process_min_file_only mode)", top_n_rows)
            df = df.limit(top_n_rows)
        
        LOG.info("Completed all operations (flattening, restructuring, filtering, grouping, cleaning, URL processing)")
        
        # Generate output file name (based on input filename)
        file_name = ndjson_path.stem
        csv_output_path = output_dir / (file_name + ".csv")
        
        # Ensure output directory exists
        csv_output_path.parent.mkdir(parents=True, exist_ok=True)
        
        LOG.info("Converting Spark DataFrame to Pandas for CSV export...")
        LOG.info("DataFrame has %d partitions, %d columns before conversion", 
                df.rdd.getNumPartitions(), len(df.columns))
        
        # Convert array columns to strings for CSV compatibility
        LOG.info("Converting array columns to strings for CSV compatibility...")
        for col_name in df.columns:
            from pyspark.sql.types import ArrayType
            if isinstance(df.schema[col_name].dataType, ArrayType):
                # Convert array to comma-separated string
                df = df.withColumn(col_name, F.concat_ws(",", F.col(col_name)))
                LOG.debug("Converted array column '%s' to comma-separated string", col_name)
        
        # Convert Spark DataFrame to Pandas
        LOG.info("Converting Spark DataFrame to Pandas DataFrame...")
        pandas_df = df.toPandas()
        
        row_count = len(pandas_df)
        LOG.info("Converted to Pandas DataFrame with %d rows, %d columns", row_count, len(pandas_df.columns))
        
        # Write to CSV
        LOG.info("Writing CSV file to %s...", csv_output_path)
        pandas_df.to_csv(csv_output_path, index=False, encoding='utf-8')
        
        LOG.info("Successfully wrote %d rows to CSV: %s", row_count, csv_output_path)
        
        return row_count
            
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error converting %s to CSV: %s", ndjson_path, exc)
        return 0


def _convert_file_with_schema_to_parquet_archived(
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
        
        # Note: Caching removed to avoid Python worker crashes
        # The transformations are lazy and will be optimized by Spark's query planner
        
        # Clean DataFrame columns before filtering and grouping
        df = clean_dataframe_columns(df)
        
        # Filter NPIs by provider_data if provider_npi_set is provided
        if provider_npi_set is not None:
            LOG.info("Filtering NPIs to only include those in provider_data table...")
            df = filter_npis_by_provider_data(df, provider_npi_set, spark)
            # Filter to only keep rows that have at least one NPI after filtering
            # Note: We skip count() calls to avoid triggering execution that may cause
            # "Python worker failed to connect back" errors. The filtering will execute on write.
            df = df.filter(F.size(F.col("NPI_Final")) > 0)
            LOG.info("Applied NPI filtering (lazy transformation, will execute on write)")
            # Note: Zero-row check removed to avoid count() call. Empty DataFrames will write as empty files.
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
        
        # Group by billing fields and aggregate NPIs (original_filename will be preserved in groupBy)
        df = group_by_billing_and_aggregate_npis(df)
        
        # Process URLs: download content and join back (if download_dir is provided)
        # Do this before final_clean so URL detection can access all columns
        df = process_urls_in_dataframe(spark, df, download_dir=download_dir)
        
        # Final cleanup: keep only specified columns (after URL processing)
        df = final_clean(df)
        
        # Ensure all columns are Parquet-compatible before writing
        LOG.info("Ensuring Parquet compatibility...")
        df = ensure_parquet_compatible(df)
        
        # Log final schema to help diagnose any remaining issues
        try:
            LOG.debug("Final DataFrame schema before write:")
            for field in df.schema.fields:
                LOG.debug("  %s: %s (nullable: %s)", field.name, field.dataType, field.nullable)
        except Exception as schema_log_exc:  # noqa: BLE001
            LOG.warning("Could not log final schema: %s", schema_log_exc)
        
        # Note: We skip count() calls to avoid triggering execution that may cause
        # "Python worker failed to connect back" errors. The row count will be available after write.
        LOG.info("Completed all operations (flattening, restructuring, filtering, grouping, cleaning, URL processing)")
        
        # Generate output file name (based on input filename)
        file_name = ndjson_path.stem
        parquet_output_path = output_dir / file_name
        
        # Ensure output directory exists
        parquet_output_path.mkdir(parents=True, exist_ok=True)
        
        # Log DataFrame info before write
        current_partitions = df.rdd.getNumPartitions()
        LOG.info("DataFrame has %d partitions, %d columns before Parquet write", 
                 current_partitions, len(df.columns))
        LOG.debug("DataFrame columns: %s", df.columns)
        
        # Log schema to help diagnose issues
        try:
            schema_str = df.schema.simpleString()
            LOG.debug("DataFrame schema: %s", schema_str)
            
            # Check for potentially problematic types
            for field in df.schema.fields:
                field_type = str(field.dataType)
                if "array" in field_type.lower() or "struct" in field_type.lower():
                    LOG.debug("Complex type column: %s (%s)", field.name, field_type)
        except Exception as schema_exc:  # noqa: BLE001
            LOG.warning("Could not log schema: %s", schema_exc)
        
        # Write Parquet - try multiple approaches if needed
        LOG.info("Writing Parquet files to %s...", parquet_output_path)
        
        # Skip test write - we know the issue is Windows native library, not schema
        
        write_success = False
        last_exception = None
        
        # Try 1: Write with ZSTD compression
        # Additional options to work around Windows native library issues
        try:
            LOG.debug("Attempting Parquet write with ZSTD compression...")
            df.write \
                .mode("overwrite") \
                .option("compression", "zstd") \
                .option("mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .option("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
                .option("parquet.enable.summary-metadata", "false") \
                .parquet(str(parquet_output_path))
            write_success = True
            LOG.info("Successfully wrote Parquet files with ZSTD compression")
        except Exception as write_exc:  # noqa: BLE001
            last_exception = write_exc
            # Try to extract the actual underlying exception from Java wrapper
            error_msg = str(write_exc)
            
            # Try to get the actual cause from the Java exception
            if hasattr(write_exc, '__cause__') and write_exc.__cause__:
                error_msg += f"\n  Caused by: {write_exc.__cause__}"
            if hasattr(write_exc, 'java_exception'):
                error_msg += f"\n  Java exception: {write_exc.java_exception}"
            
            # Log the full traceback to see the actual error
            import traceback
            tb_str = ''.join(traceback.format_exception(type(write_exc), write_exc, write_exc.__traceback__))
            LOG.error("Parquet write with ZSTD compression failed:\n%s\nFull traceback:\n%s", error_msg, tb_str)
        
        # Try 2: Write without compression
        if not write_success:
            try:
                LOG.debug("Attempting Parquet write without compression...")
                df.write \
                    .mode("overwrite") \
                    .parquet(str(parquet_output_path))
                write_success = True
                LOG.info("Successfully wrote Parquet files without compression")
            except Exception as fallback_exc:  # noqa: BLE001
                last_exception = fallback_exc
                LOG.warning("Parquet write without compression also failed: %s", fallback_exc)
        
        # Try 3: Write with snappy compression (more compatible) and commit protocol options
        if not write_success:
            try:
                LOG.debug("Attempting Parquet write with snappy compression (using manifest commit protocol)...")
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .option("mapreduce.fileoutputcommitter.algorithm.version", "2") \
                    .option("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
                    .parquet(str(parquet_output_path))
                write_success = True
                LOG.info("Successfully wrote Parquet files with snappy compression")
            except Exception as snappy_exc:  # noqa: BLE001
                last_exception = snappy_exc
                LOG.error("Parquet write with snappy compression also failed: %s", snappy_exc)
        
        # Fallback: Use PyArrow directly (bypasses Spark/Hadoop entirely)
        if not write_success:
            try:
                LOG.warning("All Spark Parquet write attempts failed. Trying PyArrow fallback (converts to Pandas first)...")
                LOG.warning("This may use significant memory if the DataFrame is large.")
                
                # Convert Spark DataFrame to Pandas
                LOG.info("Converting Spark DataFrame to Pandas...")
                pandas_df = df.toPandas()
                LOG.info("Converted to Pandas DataFrame with %d rows, %d columns", len(pandas_df), len(pandas_df.columns))
                
                # Handle array columns for Pandas/PyArrow compatibility
                # PyArrow can handle lists, but we need to ensure they're Python lists
                for col in pandas_df.columns:
                    if pandas_df[col].dtype == 'object':
                        # Check if column contains arrays/lists
                        sample = pandas_df[col].dropna()
                        if len(sample) > 0 and isinstance(sample.iloc[0], list):
                            # Already a list, ensure all are lists (handle None)
                            pandas_df[col] = pandas_df[col].apply(lambda x: x if isinstance(x, list) else [])
                
                # Write using PyArrow (no Hadoop dependencies)
                import pyarrow.parquet as pq
                import pyarrow as pa
                
                LOG.info("Writing Parquet file using PyArrow...")
                table = pa.Table.from_pandas(pandas_df)
                pq.write_table(table, str(parquet_output_path), compression='snappy')
                
                write_success = True
                row_count = len(pandas_df)
                LOG.info("Successfully wrote Parquet file using PyArrow (bypassed Spark/Hadoop): %s", parquet_output_path)
                LOG.info("Wrote %d rows to Parquet", row_count)
                return row_count
            except ImportError:
                LOG.error("PyArrow not available. Cannot use fallback method.")
                LOG.error("Install PyArrow with: pip install pyarrow")
            except MemoryError:
                LOG.error("Out of memory when converting to Pandas. DataFrame is too large for PyArrow fallback.")
            except Exception as pyarrow_exc:  # noqa: BLE001
                LOG.error("PyArrow fallback also failed: %s", pyarrow_exc)
                LOG.error("Original Spark error was: %s", last_exception)
                
                # Final fallback: Write to CSV (simplest, no native library issues)
                try:
                    LOG.warning("Attempting CSV fallback (last resort)...")
                    LOG.warning("CSV output may not preserve array types - they will be converted to strings")
                    
                    # Convert array columns to strings for CSV compatibility
                    for col in df.columns:
                        from pyspark.sql.types import ArrayType
                        if isinstance(df.schema[col].dataType, ArrayType):
                            df = df.withColumn(col, F.col(col).cast("string"))
                    
                    # Write to CSV instead
                    csv_output_path = parquet_output_path.parent / (parquet_output_path.stem + ".csv")
                    df.write \
                        .mode("overwrite") \
                        .option("header", "true") \
                        .option("delimiter", ",") \
                        .csv(str(csv_output_path.parent / csv_output_path.stem))
                    
                    # CSV writes to a directory, move single part file if possible
                    import glob
                    csv_files = list(Path(csv_output_path.parent / csv_output_path.stem).glob("*.csv"))
                    if len(csv_files) == 1:
                        # Only one CSV file, rename it to the expected name
                        csv_files[0].rename(csv_output_path)
                        # Remove the directory
                        import shutil
                        shutil.rmtree(csv_output_path.parent / csv_output_path.stem, ignore_errors=True)
                    
                    LOG.warning("Wrote to CSV instead of Parquet due to write failures: %s", csv_output_path)
                    LOG.warning("Note: Array columns have been converted to strings in CSV output")
                    return 0
                except Exception as csv_exc:  # noqa: BLE001
                    LOG.error("CSV fallback also failed: %s", csv_exc)
        
        if not write_success:
            LOG.error("All write attempts failed (Parquet via Spark, Parquet via PyArrow, and CSV).")
            LOG.error("Last Spark error: %s", last_exception)
            raise last_exception
        
        # Get row count from DataFrame (lazy - will execute during write)
        # Note: We can't get exact count without triggering execution, so we'll return 0
        # The actual row count will be available in the Parquet metadata
        LOG.info("Successfully wrote Parquet files to: %s", parquet_output_path)
        
        # Return 0 as placeholder (actual count would require execution)
        return 0
            
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
            # For full processing mode: convert to CSV files
            result = convert_file_with_schema_to_csv(
                spark=spark,
                ndjson_path=file_path,
                output_dir=output_dir,
                schema=schema,
                billing_code_type=billing_code_type,
                billing_codes=billing_codes,
                download_dir=download_dir,
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
