"""Spark DataFrame transformations and data processing functions."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

LOG = logging.getLogger("src.dataframe_transformer")


def read_ndjson_full(
    spark: SparkSession,
    ndjson_path: Path,
) -> DataFrame:
    """
    Read a newline-delimited JSON (NDJSON) file with Spark, reading the entire file.
    
    This reads the entire file (samplingRatio=1.0) to ensure all fields and types
    are captured in the schema inference.
    
    Args:
        spark: SparkSession
        ndjson_path: Path to NDJSON file
        
    Returns:
        Spark DataFrame
    """
    if not ndjson_path.exists():
        raise FileNotFoundError(f"NDJSON file not found: {ndjson_path}")
    
    LOG.info("Reading entire NDJSON file: %s", ndjson_path.name)
    
    df = (
        spark.read
        .option("multiLine", "false")  # NDJSON: one JSON object per line
        .option("samplingRatio", "1.0")  # Read entire file
        .json(str(ndjson_path))
    )
    
    LOG.info("Read %s with %d columns", ndjson_path.name, len(df.columns))
    return df


def get_file_structure_from_analysis(
    connection_string: str,
    file_name: str,
) -> Optional[list[tuple[str, int, str]]]:
    """
    Query mrf_analysis table to get the structure information for a file.
    
    Args:
        connection_string: PostgreSQL connection string
        file_name: Name of the file to query structure for
        
    Returns:
        List of tuples (path, level, value_dtype) sorted by level, or None on error
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Query for structure information: path, level, value_dtype
        query = """
            SELECT path, level, value_dtype
            FROM mrf_analysis
            WHERE file_name = %s
            ORDER BY level, path
        """
        
        cursor.execute(query, (file_name,))
        results = cursor.fetchall()
        
        LOG.info("Retrieved %d structure records from mrf_analysis for file: %s", len(results), file_name)
        
        return results
        
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Failed to query mrf_analysis for file %s: %s. Will use standard flattening.", file_name, exc)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _auto_flatten_standard(df: DataFrame) -> DataFrame:
    """
    Standard recursive flattening logic (used as fallback when mrf_analysis is not available).
    
    Recursively explode array<struct> columns and flatten struct columns into top-level columns.
    
    Column naming:
      - For struct fields, use ONLY the leaf key (subfield name) as the column name.
      - If that name already exists, append _1, _2, ... to make it unique.
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Flattened Spark DataFrame
    """
    iteration = 0
    columns_before_flattening = set(df.columns)
    
    while True:
        iteration += 1
        schema = df.schema
        columns_before_iteration = set(df.columns)

        struct_cols = [
            f for f in schema.fields
            if isinstance(f.dataType, StructType)
        ]

        array_struct_cols = [
            f for f in schema.fields
            if isinstance(f.dataType, ArrayType)
            and isinstance(f.dataType.elementType, StructType)
        ]

        if not struct_cols and not array_struct_cols:
            # Nothing left to flatten/explode
            break

        LOG.debug("Flatten iteration %d: %d struct cols, %d array<struct> cols",
                 iteration, len(struct_cols), len(array_struct_cols))

        # 1) Explode all array<struct> columns
        exploded_columns = []
        for f in array_struct_cols:
            LOG.debug("Exploding array<struct> column: %s", f.name)
            exploded_columns.append(f.name)
            df = df.withColumn(f.name, F.explode_outer(F.col(f.name)))
        
        if exploded_columns:
            LOG.info("Exploded array<struct> columns (iteration %d): %s", iteration, exploded_columns)

        # 2) Flatten all struct columns
        if struct_cols:
            struct_names = [f.name for f in struct_cols]
            LOG.debug("Flattening struct columns: %s", struct_names)

            # Track used names so we don't collide
            used_names = set(df.columns)
            new_cols = []
            added_columns = []  # Track newly added columns from flattening

            for f in df.schema.fields:
                if isinstance(f.dataType, StructType):
                    # Replace struct with its fields
                    for subf in f.dataType.fields:
                        base_name = subf.name  # <-- only last-level key
                        new_name = base_name

                        # Avoid collisions by appending _1, _2, ...
                        i = 1
                        while new_name in used_names:
                            new_name = f"{base_name}_{i}"
                            i += 1

                        used_names.add(new_name)
                        
                        # Track if this is a new column (not a renamed duplicate)
                        if new_name not in columns_before_iteration:
                            added_columns.append(new_name)

                        new_cols.append(
                            F.col(f"{f.name}.{subf.name}").alias(new_name)
                        )
                else:
                    # Keep non-struct column as-is
                    new_cols.append(F.col(f.name))

            df = df.select(new_cols)
            
            if added_columns:
                LOG.info("Added columns from flattening structs (iteration %d): %s", iteration, added_columns)
    
    # Log summary of all columns added during flattening
    columns_after_flattening = set(df.columns)
    all_added_columns = sorted(columns_after_flattening - columns_before_flattening)
    if all_added_columns:
        LOG.info("Flattening complete: Added %d new column(s) during flattening: %s", 
                len(all_added_columns), all_added_columns)
    else:
        LOG.debug("Flattening complete: No new columns were added (only structs were flattened into existing columns)")

    LOG.debug("Flattening complete after %d iteration(s)", iteration)
    return df


def auto_flatten(
    df: DataFrame,
    file_name: Optional[str] = None,
    connection_string: Optional[str] = None,
) -> DataFrame:
    """
    Recursively explode array<struct> columns and flatten struct columns into top-level columns.
    
    This function attempts to use structure information from mrf_analysis table
    to optimize flattening. If the information is not available, it falls back
    to the standard flattening behavior.
    
    Column naming:
      - For struct fields, use ONLY the leaf key (subfield name) as the column name.
      - If that name already exists, append _1, _2, ... to make it unique.
    
    Args:
        df: Input Spark DataFrame
        file_name: Optional file name to query mrf_analysis table
        connection_string: Optional PostgreSQL connection string
        
    Returns:
        Flattened Spark DataFrame
    """
    # If we don't have the necessary parameters, fall back to standard flattening
    if not file_name or not connection_string:
        LOG.debug("Missing file_name or connection_string, using standard flattening")
        return _auto_flatten_standard(df)
    
    # Try to get structure information from mrf_analysis
    structure_info = get_file_structure_from_analysis(connection_string, file_name)
    
    if not structure_info:
        LOG.debug("Could not retrieve structure info from mrf_analysis, using standard flattening")
        return _auto_flatten_standard(df)
    
    # Log what we found
    LOG.info("Using mrf_analysis structure information for enhanced flattening (file: %s)", file_name)
    LOG.debug("Structure info available: %d paths found", len(structure_info))
    
    # Filter for arrays that contain structs (value_dtype = 'list' with nested structs)
    array_struct_paths = [
        (path, level, dtype) 
        for path, level, dtype in structure_info 
        if dtype == 'list' and '[0]' in path  # Nested arrays
    ]
    
    if array_struct_paths:
        LOG.info("Found %d nested array paths in mrf_analysis", len(array_struct_paths))
        LOG.debug("Nested array paths: %s", [p[0] for p in array_struct_paths[:10]])  # Log first 10
    
    # For now, delegate to standard flattening
    # TODO: Implement enhanced flattening logic using structure_info
    return _auto_flatten_standard(df)


def restructure_negotiated_rate_df(df: DataFrame) -> DataFrame:
    """
    Convert mixed negotiated-rate dataset into a unified dataframe.
    
    Steps:
      1. Split billing rows and provider rows.
      2. Join billing rows to provider rows based on provider_references array.
      3. Aggregate all NPIs from all matching providers into a single all_npis column.
      4. Keep one row per billing record (no explosion of provider_references).
      5. Keep provider_references as an array and add all_npis with all NPIs from matching providers.
    
    Args:
        df: Input Spark DataFrame with billing and provider data
        
    Returns:
        Restructured Spark DataFrame with:
        - One row per billing record
        - provider_references kept as array
        - all_npis column containing all NPIs from all matching providers (flattened and deduplicated)
    """
    # Check if required columns exist (only check provider_group_id and provider_references)
    if "provider_group_id" not in df.columns or "provider_references" not in df.columns:
        LOG.debug("Missing required columns for restructure_negotiated_rate_df (provider_group_id or provider_references). Skipping restructuring.")
        return df
    
    # 1. Identify billing rows (billing_code not null)
    billing_df = df.filter(F.col("billing_code").isNotNull())
    
    # 2. Identify provider metadata rows (provider_group_id not null)
    # Keep npi as a list (don't explode it)
    provider_df = df.filter(F.col("provider_group_id").isNotNull())
    
    # 3. Keep provider_references as array (no explosion)
    # We'll aggregate all NPIs from all matching providers
    
    # 4. Select only provider-specific columns from provider_df
    # Get all columns that exist in billing_df (these should NOT come from provider_df)
    billing_cols_set = set(billing_df.columns)
    
    # Columns to keep from provider_df: only provider-specific ones that don't exist in billing_df
    # Always keep: provider_group_id (for join), npi (provider list)
    provider_cols_to_keep = ["provider_group_id"]
    
    # Rename npi to provider_npi to avoid ambiguity, then add it
    if "npi" in provider_df.columns:
        provider_df = provider_df.withColumnRenamed("npi", "provider_npi")
        provider_cols_to_keep.append("provider_npi")
    
    # Add any other columns from provider_df that don't exist in billing_df
    for col in provider_df.columns:
        if col not in billing_cols_set and col not in provider_cols_to_keep and col != "provider_group_id":
            provider_cols_to_keep.append(col)
    
    # Select only needed columns from provider_df
    provider_df_clean = provider_df.select(*provider_cols_to_keep)
    
    # 5. Temporarily explode provider_references to join, then aggregate back
    # Add a unique row identifier to billing_df so we can group back together
    billing_df_with_id = billing_df.withColumn(
        "_billing_row_id", 
        F.monotonically_increasing_id()
    )
    
    # Explode provider_references for the join
    billing_exploded = billing_df_with_id.withColumn(
        "provider_reference", 
        F.explode_outer("provider_references")
    )
    
    # 6. Join on provider_reference == provider_group_id
    # Rename provider_group_id in provider_df_clean to avoid ambiguity after join
    provider_df_for_join = provider_df_clean.withColumnRenamed("provider_group_id", "_join_provider_group_id")
    
    # Use broadcast join hint for small provider DataFrame to improve performance
    # Provider data is typically much smaller than billing data
    from pyspark.sql.functions import broadcast
    
    # Check if provider_df is small enough for broadcast (rough heuristic: < 100MB)
    # In production, you might want to check actual size
    # For now, always use broadcast for provider data as it's typically small
    joined = billing_exploded.join(
        broadcast(provider_df_for_join),
        on=[billing_exploded["provider_reference"] == provider_df_for_join["_join_provider_group_id"]],
        how="left"
    )
    LOG.debug("Using broadcast join for provider data (optimization)")
    
    # Drop temporary columns used for join
    joined = joined.drop("provider_reference", "_join_provider_group_id")
    
    # 7. Group back by _billing_row_id and aggregate all NPIs
    # Collect all npi arrays from all matching providers and flatten into one array
    # Handle null npi arrays by coalescing to empty array before collecting
    # Result: one row per billing record with all NPIs from all matching providers in all_npis column
    
    # Collect all npi arrays (each npi is already an array)
    # collect_list(coalesce(npi, array())) creates array of arrays: [[1,2,3], [4,5], []]
    # flatten() flattens it: [1,2,3,4,5,...] (empty arrays are ignored)
    # array_distinct() removes duplicates
    
    # Build aggregation expressions
    agg_exprs = [
        # Take first value of each billing column (they're all the same per _billing_row_id)
        *[F.first(col).alias(col) for col in billing_df.columns if col != "provider_references"],
        # Keep provider_references as-is (it's the same for all rows in the group)
        F.first("provider_references").alias("provider_references"),
    ]
    
    # Only aggregate provider_npi if it exists in the joined dataframe
    if "provider_npi" in joined.columns:
        # Collect all NPIs from all matched providers into a single flattened array
        # Coalesce null provider_npi to empty array, collect all, flatten, and remove duplicates
        agg_exprs.append(
            F.array_distinct(
                F.flatten(
                    F.collect_list(F.coalesce(F.col("provider_npi"), F.array()))
                )
            ).alias("all_npis")
        )
    else:
        # If provider_npi doesn't exist, create empty array for all_npis
        LOG.debug("provider_npi column not found after join, creating empty all_npis array")
        agg_exprs.append(F.array().alias("all_npis"))
    
    aggregated = joined.groupBy("_billing_row_id").agg(*agg_exprs).drop("_billing_row_id")
    
    return aggregated
