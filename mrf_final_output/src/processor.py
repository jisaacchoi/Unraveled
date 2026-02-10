"""Main orchestration for JSON.gz to Parquet/CSV conversion.

This module provides the main entry point for converting JSON.gz files
to Parquet or CSV format with filtering and dimension table creation.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

from src.schema_utils import simplify_column_names
from src.provider_processing import (
    process_provider_references,
    download_and_process_provider_urls,
    load_provider_npis,
)
from src.rates_processing import (
    process_in_network_rates,
    join_rates_with_providers,
)
from src.filtering import filter_rates_dataframe
from src.output_writers import convert_to_csv_compatible, write_rates_to_csv
from src.schema_inference import load_schema_from_file

LOG = logging.getLogger("src.processor")


def process_json_gz_to_csv(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    provider_csv_path: Optional[str] = None,
    filters: Optional[list[tuple[str, any]]] = None,
    sample_size: Optional[int] = None,
    explode_service_codes: bool = False,
    download_dir: Optional[Path] = None,
    skip_url_download: bool = False,
    output_format: str = "csv",
    is_serverless: bool = True,
    temp_dir: Optional[str] = None,
    enable_url_expansion: bool = True,
) -> int:
    """
    Main processing function: convert JSON.gz files to CSV/Parquet with filtering.
    
    Args:
        spark: SparkSession
        input_dir: Directory containing JSON.gz files
        output_dir: Directory to write output files
        provider_csv_path: Optional path to provider CSV file for NPI filtering
        filters: Optional list of (column_name, value) filter tuples
        sample_size: Optional number of rows to sample (for testing)
        explode_service_codes: Whether to explode service_code arrays
        download_dir: Optional directory for downloading URL content (needed when NPI is behind URLs)
        skip_url_download: If True, skip downloading URLs and use existing files in download_dir
        output_format: Output format - "csv" or "parquet" (default: "csv")
        is_serverless: If True (default), use serverless-compatible operations.
                      If False, enable caching and temp file writes for better performance.
        temp_dir: Directory for temporary files (only used when is_serverless=False).
                 Defaults to output_dir/_temp
        enable_url_expansion: Whether to enable URL provider expansion (default: True)
    
    Returns:
        Total number of rows processed
    """
    # Set up temp directory for non-serverless mode
    if not is_serverless:
        if temp_dir is None:
            temp_dir = str(output_dir / "_temp")
        LOG.info(f"Non-serverless mode: temp files will be written to {temp_dir}")
        # Set checkpoint directory for non-serverless
        try:
            spark.sparkContext.setCheckpointDir(f"{temp_dir}/checkpoints")
            LOG.info(f"Checkpoint directory set to {temp_dir}/checkpoints")
        except Exception as e:
            LOG.warning(f"Could not set checkpoint directory: {e}")
    
    # Find schema.json file (required)
    # Check if input_dir contains group subdirectories (group_1, group_2, etc.)
    group_dirs = [d for d in input_dir.iterdir() if d.is_dir() and d.name.startswith("group_")]
    
    if group_dirs:
        # Input directory contains group subdirectories - verify each group has schema.json
        LOG.info(f"Found {len(group_dirs)} group subdirectory(ies) in input directory")
        
        # Check that all groups have schema.json files
        missing_schemas = []
        for group_dir in group_dirs:
            schema_path = group_dir / "schema.json"
            if not schema_path.exists():
                missing_schemas.append(str(schema_path))
        
        if missing_schemas:
            LOG.error("Schema.json file is required for parquet conversion in all group subdirectories.")
            LOG.error(f"Missing schema files: {missing_schemas}")
            raise FileNotFoundError(f"Required schema files not found: {missing_schemas}")
        
        # Use the first group's schema (all groups should have the same schema structure)
        # If groups have different schemas, they should be processed separately
        schema_path = group_dirs[0] / "schema.json"
        LOG.info(f"Loading schema from: {schema_path}")
        schema = load_schema_from_file(schema_path)
        LOG.info(f"Using schema from {group_dirs[0].name} (assuming all groups have the same schema structure)")
    else:
        # Input directory contains files directly - look for schema.json in input_dir
        schema_path = input_dir / "schema.json"
        if not schema_path.exists():
            LOG.error(f"Schema file not found: {schema_path}")
            LOG.error("Schema.json file is required for parquet conversion. Please ensure schema.json exists in the input directory.")
            raise FileNotFoundError(f"Required schema file not found: {schema_path}")
        
        LOG.info(f"Loading schema from: {schema_path}")
        schema = load_schema_from_file(schema_path)
    
    LOG.info(f"Schema loaded successfully: {len(schema.fields)} top-level fields")
    LOG.info("Schema structure:")
    for line in schema.simpleString().split(","):
        LOG.info(f"  {line.strip()}")
    
    # Find all JSON GZ files
    json_gz_files = list(input_dir.glob("*.json.gz"))
    if group_dirs:
        # If we have group subdirectories, also find files in subdirectories
        for group_dir in group_dirs:
            json_gz_files.extend(list(group_dir.glob("*.json.gz")))
    
    if not json_gz_files:
        LOG.error(f"No .json.gz files found in {input_dir}")
        return 0
    
    # Now read all files using the schema from smallest file
    LOG.info("Reading files using text mode to avoid Photon JSON reader...")
    LOG.info(f"Note: Reading {len(json_gz_files)} files - Spark will parallelize across files and executors")
    all_files = [str(f) for f in json_gz_files]
    
    # Read all files and repartition each file individually to prevent OOM
    MB_PER_PARTITION = 300
    MIN_PARTITIONS_PER_FILE = 2
    MAX_PARTITIONS_PER_FILE = 10
    
    raw_dfs = []
    
    for i, file_path in enumerate(all_files):
        file_path_obj = Path(file_path)
        file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
        
        calculated_partitions = max(MIN_PARTITIONS_PER_FILE, int(file_size_mb / MB_PER_PARTITION))
        partitions_per_file = min(calculated_partitions, MAX_PARTITIONS_PER_FILE)
        
        LOG.info(f"Preparing file {i+1}/{len(all_files)}: {file_path_obj.name} ({file_size_mb:.1f} MB, {partitions_per_file} partitions)")
        
        # Get original filename by stripping _part#### suffix (from split files)
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
    
    LOG.info(f"All {len(all_files)} files prepared and repartitioned based on file size")
    
    # Union all DataFrames
    raw_full = raw_dfs[0]
    for df in raw_dfs[1:]:
        raw_full = raw_full.union(df)
    
    LOG.info(f"Unioned {len(raw_dfs)} DataFrames (each file split into multiple partitions based on size)")
    
    # Select only the columns we need to reduce memory usage
    available_columns = set(raw_full.columns)
    has_provider_references = "provider_references" in available_columns
    has_in_network = "in_network" in available_columns
    
    cols_to_select = ["source_file"]
    if has_in_network:
        cols_to_select.append("in_network")
    if has_provider_references:
        cols_to_select.append("provider_references")
    
    raw = raw_full.select(*cols_to_select)
    LOG.info(f"Selected columns: {cols_to_select} (reduced from {len(available_columns)} columns)")
    
    LOG.info(f"Loaded {len(all_files)} files (lazy - will process on write)")
    LOG.info(f"Has provider_references: {has_provider_references}")
    LOG.info(f"Has in_network: {has_in_network}")
    
    # Process provider references
    provider_flat, provider_urls = process_provider_references(spark, raw, has_provider_references, download_dir)
    
    LOG.info(f"After process_provider_references: provider_flat={provider_flat is not None}, provider_urls={provider_urls is not None}")
    
    if provider_flat is not None:
        LOG.info(f"Inline provider_flat columns: {provider_flat.columns}")
    
    if provider_urls is not None:
        LOG.info(f"Provider URLs columns: {provider_urls.columns}")
    
    # Track URL expansion status
    url_expansion_status = "not_needed"
    
    # If there are URL references, download and extract NPIs
    if provider_urls is not None and download_dir:
        LOG.info(f"Provider references contain URLs, processing (skip_download={skip_url_download}, enable_url_expansion={enable_url_expansion})...")
        url_provider_flat = download_and_process_provider_urls(
            spark, provider_urls, download_dir, skip_download=skip_url_download, enable_url_expansion=enable_url_expansion
        )
        
        if url_provider_flat is not None:
            url_expansion_status = "enabled"
            LOG.info(f"URL provider_flat columns: {url_provider_flat.columns}")
            LOG.info("=== EXPLAIN: url_provider_flat ===")
            url_provider_flat.explain()
            
            if provider_flat is not None:
                provider_flat = provider_flat.union(url_provider_flat)
                LOG.info("Merged inline NPIs with NPIs from downloaded URLs")
            else:
                provider_flat = url_provider_flat
                LOG.info("Using NPIs from downloaded URLs (no inline NPIs)")
        else:
            # Determine why it failed
            if not enable_url_expansion:
                url_expansion_status = "disabled"
            else:
                url_expansion_status = "failed"
            LOG.warning("download_and_process_provider_urls returned None - no NPIs extracted from URLs")
    elif provider_urls is not None and not download_dir:
        LOG.error(
            "=" * 80 + "\n"
            "ERROR: Provider references contain URLs but download_dir not specified!\n"
            "  URL expansion will be skipped. NPIs from URLs will be missing.\n"
            "  Set processing.download_directory in config to enable URL expansion.\n"
            "=" * 80
        )
        url_expansion_status = "failed_no_download_dir"
    else:
        LOG.info("No provider URLs to download")
    
    if provider_flat is not None:
        provider_flat = simplify_column_names(provider_flat)
        LOG.info(f"Final provider_flat columns: {provider_flat.columns}")
    else:
        LOG.warning("provider_flat is None - no NPI data available for join!")
    
    # Process in-network rates
    rates = process_in_network_rates(spark, raw, has_in_network, explode_service_codes)
    
    if rates is None:
        LOG.warning("No rates data to process")
        return 0
    
    LOG.info(f"Rates columns: {rates.columns}")
    LOG.info("=== EXPLAIN: rates after process_in_network_rates ===")
    rates.explain()
    
    # Join rates with providers
    rates_with_providers = join_rates_with_providers(rates, provider_flat)
    
    if rates_with_providers is None:
        LOG.warning("No rates data to write")
        return 0
    
    # Simplify column names
    LOG.info("Simplifying column names...")
    rates_with_providers = simplify_column_names(rates_with_providers)

    # If NPI is an array (after simplification), explode to one NPI per row
    try:
        npi_fields = [f for f in rates_with_providers.schema.fields if "npi" in f.name.lower()]
        for field in npi_fields:
            if isinstance(field.dataType, T.ArrayType):
                LOG.info(f"Exploding NPI array column '{field.name}' to one NPI per row...")
                rates_with_providers = rates_with_providers.withColumn(field.name, F.explode_outer(F.col(field.name)))
    except Exception as e:
        LOG.warning(f"Could not inspect/explode NPI column(s): {e}")
    
    LOG.info("=== EXPLAIN: rates_with_providers after simplification ===")
    rates_with_providers.explain()
    
    # Load provider NPIs if CSV path provided
    provider_npi_set = None
    if provider_csv_path:
        provider_npi_set = load_provider_npis(provider_csv_path)
    
    # Log column names before filtering
    LOG.info(f"DataFrame columns before filtering ({len(rates_with_providers.columns)} total):")
    for idx, col in enumerate(rates_with_providers.columns, 1):
        LOG.info(f"  {idx}. {col}")
    
    # Apply filters
    if provider_npi_set or filters:
        LOG.info("Applying filters: NPI and config-based filters...")
        rates_with_providers = filter_rates_dataframe(
            spark,
            rates_with_providers,
            provider_npi_set=provider_npi_set,
            filters=filters,
        )
    
    # Join with mrf_index table
    LOG.info("Joining with mrf_index table to get reporting_plans and reporting_entity_name...")
    try:
        mrf_index_df = spark.read.table("test.mrf_index")
        LOG.info(f"Loaded mrf_index table: {mrf_index_df.columns}")
        
        mrf_index_cols = ["file_name"]
        if "reporting_plans" in mrf_index_df.columns:
            mrf_index_cols.append("reporting_plans")
        if "reporting_entity_name" in mrf_index_df.columns:
            mrf_index_cols.append("reporting_entity_name")
        
        mrf_index_selected = mrf_index_df.select(*mrf_index_cols)
        
        rates_with_providers = rates_with_providers.join(
            mrf_index_selected,
            rates_with_providers.source_file == mrf_index_selected.file_name,
            how="left"
        ).drop(mrf_index_selected.file_name)
        
        LOG.info("Joined with mrf_index table (left join)")
        
        # Explode reporting_plans if it exists
        if "reporting_plans" in rates_with_providers.columns:
            LOG.info("Exploding reporting_plans to extract plan details...")
            
            try:
                reporting_plans_field = None
                for field in rates_with_providers.schema.fields:
                    if field.name == "reporting_plans":
                        reporting_plans_field = field
                        break
                
                if reporting_plans_field and isinstance(reporting_plans_field.dataType, T.ArrayType):
                    LOG.info("reporting_plans is an array - exploding directly...")
                    element_type = reporting_plans_field.dataType.elementType
                    LOG.info(f"reporting_plans array element type: {element_type}")
                    rates_with_providers = rates_with_providers.select(
                        *[col for col in rates_with_providers.columns if col != "reporting_plans"],
                        F.explode_outer("reporting_plans").alias("plan")
                    )
                elif reporting_plans_field and isinstance(reporting_plans_field.dataType, T.StringType):
                    LOG.info("reporting_plans is a string - parsing JSON to array of structs...")
                    plan_schema = T.ArrayType(
                        T.StructType([
                            T.StructField("plan_id", T.StringType(), True),
                            T.StructField("plan_name", T.StringType(), True),
                            T.StructField("plan_id_type", T.StringType(), True),
                            T.StructField("plan_market_type", T.StringType(), True),
                        ])
                    )
                    rates_with_providers = rates_with_providers.withColumn(
                        "plans_array",
                        F.from_json(F.col("reporting_plans"), plan_schema)
                    ).select(
                        *[col for col in rates_with_providers.columns if col != "reporting_plans"],
                        F.explode_outer("plans_array").alias("plan")
                    ).drop("plans_array")
                else:
                    LOG.info(f"reporting_plans type is {reporting_plans_field.dataType if reporting_plans_field else 'unknown'} - treating as struct...")
                    rates_with_providers = rates_with_providers.withColumn("plan", F.col("reporting_plans")).drop("reporting_plans")
                
                LOG.info("Extracting plan fields from exploded reporting_plans...")
                rates_with_providers = rates_with_providers.withColumn(
                    "plan_id",
                    F.col("plan.plan_id")
                ).withColumn(
                    "plan_name",
                    F.col("plan.plan_name")
                ).withColumn(
                    "plan_id_type",
                    F.col("plan.plan_id_type")
                ).withColumn(
                    "plan_market_type",
                    F.col("plan.plan_market_type")
                ).drop("plan")
                
                LOG.info("Extracted plan fields: plan_id, plan_name, plan_id_type, plan_market_type")
            except Exception as e:
                LOG.warning(f"Could not explode reporting_plans: {e}. Keeping reporting_plans as-is.")
        else:
            LOG.warning("reporting_plans column not found in mrf_index table")
    except Exception as e:
        LOG.warning(f"Could not join with mrf_index table: {e}. Proceeding without plan data.")
    
    LOG.info("Writing to Parquet format...")
    parquet_output_path = str(output_dir / "in_network_rates.parquet")
    
    if sample_size:
        rates_with_providers = rates_with_providers.limit(sample_size)
        LOG.info(f"Limited to {sample_size} rows for sampling")
    
    # NON-SERVERLESS OPTIMIZATION: Write intermediate result to temp parquet
    if not is_serverless:
        LOG.info("Non-serverless mode: Writing intermediate result to temp parquet to break lineage...")
        intermediate_path = f"{temp_dir}/intermediate_rates_with_providers"
        rates_with_providers.write.mode("overwrite").parquet(intermediate_path)
        LOG.info(f"  Written to: {intermediate_path}")
        rates_with_providers = spark.read.parquet(intermediate_path)
        LOG.info("  Read back from temp parquet - lineage is now clean")
    
    # NON-SERVERLESS OPTIMIZATION: Cache before writing
    if not is_serverless:
        LOG.info("Non-serverless mode: Caching DataFrame before writing...")
        rates_with_providers.persist(StorageLevel.MEMORY_AND_DISK)
        cache_count = rates_with_providers.count()
        LOG.info(f"  Cached {cache_count:,} rows in memory/disk")
    
    # Use rates_with_providers as fact table (no dimension normalization)
    # Cast all columns to string to avoid dtype issues and keep filtering consistent
    fact_df = rates_with_providers.select(
        [F.col(c).cast("string").alias(c) for c in rates_with_providers.columns]
    )
    
    # NON-SERVERLESS: Unpersist after processing
    if not is_serverless:
        LOG.info("Non-serverless mode: Unpersisting cached DataFrame...")
        rates_with_providers.unpersist()
        LOG.info("  Unpersisted")
    
    # Write fact table
    LOG.info(f"Writing fact table using ZSTD compression...")
    LOG.info(f"Fact table columns ({len(fact_df.columns)} total): {fact_df.columns}")
    fact_df.write.mode("overwrite").option("compression", "zstd").parquet(parquet_output_path)
    LOG.info(f"Written to {parquet_output_path}")
    rows_written = -1  # Row count not available (skipped to avoid expensive materialization)
    
    return rows_written
