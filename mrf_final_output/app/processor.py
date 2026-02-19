"""Core processing logic for converting JSON.gz to Parquet."""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
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
from app.db_utils import get_provider_data_by_npis

LOG = logging.getLogger("app.processor")


def _nested_array_depth(dtype: T.DataType) -> int:
    if isinstance(dtype, T.ArrayType):
        return 1 + _nested_array_depth(dtype.elementType)
    if isinstance(dtype, T.StructType):
        if not dtype.fields:
            return 0
        return max(_nested_array_depth(f.dataType) for f in dtype.fields)
    return 0


def _flatten_struct_columns_once(df: DataFrame) -> tuple[DataFrame, bool]:
    struct_fields = [f for f in df.schema.fields if isinstance(f.dataType, T.StructType)]
    if not struct_fields:
        return df, False

    struct_names = {f.name for f in struct_fields}
    select_exprs = []
    for field in df.schema.fields:
        col_name = field.name
        if col_name not in struct_names:
            select_exprs.append(F.col(f"`{col_name}`"))
            continue
        struct_type = field.dataType
        for child in struct_type.fields:
            child_name = child.name
            alias = f"{col_name}|{child_name}"
            select_exprs.append(F.col(f"`{col_name}`.`{child_name}`").alias(alias))
    return df.select(*select_exprs), True


def _flatten_all_struct_columns(df: DataFrame) -> DataFrame:
    """Flatten top-level and nested struct columns until no struct columns remain."""
    while True:
        df, flattened = _flatten_struct_columns_once(df)
        if not flattened:
            return df


def explode_all_array_columns(df):
    """
    Schema-driven, column-agnostic flattening:
    1) Flatten structs one level at a time
    2) Explode arrays from deepest schema depth to shallowest
    3) Skip empty/null arrays (no placeholder rows)
    Repeat until no struct/array columns remain.
    """
    iteration = 0
    while True:
        # Expose nested arrays inside structs as top-level columns first.
        df, flattened = _flatten_struct_columns_once(df)

        array_fields = [f for f in df.schema.fields if isinstance(f.dataType, T.ArrayType)]
        if not array_fields:
            if not flattened:
                break
            continue

        iteration += 1
        array_fields = sorted(
            array_fields,
            key=lambda f: _nested_array_depth(f.dataType),
            reverse=True,
        )
        LOG.info(
            "Explode-all iteration %d: exploding %d array column(s) deepest-first: %s",
            iteration,
            len(array_fields),
            [f.name for f in array_fields],
        )

        for field in array_fields:
            col_name = field.name
            col_ref = F.col(f"`{col_name}`")
            # Skip empty array items by filtering before explode.
            df = df.where(col_ref.isNotNull() & (F.size(col_ref) > 0))
            df = df.withColumn(col_name, F.explode(col_ref))

    return df


def log_df_count(df: Optional[DataFrame], stage: str) -> int:
    """Count actions are disabled to avoid Spark materialization and OOM."""
    if df is None:
        LOG.info("Row count after %s: <none>", stage)
        return -1
    LOG.info("Row count after %s: <skipped>", stage)
    return -1


def is_rate_like_array(array_type: T.ArrayType) -> bool:
    """Heuristic: identify arrays that contain negotiated rate-like structures."""
    element_type = array_type.elementType
    if not isinstance(element_type, T.StructType):
        return False
    field_names = {f.name for f in element_type.fields}
    if {"negotiated_prices", "negotiated_rates", "provider_references"} & field_names:
        return True
    if {"negotiated_rate", "billing_class", "negotiated_type"} & field_names:
        return True
    return False


def drop_all_null_business_rows(df: DataFrame, keep_cols: Optional[List[str]] = None) -> DataFrame:
    """
    Drop rows where every business column is null/empty.
    """
    keep_set = set(keep_cols or [])
    business_cols = [c for c in df.columns if c not in keep_set]
    if not business_cols:
        return df

    non_empty_expr = None
    for col_name in business_cols:
        expr = F.col(col_name).isNotNull() & (F.length(F.trim(F.col(col_name).cast("string"))) > 0)
        non_empty_expr = expr if non_empty_expr is None else (non_empty_expr | expr)

    if non_empty_expr is None:
        return df
    return df.filter(non_empty_expr)


def _select_join_column(columns: List[str]) -> Optional[str]:
    candidates = [
        "provider_group_id",
        "provider_references",
        "provider_reference",
    ]
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def _read_materialized_prejoin(path: Path, output_format: str) -> pd.DataFrame:
    fmt = (output_format or "parquet").strip().lower()
    if fmt == "pandas_csv":
        csv_path = path / f"{path.name}.csv"
        if not csv_path.exists():
            return pd.DataFrame()
        return pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    if not path.exists() or not any(path.iterdir()):
        return pd.DataFrame()
    return pd.read_parquet(path)


def _write_fact_pandas(
    fact_pd: pd.DataFrame,
    target_dir: Path,
    output_format: str,
    append_output: bool,
) -> int:
    target_dir.mkdir(parents=True, exist_ok=True)
    fmt = (output_format or "parquet").strip().lower()
    if fmt == "pandas_csv":
        csv_path = target_dir / f"{target_dir.name}.csv"
        write_header = not csv_path.exists() or not append_output
        fact_pd.to_csv(csv_path, index=False, mode="a" if append_output else "w", header=write_header)
        return 1

    parquet_path = target_dir / f"{target_dir.name}.parquet"
    if append_output and parquet_path.exists():
        existing_pd = pd.read_parquet(parquet_path)
        fact_pd = pd.concat([existing_pd, fact_pd], ignore_index=True)
    fact_pd.to_parquet(parquet_path, index=False)
    return 1


def _merge_pandas_on_source_file(
    left_pd: pd.DataFrame,
    right_pd: pd.DataFrame,
    suffix: str = "_struct",
) -> pd.DataFrame:
    """Left-join two pandas DataFrames on source_file with collision-safe suffixing."""
    if left_pd.empty or right_pd.empty:
        return left_pd
    if "source_file" not in left_pd.columns or "source_file" not in right_pd.columns:
        return left_pd

    right_dedup = right_pd.drop_duplicates(subset=["source_file"], keep="first")
    return left_pd.merge(
        right_dedup,
        on="source_file",
        how="left",
        suffixes=("", suffix),
    )


def process_single_file(
    spark: SparkSession,
    file_paths: List[Path],
    schema_path: Path,
    output_dir: Path,
    group_context: Optional[str] = None,
    npi_filter: Optional[List[str]] = None,
    billing_code_filter: Optional[List[str]] = None,
    explode_service_codes: bool = False,
    download_dir: Optional[Path] = None,
    skip_url_download: bool = False,
    is_serverless: bool = True,
    enable_url_expansion: bool = True,
    output_format: str = "parquet",
    append_output: bool = False,
    enable_parquet_conversion: bool = True,  # If False, skip parquet writing
    spark_environment: str = "local",
    app_config: Optional[Dict[str, Any]] = None,
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
    group_prefix = f"[group={group_context}] " if group_context else ""

    LOG.info("%sProcessing %d file(s) to Parquet", group_prefix, len(file_paths))
    LOG.info("%sOutput directory: %s", group_prefix, output_dir)
    
    # Load schema
    LOG.info("%sLoading schema from: %s", group_prefix, schema_path)
    schema = load_schema_from_file(schema_path)
    LOG.info("%sSchema loaded: %d top-level fields", group_prefix, len(schema.fields))
    
    # Read all file parts
    LOG.info("%sReading files using text mode...", group_prefix)
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
        
        LOG.info(
            "%sFile %d/%d: %s (%.1f MB, %d partitions)",
            group_prefix,
            i + 1,
            len(all_files),
            file_path_obj.name,
            file_size_mb,
            partitions_per_file,
        )
        
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
        log_df_count(file_df, f"{group_prefix}parse {file_path_obj.name}")
        
        file_df = file_df.repartition(partitions_per_file)
        raw_dfs.append(file_df)
    
    # Prepare filters
    provider_npi_set = None
    if npi_filter:
        provider_npi_set = set(npi_filter)
        LOG.info("%sUsing NPI filter: %d NPIs", group_prefix, len(provider_npi_set))
    
    filters = []
    if billing_code_filter:
        # Convert billing code filter to list format for filter_rates_dataframe
        # filter_rates_dataframe expects filters as list of (column_name, filter_value) tuples
        # where filter_value can be a list for ranges
        filters = [
            ("billing_code", billing_code_filter),
            ("billing_code_type", "CPT"),
        ]
        LOG.info("%sUsing CPT billing code filter: %d codes/ranges", group_prefix, len(billing_code_filter))

    def merge_url_status(current: str, new: str) -> str:
        if new == "enabled" or current == "enabled":
            return "enabled"
        if new.startswith("failed"):
            return new
        if current.startswith("failed"):
            return current
        if new == "disabled":
            return "disabled"
        return current

    def build_fact_from_parts(
        rates: Optional[DataFrame],
        provider_flat: Optional[DataFrame],
        struct_flat: Optional[DataFrame],
        stage_prefix: str,
    ) -> Optional[DataFrame]:
        if rates is None:
            LOG.warning("%s no rates data to process", stage_prefix)
            return None

        rates_has_npi_col = any("npi" in field.name.lower() for field in rates.schema.fields)

        provider_for_join = provider_flat
        if provider_for_join is not None and provider_npi_set:
            provider_for_join = filter_rates_dataframe(
                spark,
                provider_for_join,
                provider_npi_set=provider_npi_set,
                filters=None,
            )

        rates_with_providers = join_rates_with_providers(rates, provider_for_join)
        if rates_with_providers is None:
            LOG.warning("%s no rates data after join", stage_prefix)
            return None

        if struct_flat is not None and "source_file" in rates_with_providers.columns and "source_file" in struct_flat.columns:
            struct_payload_cols = [c for c in struct_flat.columns if c != "source_file"]
            if struct_payload_cols:
                rates_with_providers = rates_with_providers.join(
                    struct_flat.select("source_file", *struct_payload_cols).dropDuplicates(["source_file"]),
                    on="source_file",
                    how="left",
                )

        rates_with_providers = explode_all_array_columns(rates_with_providers)
        log_df_count(rates_with_providers, f"{stage_prefix} rates_with_providers (after explode all arrays)")

        rates_with_providers = simplify_column_names(rates_with_providers)
        log_df_count(rates_with_providers, f"{stage_prefix} rates_with_providers (simplified)")

        rates_with_providers = drop_all_null_business_rows(rates_with_providers, keep_cols=["source_file"])
        log_df_count(rates_with_providers, f"{stage_prefix} rates_with_providers (drop all-null business rows)")

        try:
            npi_fields = [f for f in rates_with_providers.schema.fields if "npi" in f.name.lower()]
            for field in npi_fields:
                if isinstance(field.dataType, T.ArrayType):
                    LOG.info("%s exploding NPI array column '%s' to one NPI per row...", stage_prefix, field.name)
                    rates_with_providers = rates_with_providers.withColumn(field.name, F.explode_outer(F.col(field.name)))
        except Exception as e:
            LOG.warning("%s could not inspect/explode NPI column(s): %s", stage_prefix, e)

        effective_provider_npi_set = provider_npi_set if rates_has_npi_col else None
        if provider_npi_set and not rates_has_npi_col:
            LOG.info(
                "%s rates has no top-level NPI column; skipping post-join NPI filter and using provider-only NPI filtering.",
                stage_prefix,
            )

        if effective_provider_npi_set or filters:
            LOG.info("%s applying filters...", stage_prefix)
            rates_with_providers = filter_rates_dataframe(
                spark,
                rates_with_providers,
                provider_npi_set=effective_provider_npi_set,
                filters=filters,
            )
            log_df_count(rates_with_providers, f"{stage_prefix} rates_with_providers (after filters)")

        if not is_serverless:
            LOG.info("%s non-serverless mode: writing intermediate result...", stage_prefix)
            temp_dir = output_dir / "_temp"
            temp_dir.mkdir(parents=True, exist_ok=True)
            intermediate_path = str(temp_dir / f"intermediate_rates_{stage_prefix.replace(' ', '_')}")
            rates_with_providers.write.mode("overwrite").parquet(intermediate_path)
            rates_with_providers = spark.read.parquet(intermediate_path)

        if not is_serverless:
            LOG.info("%s caching DataFrame before writing...", stage_prefix)
            rates_with_providers.persist(StorageLevel.MEMORY_AND_DISK)
            LOG.info("%s cached DataFrame (count skipped)", stage_prefix)

        fact_df_part = rates_with_providers.select(
            [F.col(c).cast("string").alias(c) for c in rates_with_providers.columns]
        )
        log_df_count(fact_df_part, f"{stage_prefix} fact_df (cast to string)")

        if not is_serverless:
            rates_with_providers.unpersist()

        return fact_df_part

    def build_fact_df_from_raw(raw_full: DataFrame, stage_prefix: str) -> tuple[Optional[DataFrame], str]:
        provider_flat, rates, struct_flat, url_status = build_provider_and_rates_from_raw(raw_full, stage_prefix)
        fact_df_part = build_fact_from_parts(rates, provider_flat, struct_flat, stage_prefix)
        return fact_df_part, url_status

    def build_provider_and_rates_from_raw(
        raw_full: DataFrame, stage_prefix: str
    ) -> tuple[Optional[DataFrame], Optional[DataFrame], Optional[DataFrame], str]:
        # Local late-join mode: materialize providers and rates separately per file,
        # then join once across all files in the group.
        top_level_array_cols: List[str] = []
        top_level_struct_cols: List[str] = []
        array_field_types: Dict[str, T.ArrayType] = {}
        for field in raw_full.schema.fields:
            if field.name == "source_file":
                continue
            if isinstance(field.dataType, T.ArrayType):
                top_level_array_cols.append(field.name)
                array_field_types[field.name] = field.dataType
            elif isinstance(field.dataType, T.StructType):
                top_level_struct_cols.append(field.name)

        cols_to_select = ["source_file"] + top_level_array_cols
        raw = raw_full.select(*cols_to_select)
        LOG.info("%s selected columns: %s", stage_prefix, cols_to_select)
        log_df_count(raw, f"{stage_prefix} select source + top-level arrays")

        struct_flat = None
        if top_level_struct_cols:
            struct_raw = raw_full.select("source_file", *top_level_struct_cols)
            struct_flat = _flatten_all_struct_columns(struct_raw)
            struct_flat = simplify_column_names(struct_flat)
            log_df_count(struct_flat, f"{stage_prefix} struct_flat (simplified)")

        provider_array_column: Optional[str] = None
        if "provider_references" in top_level_array_cols:
            provider_array_column = "provider_references"
        else:
            for array_col in top_level_array_cols:
                element_type = array_field_types[array_col].elementType
                if isinstance(element_type, T.StructType):
                    element_field_names = {f.name for f in element_type.fields}
                    if "provider_groups" in element_field_names or "location" in element_field_names:
                        provider_array_column = array_col
                        LOG.info("%s using top-level array '%s' as provider references source", stage_prefix, array_col)
                        break
        has_provider_references = provider_array_column is not None

        provider_flat, provider_urls = process_provider_references(
            spark,
            raw,
            has_provider_references,
            download_dir,
            provider_array_column=provider_array_column or "provider_references",
        )
        log_df_count(provider_flat, f"{stage_prefix} provider_flat (initial)")
        log_df_count(provider_urls, f"{stage_prefix} provider_urls (initial)")

        url_status = "not_needed"
        if provider_urls is not None and download_dir:
            LOG.info(
                "%s processing provider URLs (skip_download=%s, enable_url_expansion=%s)...",
                stage_prefix,
                skip_url_download,
                enable_url_expansion,
            )
            url_provider_flat = download_and_process_provider_urls(
                spark, provider_urls, download_dir, skip_download=skip_url_download, enable_url_expansion=enable_url_expansion
            )
            if url_provider_flat is not None:
                url_status = "enabled"
                if provider_flat is not None:
                    provider_flat = provider_flat.unionByName(url_provider_flat, allowMissingColumns=True)
                else:
                    provider_flat = url_provider_flat
                log_df_count(url_provider_flat, f"{stage_prefix} provider_flat from downloaded URLs")
                log_df_count(provider_flat, f"{stage_prefix} provider_flat (after URL merge)")
            else:
                url_status = "disabled" if not enable_url_expansion else "failed"
        elif provider_urls is not None and not download_dir:
            LOG.warning(
                "=" * 80 + "\n"
                "WARNING: Provider references contain URLs but download_dir not specified!\n"
                "  URL expansion will be skipped. NPIs from URLs will be missing.\n"
                "  Set processing.download_directory in config to enable URL expansion.\n"
                "=" * 80
            )
            url_status = "failed_no_download_dir"
        elif provider_urls is not None:
            url_status = "failed_no_download_dir"

        if provider_flat is not None:
            provider_flat = simplify_column_names(provider_flat)
            log_df_count(provider_flat, f"{stage_prefix} provider_flat (simplified)")

        rate_array_columns = [
            c for c in top_level_array_cols
            if c != provider_array_column and is_rate_like_array(array_field_types[c])
        ]
        if not rate_array_columns:
            LOG.info("%s no rate-like top-level arrays found in %s", stage_prefix, top_level_array_cols)

        rates_parts: List[DataFrame] = []
        for array_col in rate_array_columns:
            rates_part = process_in_network_rates(
                spark,
                raw,
                has_in_network=True,
                explode_service_codes=explode_service_codes,
                array_column=array_col,
                preserve_null_arrays=False,
            )
            if rates_part is not None:
                log_df_count(rates_part, f"{stage_prefix} rates part from {array_col}")
                rates_parts.append(rates_part)

        rates = None
        if rates_parts:
            rates = rates_parts[0]
            for part in rates_parts[1:]:
                rates = rates.unionByName(part, allowMissingColumns=True)
            log_df_count(rates, f"{stage_prefix} rates (union of top-level arrays)")

        return provider_flat, rates, struct_flat, url_status

    env_mode = (spark_environment or "local").strip().lower()
    if env_mode not in {"local", "cloud"}:
        LOG.warning("Unknown spark.environment=%s; defaulting to local behavior", spark_environment)
        env_mode = "local"
    LOG.info("%sUsing spark.environment=%s", group_prefix, env_mode)

    url_expansion_status = "not_needed"
    files_written = 0
    total_rows = 0
    fact_output_dir = output_dir / "fact"
    fact_before_provider_output_dir = output_dir / "fact_before_provider_data_join"
    rates_prejoin_output_dir = output_dir / "rates_prejoin"
    providers_prejoin_output_dir = output_dir / "providers_prejoin"
    structs_prejoin_output_dir = output_dir / "structs_prejoin"
    fact_output_dir.mkdir(parents=True, exist_ok=True)
    fact_before_provider_output_dir.mkdir(parents=True, exist_ok=True)
    rates_prejoin_output_dir.mkdir(parents=True, exist_ok=True)
    providers_prejoin_output_dir.mkdir(parents=True, exist_ok=True)
    structs_prejoin_output_dir.mkdir(parents=True, exist_ok=True)

    if env_mode == "cloud":
        # Cloud mode: process each file independently, union at end, then write once.
        fact_df_parts: List[DataFrame] = []
        for i, raw_df in enumerate(raw_dfs, 1):
            stage_prefix = f"{group_prefix}file[{i}/{len(raw_dfs)}]"
            fact_part, part_url_status = build_fact_df_from_raw(raw_df, stage_prefix)
            url_expansion_status = merge_url_status(url_expansion_status, part_url_status)
            if fact_part is not None:
                fact_df_parts.append(fact_part)

        if not fact_df_parts:
            LOG.warning("%sNo rates data to process", group_prefix)
            return {
                "total_rows": 0,
                "files_written": 0,
                "output_directory": str(output_dir),
            }

        if len(fact_df_parts) == 1:
            fact_df = fact_df_parts[0]
        else:
            fact_df = fact_df_parts[0]
            for part in fact_df_parts[1:]:
                fact_df = fact_df.unionByName(part, allowMissingColumns=True)
            log_df_count(fact_df, "fact_df (union at end across files)")

        if not enable_parquet_conversion:
            LOG.info("Parquet conversion disabled - skipping file writing")
            files_written = 0
        elif output_format.lower() == "pandas_csv":
            LOG.info("Writing fact table via pandas to CSV%s...", " (append)" if append_output else " (single file)")
            pandas_df = fact_df.toPandas()
            csv_path = fact_output_dir / "fact.csv"
            write_header = not csv_path.exists() or not append_output
            pandas_df.to_csv(csv_path, index=False, mode="a" if append_output else "w", header=write_header)
            files_written = 1
        else:
            LOG.info("Writing fact table as Parquet%s...", " (append)" if append_output else "")
            write_mode = "append" if append_output else "overwrite"
            fact_df.write.mode(write_mode).option("compression", "zstd").parquet(str(fact_output_dir))
            files_written = len(list(fact_output_dir.glob("*.parquet")))

        total_rows = -1
    else:
        # Local mode: write filtered pre-join datasets per file (append-only),
        # then build final fact by reading the materialized pre-join datasets.
        wrote_any_rates = False
        for i, raw_df in enumerate(raw_dfs, 1):
            stage_prefix = f"{group_prefix}file[{i}/{len(raw_dfs)}]"
            provider_part, rates_part, struct_part, part_url_status = build_provider_and_rates_from_raw(raw_df, stage_prefix)
            url_expansion_status = merge_url_status(url_expansion_status, part_url_status)
            current_append = append_output or i > 1

            # Build pre-join inspection DataFrames after applying relevant pre-join filters:
            # - billing/code filters on rates
            # - NPI filter on providers
            rates_prejoin_for_write = rates_part
            provider_prejoin_for_write = provider_part
            struct_prejoin_for_write = struct_part
            if rates_prejoin_for_write is not None:
                rates_prejoin_for_write = simplify_column_names(rates_prejoin_for_write)
                if filters:
                    rates_prejoin_for_write = filter_rates_dataframe(
                        spark,
                        rates_prejoin_for_write,
                        provider_npi_set=None,
                        filters=filters,
                    )
                wrote_any_rates = True
            if provider_prejoin_for_write is not None and provider_npi_set:
                provider_prejoin_for_write = filter_rates_dataframe(
                    spark,
                    provider_prejoin_for_write,
                    provider_npi_set=provider_npi_set,
                    filters=None,
                )
            if struct_prejoin_for_write is not None:
                struct_prejoin_for_write = struct_prejoin_for_write.dropDuplicates(["source_file"])

            # Write pre-join DataFrames for inspection (after pre-join filters).
            if enable_parquet_conversion:
                if output_format.lower() == "pandas_csv":
                    if rates_prejoin_for_write is not None:
                        rates_csv_path = rates_prejoin_output_dir / "rates_prejoin.csv"
                        rates_pd = rates_prejoin_for_write.select(
                            [F.col(c).cast("string").alias(c) for c in rates_prejoin_for_write.columns]
                        ).toPandas()
                        rates_write_header = not rates_csv_path.exists() or not current_append
                        rates_pd.to_csv(
                            rates_csv_path,
                            index=False,
                            mode="a" if current_append else "w",
                            header=rates_write_header,
                        )
                    if provider_prejoin_for_write is not None:
                        providers_csv_path = providers_prejoin_output_dir / "providers_prejoin.csv"
                        providers_pd = provider_prejoin_for_write.select(
                            [F.col(c).cast("string").alias(c) for c in provider_prejoin_for_write.columns]
                        ).toPandas()
                        providers_write_header = not providers_csv_path.exists() or not current_append
                        providers_pd.to_csv(
                            providers_csv_path,
                            index=False,
                            mode="a" if current_append else "w",
                            header=providers_write_header,
                        )
                    if struct_prejoin_for_write is not None:
                        structs_csv_path = structs_prejoin_output_dir / "structs_prejoin.csv"
                        structs_pd = struct_prejoin_for_write.select(
                            [F.col(c).cast("string").alias(c) for c in struct_prejoin_for_write.columns]
                        ).toPandas()
                        structs_write_header = not structs_csv_path.exists() or not current_append
                        structs_pd.to_csv(
                            structs_csv_path,
                            index=False,
                            mode="a" if current_append else "w",
                            header=structs_write_header,
                        )
                else:
                    if rates_prejoin_for_write is not None:
                        rates_prejoin_for_write.write.mode("append" if current_append else "overwrite").option("compression", "zstd").parquet(
                            str(rates_prejoin_output_dir)
                        )
                    if provider_prejoin_for_write is not None:
                        provider_prejoin_for_write.write.mode("append" if current_append else "overwrite").option("compression", "zstd").parquet(
                            str(providers_prejoin_output_dir)
                        )
                    if struct_prejoin_for_write is not None:
                        struct_prejoin_for_write.write.mode("append" if current_append else "overwrite").option("compression", "zstd").parquet(
                            str(structs_prejoin_output_dir)
                        )

        if not wrote_any_rates:
            LOG.warning("%sNo rates data to process", group_prefix)
            return {
                "total_rows": 0,
                "files_written": 0,
                "output_directory": str(output_dir),
            }

        if not enable_parquet_conversion:
            LOG.info("%sParquet conversion disabled - skipping final fact write", group_prefix)
            files_written = 0
            total_rows = -1
        else:
            LOG.info("%sLocal mode: building final fact from materialized pre-join datasets via pandas...", group_prefix)
            rates_pd = _read_materialized_prejoin(rates_prejoin_output_dir, output_format)
            providers_pd = _read_materialized_prejoin(providers_prejoin_output_dir, output_format)
            structs_pd = _read_materialized_prejoin(structs_prejoin_output_dir, output_format)

            if rates_pd.empty:
                LOG.warning("%sRates pre-join dataset is empty; writing empty fact outputs", group_prefix)
                _write_fact_pandas(
                    fact_pd=rates_pd,
                    target_dir=fact_before_provider_output_dir,
                    output_format=output_format,
                    append_output=append_output,
                )
                files_written = _write_fact_pandas(
                    fact_pd=rates_pd,
                    target_dir=fact_output_dir,
                    output_format=output_format,
                    append_output=append_output,
                )
                return {
                    "total_rows": 0,
                    "files_written": files_written,
                    "output_directory": str(output_dir),
                    "url_expansion_status": url_expansion_status,
                }

            rates_pd.columns = [str(c) for c in rates_pd.columns]
            providers_pd.columns = [str(c) for c in providers_pd.columns]
            structs_pd.columns = [str(c) for c in structs_pd.columns]
            # Normalize common provider key columns to string before join-key selection.
            for key_col in ("provider_references", "provider_group_id"):
                if key_col in rates_pd.columns:
                    rates_pd[key_col] = rates_pd[key_col].astype("string")
                if key_col in providers_pd.columns:
                    providers_pd[key_col] = providers_pd[key_col].astype("string")
            rates_join_col = _select_join_column(list(rates_pd.columns))
            providers_join_col = _select_join_column(list(providers_pd.columns)) if not providers_pd.empty else None

            if rates_join_col and providers_join_col and not providers_pd.empty:
                LOG.info(
                    "%sLocal pandas join columns selected: rates.%s <-> providers.%s",
                    group_prefix,
                    rates_join_col,
                    providers_join_col,
                )
                rates_pd["__join_provider_key"] = rates_pd[rates_join_col].astype(str)
                providers_pd["__join_provider_key"] = providers_pd[providers_join_col].astype(str)
                fact_pd = rates_pd.merge(
                    providers_pd,
                    on="__join_provider_key",
                    how="inner",
                    suffixes=("", "_provider"),
                )
                fact_pd = fact_pd.drop(columns=["__join_provider_key"], errors="ignore")
            else:
                LOG.warning(
                    "%sLocal pandas join skipped: rates_join_col=%s providers_join_col=%s providers_empty=%s. Writing rates-only fact.",
                    group_prefix,
                    rates_join_col,
                    providers_join_col,
                    providers_pd.empty,
                )
                fact_pd = rates_pd

            fact_pd = _merge_pandas_on_source_file(fact_pd, structs_pd, suffix="_struct")

            # Deduplicate the entire fact table before the final provider-data enrichment join.
            before_dedup_rows = len(fact_pd)
            fact_pd = fact_pd.drop_duplicates(ignore_index=True)
            after_dedup_rows = len(fact_pd)
            if after_dedup_rows != before_dedup_rows:
                LOG.info(
                    "%sDeduplicated fact table before provider-data join: %d -> %d rows",
                    group_prefix,
                    before_dedup_rows,
                    after_dedup_rows,
                )
            else:
                LOG.info("%sFact table already unique before provider-data join (%d rows)", group_prefix, after_dedup_rows)

            # Write a pre-enrichment fact snapshot before provider-data DB join.
            _write_fact_pandas(
                fact_pd=fact_pd,
                target_dir=fact_before_provider_output_dir,
                output_format=output_format,
                append_output=append_output,
            )

            # Enrich fact table with provider names/locations from DB using NPI IN (...) query.
            if app_config is not None and "npi" in fact_pd.columns:
                fact_pd["npi"] = fact_pd["npi"].astype("string")
                query_npis = [
                    npi for npi in fact_pd["npi"].dropna().astype(str).tolist()
                    if npi and npi.strip()
                ]
                query_npis = list(dict.fromkeys(query_npis))
                if query_npis:
                    try:
                        provider_rows = get_provider_data_by_npis(app_config, query_npis)
                        if provider_rows:
                            provider_cols = [
                                "npi",
                                "provider_name",
                                "practice_location",
                                "provider_org_name",
                                "provider_last_name",
                                "provider_first_name",
                                "provider_middle_name",
                                "practice_addr_line1",
                                "practice_addr_line2",
                                "practice_city",
                                "practice_state",
                                "practice_postal_code",
                                "practice_phone",
                            ]
                            provider_pd = pd.DataFrame(provider_rows, columns=provider_cols)
                            provider_pd = provider_pd.drop_duplicates(subset=["npi"], keep="first")
                            fact_pd = fact_pd.merge(provider_pd, on="npi", how="left")
                        else:
                            LOG.info("%sNo provider-data rows returned for %d NPI(s)", group_prefix, len(query_npis))
                    except Exception as e:
                        LOG.warning("%sProvider-data enrichment skipped due to query error: %s", group_prefix, e)
                else:
                    LOG.info("%sProvider-data enrichment skipped: no non-empty NPIs in fact table", group_prefix)
            else:
                LOG.info(
                    "%sProvider-data enrichment skipped: app_config=%s npi_column_present=%s",
                    group_prefix,
                    app_config is not None,
                    "npi" in fact_pd.columns,
                )

            # Write post-enrichment fact table.
            files_written = _write_fact_pandas(
                fact_pd=fact_pd,
                target_dir=fact_output_dir,
                output_format=output_format,
                append_output=append_output,
            )

            total_rows = len(fact_pd)
    
    LOG.info("%sProcessing complete: %s rows, %d files", group_prefix, format(total_rows, ","), files_written)
    
    return {
        "total_rows": total_rows,
        "files_written": files_written,
        "output_directory": str(output_dir),
        "url_expansion_status": url_expansion_status,
    }
