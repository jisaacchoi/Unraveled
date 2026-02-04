"""Dimension table creation for fact/dimension modeling."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

LOG = logging.getLogger("src.convert.dimension_tables")


def create_dimension_tables(
    df: DataFrame,
    enabled: bool = True,
) -> tuple[DataFrame, dict[str, DataFrame]]:
    """
    Create dimension (dictionary) tables from repetitive columns and replace with hash IDs in fact table.
    
    Dimension tables:
    - dim_billing_code_service: description
    
    Args:
        df: DataFrame with fact and dimension columns
        enabled: If False, skip dimension table creation and return df unchanged
        
    Returns:
        Tuple of (fact_df with IDs only, dict of dimension DataFrames)
    """
    fact_df = df
    dim_tables = {}
    
    if not enabled:
        LOG.info("Dimension table creation disabled - returning fact table unchanged")
        return fact_df, dim_tables
    
    # Dimension: dim_billing_code_service (billing_code, billing_code_type, billing_code_type_version, description)
    dim_cols_service = ["description"]
    available_service = [col for col in dim_cols_service if col in fact_df.columns]
    LOG.info(f"Checking for dim_billing_code_service column. Available: {available_service} (out of {dim_cols_service})")
    if available_service:
        LOG.info(f"Creating dim_billing_code_service from columns: {available_service}")
        dim_service = fact_df.select(*available_service).distinct()
        dim_service = dim_service.withColumn(
            "billing_code_service_id",
            (F.abs(F.hash(F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in available_service]))) % (2**31 - 1)).cast("int")
        )
        dim_tables["billing_code_service"] = dim_service.select("billing_code_service_id", *available_service)
        
        fact_df = fact_df.join(
            dim_service.select("billing_code_service_id", *available_service),
            on=available_service,
            how="left"
        ).drop(*available_service)
        LOG.info(f"  Created unique billing_code_service records")
    
    LOG.info(f"Dimension tables created: {list(dim_tables.keys())}")
    LOG.info(f"Fact table columns after normalization: {len(fact_df.columns)} columns")
    LOG.info(f"  Sample columns: {fact_df.columns[:10]}...")
    
    # Log data types for dimension IDs and fact table schema
    LOG.info("Checking data types of dimension IDs in fact table...")
    id_columns = [col for col in fact_df.columns if col.endswith("_id") or col.endswith("_dim")]
    if id_columns:
        for col_name in id_columns:
            for field in fact_df.schema.fields:
                if field.name == col_name:
                    LOG.info(f"  {col_name}: {field.dataType}")
    
    return fact_df, dim_tables
