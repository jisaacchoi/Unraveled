"""Filtering logic for rates DataFrame."""
from __future__ import annotations

import logging
import re
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

LOG = logging.getLogger("src.convert.filtering")


def filter_rates_dataframe(
    spark: SparkSession,
    rates_for_csv: DataFrame,
    provider_npi_set: Optional[set[str]] = None,
    filters: Optional[list[tuple[str, any]]] = None,
) -> DataFrame:
    """
    Apply filters to rates DataFrame: NPI and config-based filters.
    
    Args:
        spark: SparkSession
        rates_for_csv: DataFrame to filter
        provider_npi_set: Optional set of valid NPI strings
        filters: Optional list of (column_name, filter_value) tuples from config
    
    Returns:
        Filtered DataFrame
    """
    df = rates_for_csv
    
    # Log what filters we received
    LOG.info(f"Filter config received: provider_npi_set={len(provider_npi_set) if provider_npi_set else 0} NPIs, filters={filters}")
    
    # 1. Filter by NPI
    if provider_npi_set and len(provider_npi_set) > 0:
        if "npi" in df.columns:
            # Use UDF with closure (serverless-compatible, no broadcast needed)
            # The set is captured in the closure and serialized with the UDF
            npi_set = provider_npi_set  # Capture in closure
            def npi_in_set(npi_val):
                if npi_val is None:
                    return False
                return str(npi_val) in npi_set
            npi_filter_udf = F.udf(npi_in_set, T.BooleanType())
            df = df.filter(npi_filter_udf(F.col("npi")))
            LOG.info("Applied NPI filter (lazy)")
        else:
            LOG.warning("'npi' column not found, skipping NPI filter")
    
    # 2. Apply config-based filters
    if filters:
        LOG.info(f"Applying {len(filters)} config-based filter(s)...")
        for column_name, filter_value in filters:
            if column_name not in df.columns:
                LOG.warning(f"Column '{column_name}' not found in DataFrame, skipping filter")
                continue
            
            LOG.info(f"Applying filter: column='{column_name}', value={filter_value}")
            
            if isinstance(filter_value, str):
                # String filter: check if column contains the value (case-insensitive)
                df = df.filter(
                    F.lower(F.col(column_name)).contains(filter_value.lower())
                )
                LOG.info(f"Applied string filter on {column_name} (lazy)")
            
            elif isinstance(filter_value, list):
                # List filter: could be ranges or exact values
                # Check if it looks like ranges (strings with dashes)
                if all(isinstance(item, str) and ('–' in item or '-' in item or '—' in item) for item in filter_value):
                    # It's a list of ranges
                    # Create a UDF to safely convert billing codes to numeric
                    def safe_to_double(value):
                        if value is None:
                            return None
                        try:
                            return float(str(value))
                        except (ValueError, TypeError):
                            return None
                    
                    safe_to_double_udf = F.udf(safe_to_double, T.DoubleType())
                    
                    range_conditions = []
                    for range_str in filter_value:
                        match = re.match(r'^(.+?)[–—\-](.+)$', range_str.strip())
                        if match:
                            start = match.group(1).strip()
                            end = match.group(2).strip()
                            try:
                                start_num = float(start)
                                end_num = float(end)
                                # Use UDF to safely convert values to numeric
                                col_numeric = safe_to_double_udf(F.col(column_name))
                                range_conditions.append(
                                    col_numeric.isNotNull() &
                                    col_numeric.between(start_num, end_num)
                                )
                            except (ValueError, TypeError):
                                # If start/end are not numeric, use string comparison
                                range_conditions.append(
                                    (F.col(column_name) >= start) & (F.col(column_name) <= end)
                                )
                    
                    if range_conditions:
                        combined_range_condition = range_conditions[0]
                        for cond in range_conditions[1:]:
                            combined_range_condition = combined_range_condition | cond
                        df = df.filter(combined_range_condition)
                        LOG.info(f"Applied range filter on {column_name} (lazy)")
                else:
                    # It's a list of exact values
                    df = df.filter(F.col(column_name).isin(filter_value))
                    LOG.info(f"Applied list filter on {column_name} (lazy)")
            
            else:
                # Single value filter (exact match)
                df = df.filter(F.col(column_name) == filter_value)
                LOG.info(f"Applied exact match filter on {column_name} (lazy)")
    
    LOG.info("All filters applied (lazy - will execute on write)")
    return df
