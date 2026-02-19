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
            # Use native Spark expression instead of Python UDF to avoid Python worker crashes.
            npi_values = [str(npi) for npi in provider_npi_set]
            df = df.filter(F.col("npi").cast("string").isin(npi_values))
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
                    # Use native cast to double for numeric comparisons; invalid values become null.
                    col_str = F.trim(F.col(column_name).cast("string"))
                    col_numeric = col_str.cast(T.DoubleType())

                    range_conditions = []
                    for range_str in filter_value:
                        match = re.match(r'^(.+?)[–—\-](.+)$', range_str.strip())
                        if match:
                            start = match.group(1).strip()
                            end = match.group(2).strip()
                            try:
                                start_num = float(start)
                                end_num = float(end)
                                range_conditions.append(
                                    col_numeric.isNotNull() &
                                    col_numeric.between(start_num, end_num)
                                )
                            except (ValueError, TypeError):
                                # If start/end are not numeric, use string comparison
                                range_conditions.append(
                                    (col_str >= start) & (col_str <= end)
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
