"""Processing logic for in-network rates extraction and joining."""
from __future__ import annotations

import logging
import traceback
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from src.schema_utils import process_struct_level

LOG = logging.getLogger("src.rates_processing")


def process_in_network_rates(
    spark: SparkSession,
    raw: DataFrame,
    has_in_network: bool,
    explode_service_codes: bool = False,
    array_column: str = "in_network",
    preserve_null_arrays: bool = True,
) -> Optional[DataFrame]:
    """
    Process in-network negotiated rates from raw DataFrame.
    
    Returns:
        DataFrame with flattened rates, or None if target array field doesn't exist
    """
    if not has_in_network:
        LOG.info("No %s field found. Skipping rates processing.", array_column)
        return None
    
    LOG.info("Processing negotiated rates from top-level array '%s'...", array_column)
    
    try:
        base_cols = ["source_file"]
        
        explode_array = F.explode_outer if preserve_null_arrays else F.explode

        # Step 1: Explode target top-level array
        df = raw.select(
            *base_cols,
            explode_array(F.col(array_column)).alias("in_network_item")
        )
        
        # Step 2: Process in_network_item level - extract scalars, identify arrays
        scalar_cols, array_fields = process_struct_level(df, "in_network_item", base_cols)
        LOG.info(f"in_network_item: {len(scalar_cols)} scalars, {len(array_fields)} arrays")
        
        # Select base + scalars, keep arrays for explosion
        select_cols = [F.col(col) for col in base_cols] + scalar_cols
        for array_name, array_path, array_alias, _ in array_fields:
            select_cols.append(F.col(array_path).alias(array_alias))
        
        df = df.select(*select_cols)
        
        # Step 3: Explode arrays found at this level.
        # Keep provider reference arrays optional (explode_outer) so empty provider refs
        # do not erase otherwise valid negotiated price rows.
        for array_name, array_path, array_alias, element_type in array_fields:
            LOG.info(f"Exploding {array_path} (alias: {array_alias})...")
            
            # Get current columns (excluding the array we're about to explode)
            current_cols = [F.col(col) for col in df.columns if col != array_alias]
            
            # Explode the array.
            # Provider reference arrays are optional in many files, so preserve rows
            # when they are null/empty.
            explode_expr_fn = F.explode_outer if "provider_reference" in array_name.lower() else explode_array
            explode_expr = explode_expr_fn(F.col(array_alias)).alias(array_alias)
            
            df = df.select(
                *current_cols,
                explode_expr
            )
            
            # If array element is a struct, extract its scalars
            if isinstance(element_type, T.StructType):
                current_col_names = [col for col in df.columns if col != array_alias]
                nested_scalars, nested_arrays = process_struct_level(df, array_alias, current_col_names)
                LOG.info(f"  {array_alias}: {len(nested_scalars)} scalars, {len(nested_arrays)} arrays")
                
                # Select current columns + extracted scalars + remaining arrays
                new_cols = current_cols + nested_scalars
                for nested_array_name, nested_array_path, nested_array_alias, _ in nested_arrays:
                    new_cols.append(F.col(nested_array_path).alias(nested_array_alias))
                
                # Exclude the struct column itself
                df = df.select(*new_cols)
                
                # Process nested arrays if any
                for nested_array_name, nested_array_path, nested_array_alias, nested_element_type in nested_arrays:
                    LOG.info(f"  Exploding nested {nested_array_path} (alias: {nested_array_alias})...")
                    
                    nested_current_cols = [F.col(col) for col in df.columns if col != nested_array_alias]
                    
                    nested_explode_expr_fn = (
                        F.explode_outer if "provider_reference" in nested_array_name.lower() else explode_array
                    )
                    nested_explode_expr = nested_explode_expr_fn(F.col(nested_array_alias)).alias(nested_array_alias)
                    
                    df = df.select(
                        *nested_current_cols,
                        nested_explode_expr
                    )
                    
                    # Extract scalars from nested array element if it's a struct
                    if isinstance(nested_element_type, T.StructType):
                        nested_col_names = [col for col in df.columns if col != nested_array_alias]
                        nested_scalars2, nested_arrays2 = process_struct_level(df, nested_array_alias, nested_col_names)
                        LOG.info(f"    {nested_array_alias}: {len(nested_scalars2)} scalars, {len(nested_arrays2)} arrays")
                        
                        # Keep both scalars and arrays (like service_code)
                        select_cols2 = nested_current_cols + nested_scalars2
                        for arr_name, arr_path, arr_alias, _ in nested_arrays2:
                            select_cols2.append(F.col(arr_path).alias(arr_alias))
                        df = df.select(*select_cols2)
        
        # Final DataFrame has all scalars extracted
        rates = df
        
        # Optional: explode service_code array to one per row
        if explode_service_codes:
            service_code_col = None
            for col in rates.columns:
                if "service_code" in col:
                    service_code_col = col
                    break
            
            if service_code_col:
                is_array = False
                try:
                    for field in rates.schema.fields:
                        if field.name == service_code_col:
                            if isinstance(field.dataType, T.ArrayType):
                                is_array = True
                            break
                except:
                    pass
                
                if is_array:
                    LOG.info(f"Exploding {service_code_col}...")
                    rates = rates.withColumn(service_code_col, F.explode_outer(service_code_col))
        
        LOG.info("Rates processing complete for array '%s'.", array_column)
        return rates
        
    except Exception as e:
        LOG.error(f"Error processing rates: {e}")
        traceback.print_exc()
        return None


def join_rates_with_providers(
    rates: Optional[DataFrame],
    provider_flat: Optional[DataFrame],
) -> Optional[DataFrame]:
    """
    Join rates with providers on provider_group_id.
    
    Returns:
        DataFrame with all columns from rates + npi from providers, or None if rates is None
    """
    if rates is None:
        LOG.info("No rates dataframe to join. Skipping join step.")
        return None
    elif provider_flat is None:
        LOG.info("No providers dataframe available. Skipping join step.")
        return rates
    else:
        candidate_keys = ["provider_group_id", "provider_reference", "provider_references"]

        def pick_join_col(columns: list[str]) -> Optional[str]:
            lower_to_original = {c.lower(): c for c in columns}

            # Prefer exact column-name matches first.
            for key in candidate_keys:
                if key in lower_to_original:
                    return lower_to_original[key]

            # Fallback to nested/aliased names that end with or contain the candidate key.
            for key in candidate_keys:
                suffix = f"|{key}"
                for col_name in columns:
                    c = col_name.lower()
                    if c.endswith(suffix) or suffix in c or key in c:
                        return col_name
            return None

        rates_join_col = pick_join_col(rates.columns)
        provider_join_col = pick_join_col(provider_flat.columns)

        if rates_join_col is None:
            LOG.info("No join key found in rates. Candidate keys: %s. Skipping join step.", candidate_keys)
            return rates
        if provider_join_col is None:
            LOG.info("No join key found in provider_flat. Candidate keys: %s. Skipping join step.", candidate_keys)
            return rates

        LOG.info(
            "Joining rates with providers using rates column '%s' and provider column '%s'.",
            rates_join_col,
            provider_join_col,
        )

        rates_for_join = rates.withColumn("__join_provider_key", F.col(rates_join_col).cast("string"))
        provider_for_join = provider_flat.withColumn("__join_provider_key", F.col(provider_join_col).cast("string"))

        provider_cols = ["__join_provider_key", "npi"]
        if "provider_url_file" in provider_flat.columns:
            provider_cols.append("provider_url_file")

        LOG.info("Provider columns selected for join payload: %s", provider_cols)
        LOG.info("rates columns: %s", rates.columns)
        LOG.info("provider columns: %s", provider_flat.columns)

        rates_with_providers = rates_for_join.join(
            provider_for_join.select(*provider_cols),
            on="__join_provider_key",
            how="left",
        ).drop("__join_provider_key")

        LOG.info("Join complete (lazy - will execute on write).")
        return rates_with_providers
