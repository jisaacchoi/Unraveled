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
) -> Optional[DataFrame]:
    """
    Process in-network negotiated rates from raw DataFrame.
    
    Returns:
        DataFrame with flattened rates, or None if in_network field doesn't exist
    """
    if not has_in_network:
        LOG.info("No in_network field found. Skipping rates processing.")
        return None
    
    LOG.info("Processing in-network negotiated rates...")
    
    try:
        base_cols = ["source_file"]
        
        # Step 1: Explode in_network array and limit to first 10k rows
        df = raw.select(
            *base_cols,
            F.explode_outer("in_network").alias("in_network_item")
        )
        
        # Step 2: Process in_network_item level - extract scalars, identify arrays
        scalar_cols, array_fields = process_struct_level(df, "in_network_item", base_cols)
        LOG.info(f"in_network_item: {len(scalar_cols)} scalars, {len(array_fields)} arrays")
        
        # Select base + scalars, keep arrays for explosion
        select_cols = [F.col(col) for col in base_cols] + scalar_cols
        for array_name, array_path, array_alias, _ in array_fields:
            select_cols.append(F.col(array_path).alias(array_alias))
        
        df = df.select(*select_cols)
        
        # Step 3: Explode arrays in order (covered_services first, then negotiated_rates)
        for array_name, array_path, array_alias, element_type in array_fields:
            LOG.info(f"Exploding {array_path} (alias: {array_alias})...")
            
            # Get current columns (excluding the array we're about to explode)
            current_cols = [F.col(col) for col in df.columns if col != array_alias]
            
            # Explode the array
            explode_expr = F.explode_outer(F.col(array_alias)).alias(array_alias)
            
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
                    
                    nested_explode_expr = F.explode_outer(F.col(nested_array_alias)).alias(nested_array_alias)
                    
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
        
        LOG.info("In-network rates processing complete.")
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
        # Find column in rates that contains provider_group_id
        provider_group_id_col = None
        for col in rates.columns:
            if "provider_group_id" in col:
                provider_group_id_col = col
                break
        
        if provider_group_id_col is None:
            # Also check for provider_reference pattern (might be an array)
            for col in rates.columns:
                if "provider_reference" in col.lower():
                    provider_group_id_col = col
                    break
        
        if provider_group_id_col is None:
            LOG.info("No provider_group_id or provider_reference column found in rates. Skipping join step.")
            return rates
        else:
            LOG.info(f"Found provider column: {provider_group_id_col}")
            LOG.info("Joining rates with providers on provider_group_id...")
            
            # Check if it's an array (needs to be exploded) or already a scalar
            is_array = False
            try:
                for field in rates.schema.fields:
                    if field.name == provider_group_id_col:
                        if isinstance(field.dataType, T.ArrayType):
                            is_array = True
                        break
            except:
                pass
            
            if is_array:
                # Explode the array to get individual provider_group_id values, cast to string
                rate_cols = [col for col in rates.columns if col != provider_group_id_col]
                rates_exploded = rates.select(
                    *rate_cols,
                    F.explode_outer(provider_group_id_col).cast("string").alias("provider_group_id")
                )
            else:
                # Already a scalar, just rename if needed, cast to string
                rate_cols = [col for col in rates.columns if col != provider_group_id_col]
                if provider_group_id_col != "provider_group_id":
                    rates_exploded = rates.select(
                        *rate_cols,
                        F.col(provider_group_id_col).cast("string").alias("provider_group_id")
                    )
                else:
                    # Cast existing column to string
                    rates_exploded = rates.withColumn("provider_group_id", F.col("provider_group_id").cast("string"))
            
            # Join with providers on provider_group_id
            # Include provider_url_file if it exists (for URL-based provider references)
            provider_cols = ["provider_group_id", "npi"]
            if "provider_url_file" in provider_flat.columns:
                provider_cols.append("provider_url_file")
            
            LOG.info(f"Joining on provider_group_id. Provider columns to select: {provider_cols}")
            LOG.info(f"rates_exploded columns: {rates_exploded.columns}")
            LOG.info(f"provider_flat columns: {provider_flat.columns}")
            
            rates_with_providers = rates_exploded.join(
                provider_flat.select(*provider_cols),
                on="provider_group_id",
                how="left"
            )
            
            LOG.info("Join complete (lazy - will execute on write).")
            return rates_with_providers
