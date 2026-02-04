"""Output writers for CSV and Parquet formats."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, functions as F, types as T

LOG = logging.getLogger("src.convert.output_writers")


def convert_to_csv_compatible(df: DataFrame) -> DataFrame:
    """
    Convert array and struct columns to JSON strings for CSV compatibility.
    
    Returns:
        DataFrame with arrays and structs converted to JSON strings
    """
    csv_cols = []
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    
    for col in df.columns:
        col_type = schema_dict.get(col)
        
        # Convert arrays and structs to JSON strings
        if isinstance(col_type, T.ArrayType) or isinstance(col_type, T.StructType):
            csv_cols.append(F.to_json(F.col(col)).alias(col))
        else:
            csv_cols.append(col)
    
    return df.select(*csv_cols)


def write_rates_to_csv(
    rates_for_csv: DataFrame,
    output_path: Path,
    sample_size: Optional[int] = None,
    seed: int = 42,
    chunk_size: int = 100000,
) -> int:
    """
    Write rates DataFrame to CSV file using Spark's native CSV writer.
    
    Args:
        rates_for_csv: DataFrame to write
        output_path: Path to output CSV file
        sample_size: Optional number of rows to sample (for testing)
        seed: Random seed for sampling
        chunk_size: Not used - kept for API compatibility
    
    Returns:
        Number of rows written
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if sample_size:
        LOG.info(f"Limiting to {sample_size:,} rows for output...")
        rates_sample = rates_for_csv.limit(sample_size)
    else:
        rates_sample = rates_for_csv
    
    # Log DataFrame info before conversion
    LOG.info(f"DataFrame schema: {len(rates_sample.columns)} columns")
    LOG.info(f"Columns: {rates_sample.columns}")
    
    # Conversion to pandas dataframe
    LOG.info("Converting to pandas DataFrame...")
    rates_pandas = rates_sample.toPandas()
    
    total_rows = len(rates_pandas)
    LOG.info(f"Converted to pandas. Shape: {rates_pandas.shape}")
    LOG.info(f"Writing to CSV in chunks of {chunk_size:,} rows to separate files")
    
    # Write in chunks to separate files
    total_written = 0
    file_num = 0
    base_name = output_path.stem
    suffix = output_path.suffix
    
    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        chunk = rates_pandas.iloc[start_idx:end_idx]
        
        # Create numbered filename: rates_001.csv, rates_002.csv, etc.
        chunk_path = output_path.parent / f"{base_name}_{file_num:03d}{suffix}"
        
        chunk.to_csv(
            chunk_path,
            index=False,
            header=True,
            mode='w'
        )
        
        total_written += len(chunk)
        file_num += 1
        LOG.info(f"Written {chunk_path.name}: {len(chunk):,} rows ({total_written:,} / {total_rows:,} total)")
    
    LOG.info(f"In-network rates written successfully: {file_num} files, {total_written:,} total rows")
    
    return total_written
