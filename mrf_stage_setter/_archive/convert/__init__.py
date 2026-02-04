"""Convert module for JSON.gz to Parquet/CSV conversion."""
from src.convert.processor import process_json_gz_to_csv

__all__ = [
    "process_json_gz_to_csv",
]
