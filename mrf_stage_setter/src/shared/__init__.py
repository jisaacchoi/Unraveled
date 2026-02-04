"""Shared utility modules used across multiple commands."""
from src.shared.config import (
    configure_logging,
    get_log_file_path,
    load_config,
)
from src.shared.database import (
    build_connection_string,
    ensure_table_exists,
)
from src.shared.file_mover import move_files_to_analyzed
from src.shared.json_reader import is_json_file, open_json_file
from src.shared.path_helper import ensure_directories_from_config
from src.shared.spark_session import create_spark_session
from src.shared.url_content_downloader import (
    detect_url_column,
    download_url_content,
    extract_unique_urls_with_row_ids,
    join_url_content_to_dataframe,
)

__all__ = [
    "configure_logging",
    "get_log_file_path",
    "load_config",
    "build_connection_string",
    "ensure_table_exists",
    "move_files_to_analyzed",
    "is_json_file",
    "open_json_file",
    "ensure_directories_from_config",
    "create_spark_session",
    "detect_url_column",
    "download_url_content",
    "extract_unique_urls_with_row_ids",
    "join_url_content_to_dataframe",
]
