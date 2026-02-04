"""Helper functions for creating and managing directory paths from config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

LOG = logging.getLogger("src.path_helper")


def ensure_directories_from_config(config: Dict[str, Any]) -> list[Path]:
    """
    Create all necessary directories specified in the config file.
    
    This function extracts all directory paths from the pipeline configuration
    and creates them if they don't exist. Directories are created recursively.
    
    Args:
        config: Configuration dictionary loaded from YAML
        
    Returns:
        List of Path objects for all directories that were created or already existed
    """
    directories: list[Path] = []
    pipeline_config = config.get("pipeline", {})
    
    # Stage 1: Download
    download_cfg = pipeline_config.get("download", {})
    if download_cfg:
        output_dir = download_cfg.get("output_directory")
        if output_dir:
            directories.append(Path(output_dir))
    
    # Pipeline-level directories (shared by steps 2, 3, 4)
    pipeline_input_dir = pipeline_config.get("input_directory")
    if pipeline_input_dir:
        directories.append(Path(pipeline_input_dir))
    pipeline_output_dir = pipeline_config.get("output_directory")
    if pipeline_output_dir:
        directories.append(Path(pipeline_output_dir))
    
    # Stage 2: Ingest (backward compatibility - check step level if pipeline level not found)
    ingest_cfg = pipeline_config.get("ingest", {})
    if ingest_cfg:
        input_dir = ingest_cfg.get("input_directory")
        if input_dir and not pipeline_input_dir:  # Only add if not already added from pipeline level
            directories.append(Path(input_dir))
        output_dir = ingest_cfg.get("output_directory")
        if output_dir and not pipeline_output_dir:  # Only add if not already added from pipeline level
            directories.append(Path(output_dir))
    
    # Stage 3: Shape detection (backward compatibility - check step level if pipeline level not found)
    shape_detection_cfg = pipeline_config.get("shape_detection", {})
    if shape_detection_cfg:
        input_dir = shape_detection_cfg.get("input_directory")
        if input_dir and not pipeline_input_dir:  # Only add if not already added from pipeline level
            directories.append(Path(input_dir))
        analyzed_dir = shape_detection_cfg.get("analyzed_directory")
        if analyzed_dir:
            directories.append(Path(analyzed_dir))
    
    # Stage 4: Shape grouping (backward compatibility - check step level if pipeline level not found)
    shape_grouping_cfg = pipeline_config.get("shape_grouping", {})
    if shape_grouping_cfg:
        input_dir = shape_grouping_cfg.get("input_directory")
        if input_dir and not pipeline_input_dir:  # Only add if not already added from pipeline level
            directories.append(Path(input_dir))
        output_dir = shape_grouping_cfg.get("output_directory")
        if output_dir and not pipeline_output_dir:  # Only add if not already added from pipeline level
            directories.append(Path(output_dir))
    
    # Stage 4: Schema generation
    schema_gen_cfg = pipeline_config.get("schema_generation", {})
    if schema_gen_cfg:
        input_dir = schema_gen_cfg.get("input_directory")
        if input_dir:
            directories.append(Path(input_dir))
        output_dir = schema_gen_cfg.get("output_directory")
        if output_dir:
            directories.append(Path(output_dir))
        analyzed_dir = schema_gen_cfg.get("analyzed_directory")
        if analyzed_dir:
            directories.append(Path(analyzed_dir))
    
    # Stage 5: JSON.gz to Parquet conversion
    json_gz_cfg = pipeline_config.get("json_gz_to_parquet", {})
    if json_gz_cfg:
        input_dir = json_gz_cfg.get("input_directory")
        if input_dir:
            directories.append(Path(input_dir))
        output_dir = json_gz_cfg.get("output_directory")
        if output_dir:
            directories.append(Path(output_dir))
        schema_dir = json_gz_cfg.get("schema_directory")
        if schema_dir:
            directories.append(Path(schema_dir))
        download_dir = json_gz_cfg.get("download_directory")
        if download_dir:
            directories.append(Path(download_dir))
    
    # Remove duplicates while preserving order
    seen = set()
    unique_directories = []
    for dir_path in directories:
        if dir_path not in seen:
            seen.add(dir_path)
            unique_directories.append(dir_path)
    
    # Create logs directory from config (if file logging is enabled)
    logger_cfg = config.get("logging", {})
    file_logging_cfg = logger_cfg.get("file_logging", {})
    if file_logging_cfg.get("enabled", True):
        log_directory = Path(file_logging_cfg.get("log_directory", "logs"))
        log_directory.mkdir(parents=True, exist_ok=True)
    
    # Create directories
    created_count = 0
    existing_count = 0
    
    for dir_path in unique_directories:
        try:
            # Check if directory already exists before creating
            if dir_path.exists() and dir_path.is_dir():
                existing_count += 1
                LOG.debug("Directory already exists: %s", dir_path)
            else:
                dir_path.mkdir(parents=True, exist_ok=True)
                created_count += 1
                LOG.debug("Created directory: %s", dir_path)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed to create directory %s: %s", dir_path, exc)
    
    if created_count > 0 or existing_count > 0:
        LOG.info("Ensured %d directory(ies) exist (%d created, %d already existed)", 
                len(unique_directories), created_count, existing_count)
    
    return unique_directories

