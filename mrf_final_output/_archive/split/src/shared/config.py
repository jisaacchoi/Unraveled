"""Helpers for loading YAML configs and configuring logging."""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def _substitute_variables(value: Any, variables: Dict[str, str]) -> Any:
    """
    Recursively substitute variables in string values.
    
    Variables are in the format ${variable_name} and are replaced with values from the variables dict.
    """
    if isinstance(value, str):
        # Replace ${variable_name} with values from variables dict
        for var_name, var_value in variables.items():
            value = value.replace(f"${{{var_name}}}", str(var_value))
        return value
    elif isinstance(value, dict):
        return {k: _substitute_variables(v, variables) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_variables(item, variables) for item in value]
    else:
        return value


def load_config(path: Path) -> Dict[str, Any]:
    """
    Load the YAML config file from disk and substitute variables.
    
    Variables are defined in the 'run_config' section and can be used throughout
    the config using ${variable_name} syntax.
    
    Available variables:
    - ${base_directory}: Base directory for data files (from run_config.base_directory)
    - ${source_name}: Source/payer name (from run_config.source_name)
    - ${run_date}: Run date (from run_config.run_date)
    """
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    
    # Build variables dictionary from run_config and application sections
    variables = {}
    
    # Get base_directory, run_date, and source_name from run_config
    run_config = config.get("run_config", {})
    if "base_directory" in run_config:
        variables["base_directory"] = run_config["base_directory"]
    if "run_date" in run_config:
        variables["run_date"] = run_config["run_date"]
    if "source_name" in run_config:
        variables["source_name"] = run_config["source_name"]
    
    # Backward compatibility: check application section and top-level payer if not in run_config
    if "source_name" not in variables:
        app_config = config.get("application", {})
        if "source_name" in app_config:
            variables["source_name"] = app_config["source_name"]
        elif "payer" in config:
            variables["source_name"] = config["payer"]
    
    # Substitute variables throughout the config
    if variables:
        config = _substitute_variables(config, variables)
    
    return config


def get_log_file_path(config: Dict[str, Any], command_name: str) -> Optional[Path]:
    """
    Get the log file path for a command based on configuration.
    
    Args:
        config: Configuration dictionary
        command_name: Name of the command (e.g., "download", "ingest", etc.)
        
    Returns:
        Path to log file if file logging is enabled, None otherwise
    """
    logger_cfg = config.get("logging", config.get("logger", {}))
    file_logging_cfg = logger_cfg.get("file_logging", {})
    
    # Check if file logging is enabled
    if not file_logging_cfg.get("enabled", True):
        return None
    
    # Get log directory (default: "logs")
    log_directory = Path(file_logging_cfg.get("log_directory", "logs"))
    
    # Get log file name from config, or use default
    log_files = file_logging_cfg.get("log_files", {})
    log_filename = log_files.get(command_name)
    
    # Default log file names if not specified in config
    if not log_filename:
        default_names = {
            "download": "00_download.log",
            "ingest": "01_ingest.log",
            "detect_group_generate_schemas": "02_detect_group_generate_schemas.log",  # Deprecated, kept for backward compatibility
            "analyze": "03_analyze.log",
            "group_schemas": "04_group_schemas.log",
            "convert_json_gz_to_parquet": "03_convert_json_gz_to_parquet.log",
            "jsongz_to_parquet_databricks": "05_jsongz_to_parquet_databricks.log",
        }
        log_filename = default_names.get(command_name, f"{command_name}.log")
    
    return log_directory / log_filename


def configure_logging(config: Dict[str, Any], log_file: Optional[Path] = None) -> None:
    """
    Configure logging using the logging block from the YAML config.
    
    Args:
        config: Configuration dictionary
        log_file: Optional path to log file. If provided, logs will be written to file.
                 If None, logs will be written to console (default).
    """
    logger_cfg = config.get("logging", config.get("logger", {}))  # Support both old and new key names
    level_name = logger_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    format_str = logger_cfg.get("format", "%(asctime)s %(levelname)s %(name)s - %(message)s")
    datefmt = logger_cfg.get("datefmt")
    
    # Clear any existing handlers
    logging.root.handlers = []
    
    if log_file:
        # File logging: write to file only
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(format_str, datefmt=datefmt))
        logging.root.addHandler(file_handler)
        logging.root.setLevel(level)
    else:
        # Console logging: write to console only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(logging.Formatter(format_str, datefmt=datefmt))
        logging.root.addHandler(console_handler)
        logging.root.setLevel(level)

