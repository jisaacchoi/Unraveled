#!/usr/bin/env python3
"""
Download MRF payloads defined in config/config.yaml.

Usage:
    python commands/01_download.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string
from src.download.mrf_downloader import run_download

LOG = logging.getLogger("commands.download")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(description="Download payer MRF payloads")
    parser.add_argument("--config", default=Path("config.yaml"), type=Path, help="Config file path")
    parser.add_argument("--file-pattern", type=str, help="Download only files matching this pattern (substring match in filename or URL)")
    parser.add_argument("--file-url", type=str, help="Download only the file with this exact URL")
    return parser


def main() -> int:
    """Entrypoint for the download command."""
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "download")
    configure_logging(config, log_file=log_file)
    
    # Parse config values
    run_config = config.get("run_config", {})
    payer = run_config.get("source_name") or config.get("application", {}).get("source_name") or config.get("payer")
    if not payer:
        raise ValueError("Config missing 'run_config.source_name' (or 'application.source_name' or 'payer' for backward compatibility)")
    
    pipeline_config = config.get("pipeline", {})
    download_cfg = pipeline_config.get("download", config.get("download", {}))
    output_root = Path(download_cfg.get("output_directory", download_cfg.get("output_root", "data/mrf/raw")))
    threads = int(download_cfg.get("num_threads", download_cfg.get("threads", os.cpu_count() or 4)))
    chunk_size = int(download_cfg.get("chunk_size_bytes", download_cfg.get("chunk_size", 16 * 1024)))
    max_file_size_gb = download_cfg.get("max_file_size_gb")
    if max_file_size_gb is not None:
        max_file_size_gb = float(max_file_size_gb)
    max_total_size_gb = download_cfg.get("max_total_size_gb")
    if max_total_size_gb is not None:
        max_total_size_gb = float(max_total_size_gb)
    folder_name = download_cfg.get("subfolder_name", download_cfg.get("folder_name"))
    if not folder_name:
        raise ValueError("Config download section must define 'folder_name'")
    
    # Get index sources
    data_source = config.get("data_source", config.get("inputs", {}))
    index_url = data_source.get("index_url")
    index_path = data_source.get("index_path")
    
    # Get database connection string
    db_config = config.get("database", {})
    connection_string: str | None = None
    try:
        connection_string = build_connection_string(db_config)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Could not build database connection string, mrf_index tracking will be disabled: %s", exc)
        connection_string = None
    
    LOG.info("Starting download for config %s", args.config)
    
    # Get file filter options
    file_pattern = getattr(args, 'file_pattern', None)
    file_url = getattr(args, 'file_url', None)
    
    if file_pattern and file_url:
        LOG.error("Cannot specify both --file-pattern and --file-url. Use only one.")
        return 1
    
    return run_download(
        payer=payer,
        output_root=output_root,
        folder_name=folder_name,
        index_url=index_url,
        index_path=index_path,
        threads=threads,
        chunk_size=chunk_size,
        max_file_size_gb=max_file_size_gb,
        max_total_size_gb=max_total_size_gb,
        connection_string=connection_string,
        file_pattern=file_pattern,
        file_url=file_url,
    )


if __name__ == "__main__":
    raise SystemExit(main())

