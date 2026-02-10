#!/usr/bin/env python3
"""
Analyze: Comprehensive analysis of JSON structures from mrf_landing.

This enhanced version processes ALL items for each record_type to build
comprehensive structure, prioritizing items with rare keys but including
common items for completeness.

Usage:
    python commands/03_analyze_comprehensive.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string
from src.detect_shapes.comprehensive_structure_analyzer import run_comprehensive_shape_analysis
from src.detect_shapes.structure_analyzer import create_mrf_analysis_table

LOG = logging.getLogger("commands.analyze_comprehensive")


def main() -> int:
    """Entrypoint for the comprehensive analyze command."""
    parser = argparse.ArgumentParser(
        description="Comprehensively analyze JSON structures from mrf_landing (processes all items)"
    )
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config file
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "analyze")
    configure_logging(config, log_file=log_file)
    
    # Get database connection
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("Config missing 'database' section")
        return 1
    
    connection_string = build_connection_string(db_config)
    
    # Get detect_shapes configuration
    pipeline_config = config.get("pipeline", {})
    detect_shapes_cfg = pipeline_config.get("shape_detection", config.get("detect_shapes", {}))
    drop_table_if_exists = detect_shapes_cfg.get("drop_table_if_exists", False)
    batch_size = detect_shapes_cfg.get("insert_batch_size", detect_shapes_cfg.get("batch_size", 100))
    fetch_batch_size = detect_shapes_cfg.get("fetch_batch_size", 10000)
    max_list_items = detect_shapes_cfg.get("max_list_items", 10)
    max_url_downloads = detect_shapes_cfg.get("max_url_downloads", None)
    url_content_download_path = detect_shapes_cfg.get("url_content_download_path")
    
    # Read input_directory from pipeline level, fallback to step level
    input_directory_cfg = pipeline_config.get("input_directory") or detect_shapes_cfg.get("input_directory")
    
    # Convert download path to Path object if provided
    download_path = None
    if url_content_download_path:
        download_path = Path(url_content_download_path)
    
    # Convert input directory to Path object if provided
    input_directory = None
    if input_directory_cfg:
        input_directory = Path(input_directory_cfg)
    
    LOG.info("Using config from %s", args.config)
    LOG.info("Drop table if exists: %s", drop_table_if_exists)
    LOG.info("Batch size: %d, Fetch batch size: %d", batch_size, fetch_batch_size)
    if input_directory:
        LOG.info("Input directory: %s (processing files one at a time for better performance)", input_directory)
    else:
        LOG.error("Input directory is required for comprehensive analysis")
        return 1
    
    try:
        # Ensure mrf_analysis table exists
        create_mrf_analysis_table(connection_string, drop_if_exists=drop_table_if_exists)
        
        # Get analyzed directory for file movement
        analyzed_directory = None
        analyzed_directory_cfg = detect_shapes_cfg.get("analyzed_directory")
        if analyzed_directory_cfg:
            analyzed_directory = Path(analyzed_directory_cfg)
            analyzed_directory.mkdir(parents=True, exist_ok=True)
        
        # Run comprehensive shape analysis
        LOG.info("=== Running comprehensive shape analysis ===")
        LOG.info("This will process ALL items for each record_type to build comprehensive structure")
        analysis_result = run_comprehensive_shape_analysis(
            connection_string=connection_string,
            batch_size=batch_size,
            fetch_batch_size=fetch_batch_size,
            download_path=download_path,
            input_directory=input_directory,
            analyzed_directory=analyzed_directory,
            max_list_items=max_list_items,
            max_url_downloads=max_url_downloads,
        )
        
        if analysis_result[0] < 0:  # Error occurred
            LOG.error("Comprehensive shape analysis failed")
            return 1
        
        total_records, url_records, processed_files = analysis_result
        LOG.info("Comprehensive shape analysis complete: %d records inserted, %d URL records, %d files analyzed",
                 total_records, url_records, len(processed_files))
        
        # Files are renamed with _analyzed_ prefix during analysis
        if processed_files:
            LOG.info("Files were renamed with _analyzed_ prefix during analysis")
        
        return 0
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error running comprehensive shape detection: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
