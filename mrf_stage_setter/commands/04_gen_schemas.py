#!/usr/bin/env python3
"""
Refresh schema groups and generate schema JSON files.

This command:
1. Reads files from input_directory (files with _ingested_analyzed_ prefix)
2. Refreshes the schema_groups table from mrf_analysis data, grouping files by schema signature
3. Moves files from input_directory to output_directory
4. Moves files into output_directory without group hash prefixing
5. Generates schema JSON files for each file using mrf_analysis table (<file>_schema.json)

File Flow:
    input_directory/
        _ingested_analyzed_file1.json.gz
        _ingested_analyzed_file2.json.gz
    ↓ (moved and renamed)
    output_directory/
        _ingested_analyzed_file1.json.gz
        _ingested_analyzed_file2.json.gz

Usage:
    python commands/04_group_split_schemas.py --config config.yaml
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
from src.generate_schemas.schema_orchestrator import run_full_pipeline
from src.generate_schemas.schema_groups_db import get_all_file_names_from_schema_groups
from src.generate_schemas.schema_inference import generate_schema_for_file

LOG = logging.getLogger("commands.gen_schemas")


def _build_empty_marker_name(filename: str) -> str:
    """
    Build an empty marker filename for a file in the output directory.
    Strips processing prefixes before adding _empty_.
    """
    from src.shared.file_prefix import PREFIX_ANALYZED, PREFIX_INGESTED

    name = filename
    combined_prefix = PREFIX_INGESTED + PREFIX_ANALYZED[1:]  # _ingested_analyzed_
    if name.startswith(combined_prefix):
        name = name[len(combined_prefix):]
    elif name.startswith(PREFIX_INGESTED):
        name = name[len(PREFIX_INGESTED):]
    elif name.startswith(PREFIX_ANALYZED):
        name = name[len(PREFIX_ANALYZED):]

    if name.startswith("_empty_"):
        return name
    return f"_empty_{name}"


def _is_empty_marker(filename: str) -> bool:
    if filename.startswith("_empty_"):
        return True
    return False


def main() -> int:
    """Entrypoint for the schema group refresh command."""
    parser = argparse.ArgumentParser(
        description="Refresh schema groups table from mrf_analysis data"
    )
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config file
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    # Use "gen_schemas" to match config.yaml log_files section and run_pipeline.py
    log_file = get_log_file_path(config, "gen_schemas")
    configure_logging(config, log_file=log_file)
    
    # Get database connection
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("Config missing 'database' section")
        return 1
    
    connection_string = build_connection_string(db_config)
    
    # Get detect_shapes configuration (for batch sizes, though we don't use detect)
    pipeline_config = config.get("pipeline", {})
    detect_shapes_cfg = pipeline_config.get("shape_detection", config.get("detect_shapes", {}))
    batch_size = detect_shapes_cfg.get("insert_batch_size", detect_shapes_cfg.get("batch_size", 100))
    fetch_batch_size = detect_shapes_cfg.get("fetch_batch_size", 10000)
    drop_table_if_exists = detect_shapes_cfg.get("drop_table_if_exists", False)
    
    # Get shape_grouping configuration
    shape_grouping_cfg = pipeline_config.get("shape_grouping", {})
    if not shape_grouping_cfg:
        LOG.error("No shape_grouping section found in config.pipeline")
        LOG.error("Available pipeline sections: %s", list(pipeline_config.keys()))
        return 1
    
    # Read input_directory and output_directory from pipeline level, fallback to step level for backward compatibility
    input_directory_cfg = pipeline_config.get("input_directory") or shape_grouping_cfg.get("input_directory")
    output_directory_cfg = pipeline_config.get("output_directory") or shape_grouping_cfg.get("output_directory")
    refresh_schema_groups = shape_grouping_cfg.get("refresh_schema_groups", False)
    overwrite_existing_schemas = shape_grouping_cfg.get("overwrite_existing_schemas", True)
    
    end_delete_after_gen_schemas = shape_grouping_cfg.get("end_delete_after_gen_schemas", False)
    # Schema filename is now based on file name (<file>_schema.json)
    
    if not input_directory_cfg:
        LOG.error("No input_directory in config.pipeline or config.pipeline.shape_grouping")
        return 1
    
    if not output_directory_cfg:
        LOG.error("No output_directory in config.pipeline or config.pipeline.shape_grouping")
        return 1
    
    input_directory = Path(input_directory_cfg)
    output_directory = Path(output_directory_cfg)
    
    if not input_directory.exists():
        LOG.error("Input directory does not exist: %s", input_directory)
        return 1
    
    if not input_directory.is_dir():
        LOG.error("Input path must be a directory: %s", input_directory)
        return 1
    
    LOG.info("Using config from %s", args.config)
    LOG.info("Batch size: %d, Fetch batch size: %d", batch_size, fetch_batch_size)
    LOG.info("Refresh schema groups: %s", refresh_schema_groups)
    LOG.info("End delete after gen_schemas: %s", end_delete_after_gen_schemas)
    LOG.info("Overwrite existing schemas: %s", overwrite_existing_schemas)
    LOG.info("=== Directory Configuration ===")
    LOG.info("Input directory (source): %s", input_directory)
    LOG.info("  - Files will be read from this directory")
    LOG.info("  - Only files with _ingested_analyzed_ prefix will be processed")
    LOG.info("Output directory (destination): %s", output_directory)
    LOG.info("  - Files will be moved from input directory to this directory")
    LOG.info("  - Files are moved to output directory without group hash prefixes")
    
    try:
        # Run group pipeline (detect is disabled, only group is enabled)
        # 
        # Process flow:
        # 1. Read files from input_directory (files with _ingested_analyzed_ prefix)
        # 2. Refresh schema_groups table from mrf_analysis data
        # 3. Move files from input_directory to output_directory root
        # 4. Rename files with group_<hash>__ prefix (hash based on schema_sig) within output_directory
        # 5. Generate schema JSON files for each file
        LOG.info("=== Starting Schema Grouping Pipeline ===")
        LOG.info("Step 1: Reading files from input directory: %s", input_directory)
        LOG.info("Step 2: Refreshing schema_groups table from mrf_analysis data")
        LOG.info("Step 3: Moving files to output directory: %s", output_directory)
        LOG.info("Step 4: Moving files without schema group hash prefixing")
        
        result = run_full_pipeline(
            connection_string=connection_string,
            spark=None,  # Not used
            input_directory=input_directory,  # Source: files read from here
            schema_output_directory=None,  # Not used
            batch_size=batch_size,
            fetch_batch_size=fetch_batch_size,
            drop_table_if_exists=drop_table_if_exists,
            refresh_schema_groups=refresh_schema_groups,
            download_path=None,
            analyzed_directory=None,
            files_output_directory=output_directory,  # Destination: files moved here and prefixed with group_<hash>__
            enable_detect=False,  # Always disabled for this command
            enable_group=True,   # Always enabled for this command
        )
        
        if result == 0:
            LOG.info("Schema groups refreshed successfully.")
        else:
            LOG.error("Schema group refresh completed with errors")
            return result
        
        # Step 2: Generate schema JSON files (always enabled)
        LOG.info("=== Generating schema JSON files from mrf_analysis ===")
        
        if not output_directory.exists():
            LOG.warning("Output directory does not exist, skipping schema generation: %s", output_directory)
        else:
            # Process each file (one schema per file)
            try:
                file_names = get_all_file_names_from_schema_groups(connection_string)
                LOG.info("Found %d file(s) to process for schema generation", len(file_names))

                schemas_created = 0
                from src.generate_schemas.schema_inference import file_name_to_schema_filename
                for file_name in file_names:
                    try:
                        schema_path = output_directory / file_name_to_schema_filename(file_name)
                        if schema_path.exists() and not overwrite_existing_schemas:
                            LOG.info("Schema already exists, skipping: %s", schema_path)
                            continue
                        # Generate schema for each file
                        schema_path = generate_schema_for_file(
                            output_directory=output_directory,
                            file_name=file_name,
                            connection_string=connection_string,
                        )
                        if schema_path:
                            schemas_created += 1
                            LOG.info("Generated schema for file: %s", schema_path)

                            if end_delete_after_gen_schemas:
                                matches = [
                                    p for p in output_directory.glob(f"*{file_name}")
                                    if not _is_empty_marker(p.name)
                                ]
                                source_path = matches[0] if matches else None

                                if source_path and source_path.exists():
                                    if _is_empty_marker(source_path.name):
                                        LOG.debug("File already empty marker, skipping: %s", source_path.name)
                                        continue
                                    marker_name = _build_empty_marker_name(source_path.name)
                                    marker_path = source_path.with_name(marker_name)
                                    try:
                                        source_path.unlink()
                                        marker_path.touch(exist_ok=True)
                                        LOG.info("Emptied file and created marker: %s", marker_path.name)
                                    except Exception as exc:  # noqa: BLE001
                                        LOG.warning("Failed to create empty marker for %s: %s", source_path, exc)
                                else:
                                    LOG.warning("Could not locate file in output directory to empty: %s", file_name)
                    except Exception as exc:  # noqa: BLE001
                        LOG.exception("Error generating schema for file: %s", exc)
                        continue
                
                LOG.info("Schema generation complete: %d schema file(s) created", schemas_created)
            except Exception as exc:  # noqa: BLE001
                LOG.exception("Error generating schemas: %s", exc)
                LOG.warning("Schema generation failed, but continuing...")
        
        return result
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error running pipeline: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
