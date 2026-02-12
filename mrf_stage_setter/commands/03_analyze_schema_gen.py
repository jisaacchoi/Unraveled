#!/usr/bin/env python3
"""
Analyze and generate schemas: Comprehensive analysis of JSON structures and schema generation.

This command:
1. Analyzes JSON structures from mrf_landing (comprehensive analysis)
2. Generates schema JSON files for each analyzed file using mrf_analysis table

Usage:
    python commands/03_analyze_schema_gen.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string
from src.detect_shapes.comprehensive_structure_analyzer import run_comprehensive_shape_analysis
from src.detect_shapes.structure_analyzer import create_mrf_analysis_table
from src.generate_schemas.schema_inference import generate_schema_for_file, file_name_to_schema_filename
from src.shared.file_prefix import PREFIX_INGESTED, PREFIX_ANALYZED

LOG = logging.getLogger("commands.analyze_schema_gen")


def get_all_file_names_from_mrf_analysis(connection_string: str) -> list[str]:
    """
    Get all distinct file names from mrf_analysis table.
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        List of all distinct file names in mrf_analysis
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT file_name 
            FROM mrf_analysis 
            ORDER BY file_name
        """)
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting file names from mrf_analysis: %s", exc)
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _build_empty_marker_name(filename: str) -> str:
    """
    Build an empty marker filename for a file.
    Strips ALL processing prefixes before adding _empty_.
    """
    name = Path(filename).name

    # Canonicalize existing empty markers too:
    # _empty_analyzed_foo.json.gz -> _empty_foo.json.gz
    if name.startswith("_empty_"):
        name = name[len("_empty_"):]

    final_name = _strip_processing_prefixes(name)
    return f"_empty_{final_name}"


def _is_empty_marker(filename: str) -> bool:
    if filename.startswith("_empty_"):
        return True
    return False


def _normalize_file_name(file_name: str) -> str:
    """
    Normalize file name by stripping all processing prefixes.
    Database stores original file names without any prefixes.
    """
    return _strip_processing_prefixes(file_name)


def _strip_processing_prefixes(filename: str) -> str:
    """Strip processing prefixes from the front of a file name."""
    name = Path(filename).name
    # Remove one or more leading ingested/analyzed prefixes, with or without leading "_".
    return re.sub(r"^(?:(?:_?ingested_|_?analyzed_)+)", "", name)


def main() -> int:
    """Entrypoint for the analyze and schema generation command."""
    parser = argparse.ArgumentParser(
        description="Comprehensively analyze JSON structures and generate schemas"
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
    
    # Get shape_grouping configuration for schema generation
    shape_grouping_cfg = pipeline_config.get("shape_grouping", {})
    output_directory_cfg = pipeline_config.get("output_directory") or shape_grouping_cfg.get("output_directory")
    overwrite_existing_schemas = shape_grouping_cfg.get("overwrite_existing_schemas", True)
    end_delete_after_gen_schemas = shape_grouping_cfg.get("end_delete_after_gen_schemas", False)
    
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
    
    # Convert output directory to Path object if provided
    output_directory = None
    if output_directory_cfg:
        output_directory = Path(output_directory_cfg)
    
    LOG.info("Using config from %s", args.config)
    LOG.info("Drop table if exists: %s", drop_table_if_exists)
    LOG.info("Batch size: %d, Fetch batch size: %d", batch_size, fetch_batch_size)
    if input_directory:
        LOG.info("Input directory: %s", input_directory)
    else:
        LOG.error("Input directory is required")
        return 1
    
    if output_directory:
        LOG.info("Output directory: %s", output_directory)
    else:
        LOG.warning("Output directory not specified, schema generation will be skipped")
    
    try:
        # Step 1: Ensure mrf_analysis table exists
        create_mrf_analysis_table(connection_string, drop_if_exists=drop_table_if_exists)
        
        # Step 2: Run comprehensive shape analysis
        # Files are renamed in-place with _analyzed_ prefix after successful analysis
        LOG.info("=== Step 1: Running comprehensive shape analysis ===")
        LOG.info("This will process ALL items for each record_type to build comprehensive structure")
        analysis_result = run_comprehensive_shape_analysis(
            connection_string=connection_string,
            batch_size=batch_size,
            fetch_batch_size=fetch_batch_size,
            download_path=download_path,
            input_directory=input_directory,
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
        
        # Step 3: Generate schemas for all ingested files and ensure they are analyzed
        if output_directory:
            LOG.info("=== Step 2: Generating schemas for all ingested files ===")
            
            if not output_directory.exists():
                LOG.info("Output directory does not exist, creating it: %s", output_directory)
                output_directory.mkdir(parents=True, exist_ok=True)
            
            from src.shared.file_prefix import has_prefix, PREFIX_INGESTED, PREFIX_ANALYZED, add_prefix
            from src.shared.json_reader import is_json_file
            
            # Find all ingested files (includes _ingested_ and _ingested_analyzed_)
            ingested_files = []
            for file_path in input_directory.iterdir():
                if file_path.is_file() and is_json_file(file_path):
                    if has_prefix(file_path, PREFIX_INGESTED) and not _is_empty_marker(file_path.name):
                        ingested_files.append(file_path)
            
            if not ingested_files:
                LOG.info("No _ingested_ files found in input directory")
            else:
                LOG.info("Found %d ingested file(s) to process for schema generation", len(ingested_files))
                
                schemas_created = 0
                files_renamed = 0
                
                for source_path in ingested_files:
                    # Normalize file name (strip _ingested_ prefix) for database queries
                    normalized_file_name = _normalize_file_name(source_path.name)
                    
                    try:
                        # Check if schema already exists
                        schema_path = output_directory / file_name_to_schema_filename(normalized_file_name)
                        if schema_path.exists() and not overwrite_existing_schemas:
                            LOG.info("Schema already exists, skipping: %s", schema_path)
                            # Still ensure the file has analyzed prefix even if schema exists
                            try:
                                analyzed_path = add_prefix(source_path, PREFIX_ANALYZED)
                                if analyzed_path != source_path and not analyzed_path.exists():
                                    source_path.rename(analyzed_path)
                                    files_renamed += 1
                                    LOG.info("Renamed file after schema check: %s -> %s", source_path.name, analyzed_path.name)
                            except Exception as rename_exc:  # noqa: BLE001
                                LOG.warning("Failed to rename file %s: %s", source_path.name, rename_exc)
                            continue
                        
                        # Generate schema for this file
                        schema_path = generate_schema_for_file(
                            output_directory=output_directory,
                            file_name=normalized_file_name,  # Use normalized name for DB query
                            connection_string=connection_string,
                        )
                        
                        if schema_path:
                            schemas_created += 1
                            LOG.info("Generated schema for file: %s", schema_path)
                            
                            # After schema is created successfully, ensure file has _analyzed_ prefix
                            try:
                                analyzed_path = add_prefix(source_path, PREFIX_ANALYZED)
                                if analyzed_path != source_path and not analyzed_path.exists():
                                    source_path.rename(analyzed_path)
                                    files_renamed += 1
                                    LOG.info("Renamed file after schema generation: %s -> %s", source_path.name, analyzed_path.name)
                                elif analyzed_path.exists():
                                    LOG.debug("File %s already has _analyzed_ prefix or target exists", source_path.name)
                            except Exception as rename_exc:  # noqa: BLE001
                                LOG.warning("Failed to rename file %s after schema generation: %s", source_path.name, rename_exc)
                        else:
                            LOG.warning("Failed to generate schema for file: %s", normalized_file_name)
                    except Exception as exc:  # noqa: BLE001
                        LOG.exception("Error generating schema for file %s: %s", source_path.name, exc)
                        continue
                
                LOG.info("Schema generation complete: %d schema file(s) created, %d file(s) renamed", 
                         schemas_created, files_renamed)
        
        # Step 3: Move all _ingested_analyzed_ files to output directory (WITHOUT stripping prefixes)
        if output_directory:
            LOG.info("=== Step 3: Moving _ingested_analyzed_ files to output directory ===")
            
            if not output_directory.exists():
                LOG.info("Output directory does not exist, creating it: %s", output_directory)
                output_directory.mkdir(parents=True, exist_ok=True)
            
            from src.shared.file_prefix import has_all_prefixes
            from src.shared.json_reader import is_json_file
            
            # Find all _ingested_analyzed_ files
            files_to_move = []
            for file_path in input_directory.iterdir():
                if file_path.is_file() and is_json_file(file_path):
                    if has_all_prefixes(file_path, [PREFIX_INGESTED, PREFIX_ANALYZED]):
                        files_to_move.append(file_path)
            
            if files_to_move:
                LOG.info("Found %d _ingested_analyzed_ file(s) to move to output directory", len(files_to_move))
                moved_count = 0
                failed_count = 0
                
                for source_path in files_to_move:
                    try:
                        # Move file WITHOUT stripping prefixes (keep original name with prefixes)
                        dest_path = output_directory / source_path.name
                        
                        # Use shutil.move for better Windows compatibility
                        import shutil
                        shutil.move(str(source_path), str(dest_path))
                        moved_count += 1
                        LOG.info("Moved file: %s -> %s", source_path.name, dest_path.name)
                    except Exception as move_exc:  # noqa: BLE001
                        LOG.warning("Failed to move file %s to output directory: %s", source_path.name, move_exc)
                        failed_count += 1
                
                LOG.info("Moved %d file(s) to output directory (%d failed)", moved_count, failed_count)
            else:
                LOG.info("No _ingested_analyzed_ files found in input directory to move")
        
        # Step 4: Strip all prefixes from moved files in output directory (not schema files)
        if output_directory:
            LOG.info("=== Step 4: Stripping all prefixes from moved files in output directory ===")
            
            from src.shared.file_prefix import has_prefix
            from src.shared.json_reader import is_json_file
            
            files_to_rename = []
            for file_path in output_directory.iterdir():
                if file_path.is_file() and is_json_file(file_path):
                    # Skip schema files (they end with _schema.json)
                    if file_path.name.endswith("_schema.json"):
                        continue
                    # Skip empty markers (they start with _empty_)
                    if _is_empty_marker(file_path.name):
                        continue
                    # Check if file has any processing prefix
                    if (has_prefix(file_path, PREFIX_INGESTED) or 
                        has_prefix(file_path, PREFIX_ANALYZED) or
                        file_path.name.startswith("_ingested_analyzed_")):
                        files_to_rename.append(file_path)
            
            if files_to_rename:
                LOG.info("Found %d moved file(s) with prefixes to strip in output directory", len(files_to_rename))
                renamed_count = 0
                failed_count = 0
                
                for source_path in files_to_rename:
                    try:
                        stripped_name = _strip_processing_prefixes(source_path.name)

                        # Only rename if the name changed
                        if stripped_name != source_path.name:
                            dest_path = output_directory / stripped_name
                            if dest_path.exists():
                                LOG.warning("Target file already exists, skipping rename: %s -> %s", 
                                          source_path.name, dest_path.name)
                                continue
                            
                            source_path.rename(dest_path)
                            renamed_count += 1
                            LOG.info("Stripped prefixes: %s -> %s", source_path.name, dest_path.name)
                    except Exception as rename_exc:  # noqa: BLE001
                        LOG.warning("Failed to strip prefixes from file %s: %s", source_path.name, rename_exc)
                        failed_count += 1
                
                LOG.info("Stripped prefixes from %d moved file(s) (%d failed)", renamed_count, failed_count)
            else:
                LOG.info("No moved files with prefixes found in output directory")

            # Normalize existing empty marker names as well
            # Example: _empty_analyzed_foo.json.gz -> _empty_foo.json.gz
            empty_markers_to_rename = []
            for file_path in output_directory.iterdir():
                if file_path.is_file() and _is_empty_marker(file_path.name):
                    normalized_marker_name = _build_empty_marker_name(file_path.name)
                    if normalized_marker_name != file_path.name:
                        empty_markers_to_rename.append((file_path, normalized_marker_name))

            if empty_markers_to_rename:
                LOG.info("Found %d empty marker(s) to normalize in output directory", len(empty_markers_to_rename))
                normalized_count = 0
                skipped_count = 0
                failed_count = 0

                for source_path, normalized_name in empty_markers_to_rename:
                    try:
                        dest_path = output_directory / normalized_name
                        if dest_path.exists():
                            LOG.warning("Target empty marker already exists, skipping rename: %s -> %s",
                                        source_path.name, dest_path.name)
                            skipped_count += 1
                            continue

                        source_path.rename(dest_path)
                        normalized_count += 1
                        LOG.info("Normalized empty marker: %s -> %s", source_path.name, dest_path.name)
                    except Exception as rename_exc:  # noqa: BLE001
                        LOG.warning("Failed to normalize empty marker %s: %s", source_path.name, rename_exc)
                        failed_count += 1

                LOG.info("Normalized %d empty marker(s) (%d skipped, %d failed)",
                         normalized_count, skipped_count, failed_count)
            else:
                LOG.info("No empty marker normalization needed in output directory")
        
        # Step 5: Empty MOVED FILES if enabled (not schema files)
        # At this point, moved files should have NO prefixes (Step 4 stripped them)
        if output_directory and end_delete_after_gen_schemas:
            LOG.info("=== Step 5: Emptying moved files and creating markers ===")
            
            from src.shared.json_reader import is_json_file
            
            files_to_empty = []
            for file_path in output_directory.iterdir():
                if file_path.is_file() and is_json_file(file_path):
                    # Skip schema files (they end with _schema.json)
                    if file_path.name.endswith("_schema.json"):
                        continue
                    # Skip empty markers (they start with _empty_)
                    if _is_empty_marker(file_path.name):
                        continue
                    # Prefix cleanup is enforced again during emptying for safety.
                    files_to_empty.append(file_path)
            
            if files_to_empty:
                LOG.info("Found %d moved file(s) to empty in output directory", len(files_to_empty))
                emptied_count = 0
                failed_count = 0
                
                for source_path in files_to_empty:
                    try:
                        # Always canonicalize first: strip all processing prefixes before emptying.
                        canonical_name = _strip_processing_prefixes(source_path.name)
                        canonical_path = output_directory / canonical_name
                        working_path = source_path

                        if canonical_name != source_path.name:
                            if canonical_path.exists():
                                LOG.warning(
                                    "Cannot canonicalize file before emptying, target exists: %s -> %s",
                                    source_path.name,
                                    canonical_path.name,
                                )
                                failed_count += 1
                                continue
                            source_path.rename(canonical_path)
                            working_path = canonical_path
                            LOG.info(
                                "Canonicalized moved file before emptying: %s -> %s",
                                source_path.name,
                                canonical_path.name,
                            )

                        # Create marker from canonical name
                        marker_name = _build_empty_marker_name(working_path.name)
                        marker_path = working_path.with_name(marker_name)

                        working_path.unlink()
                        marker_path.touch(exist_ok=True)
                        emptied_count += 1
                        LOG.info("Emptied moved file and created marker: %s -> %s", working_path.name, marker_path.name)
                    except Exception as exc:  # noqa: BLE001
                        LOG.warning("Failed to create empty marker for %s: %s", source_path.name, exc)
                        failed_count += 1
                
                LOG.info("Emptied %d moved file(s) and created markers (%d failed)", emptied_count, failed_count)
            else:
                LOG.info("No moved files to empty in output directory")
        else:
            LOG.info("File emptying is disabled")
        
        return 0
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error running analyze and schema generation: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
