#!/usr/bin/env python3
"""
Combined command: Detect shapes, refresh schema groups, and generate schemas.

This command combines three operations:
1. Detect shapes: Analyze JSON structures from mrf_landing and store in mrf_analysis table
2. Refresh schema groups: Refresh schema_groups table from mrf_analysis data
3. Generate schemas: Generate PySpark schemas (only if new schema groups are detected)

The generate_schemas step only runs if the unique min_file_name values change after
refreshing schema_groups, indicating new schema groups were added.

Usage:
    python commands/02_detect_group_generate_schemas.py --config config.yaml
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
from src.detect_shapes.structure_analyzer import (
    create_mrf_analysis_table,
    run_shape_analysis,
)
from src.generate_schemas.schema_orchestrator import run_full_pipeline
from src.shared.spark_session import create_spark_session

LOG = logging.getLogger("commands.detect_shapes_and_generate_schemas")


def parse_bool(value: str) -> bool:
    """Parse a string to boolean value."""
    value_lower = value.lower().strip()
    if value_lower in ("true", "1", "yes", "on"):
        return True
    elif value_lower in ("false", "0", "no", "off"):
        return False
    else:
        raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}. Use 'true' or 'false'.")


def main() -> int:
    """Entrypoint for the combined detect_shapes_and_generate_schemas command."""
    parser = argparse.ArgumentParser(
        description="Detect shapes, refresh schema groups, and generate schemas if needed"
    )
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    parser.add_argument(
        "-d", "--detect",
        type=parse_bool,
        default="true",
        help="Enable/disable shape detection (default: true). Use: -d true or -d false"
    )
    parser.add_argument(
        "-gr", "--group",
        type=parse_bool,
        default="true",
        help="Enable/disable schema group refresh (default: true). Use: -gr true or -gr false"
    )
    parser.add_argument(
        "-gen", "--generate",
        type=parse_bool,
        default="true",
        help="Enable/disable schema generation (default: true). Use: -gen true or -gen false"
    )
    args = parser.parse_args()
    
    # Load config file
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "detect_group_generate_schemas")
    configure_logging(config, log_file=log_file)
    
    # Get cloud configuration
    cloud_config = config.get("cloud", {})
    
    # Get database connection
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("Config missing 'database' section")
        return 1
    
    connection_string = build_connection_string(db_config, cloud_config)
    
    # Get detect_shapes configuration
    pipeline_config = config.get("pipeline", {})
    detect_shapes_cfg = pipeline_config.get("shape_detection", config.get("detect_shapes", {}))
    drop_table_if_exists = detect_shapes_cfg.get("drop_table_if_exists", False)
    batch_size = detect_shapes_cfg.get("insert_batch_size", detect_shapes_cfg.get("batch_size", 100))
    fetch_batch_size = detect_shapes_cfg.get("fetch_batch_size", 10000)
    url_content_download_path = detect_shapes_cfg.get("url_content_download_path")
    
    # Get schema generation configuration (only needed if group or generate is enabled)
    input_directory = None
    schema_output_directory = None
    analyzed_directory = None
    refresh_schema_groups = False
    
    if args.group or args.generate:
        schema_gen_cfg = pipeline_config.get("schema_generation", {})
        input_path_cfg = schema_gen_cfg.get("input_directory")
        output_path_cfg = schema_gen_cfg.get("output_directory")
        analyzed_path_cfg = schema_gen_cfg.get("analyzed_directory")
        refresh_schema_groups = schema_gen_cfg.get("refresh_schema_groups", False)
        
        if not input_path_cfg:
            LOG.error("No input_directory in config.pipeline.schema_generation (required when group or generate is enabled)")
            return 1
        
        input_directory = Path(input_path_cfg)
        
        if not input_directory.exists():
            LOG.error("Input directory does not exist: %s", input_directory)
            return 1
        
        if not input_directory.is_dir():
            LOG.error("Input path must be a directory: %s", input_directory)
            return 1
        
        # Schema output directory only needed if generate is enabled
        if args.generate:
            if not output_path_cfg:
                LOG.error("No output_directory in config.pipeline.schema_generation (required when generate is enabled)")
                return 1
            schema_output_directory = Path(output_path_cfg)
        else:
            schema_output_directory = Path(output_path_cfg) if output_path_cfg else None
        
        analyzed_directory = Path(analyzed_path_cfg) if analyzed_path_cfg else None
    
    # Convert download path to Path object if provided
    download_path = None
    if url_content_download_path:
        download_path = Path(url_content_download_path)
    
    # Validate flag combinations
    # If generate is enabled but group is disabled, check if schema_groups table exists
    if args.generate and not args.group:
        # Check if schema_groups table exists in database
        try:
            import psycopg2
            conn = psycopg2.connect(connection_string)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'schema_groups'
                );
            """)
            table_exists = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if not table_exists:
                LOG.error("Cannot generate schemas without grouping enabled. Schema generation requires schema_groups table.")
                LOG.error("The schema_groups table does not exist. Either:")
                LOG.error("  1. Enable grouping (-gr true) to create/refresh the table, or")
                LOG.error("  2. Run step 3 first to create the schema_groups table, or")
                LOG.error("  3. Disable generation (-gen false)")
                return 1
            else:
                LOG.info("schema_groups table exists, proceeding with schema generation without refreshing groups")
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Could not verify if schema_groups table exists: %s", exc)
            LOG.warning("Proceeding anyway - will fail later if table doesn't exist")
    
    LOG.info("Using config from %s", args.config)
    LOG.info("Drop table if exists: %s", drop_table_if_exists)
    LOG.info("Batch size: %d, Fetch batch size: %d", batch_size, fetch_batch_size)
    LOG.info("Refresh schema groups: %s", refresh_schema_groups)
    LOG.info("Operation flags: detect=%s, group=%s, generate=%s", args.detect, args.group, args.generate)
    
    try:
        # Ensure mrf_analysis table exists (only if detect is enabled)
        if args.detect:
            create_mrf_analysis_table(connection_string, drop_if_exists=drop_table_if_exists)
        
        # Only create SparkSession if generate is enabled
        spark = None
        if args.generate:
            spark = create_spark_session()
        
        try:
            result, schemas_generated = run_full_pipeline(
                connection_string=connection_string,
                spark=spark,
                input_directory=input_directory,
                schema_output_directory=schema_output_directory,
                batch_size=batch_size,
                fetch_batch_size=fetch_batch_size,
                drop_table_if_exists=drop_table_if_exists,
                refresh_schema_groups=refresh_schema_groups,
                download_path=download_path,
                analyzed_directory=analyzed_directory,
                files_output_directory=None,  # Not used in this test script
                enable_detect=args.detect,
                enable_group=args.group,
                enable_generate=args.generate,
            )
            
            if result == 0:
                if schemas_generated:
                    LOG.info("Pipeline completed successfully. Schemas were generated.")
                else:
                    LOG.info("Pipeline completed successfully. No new schemas were generated.")
            else:
                LOG.error("Pipeline completed with errors")
            
            return result
        finally:
            if spark:
                spark.stop()
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error running pipeline: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

