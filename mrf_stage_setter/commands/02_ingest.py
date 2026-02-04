#!/usr/bin/env python3
"""
Stream MRF JSON files (.json or .json.gz) into PostgreSQL mrf_landing table.

Extracts individual records from .in_network[] and .provider_references[] arrays,
storing each record as a separate row with a record_type column indicating the source array.

Usage:
    python commands/02_ingest.py --config config.yaml
"""
from __future__ import annotations

import argparse
import logging
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Tuple

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string, ensure_table_exists
from src.ingest.mrf_ingester import ingest_file
from src.ingest.indexed_gzip_ingester import ingest_file_indexed_gzip
from src.shared.file_prefix import rename_with_prefix, PREFIX_INGESTED

LOG = logging.getLogger("commands.ingest")


def _ingest_single_file_worker(
    file_info: Tuple[str, str, str, int, int, int, int, Optional[str], Optional[str], bool, str, Optional[str], int, int, int]
) -> Tuple[int, str, str]:
    """
    Worker function for multiprocessing - ingests a single file.
    
    Args:
        file_info: Tuple of (file_path_str, source_name, connection_string, batch_size, max_array_items, file_index, total_files, log_file_path, index_path, use_indexed_gzip, log_format, log_datefmt, scan_chunk_read_bytes, max_span_size, num_key_threads, progress_every_chunks)
        Note: Paths are passed as strings for pickling compatibility
        
    Returns:
        Tuple of (result_code, file_path_str, error_message)
        - result_code: 0 on success, 1 on error
        - file_path_str: File path for identification
        - error_message: Error message if failed, empty string if succeeded
    """
    file_path_str, source_name, connection_string, batch_size, max_array_items, file_index, total_files, log_file_path, index_path, use_indexed_gzip, log_format, log_datefmt, scan_chunk_read_bytes, max_span_size, num_key_threads, progress_every_chunks = file_info
    
    # Get process ID for logging identification
    process_id = os.getpid()
    
    # Set up logging for this worker process
    # On Windows multiprocessing, workers don't inherit logging configuration, so we need to set it up
    # Configure handlers for all relevant loggers
    if log_file_path:
        # Use the same log file as the main process
        log_file = Path(log_file_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")  # Append mode so all workers write to same file
    else:
        # Fall back to console logging
        handler = logging.StreamHandler()
    
    handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))
    handler.setLevel(logging.INFO)
    
    # Configure all relevant loggers to use the same handler
    loggers_to_configure = [
        "commands.ingest",
        "src.indexed_gzip_ingester",
        "src.ingest.indexed_gzip_ingester",  # In case of different import path
    ]
    
    for logger_name in loggers_to_configure:
        logger = logging.getLogger(logger_name)
        # Clear existing handlers to ensure our format is used (PID format from config)
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    
    # Use commands.ingest as the main worker logger
    worker_log = logging.getLogger("commands.ingest")
    
    # Convert string path back to Path object
    file_path = Path(file_path_str)
    
    try:
        worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Starting ingestion: {file_path.name}")
        
        # Choose ingestion method based on flag (determined by file type and size)
        if use_indexed_gzip:
            # Use indexed_gzip ingestion for large .json.gz files
            index_path_obj = Path(index_path) if index_path else None
            result = ingest_file_indexed_gzip(
                connection_string=connection_string,
                file_path=file_path,
                source_name=source_name,
                batch_size=batch_size,
                show_progress=False,  # Disable progress bar in multiprocessing
                max_array_items=max_array_items,
                index_path=index_path_obj,
                chunk_size=scan_chunk_read_bytes,
                max_span_size=max_span_size,
                num_key_threads=num_key_threads,
                progress_every_chunks=progress_every_chunks,
            )
        else:
            # Use regular ijson ingestion for .json files or small .json.gz files
            result = ingest_file(
                connection_string=connection_string,
                file_path=file_path,
                source_name=source_name,
                batch_size=batch_size,
                max_array_items=max_array_items,
            )
        
        if result == 0:
            # Rename file with _ingested_ prefix (in place, no move)
            try:
                new_path = rename_with_prefix(file_path, PREFIX_INGESTED)
                worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Successfully ingested and renamed: {file_path.name} -> {new_path.name}")
            except Exception as rename_exc:  # noqa: BLE001
                worker_log.warning(f"[PID:{process_id}] [{file_index}/{total_files}] Ingestion succeeded but failed renaming {file_path.name}: {rename_exc}")
            
            worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Successfully ingested: {file_path.name}")
            return (0, file_path_str, "")
        else:
            error_msg = f"Failed to ingest {file_path.name}"
            worker_log.error(f"[PID:{process_id}] [{file_index}/{total_files}] {error_msg}")
            return (1, file_path_str, error_msg)
            
    except Exception as exc:  # noqa: BLE001
        error_msg = f"Error ingesting {file_path.name}: {exc}"
        worker_log.exception(f"[PID:{process_id}] [{file_index}/{total_files}] {error_msg}")
        return (1, file_path_str, error_msg)


def main() -> int:
    """Entrypoint for the ingest command."""
    parser = argparse.ArgumentParser(description="Ingest MRF files into PostgreSQL")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "ingest")
    configure_logging(config, log_file=log_file)
    
    # Get database connection (support both old and new config structure)
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("Config missing 'database' section")
        return 1

    connection_string = build_connection_string(db_config)
    
    # Get ingestion settings (support both old and new config structure)
    pipeline_config = config.get("pipeline", {})
    ingestion_cfg = pipeline_config.get("ingest", config.get("ingestion", {}))
    auto_create = ingestion_cfg.get("auto_create_tables", True)
    batch_size = ingestion_cfg.get("batch_size", 5000)  # Increased default (optimization #1)
    max_array_items = ingestion_cfg.get("max_array_items", 100)
    num_workers = ingestion_cfg.get("num_workers", 1)
    drop_table_if_exists = ingestion_cfg.get("drop_tables_if_exists", ingestion_cfg.get("drop_table_if_exists", False))
    
    # Indexed gzip settings
    index_path_cfg = ingestion_cfg.get("index_path")  # Optional: directory or file path for indexes
    min_file_size_mb_for_indexed_gzip = ingestion_cfg.get("min_file_size_mb_for_indexed_gzip", 0)  # Default: 0 = use indexed_gzip for all .json.gz files
    chunk_size = ingestion_cfg.get("chunk_size_bytes", 16 * 1024 * 1024)  # Default: 16MB (optimization #3)
    max_span_size = ingestion_cfg.get("max_span_size_bytes", 50 * 1024 * 1024)  # Default: 50MB (optimization #8)
    num_key_threads = ingestion_cfg.get("num_key_threads", 1)  # Default: 1 (sequential, optimization #4)
    progress_every_chunks = ingestion_cfg.get("progress_every_chunks", 5)  # Default: 5 (log every 5 chunks)
    
    # Auto-detect CPU count if num_workers is None
    if num_workers is None:
        num_workers = os.cpu_count() or 1
        LOG.info("Auto-detected CPU count: %d workers", num_workers)
    
    LOG.info("Ingestion configuration: batch_size=%d, max_array_items=%d, num_workers=%d, min_file_size_mb_for_indexed_gzip=%s", 
             batch_size, max_array_items, num_workers, min_file_size_mb_for_indexed_gzip)
    
    # Ensure table exists once before processing any files
    if auto_create:
        try:
            ensure_table_exists(connection_string, "mrf_landing", drop_if_exists=drop_table_if_exists)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to ensure table exists: %s", exc)
            return 1
    
    # Determine file(s) to ingest from config
    input_path = pipeline_config.get("input_directory") or ingestion_cfg.get("input_directory", ingestion_cfg.get("input_path"))
    if not input_path:
        LOG.error("No input_directory in config.pipeline or config.pipeline.ingest")
        return 1
    input_path = Path(input_path)
    
    if input_path.is_file():
        files_to_ingest = [input_path]
    elif input_path.is_dir():
        # Find both .json and .json.gz files (top-level only, not recursive)
        # Only process files with NO prefix (exclude _ingested_, _analyzed_, _ready_)
        # Exclude .part files (temporary download files)
        from src.shared.json_reader import is_json_file
        from src.shared.file_prefix import has_prefix, PREFIX_INGESTED, PREFIX_ANALYZED, PREFIX_READY
        all_json_files = [
            f for f in input_path.iterdir() 
            if f.is_file() 
            and is_json_file(f) 
            and not has_prefix(f, PREFIX_INGESTED)  # Exclude ingested files
            and not has_prefix(f, PREFIX_ANALYZED)  # Exclude analyzed files
            and not has_prefix(f, PREFIX_READY)  # Exclude ready files
            and not f.name.endswith(".part")  # Exclude temporary download files
            and ".part" not in f.name  # Exclude any file with .part in name
        ]
        files_to_ingest = sorted(all_json_files)  # Sort for consistent processing order
        if not files_to_ingest:
            LOG.info("No .json or .json.gz files found in %s (excluding already ingested/analyzed files)", input_path)
            LOG.info("Note: Only top-level files are scanned (subdirectories are not searched)")
            LOG.info("No files to process - waiting for next poll")
            return 0
        else:
            LOG.info("Found %d JSON file(s) in %s (excluding already ingested/analyzed files)", len(files_to_ingest), input_path)
    else:
        LOG.error("Input path does not exist: %s", input_path)
        return 1
    
    # Get source name from config
    run_config = config.get("run_config", {})
    app_config = config.get("application", {})
    source_name = run_config.get("source_name") or app_config.get("source_name") or config.get("payer", "unknown")
    
    LOG.info("Ingesting %d file(s) into database", len(files_to_ingest))
    
    # Get log file path for worker processes
    log_file = get_log_file_path(config, "ingest")
    log_file_path = str(log_file) if log_file else None
    
    # Get logging format from config to pass to workers
    logger_cfg = config.get("logging", config.get("logger", {}))
    log_format = logger_cfg.get("format", "%(asctime)s [PID:%(process)d] %(levelname)s %(name)s - %(message)s")
    log_datefmt = logger_cfg.get("datefmt", "%Y-%m-%d %H:%M:%S")
    
    # Determine index path strategy
    index_path_base = Path(index_path_cfg) if index_path_cfg else None
    if index_path_base:
        index_path_base.mkdir(parents=True, exist_ok=True)
        LOG.info("Index directory: %s", index_path_base)
    
    # Always use multiprocessing (even if num_workers=1, it's still a worker process)
    if len(files_to_ingest) == 0:
        LOG.error("No files to ingest")
        return 1
    
    # Set multiprocessing start method for Windows compatibility
    if sys.platform == "win32":
        multiprocessing.set_start_method("spawn", force=True)
    
    LOG.info("Using %d worker process(es) to ingest %d file(s)", num_workers, len(files_to_ingest))
    
    # Prepare file info tuples for workers
    file_tasks = []
    for i, file_path in enumerate(files_to_ingest, 1):
        # Determine ingestion method based on file type and size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        use_indexed_gzip = False
        
        if file_path.name.endswith('.gz'):
            # For .json.gz files, use indexed_gzip if file size >= threshold
            if file_size_mb >= min_file_size_mb_for_indexed_gzip:
                use_indexed_gzip = True
                # Determine index path
                if index_path_base:
                    file_index_path = index_path_base / f"{file_path.name}.gzi"
                else:
                    file_index_path = file_path.with_suffix(file_path.suffix + '.gzi')
            else:
                file_index_path = None
        else:
            # .json files always use ijson
            file_index_path = None
        
        file_tasks.append((
            str(file_path),  # file_path_str
            source_name,
            connection_string,
            batch_size,
            max_array_items,
            i,  # file_index
            len(files_to_ingest),  # total_files
            log_file_path,
            str(file_index_path) if file_index_path else None,  # index_path
            use_indexed_gzip,  # Flag to indicate which ingestion method to use
            log_format,  # log_format
            log_datefmt,  # log_datefmt
            chunk_size,  # scan_chunk_read_bytes (optimization #3)
            max_span_size,  # max_span_size (optimization #8)
            num_key_threads,  # num_key_threads (optimization #4)
            progress_every_chunks,  # progress_every_chunks (chunk logging frequency)
        ))
    
    # Process files in parallel
    success_count = 0
    failed_files = []
    
    try:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(_ingest_single_file_worker, task): task[0] 
                for task in file_tasks
            }
            
            LOG.info("All %d task(s) submitted, waiting for results...", len(file_tasks))
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_file):
                file_path_str = future_to_file[future]
                try:
                    result_code, _, error_msg = future.result()
                    if result_code == 0:
                        success_count += 1
                    else:
                        failed_files.append((file_path_str, error_msg))
                except Exception as exc:  # noqa: BLE001
                    LOG.exception("Error processing file %s: %s", file_path_str, exc)
                    failed_files.append((file_path_str, str(exc)))
    
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Multiprocessing failed: %s", exc)
        return 1
    
    if failed_files:
        LOG.error("Failed to ingest %d file(s):", len(failed_files))
        for file_path_str, error_msg in failed_files:
            LOG.error("  %s: %s", file_path_str, error_msg)
    
    LOG.info("Ingested %d/%d files successfully", success_count, len(files_to_ingest))
    return 0 if success_count == len(files_to_ingest) else 1


if __name__ == "__main__":
    raise SystemExit(main())

