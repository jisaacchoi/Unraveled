#!/usr/bin/env python3
"""
Split large JSON.gz files using indexed_gzip for efficient random access.

This command:
1. Processes files in group subfolders (group_<hash> based on schema_sig) created by commands/04_gen_schemas.py
2. Uses indexed_gzip indexes generated during ingestion
3. Splits files larger than min_file_size_mb_for_indexed_gzip into smaller parts
4. Part 000 contains all scalar values (from mrf_analysis)
5. Parts 001+ contain array chunks (y items per file)

Usage:
    python commands/05_split.py --config config.yaml
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
from src.shared.database import build_connection_string
from src.split.indexed_gzip_splitter import split_json_gz_with_indexed_gzip

LOG = logging.getLogger("commands.split")


def _split_single_file_worker(
    file_info: Tuple[str, str, str, str, Optional[str], int, int, float, int, int, Optional[str], str, Optional[str], int]
) -> Tuple[int, str, str]:
    """
    Worker function for multiprocessing - splits a single file.
    
    Args:
        file_info: Tuple of (file_path_str, group_name, output_dir_str, connection_string, index_path_str, chunk_read_bytes, size_per_file_bytes, min_file_size_mb, file_index, total_files, log_file_path, log_format, log_datefmt, num_array_threads, log_chunk_progress)
        Note: Paths are passed as strings for pickling compatibility
        
    Returns:
        Tuple of (result_code, file_path_str, error_message)
        - result_code: 0 on success, 1 on error
        - file_path_str: File path for identification
        - error_message: Error message if failed, empty string if succeeded
    """
    file_path_str, group_name, output_dir_str, connection_string, index_path_str, chunk_read_bytes, size_per_file_bytes, min_file_size_mb, file_index, total_files, log_file_path, log_format, log_datefmt, num_array_threads, log_chunk_progress = file_info
    
    # Get process ID for logging identification
    process_id = os.getpid()
    
    # Set up logging for this worker process
    if log_file_path:
        log_file = Path(log_file_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    else:
        handler = logging.StreamHandler()
    
    handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))
    handler.setLevel(logging.INFO)
    
    # Configure all relevant loggers
    loggers_to_configure = [
        "commands.split",
        "src.split.indexed_gzip_splitter",
        "src.indexed_gzip_ingester",  # For scan_unique_keys_with_offsets() progress logs
    ]
    
    for logger_name in loggers_to_configure:
        logger = logging.getLogger(logger_name)
        # Clear existing handlers to ensure our format is used (PID format from config)
        logger.handlers.clear()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    
    worker_log = logging.getLogger("commands.split")
    
    # Convert string paths back to Path objects
    file_path = Path(file_path_str)
    output_dir = Path(output_dir_str)
    index_path = Path(index_path_str) if index_path_str else None
    
    try:
        worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Starting split: {group_name}/{file_path.name}")
        
        # Check file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if min_file_size_mb > 0 and file_size_mb < min_file_size_mb:
            worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Skipping {file_path.name} (size {file_size_mb:.1f} MB < {min_file_size_mb} MB)")
            return (0, file_path_str, "")
        
        # Split the file
        parts_created = split_json_gz_with_indexed_gzip(
            input_path=file_path,
            output_dir=output_dir,
            connection_string=connection_string,
            index_path=index_path,
            chunk_read_bytes=chunk_read_bytes,
            size_per_file_bytes=size_per_file_bytes,
            file_name=file_path.name,
            num_array_threads=num_array_threads,
            log_chunk_progress=log_chunk_progress,
        )
        
        if parts_created > 0:
            # Rename original file by appending .part to mark it as split (keeps .json.gz extension)
            try:
                part_path = file_path.with_name(file_path.name + ".part")
                file_path.rename(part_path)
                worker_log.info(f"[PID:{process_id}] [{file_index}/{total_files}] Successfully split {file_path.name} into {parts_created} part(s), renamed original to {part_path.name}")
            except Exception as rename_exc:  # noqa: BLE001
                worker_log.warning(f"[PID:{process_id}] [{file_index}/{total_files}] Successfully split {file_path.name} but failed to rename: {rename_exc}")
            return (0, file_path_str, "")
        else:
            error_msg = f"Failed to split {file_path.name} (no parts created)"
            worker_log.warning(f"[PID:{process_id}] [{file_index}/{total_files}] {error_msg}")
            return (1, file_path_str, error_msg)
            
    except Exception as exc:  # noqa: BLE001
        error_msg = f"Error splitting {file_path.name}: {exc}"
        worker_log.exception(f"[PID:{process_id}] [{file_index}/{total_files}] {error_msg}")
        return (1, file_path_str, error_msg)


def main() -> int:
    """Entrypoint for the split command."""
    parser = argparse.ArgumentParser(description="Split large JSON.gz files using indexed_gzip")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"), help="Config file path")
    args = parser.parse_args()
    
    # Load config
    if not args.config.exists():
        LOG.error("Config file not found: %s", args.config)
        return 1
    
    config = load_config(args.config)
    
    # Get log file path from config
    log_file = get_log_file_path(config, "split")
    configure_logging(config, log_file=log_file)
    
    # Get database connection
    db_config = config.get("database", {})
    if not db_config:
        LOG.error("Config missing 'database' section")
        return 1
    
    connection_string = build_connection_string(db_config)
    
    # Get split configuration
    pipeline_config = config.get("pipeline", {})
    shape_grouping_cfg = pipeline_config.get("shape_grouping", {})
    split_cfg = shape_grouping_cfg.get("split_files", {})
    
    if not split_cfg.get("enabled", False):
        LOG.info("Split files is disabled in config, exiting")
        return 0
    
    # Get settings
    chunk_read_bytes = split_cfg.get("split_chunk_read_bytes", 8 * 1024 * 1024)  # Default: 8MB
    size_per_file_mb = split_cfg.get("split_size_per_file_mb", 100)  # Default: 100 MB
    size_per_file_bytes = int(size_per_file_mb * 1024 * 1024)  # Convert MB to bytes
    num_workers = split_cfg.get("num_workers", 1)
    num_array_threads = split_cfg.get("num_array_threads", 1)  # Default: 1 (sequential)
    log_chunk_progress = split_cfg.get("log_chunk_progress", False)  # Default: false (less verbose)
    
    # Get min_file_size_mb from ingest config (same threshold as indexed_gzip ingestion)
    ingestion_cfg = pipeline_config.get("ingest", {})
    min_file_size_mb = ingestion_cfg.get("min_file_size_mb_for_indexed_gzip", 0)
    
    # Get index path from ingest config
    index_path_cfg = ingestion_cfg.get("index_path")
    
    # Auto-detect CPU count if num_workers is None
    if num_workers is None:
        num_workers = max(1, (os.cpu_count() or 1) - 1)  # Leave one core free
        LOG.info("Auto-detected CPU count: %d workers", num_workers)
    
    LOG.info("Split configuration: chunk_read_bytes=%d, size_per_file=%.1f MB, num_workers=%d, num_array_threads=%d, min_file_size_mb=%.1f",
             chunk_read_bytes, size_per_file_mb, num_workers, num_array_threads, min_file_size_mb)
    
    # Get input directory (should be the output_directory from gen_schemas, which contains group subfolders)
    input_directory_cfg = pipeline_config.get("output_directory")
    if not input_directory_cfg:
        LOG.error("No output_directory in config.pipeline (this should be the directory with group subfolders from gen_schemas)")
        return 1
    
    input_directory = Path(input_directory_cfg)
    
    if not input_directory.exists():
        LOG.error("Input directory does not exist: %s", input_directory)
        return 1
    
    if not input_directory.is_dir():
        LOG.error("Input path must be a directory: %s", input_directory)
        return 1
    
    # Find all group subfolders (group_1, group_2, etc.)
    group_dirs = [d for d in input_directory.iterdir() if d.is_dir() and d.name.startswith("group_")]
    
    if not group_dirs:
        LOG.info("No group subfolders found in %s", input_directory)
        LOG.info("Note: Group subfolders are created by commands/04_gen_schemas.py")
        return 0
    
    LOG.info("Found %d group subfolder(s) to process", len(group_dirs))
    
    # Collect all files to split from all group subfolders
    files_to_split = []
    for group_dir in sorted(group_dirs):
        # Find .json.gz files in this group folder (exclude already split files with _part prefix)
        json_gz_files = [
            f for f in group_dir.iterdir()
            if f.is_file()
            and f.name.endswith(".json.gz")
            and "_part" not in f.name  # Exclude already split files
        ]
        
        for file_path in sorted(json_gz_files):
            files_to_split.append((file_path, group_dir.name, group_dir))  # (file_path, group_name, output_dir)
    
    if not files_to_split:
        LOG.info("No .json.gz files found in group subfolders (excluding already split files)")
        return 0
    
    LOG.info("Found %d file(s) to split across %d group subfolder(s)", len(files_to_split), len(group_dirs))
    
    # Get log file path for worker processes (log_file already defined above)
    log_file_path = str(log_file) if log_file else None
    
    # Get logging format from config to pass to workers
    logger_cfg = config.get("logging", config.get("logger", {}))
    log_format = logger_cfg.get("format", "%(asctime)s [PID:%(process)d] %(levelname)s %(name)s - %(message)s")
    log_datefmt = logger_cfg.get("datefmt", "%Y-%m-%d %H:%M:%S")
    
    # Determine index path base
    index_path_base = Path(index_path_cfg) if index_path_cfg else None
    
    # Prepare file info tuples for workers
    file_tasks = []
    for file_index, (file_path, group_name, output_dir) in enumerate(files_to_split, 1):
        # Determine index path for this file
        if index_path_base:
            file_index_path = index_path_base / f"{file_path.name}.gzi"
        else:
            file_index_path = file_path.with_suffix(file_path.suffix + ".gzi")
        
        file_tasks.append((
            str(file_path),  # file_path_str
            group_name,
            str(output_dir),  # output_dir_str (same as group_dir)
            connection_string,  # connection_string
            str(file_index_path) if file_index_path.exists() else None,  # index_path_str
            chunk_read_bytes,
            size_per_file_bytes,
            min_file_size_mb,
            file_index,
            len(files_to_split),
            str(log_file) if log_file else None,  # log_file_path
            log_format,  # log_format
            log_datefmt,  # log_datefmt
            num_array_threads,  # num_array_threads
            log_chunk_progress,  # log_chunk_progress
        ))
    
    # Process files
    if len(files_to_split) == 0:
        LOG.error("No files to split")
        return 1
    
    # Set multiprocessing start method for Windows compatibility
    if sys.platform == "win32":
        multiprocessing.set_start_method("spawn", force=True)
    
    LOG.info("Using %d worker process(es) to split %d file(s)", num_workers, len(files_to_split))
    
    success_count = 0
    failed_files = []
    
    try:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(_split_single_file_worker, task): task[0]
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
        LOG.error("Failed to split %d file(s):", len(failed_files))
        for file_path_str, error_msg in failed_files:
            LOG.error("  %s: %s", file_path_str, error_msg)
    
    LOG.info("Split complete: processed %d/%d files successfully", success_count, len(files_to_split))
    
    return 0 if success_count == len(files_to_split) else 1


if __name__ == "__main__":
    raise SystemExit(main())
