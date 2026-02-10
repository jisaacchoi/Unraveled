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
import time
from pathlib import Path
from typing import Optional, Tuple

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.config import configure_logging, get_log_file_path, load_config
from src.shared.database import build_connection_string, ensure_table_exists
from src.ingest.mrf_ingester import ingest_file
from src.ingest.indexed_gzip_ingester import ingest_file_indexed_gzip
from src.shared.file_prefix import PREFIX_INGESTED

LOG = logging.getLogger("commands.ingest")


def _ingest_single_file_worker(
    file_info: Tuple[str, str, str, int, int, int, Optional[str], str, Optional[str], int, int, int, int, float, float, Optional[int]]
) -> int:
    """
    Worker function for multiprocessing - repeatedly scans and ingests files.
    
    Args:
        file_info: Tuple of (input_dir_str, source_name, connection_string, batch_size, max_array_items, worker_id, log_file_path, index_path_base, log_format, log_datefmt, scan_chunk_read_bytes, max_span_size, num_key_threads, progress_every_chunks, poll_interval_seconds, min_file_size_mb_for_indexed_gzip, max_idle_polls)
        Note: Paths are passed as strings for pickling compatibility
        
    Returns:
        Exit code: 0 on clean completion, 1 on error
    """
    input_dir_str, source_name, connection_string, batch_size, max_array_items, worker_id, log_file_path, index_path_base_str, log_format, log_datefmt, scan_chunk_read_bytes, max_span_size, num_key_threads, progress_every_chunks, poll_interval_seconds, min_file_size_mb_for_indexed_gzip, max_idle_polls = file_info
    
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
    
    from src.shared.json_reader import is_json_file
    from src.shared.file_prefix import has_prefix, PREFIX_ANALYZED, PREFIX_READY, PREFIX_INGESTED
    
    input_dir = Path(input_dir_str)
    index_path_base = Path(index_path_base_str) if index_path_base_str else None
    worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] Starting worker loop in {input_dir}")
    
    def _scan_candidates() -> list[Path]:
        candidates = [
            f for f in input_dir.iterdir()
            if f.is_file()
            and is_json_file(f)
            and not has_prefix(f, PREFIX_INGESTED)
            and not has_prefix(f, PREFIX_ANALYZED)
            and not has_prefix(f, PREFIX_READY)
            and not f.name.endswith(".part")
            and ".part" not in f.name
        ]
        return sorted(candidates)
    
    def _claim_file(file_path: Path) -> Optional[Tuple[Path, str]]:
        original_name = file_path.name
        part_path = file_path.with_name(f"{file_path.name}.part")
        try:
            file_path.rename(part_path)
            return part_path, original_name
        except FileNotFoundError:
            return None
        except PermissionError:
            return None
        except OSError:
            return None
    
    def _finalize_ingested_file(part_path: Path, original_name: str) -> Optional[Path]:
        final_name = f"{PREFIX_INGESTED}{original_name}"
        final_path = part_path.with_name(final_name)
        part_path.replace(final_path)
        return final_path
    
    def _restore_original(part_path: Path, original_name: str) -> None:
        try:
            original_path = part_path.with_name(original_name)
            if part_path.exists():
                try:
                    part_path.replace(original_path)
                except FileNotFoundError:
                    return
        except Exception:  # noqa: BLE001
            worker_log.exception(f"[PID:{process_id}] [worker:{worker_id}] Failed restoring {part_path.name} -> {original_name}")
    
    idle_polls = 0
    # max_idle_polls <= -1 means run forever (never exit due to idle)
    if max_idle_polls is not None and max_idle_polls < 0:
        max_idle_polls = None
    poll_interval_seconds = max(poll_interval_seconds, 0.5)
    
    while True:
        claimed = None
        for candidate in _scan_candidates():
            claimed = _claim_file(candidate)
            if claimed:
                break
        
        if not claimed:
            idle_polls += 1
            if max_idle_polls is not None and idle_polls > max_idle_polls:
                worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] No files found after {idle_polls} idle polls. Exiting.")
                return 0
            worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] No files found. Sleeping {poll_interval_seconds:.1f}s before rescan.")
            time.sleep(poll_interval_seconds)
            continue
        
        idle_polls = 0
        part_path, original_name = claimed
        worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] Claimed: {original_name} -> {part_path.name}")
        
        try:
            file_size_mb = part_path.stat().st_size / (1024 * 1024)
            use_indexed_gzip = False
            file_index_path = None
            
            if original_name.endswith(".gz"):
                if file_size_mb >= min_file_size_mb_for_indexed_gzip:
                    use_indexed_gzip = True
                if index_path_base:
                    file_index_path = index_path_base / f"{original_name}.gzi"
                else:
                    file_index_path = part_path.with_name(f"{original_name}.gzi")
            else:
                file_index_path = None
            
            if use_indexed_gzip:
                result = ingest_file_indexed_gzip(
                    connection_string=connection_string,
                    file_path=part_path,
                    source_name=source_name,
                    batch_size=batch_size,
                    show_progress=False,
                    max_array_items=max_array_items,
                    index_path=file_index_path,
                    chunk_size=scan_chunk_read_bytes,
                    max_span_size=max_span_size,
                    num_key_threads=num_key_threads,
                    progress_every_chunks=progress_every_chunks,
                )
            else:
                result = ingest_file(
                    connection_string=connection_string,
                    file_path=part_path,
                    source_name=source_name,
                    batch_size=batch_size,
                    max_array_items=max_array_items,
                )
            
            if result == 0:
                try:
                    new_path = _finalize_ingested_file(part_path, original_name)
                    worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] Successfully ingested and renamed: {part_path.name} -> {new_path.name}")
                except Exception as rename_exc:  # noqa: BLE001
                    worker_log.warning(f"[PID:{process_id}] [worker:{worker_id}] Ingestion succeeded but failed renaming {part_path.name}: {rename_exc}")
                worker_log.info(f"[PID:{process_id}] [worker:{worker_id}] Successfully ingested: {original_name}")
            else:
                worker_log.error(f"[PID:{process_id}] [worker:{worker_id}] Failed to ingest {original_name}")
                _restore_original(part_path, original_name)
        except Exception as exc:  # noqa: BLE001
            worker_log.exception(f"[PID:{process_id}] [worker:{worker_id}] Error ingesting {original_name}: {exc}")
            _restore_original(part_path, original_name)


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
    poll_interval_seconds = ingestion_cfg.get("poll_interval_seconds", 2.0)
    max_idle_polls = ingestion_cfg.get("max_idle_polls", 2)
    
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
    
    if not input_path.exists():
        LOG.error("Input path does not exist: %s", input_path)
        return 1
    
    # Get source name from config
    run_config = config.get("run_config", {})
    app_config = config.get("application", {})
    source_name = run_config.get("source_name") or app_config.get("source_name") or config.get("payer", "unknown")
    
    if input_path.is_file():
        LOG.info("Ingesting single file into database: %s", input_path)
    else:
        LOG.info("Ingesting files from directory: %s", input_path)
    
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
    
    # Set multiprocessing start method for Windows compatibility
    if sys.platform == "win32":
        multiprocessing.set_start_method("spawn", force=True)
    
    if input_path.is_file():
        file_path = input_path
        original_name = file_path.name
        part_path = file_path.with_name(f"{file_path.name}.part")
        try:
            file_path.rename(part_path)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to claim file %s: %s", file_path, exc)
            return 1
        
        try:
            file_size_mb = part_path.stat().st_size / (1024 * 1024)
            use_indexed_gzip = False
            file_index_path = None
            
            if original_name.endswith(".gz"):
                if file_size_mb >= min_file_size_mb_for_indexed_gzip:
                    use_indexed_gzip = True
                if index_path_base:
                    file_index_path = index_path_base / f"{original_name}.gzi"
                else:
                    file_index_path = part_path.with_name(f"{original_name}.gzi")
            
            if use_indexed_gzip:
                result = ingest_file_indexed_gzip(
                    connection_string=connection_string,
                    file_path=part_path,
                    source_name=source_name,
                    batch_size=batch_size,
                    show_progress=True,
                    max_array_items=max_array_items,
                    index_path=file_index_path,
                    chunk_size=chunk_size,
                    max_span_size=max_span_size,
                    num_key_threads=num_key_threads,
                    progress_every_chunks=progress_every_chunks,
                )
            else:
                result = ingest_file(
                    connection_string=connection_string,
                    file_path=part_path,
                    source_name=source_name,
                    batch_size=batch_size,
                    max_array_items=max_array_items,
                )
            
            if result == 0:
                final_name = f"{PREFIX_INGESTED}{original_name}"
                final_path = part_path.with_name(final_name)
                part_path.replace(final_path)
                LOG.info("Successfully ingested and renamed: %s -> %s", part_path.name, final_path.name)
                return 0
            LOG.error("Failed to ingest %s", original_name)
            part_path.replace(part_path.with_name(original_name))
            return 1
        except Exception as exc:  # noqa: BLE001
            LOG.exception("Error ingesting %s: %s", original_name, exc)
            try:
                part_path.replace(part_path.with_name(original_name))
            except Exception:  # noqa: BLE001
                LOG.exception("Failed restoring %s after error", original_name)
            return 1
    
    LOG.info("Using %d worker process(es) to ingest files", num_workers)
    
    # Prepare worker info tuple
    worker_info = (
        str(input_path),  # input_dir_str
        source_name,
        connection_string,
        batch_size,
        max_array_items,
        0,  # worker_id (set per worker below)
        log_file_path,
        str(index_path_base) if index_path_base else None,
        log_format,
        log_datefmt,
        chunk_size,
        max_span_size,
        num_key_threads,
        progress_every_chunks,
        poll_interval_seconds,
        min_file_size_mb_for_indexed_gzip,
        max_idle_polls,
    )
    
    processes = []
    for worker_id in range(1, num_workers + 1):
        worker_args = list(worker_info)
        worker_args[5] = worker_id
        proc = multiprocessing.Process(target=_ingest_single_file_worker, args=(tuple(worker_args),))
        proc.start()
        processes.append(proc)
    
    for proc in processes:
        proc.join()
    
    failed = [p for p in processes if p.exitcode not in (0, None)]
    if failed:
        LOG.error("One or more workers failed (%d/%d).", len(failed), len(processes))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
