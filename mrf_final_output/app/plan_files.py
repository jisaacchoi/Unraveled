"""Helpers for downloading plan files and splitting large JSON.gz files."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional

from app.api_config import get_config_value
from app.db_utils import get_file_urls_by_plan, get_file_urls_by_file_name
from app.utils import ensure_directory
from src.mrf_downloader import download_file, filename_from_url, session_with_retries
from src.shared.database import build_connection_string
from src.ingest.indexed_gzip_ingester import build_and_export_index
from src.split.indexed_gzip_splitter import split_json_gz_with_indexed_gzip

LOG = logging.getLogger("app.plan_files")


def _get_download_dir(config) -> Optional[Path]:
    download_dir = get_config_value(config, "paths.plan_download_directory", None)
    if not download_dir:
        download_dir = get_config_value(config, "processing.plan_download_directory", None)
    return Path(download_dir) if download_dir else None


def download_files_by_name(config, file_name: str) -> List[Path]:
    """
    Download files referenced by mrf_index.file_url for a given file name.

    Returns list of downloaded file paths (existing files are included).
    """
    enabled = get_config_value(config, "processing.enable_plan_download", True)
    if not enabled:
        LOG.info("Plan download disabled by config")
        return []

    download_dir = _get_download_dir(config)
    if not download_dir:
        LOG.info("Plan download directory not configured; skipping download")
        return []

    file_records = get_file_urls_by_file_name(config, file_name)
    if not file_records:
        LOG.warning(
            "No file_url records found in mrf_index for file_name=%s that are also in mrf_analyzed. "
            "Only analyzed files will be downloaded.",
            file_name
        )
        return []

    ensure_directory(download_dir)

    chunk_size_bytes = int(get_config_value(config, "processing.plan_download_chunk_size_bytes", 16 * 1024))
    max_file_size_gb = 20.0

    sess = session_with_retries()
    downloaded: List[Path] = []
    for url, filename_from_db in file_records:
        # Use filename from database if available, otherwise extract from URL
        filename = filename_from_db if filename_from_db else filename_from_url(url, "plan_file.json.gz")
        dest = download_dir / filename
        if dest.exists():
            LOG.info("File already exists, skipping download: %s", dest)
            downloaded.append(dest)
            continue
        try:
            download_file(sess, url, dest, chunk_size=chunk_size_bytes, max_file_size_gb=max_file_size_gb)
            
            # Ensure .part file is renamed to final file (cleanup if rename failed)
            part_file = dest.with_suffix(dest.suffix + ".part")
            if part_file.exists():
                LOG.warning("Found .part file after download completed, attempting to rename: %s", part_file)
                import time
                max_retries = 5
                retry_delay = 0.2
                for attempt in range(max_retries):
                    try:
                        part_file.replace(dest)
                        LOG.info("Successfully renamed .part file to final file: %s", dest)
                        break
                    except (PermissionError, OSError) as e:
                        if attempt < max_retries - 1:
                            LOG.debug("Retry %d/%d: File still in use, waiting %.1f seconds...", 
                                     attempt + 1, max_retries, retry_delay)
                            time.sleep(retry_delay)
                            retry_delay *= 1.5
                        else:
                            LOG.error("Failed to rename .part file after %d attempts: %s", max_retries, e)
                            raise
            
            # Verify final file exists and .part file is gone
            if not dest.exists():
                raise FileNotFoundError(f"Downloaded file does not exist after download: {dest}")
            if part_file.exists():
                raise RuntimeError(f".part file still exists after download: {part_file}")
            
            downloaded.append(dest)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to download %s: %s", url, exc)
            continue

    return downloaded


def download_plan_files(config, plan_name: str) -> List[Path]:
    """
    Download plan files referenced by mrf_index.file_url for a given plan name.

    Returns list of downloaded file paths (existing files are included).
    """
    enabled = get_config_value(config, "processing.enable_plan_download", True)
    if not enabled:
        LOG.info("Plan download disabled by config")
        return []

    download_dir = _get_download_dir(config)
    if not download_dir:
        LOG.info("Plan download directory not configured; skipping download")
        return []

    file_records = get_file_urls_by_plan(config, plan_name)
    if not file_records:
        LOG.warning(
            "No file_url records found in mrf_index for plan_name=%s that are also in mrf_analyzed. "
            "Only analyzed files will be downloaded.",
            plan_name
        )
        return []

    ensure_directory(download_dir)

    chunk_size_bytes = int(get_config_value(config, "processing.plan_download_chunk_size_bytes", 16 * 1024))
    max_file_size_gb = 20.0

    sess = session_with_retries()
    downloaded: List[Path] = []
    for url, filename_from_db in file_records:
        # Use filename from database if available, otherwise extract from URL
        filename = filename_from_db if filename_from_db else filename_from_url(url, "plan_file.json.gz")
        dest = download_dir / filename
        if dest.exists():
            LOG.info("File already exists, skipping download: %s", dest)
            downloaded.append(dest)
            continue
        try:
            download_file(sess, url, dest, chunk_size=chunk_size_bytes, max_file_size_gb=max_file_size_gb)
            
            # Ensure .part file is renamed to final file (cleanup if rename failed)
            part_file = dest.with_suffix(dest.suffix + ".part")
            if part_file.exists():
                LOG.warning("Found .part file after download completed, attempting to rename: %s", part_file)
                import time
                max_retries = 5
                retry_delay = 0.2
                for attempt in range(max_retries):
                    try:
                        part_file.replace(dest)
                        LOG.info("Successfully renamed .part file to final file: %s", dest)
                        break
                    except (PermissionError, OSError) as e:
                        if attempt < max_retries - 1:
                            LOG.debug("Retry %d/%d: File still in use, waiting %.1f seconds...", 
                                     attempt + 1, max_retries, retry_delay)
                            time.sleep(retry_delay)
                            retry_delay *= 1.5
                        else:
                            LOG.error("Failed to rename .part file after %d attempts: %s", max_retries, e)
                            raise
            
            # Verify final file exists and .part file is gone
            if not dest.exists():
                raise FileNotFoundError(f"Downloaded file does not exist after download: {dest}")
            if part_file.exists():
                raise RuntimeError(f".part file still exists after download: {part_file}")
            
            downloaded.append(dest)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to download %s: %s", url, exc)
            continue

    return downloaded


def ensure_index_for_file(file_path: Path, index_dir: Optional[Path]) -> Optional[Path]:
    """
    Ensure a .gzi index exists for the given .json.gz file.
    Returns the index path if available, else None.
    """
    if index_dir:
        ensure_directory(index_dir)
        index_path = index_dir / f"{file_path.name}.gzi"
    else:
        index_path = file_path.with_suffix(file_path.suffix + ".gzi")

    if index_path.exists():
        return index_path

    try:
        build_and_export_index(str(file_path), str(index_path))
        return index_path
    except Exception as exc:  # noqa: BLE001
        LOG.error("Failed to build index for %s: %s", file_path, exc)
        return None


def split_files_if_needed(
    config,
    file_paths: Iterable[Path],
    structure_roots: Optional[Iterable[Path]] = None,
) -> List[Path]:
    """
    Split large .json.gz files based on config and return updated file paths.
    """
    split_cfg = get_config_value(config, "pipeline.split_files", {}) or {}
    if not split_cfg.get("enabled", False):
        return list(file_paths)

    db_config = get_config_value(config, "database", {}) or {}
    connection_string = build_connection_string(db_config)

    chunk_read_bytes = int(split_cfg.get("split_chunk_read_bytes", 16 * 1024 * 1024))
    size_per_file_mb = float(split_cfg.get("split_size_per_file_mb", 100))
    size_per_file_bytes = int(size_per_file_mb * 1024 * 1024)
    num_array_threads = int(split_cfg.get("num_array_threads", 1))
    log_chunk_progress = bool(split_cfg.get("log_chunk_progress", False))
    adaptive_chunk_target_items = int(split_cfg.get("adaptive_chunk_target_items", 50))
    adaptive_chunk_min_size_mb = float(split_cfg.get("adaptive_chunk_min_size_mb", 1))
    adaptive_chunk_min_size = int(adaptive_chunk_min_size_mb * 1024 * 1024)

    min_file_size_mb = float(split_cfg.get("min_file_size_mb_for_indexed_gzip", 0))
    index_path_cfg = split_cfg.get("index_path")
    index_dir = Path(index_path_cfg) if index_path_cfg else None

    structure_dirs_by_name = {}
    if structure_roots:
        for root in structure_roots:
            if not root or not root.exists() or not root.is_dir():
                continue
            for marker in root.rglob("*"):
                if marker.is_file():
                    structure_dirs_by_name[marker.name] = marker.parent

    updated_paths: List[Path] = []
    for file_path in file_paths:
        if not file_path.exists():
            continue
        if "_part" in file_path.name:
            updated_paths.append(file_path)
            continue
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        if min_file_size_mb > 0 and file_size_mb < min_file_size_mb:
            updated_paths.append(file_path)
            continue

        index_path = ensure_index_for_file(file_path, index_dir)
        if not index_path:
            updated_paths.append(file_path)
            continue

        try:
            output_dir = structure_dirs_by_name.get(file_path.name, file_path.parent)
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
                adaptive_chunk_target_items=adaptive_chunk_target_items,
                adaptive_chunk_min_size=adaptive_chunk_min_size,
            )
            if parts_created > 0:
                try:
                    file_path.rename(file_path.with_name(file_path.name + ".part"))
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Split succeeded but failed to rename %s: %s", file_path, exc)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to split %s: %s", file_path, exc)

        updated_paths.append(file_path)

    return updated_paths
