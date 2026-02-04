"""Core download logic shared by CLI wrappers."""
from __future__ import annotations

import gzip
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
import requests
from requests.adapters import HTTPAdapter, Retry

LOG = logging.getLogger("src.mrf_downloader")


def ensure_mrf_index_table(connection_string: str) -> None:
    """Ensure mrf_index table exists."""
    from src.shared.database import ensure_table_exists
    
    ddl_path = Path(__file__).parent.parent.parent / "sql" / "create_mrf_index_table.sql"
    ensure_table_exists(connection_string, "mrf_index", ddl_path=ddl_path, drop_if_exists=False)


def ensure_mrf_views(connection_string: str) -> None:
    """Ensure MRF views exist by executing create_views.sql."""
    view_sql_path = Path(__file__).parent.parent.parent / "sql" / "create_views.sql"
    
    if not view_sql_path.exists():
        LOG.warning("View SQL file not found at %s, skipping view creation", view_sql_path)
        return
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        LOG.info("Creating MRF views from %s", view_sql_path)
        with open(view_sql_path, "r", encoding="utf-8") as fh:
            view_sql = fh.read()
        
        cursor.execute(view_sql)
        LOG.info("Successfully created MRF views")
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Failed to create MRF views: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_mrf_index_record(
    connection_string: str,
    source_name: str,
    file_metadata: Dict[str, Any],
    file_name: str,
    file_path: Optional[str] = None,
) -> int:
    """
    Insert a record into mrf_index table.
    
    Returns:
        The inserted record ID
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO mrf_index (
                source_name,
                reporting_entity_name,
                reporting_entity_type,
                file_url,
                file_description,
                file_name,
                file_path,
                reporting_plans,
                download_status,
                created_at,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', now(), now())
            RETURNING id
            """,
            (
                source_name,
                file_metadata.get("reporting_entity_name"),
                file_metadata.get("reporting_entity_type"),
                file_metadata.get("url"),
                file_metadata.get("description"),
                file_name,
                file_path,
                json.dumps(file_metadata.get("reporting_plans", [])),
            ),
        )
        record_id = cursor.fetchone()[0]
        return record_id
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error inserting mrf_index record: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_mrf_index_record(
    connection_string: str,
    file_url: str,
    status: str,
    file_path: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update mrf_index record with download status and file information."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Build update query based on what fields are provided
        updates = ["download_status = %s", "updated_at = now()"]
        values = [status]
        
        if status == "downloading":
            updates.append("download_started_at = COALESCE(download_started_at, now())")
        elif status in ("completed", "failed", "skipped"):
            updates.append("download_completed_at = now()")
        
        if file_path is not None:
            updates.append("file_path = %s")
            values.append(file_path)
        
        if file_size_bytes is not None:
            updates.append("file_size_bytes = %s")
            values.append(file_size_bytes)
        
        if error_message is not None:
            updates.append("error_message = %s")
            values.append(error_message)
        
        values.append(file_url)
        
        # Update only records that are still pending or downloading (not already completed/failed/skipped)
        # This prevents updating old/completed records if duplicates exist from running the command multiple times
        query = f"""
            UPDATE mrf_index
            SET {', '.join(updates)}
            WHERE file_url = %s
            AND download_status IN ('pending', 'downloading')
        """
        
        cursor.execute(query, values)
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error updating mrf_index record for %s: %s", file_url, exc)
        # Don't raise - download should continue even if DB update fails
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def session_with_retries(backoff: float = 0.3, retries: int = 5) -> requests.Session:
    """Return a requests session with basic retry settings."""
    sess = requests.Session()
    # Add User-Agent header to avoid blocking by servers that require it
    sess.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    sess.mount("http://", HTTPAdapter(max_retries=retry))
    return sess


def iter_in_network_urls(index_obj: dict) -> Iterable[dict]:
    """
    Yield each in-network file with metadata from a payer index JSON.
    
    Yields dictionaries with keys:
    - url: File URL (location)
    - description: File description
    - reporting_entity_name: Reporting entity name
    - reporting_entity_type: Reporting entity type
    - reporting_plans: List of plan objects (plan_name, plan_id_type, plan_id, plan_market_type)
    """
    reporting_entity_name = index_obj.get("reporting_entity_name")
    reporting_entity_type = index_obj.get("reporting_entity_type")
    
    for block in index_obj.get("reporting_structure", []):
        reporting_plans = block.get("reporting_plans", [])
        for entry in block.get("in_network_files", []):
            url = entry.get("location")
            if url:
                yield {
                    "url": url,
                    "description": entry.get("description"),
                    "reporting_entity_name": reporting_entity_name,
                    "reporting_entity_type": reporting_entity_type,
                    "reporting_plans": reporting_plans,
                }


def filename_from_url(url: str, fallback: str) -> str:
    """Extract filename from URL path, falling back when absent."""
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name
    return name or fallback


def unique_destination(base: Path, filename: str) -> Path:
    """Ensure destination file name is unique within base directory."""
    # Directory creation handled by ensure_directories_from_config()
    dest = base / filename
    if not dest.exists() and not (dest.with_suffix(dest.suffix + ".part")).exists():
        return dest
    stem = dest.stem
    suffix = "".join(dest.suffixes)
    counter = 1
    while True:
        candidate = base / f"{stem}_{counter}{suffix}"
        if not candidate.exists() and not (candidate.with_suffix(candidate.suffix + ".part")).exists():
            return candidate
        counter += 1


def download_file(
    sess: requests.Session,
    url: str,
    dest: Path,
    chunk_size: int = 16 * 1024,
    max_file_size_gb: float = 20.0,
) -> None:
    """
    Download a gzip file with streaming write.
    
    Args:
        sess: Requests session
        url: URL to download
        dest: Destination file path
        chunk_size: Chunk size for streaming
        max_file_size_gb: Maximum file size in GB - if exceeded, download is stopped and file deleted
        
    Raises:
        ValueError: If file size exceeds max_file_size_gb
    """
    # Ensure destination directory exists
    dest.parent.mkdir(parents=True, exist_ok=True)
    LOG.info("Downloading %s -> %s", url, dest)
    max_file_size_bytes = max_file_size_gb * 1024 * 1024 * 1024  # Convert GB to bytes
    check_interval_chunks = 100  # Check file size every 100 chunks
    
    with sess.get(url, stream=True, timeout=120) as resp:
        # Check for 403 Forbidden - often means expired signed URL
        if resp.status_code == 403:
            # Check if URL has Expires parameter (signed URL)
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            expires = params.get('Expires', [None])[0]
            if expires:
                try:
                    expires_timestamp = int(expires)
                    expires_dt = datetime.fromtimestamp(expires_timestamp, tz=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if expires_dt < now:
                        error_msg = f"403 Forbidden: Signed URL expired at {expires_dt.isoformat()} (current time: {now.isoformat()})"
                    else:
                        error_msg = f"403 Forbidden: Signed URL may be invalid (expires at {expires_dt.isoformat()})"
                except (ValueError, TypeError):
                    error_msg = "403 Forbidden: Signed URL may be expired or invalid"
            else:
                error_msg = "403 Forbidden: Access denied (may require authentication or valid session)"
            LOG.warning("%s | URL: %s", error_msg, url)
        
        # Raise for any HTTP error (including 403)
        resp.raise_for_status()
        
        # Check Content-Length header first to avoid downloading files that are too large
        # Store the size in the exception so we can capture it later
        content_length = resp.headers.get('Content-Length')
        file_size_from_header = None
        if content_length:
            try:
                file_size_from_header = int(content_length)
                if file_size_from_header > max_file_size_bytes:
                    # Create a ValueError with the file size included for later extraction
                    error = ValueError(
                        f"File size ({file_size_from_header / (1024**3):.2f} GB) exceeds limit "
                        f"({max_file_size_gb} GB). File not downloaded."
                    )
                    # Store the size as an attribute so we can retrieve it later
                    error.file_size_bytes = file_size_from_header
                    raise error
            except (ValueError, TypeError) as e:
                # If Content-Length is invalid, log and continue with streaming check
                if not isinstance(e, ValueError) or not hasattr(e, 'file_size_bytes'):
                    LOG.debug("Invalid Content-Length header '%s': %s. Will check size during download.", content_length, e)
                else:
                    # Re-raise if it's our size limit error
                    raise
        
        tmp = dest.with_suffix(dest.suffix + ".part")
        chunk_count = 0
        
        try:
            # Write to temporary file
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
                        chunk_count += 1
                        
                        # Check file size periodically (in case Content-Length was missing or wrong)
                        if chunk_count % check_interval_chunks == 0:
                            current_size = tmp.stat().st_size
                            if current_size > max_file_size_bytes:
                                # Close file handle before deleting
                                fh.close()
                                # Small delay to ensure file handle is released on Windows
                                time.sleep(0.1)
                                tmp.unlink(missing_ok=True)
                                raise ValueError(
                                    f"File size ({current_size / (1024**3):.2f} GB) exceeds limit "
                                    f"({max_file_size_gb} GB). Download stopped and file deleted."
                                )
            
            # File handle is now closed, safe to check size and rename
            # Final size check before renaming
            final_size = tmp.stat().st_size
            if final_size > max_file_size_bytes:
                # Small delay to ensure file handle is released on Windows
                time.sleep(0.1)
                tmp.unlink(missing_ok=True)
                raise ValueError(
                    f"File size ({final_size / (1024**3):.2f} GB) exceeds limit "
                    f"({max_file_size_gb} GB). File deleted."
                )
            
            # On Windows, we need to ensure the file handle is fully released
            # Use a retry mechanism with small delays
            max_retries = 5
            retry_delay = 0.2
            for attempt in range(max_retries):
                try:
                    tmp.replace(dest)
                    break  # Success, exit retry loop
                except (PermissionError, OSError) as e:
                    if attempt < max_retries - 1:
                        LOG.debug("Retry %d/%d: File still in use, waiting %.1f seconds...", 
                                 attempt + 1, max_retries, retry_delay)
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                    else:
                        # Last attempt failed, raise the error
                        raise
                        
        except Exception as exc:  # noqa: BLE001
            # Clean up .part file on any error
            # Ensure file handle is closed and wait a bit before cleanup
            time.sleep(0.1)
            try:
                tmp.unlink(missing_ok=True)
            except (PermissionError, OSError):
                # If we can't delete it now, log and continue
                LOG.warning("Could not delete temporary file %s (may be cleaned up later)", tmp)
            raise


def load_index_source(config: dict, sess: requests.Session) -> dict:
    """Load index JSON from URL or local path defined in config.
    
    Supports:
    - Remote URL (index_url): Downloads and parses JSON
    - Local JSON file (index_path): Reads and parses JSON
    - Local JSON.gz file (index_path): Decompresses and parses JSON
    """
    # Support both old and new config structure
    data_source = config.get("data_source", config.get("inputs", {}))
    index_url = data_source.get("index_url")
    index_path = data_source.get("index_path")
    if index_url:
        LOG.info("Fetching remote index %s", index_url)
        return sess.get(index_url, timeout=60).json()
    if index_path:
        index_path_obj = Path(index_path)
        if not index_path_obj.exists():
            raise FileNotFoundError(f"Index file or directory not found: {index_path}")
        
        # Check if file is gzipped
        if index_path_obj.suffix == ".gz" or str(index_path_obj).endswith(".json.gz"):
            LOG.info("Loading local gzipped index %s", index_path)
            with gzip.open(index_path_obj, "rt", encoding="utf-8") as fh:
                return json.load(fh)
        else:
            LOG.info("Loading local index %s", index_path)
            with open(index_path_obj, "r", encoding="utf-8") as fh:
                return json.load(fh)
    raise ValueError("Config must define data_source.index_url or data_source.index_path")


def load_index_sources(
    sess: requests.Session,
    index_url: Optional[str | List[str]] = None,
    index_path: Optional[str | List[str]] = None,
) -> List[dict]:
    """
    Load index JSON files from URL(s), local file(s), or directory/directories.
    
    Supports:
    - Single or list of remote URLs (index_url): Downloads and parses each JSON sequentially
    - Single or list of local JSON/JSON.gz files (index_path): Reads and parses each JSON sequentially
    - Single or list of local directories (index_path): Finds all .json and .json.gz files in each directory
    - Both index_url and index_path can be specified - URLs are processed first, then paths
    
    Args:
        sess: Requests session for downloading URLs
        index_url: Single URL or list of URLs to MRF index JSON files
        index_path: Single path or list of paths to local index files/directories
    
    Returns:
        List of index JSON dictionaries (all sources combined)
    """
    
    index_list = []
    
    # Process index_url (can be a single URL or a list)
    if index_url:
        # Convert single value to list for uniform processing
        if isinstance(index_url, str):
            index_urls = [index_url]
        else:
            index_urls = index_url
        
        LOG.info("Processing %d index URL(s)...", len(index_urls))
        for i, url in enumerate(index_urls, 1):
            try:
                LOG.info("[%d/%d] Fetching remote index: %s", i, len(index_urls), url)
                index_json = sess.get(url, timeout=60).json()
                index_list.append(index_json)
                LOG.info("[%d/%d] Successfully loaded index from URL", i, len(index_urls))
            except Exception as exc:  # noqa: BLE001
                LOG.error("[%d/%d] Failed to fetch index from URL %s: %s", i, len(index_urls), url, exc)
                raise
    
    # Process index_path (can be a single path or a list)
    if index_path:
        # Convert single value to list for uniform processing
        if isinstance(index_path, str):
            index_paths = [index_path]
        else:
            index_paths = index_path
        
        LOG.info("Processing %d index path(s)...", len(index_paths))
        for i, path in enumerate(index_paths, 1):
            index_path_obj = Path(path)
            if not index_path_obj.exists():
                raise FileNotFoundError(f"Index file or directory not found: {path}")
            
            # Check if it's a directory
            if index_path_obj.is_dir():
                LOG.info("[%d/%d] Index path is a directory, searching for index files: %s", i, len(index_paths), path)
                # Find all .json and .json.gz files in the directory
                json_files = list(index_path_obj.glob("*.json"))
                json_gz_files = list(index_path_obj.glob("*.json.gz"))
                all_files = sorted(json_files + json_gz_files)
                
                if not all_files:
                    LOG.warning("[%d/%d] No index files (.json or .json.gz) found in directory: %s (skipping)", 
                               i, len(index_paths), path)
                    continue
                
                LOG.info("[%d/%d] Found %d index file(s) in directory: %s", i, len(index_paths), len(all_files), path)
                
                for file_path in all_files:
                    try:
                        # Check if file is gzipped
                        if file_path.suffix == ".gz" or str(file_path).endswith(".json.gz"):
                            LOG.info("  Loading gzipped index file: %s", file_path.name)
                            with gzip.open(file_path, "rt", encoding="utf-8") as fh:
                                index_list.append(json.load(fh))
                        else:
                            LOG.info("  Loading index file: %s", file_path.name)
                            with open(file_path, "r", encoding="utf-8") as fh:
                                index_list.append(json.load(fh))
                    except Exception as exc:  # noqa: BLE001
                        LOG.error("  Failed to load index file %s: %s (skipping)", file_path.name, exc)
                        LOG.exception("Full error details:")
                        # Continue with other files even if one fails
                        continue
            else:
                # It's a file, not a directory
                try:
                    # Check if file is gzipped
                    if index_path_obj.suffix == ".gz" or str(index_path_obj).endswith(".json.gz"):
                        LOG.info("[%d/%d] Loading local gzipped index: %s", i, len(index_paths), path)
                        with gzip.open(index_path_obj, "rt", encoding="utf-8") as fh:
                            index_list.append(json.load(fh))
                    else:
                        LOG.info("[%d/%d] Loading local index: %s", i, len(index_paths), path)
                        with open(index_path_obj, "r", encoding="utf-8") as fh:
                            index_list.append(json.load(fh))
                except Exception as exc:  # noqa: BLE001
                    LOG.error("[%d/%d] Failed to load index file %s: %s", i, len(index_paths), path, exc)
                    raise
    
    if not index_url and not index_path:
        raise ValueError("Must provide at least one of index_url or index_path")
    
    if not index_list:
        raise ValueError("No index files were successfully loaded")
    
    LOG.info("Successfully loaded %d index source(s) total", len(index_list))
    return index_list


def calculate_directory_size(directory: Path) -> int:
    """
    Calculate total size of all files in a directory (recursive).
    
    Args:
        directory: Directory to calculate size for
        
    Returns:
        Total size in bytes
    """
    total_size = 0
    if not directory.exists():
        return 0
    
    try:
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                try:
                    total_size += file_path.stat().st_size
                except (OSError, PermissionError):
                    # Skip files we can't access
                    continue
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Error calculating directory size for %s: %s", directory, exc)
    
    return total_size


def check_and_poll_for_size(
    input_directory: Path,
    max_total_size_gb: float,
) -> bool:
    """
    Check if directory size exceeds limit.
    
    Note: Polling is now handled externally by run_pipeline.py. This function
    only performs a single check and returns immediately.
    
    Args:
        input_directory: Directory to check
        max_total_size_gb: Maximum total size in GB
        
    Returns:
        True if should continue (size is under limit), False if should stop (over limit)
    """
    max_total_size_bytes = max_total_size_gb * 1024 * 1024 * 1024
    
    # Check size once
    total_size = calculate_directory_size(input_directory)
    total_size_gb = total_size / (1024 * 1024 * 1024)
    
    if total_size <= max_total_size_bytes:
        LOG.debug("Directory size check: %.2f GB (under limit of %.2f GB)", total_size_gb, max_total_size_gb)
        return True
    
    LOG.warning(
        "Directory size exceeds limit: %.2f GB > %.2f GB. Exiting to allow external polling.",
        total_size_gb, max_total_size_gb
    )
    return False


def download_urls_chunk(
    url_tasks: List[tuple[Dict[str, Any], str, int, Path]],
    base: Path,
    chunk_size: int,
    max_file_size_gb: float,
    stats: dict,
    stats_lock: threading.Lock,
    connection_string: Optional[str] = None,
    input_directory: Optional[Path] = None,
    max_total_size_gb: Optional[float] = None,
    stop_flag: Optional[threading.Event] = None,
) -> None:
    """
    Download a chunk of URLs sequentially.
    
    Args:
        url_tasks: List of (file_metadata, url, index, dest) tuples to download
        base: Base directory for downloads
        chunk_size: Chunk size for streaming
        max_file_size_gb: Maximum file size in GB
        stats: Shared stats dictionary (completed, failed, skipped, downloaded_files)
        stats_lock: Lock for thread-safe stats updates
        connection_string: Optional database connection string for updating mrf_index table
        input_directory: Directory to check total size against (if max_total_size_gb is set)
        max_total_size_gb: Maximum total size in GB for all files in input_directory (None to disable)
        stop_flag: Threading event to signal stopping (if set, will stop when triggered)
    """
    sess = session_with_retries()
    
    for file_metadata, url, idx, dest in url_tasks:
        # Check if stop flag is set
        if stop_flag and stop_flag.is_set():
            LOG.info("Stop flag set, stopping download chunk")
            break
        # Update status to downloading
        if connection_string:
            try:
                update_mrf_index_record(connection_string, url, "downloading")
            except Exception:  # noqa: BLE001
                pass  # Continue with download even if DB update fails
        
        try:
            download_file(sess, url, dest, chunk_size, max_file_size_gb)
            
            # Update status to completed
            file_size_bytes = dest.stat().st_size
            relative_path = str(dest.relative_to(base.parent.parent)) if base.parent.parent in dest.parents else str(dest)
            if connection_string:
                try:
                    update_mrf_index_record(
                        connection_string,
                        url,
                        "completed",
                        file_path=relative_path,
                        file_size_bytes=file_size_bytes,
                    )
                except Exception:  # noqa: BLE001
                    pass  # Continue even if DB update fails
            
            # Check total directory size after each download (if enabled)
            if max_total_size_gb is not None and input_directory:
                total_size = calculate_directory_size(input_directory)
                total_size_gb = total_size / (1024 * 1024 * 1024)
                max_total_size_bytes = max_total_size_gb * 1024 * 1024 * 1024
                
                if total_size > max_total_size_bytes:
                    LOG.warning(
                        "Directory size exceeds limit after downloading %s: %.2f GB > %.2f GB. Setting stop flag.",
                        dest.name, total_size_gb, max_total_size_gb
                    )
                    if stop_flag:
                        stop_flag.set()
                    break  # Stop this thread's downloads
            
            # Update stats on success
            with stats_lock:
                stats["completed"] += 1
                stats["downloaded_files"].append(dest)
                completed = stats["completed"]
                failed = stats["failed"]
                skipped = stats["skipped"]
                total = stats["total"]
                remaining = total - completed - failed - skipped
                
                file_size_mb = dest.stat().st_size / (1024 * 1024)
                LOG.info(
                    "✓ Downloaded successfully: %s -> %s (%.2f MB) | Progress: %d/%d completed, %d remaining, %d failed, %d skipped",
                    url,
                    dest.name,
                    file_size_mb,
                    completed,
                    total,
                    remaining,
                    failed,
                    skipped,
                )
        except ValueError as exc:
            # File size limit exceeded
            if connection_string:
                try:
                    # Try to get file size from exception attribute, error message, or file
                    file_size_bytes = None
                    
                    # First, check if the exception has the file_size_bytes attribute (from Content-Length check)
                    if hasattr(exc, 'file_size_bytes'):
                        file_size_bytes = exc.file_size_bytes
                    
                    # If not, try to extract size from error message (e.g., "File size (25.5 GB) exceeds limit")
                    if file_size_bytes is None:
                        import re
                        size_match = re.search(r'File size \(([\d.]+) GB\)', str(exc))
                        if size_match:
                            try:
                                file_size_bytes = int(float(size_match.group(1)) * 1024 * 1024 * 1024)
                            except (ValueError, TypeError):
                                pass
                    
                    # If still not found, try to get from file if it exists (partial download)
                    if file_size_bytes is None and dest.exists():
                        try:
                            file_size_bytes = dest.stat().st_size
                        except Exception:  # noqa: BLE001
                            pass  # If we can't get size, continue without it
                    
                    # If still None, log a warning
                    if file_size_bytes is None:
                        LOG.warning("Could not determine file size for skipped file: %s", url)
                    
                    update_mrf_index_record(
                        connection_string,
                        url,
                        "skipped",
                        file_path=relative_path if dest.exists() else None,
                        file_size_bytes=file_size_bytes,
                        error_message=str(exc),
                    )
                except Exception:  # noqa: BLE001
                    pass
            
            with stats_lock:
                stats["skipped"] += 1
                completed = stats["completed"]
                failed = stats["failed"]
                skipped = stats["skipped"]
                total = stats["total"]
                remaining = total - completed - failed - skipped
                
                file_dropped = not dest.exists()
                if file_dropped:
                    LOG.warning(
                        "✗ Size limit exceeded (file dropped): %s | File size exceeded %.1f GB limit, download stopped and file deleted | Progress: %d/%d completed, %d remaining, %d failed, %d skipped",
                        url,
                        max_file_size_gb,
                        completed,
                        total,
                        remaining,
                        failed,
                        skipped,
                    )
                else:
                    LOG.warning(
                        "✗ Size limit exceeded (file kept): %s | File size exceeded %.1f GB limit | Progress: %d/%d completed, %d remaining, %d failed, %d skipped",
                        url,
                        max_file_size_gb,
                        completed,
                        total,
                        remaining,
                        failed,
                        skipped,
                    )
        except Exception as exc:  # noqa: BLE001
            # Download failed
            # Update status to failed
            if connection_string:
                try:
                    # Try to get file size if partial file exists
                    file_size_bytes = None
                    if dest.exists():
                        try:
                            file_size_bytes = dest.stat().st_size
                        except Exception:  # noqa: BLE001
                            pass  # If we can't get size, continue without it
                    
                    update_mrf_index_record(
                        connection_string,
                        url,
                        "failed",
                        file_path=relative_path if dest.exists() else None,
                        file_size_bytes=file_size_bytes,
                        error_message=str(exc),
                    )
                except Exception:  # noqa: BLE001
                    pass  # Continue even if DB update fails
            
            with stats_lock:
                stats["failed"] += 1
                completed = stats["completed"]
                failed = stats["failed"]
                skipped = stats["skipped"]
                total = stats["total"]
                remaining = total - completed - failed - skipped
                
                LOG.exception(
                    "✗ Download failed: %s | Error: %s | Progress: %d/%d completed, %d remaining, %d failed, %d skipped",
                    url,
                    exc,
                    completed,
                    total,
                    remaining,
                    failed,
                    skipped,
                )


def run_download(
    payer: str,
    output_root: Path,
    folder_name: str,
    index_url: Optional[str | List[str]] = None,
    index_path: Optional[str | List[str]] = None,
    threads: int = 4,
    chunk_size: int = 16 * 1024,
    max_file_size_gb: Optional[float] = None,
    max_total_size_gb: Optional[float] = None,
    connection_string: Optional[str] = None,
) -> int:
    """
    Download each MRF url found in the index using optional multi-threading.
    
    Note: Polling for size limits is now handled externally by run_pipeline.py.
    If size limit is exceeded, this function returns exit code 1 and the caller
    should handle polling/retries.
    
    Args:
        payer: Source/payer identifier
        output_root: Base directory where downloaded files are stored
        folder_name: Subfolder name within output_root
        index_url: Single URL or list of URLs to MRF index JSON files
        index_path: Single path or list of paths to local index files/directories
        threads: Number of parallel download threads
        chunk_size: Chunk size in bytes for streaming downloads
        max_file_size_gb: Maximum file size in GB (None to disable)
        max_total_size_gb: Maximum total size in GB for all files (None to disable)
        connection_string: Database connection string for mrf_index table (None to disable)
    """
    if not folder_name:
        raise ValueError("folder_name must be provided")
    
    if max_file_size_gb is None:
        max_file_size_gb = 20.0  # Default if not specified

    sess = session_with_retries()
    index_json_list = load_index_sources(sess, index_url=index_url, index_path=index_path)

    # Collect all file metadata from all index files
    LOG.info("Collecting in-network file metadata from %d index file(s)...", len(index_json_list))
    file_metadata_list: List[Dict[str, Any]] = []
    for idx, index_json in enumerate(index_json_list, 1):
        LOG.info("Processing index file %d/%d...", idx, len(index_json_list))
        file_metadata_list.extend(iter_in_network_urls(index_json))
    
    index_file_count = len(file_metadata_list)
    
    if not file_metadata_list:
        LOG.warning("No in-network URLs found in index")
        return 1
    
    LOG.info("Found %d in-network file(s) in index file", index_file_count)
    LOG.info("Maximum file size limit: %.1f GB", max_file_size_gb)

    # Setup database connection for mrf_index table if provided
    if connection_string:
        try:
            ensure_mrf_index_table(connection_string)
            ensure_mrf_views(connection_string)
            LOG.info("Using database connection for mrf_index table")
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Could not setup database, mrf_index tracking will be disabled: %s", exc)
            connection_string = None
    else:
        LOG.info("Database connection not provided, mrf_index tracking will be disabled")

    base = output_root / payer / folder_name
    # Ensure the download directory exists
    base.mkdir(parents=True, exist_ok=True)
    LOG.info("Download directory: %s", base)

    # Prepare URL tasks with metadata, indices and destinations
    url_tasks: List[tuple[Dict[str, Any], str, int, Path]] = []
    
    # Batch insert records into mrf_index table if database connection is available
    if connection_string:
        conn = None
        cursor = None
        try:
            LOG.info("Inserting %d records into mrf_index table...", index_file_count)
            conn = psycopg2.connect(connection_string)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Verify table exists and check its structure
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'mrf_index'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            LOG.debug("mrf_index table columns: %s", columns)
            
            # Check for unique constraints
            cursor.execute("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints 
                WHERE table_name = 'mrf_index' 
                AND constraint_type = 'UNIQUE'
            """)
            constraints = cursor.fetchall()
            LOG.info("Unique constraints on mrf_index: %s", constraints)
            
            if not constraints:
                LOG.warning("No unique constraints found on mrf_index table. The ON CONFLICT clause may fail.")
            
            # Get count before insert to verify how many were actually inserted
            cursor.execute("SELECT COUNT(*) FROM mrf_index WHERE source_name = %s", (payer,))
            count_before = cursor.fetchone()[0]
            LOG.info("Current record count in mrf_index for source '%s': %d", payer, count_before)
            
            batch_data = []
            for idx, file_metadata in enumerate(file_metadata_list, start=1):
                url = file_metadata["url"]
                filename = filename_from_url(url, f"file_{idx:04d}.json.gz")
                dest = unique_destination(base, filename)
                relative_path = str(dest.relative_to(output_root)) if output_root in dest.parents else str(dest)
                
                batch_data.append((
                    payer,
                    file_metadata.get("reporting_entity_name"),
                    file_metadata.get("reporting_entity_type"),
                    file_metadata.get("url"),
                    file_metadata.get("description"),
                    filename,
                    relative_path,
                    json.dumps(file_metadata.get("reporting_plans", [])),
                ))
            
            # Batch insert using execute_batch for better performance
            # Use ON CONFLICT DO NOTHING to prevent duplicate records if command is run multiple times
            # Process in smaller batches to avoid potential issues with large batches
            from psycopg2.extras import execute_batch
            insert_sql = """
                INSERT INTO mrf_index (
                    source_name,
                    reporting_entity_name,
                    reporting_entity_type,
                    file_url,
                    file_description,
                    file_name,
                    file_path,
                    reporting_plans,
                    download_status,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', now(), now())
                ON CONFLICT (source_name, file_url) DO NOTHING
            """
            
            # Process in batches of 1000 to avoid potential memory/performance issues
            batch_size = 1000
            total_inserted = 0
            errors_encountered = []
            for i in range(0, len(batch_data), batch_size):
                batch_chunk = batch_data[i:i + batch_size]
                try:
                    execute_batch(cursor, insert_sql, batch_chunk, page_size=100)
                    total_inserted += len(batch_chunk)
                    if (i + batch_size) % 5000 == 0 or (i + batch_size) >= len(batch_data):
                        LOG.info("Processed %d/%d records for batch insert...", min(i + batch_size, len(batch_data)), len(batch_data))
                except Exception as batch_exc:  # noqa: BLE001
                    error_msg = str(batch_exc)
                    errors_encountered.append((i, error_msg))
                    LOG.error("Error inserting batch %d-%d: %s", i, min(i + batch_size, len(batch_data)), batch_exc)
                    LOG.exception("Full traceback for batch insert error:")
                    # Try inserting first record individually to see the exact error
                    if len(batch_chunk) > 0:
                        try:
                            test_record = batch_chunk[0]
                            cursor.execute(insert_sql, test_record)
                            LOG.info("Single record insert test succeeded")
                        except Exception as single_exc:  # noqa: BLE001
                            LOG.error("Single record insert test failed: %s", single_exc)
                            LOG.error("Test record data: %s", test_record[:3])  # Log first 3 fields
                    # Continue with next batch
            
            # Verify actual count after insert
            cursor.execute("SELECT COUNT(*) FROM mrf_index WHERE source_name = %s", (payer,))
            count_after = cursor.fetchone()[0]
            actually_inserted = count_after - count_before
            
            if errors_encountered:
                LOG.error("Batch insert encountered %d errors. First few errors:", len(errors_encountered))
                for batch_idx, error_msg in errors_encountered[:5]:
                    LOG.error("  Batch %d: %s", batch_idx, error_msg)
                if len(errors_encountered) > 5:
                    LOG.error("  ... and %d more errors", len(errors_encountered) - 5)
            
            if actually_inserted == 0 and len(batch_data) > 0:
                # Check if records already exist (this is OK - we can proceed with existing records)
                if count_after == count_before:
                    LOG.info(
                        "All %d records already exist in mrf_index (no new records to insert). "
                        "Proceeding with existing records for downloads.",
                        len(batch_data)
                    )
                else:
                    # This shouldn't happen, but log it as a warning
                    LOG.warning(
                        "No records were inserted, but count changed unexpectedly. "
                        "Attempted: %d, Inserted: %d, Before: %d, After: %d.",
                        len(batch_data),
                        actually_inserted,
                        count_before,
                        count_after
                    )
            elif actually_inserted < len(batch_data):
                LOG.warning(
                    "Batch insert completed: %d records attempted, %d actually inserted, %d already existed. "
                    "Before: %d, After: %d",
                    len(batch_data),
                    actually_inserted,
                    len(batch_data) - actually_inserted,
                    count_before,
                    count_after
                )
            else:
                LOG.info("Successfully inserted %d records into mrf_index table", actually_inserted)
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed to batch insert mrf_index records: %s", exc)
            LOG.exception("Full error details:")
            LOG.error("Cannot proceed without mrf_index records. Exiting.")
            return 1
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    # Query mrf_index table to get the list of URLs to download (source of truth)
    # MANDATORY: Only use database, no fallback to index file
    if not connection_string:
        LOG.error("Database connection is required. Cannot proceed without mrf_index table.")
        return 1
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Verify records were inserted
        cursor.execute("SELECT COUNT(*) FROM mrf_index WHERE source_name = %s", (payer,))
        db_count = cursor.fetchone()[0]
        
        if db_count == 0:
            LOG.error("CRITICAL: mrf_index table is empty for source '%s'. No records were inserted. Cannot proceed with downloads.", payer)
            LOG.error("Please check the batch insert logs above for errors.")
            return 1
        
        LOG.info("Found %d records in mrf_index table for source '%s'", db_count, payer)
        
        # Query for pending downloads (or all if we want to retry failed ones)
        cursor.execute("""
            SELECT id, file_url, file_name, reporting_entity_name, reporting_entity_type, 
                   file_description, reporting_plans
            FROM mrf_index 
            WHERE source_name = %s 
            AND download_status NOT IN ('completed', 'skipped')
            ORDER BY id
        """, (payer,))
        
        db_records = cursor.fetchall()
        total_urls = len(db_records)
        LOG.info("Found %d URLs to download from mrf_index table (status: pending or failed)", total_urls)
        
        if total_urls == 0:
            LOG.info("No pending or failed downloads found in mrf_index. All downloads may already be completed.")
            return 0
        
        # Build url_tasks from database records ONLY
        for db_record in db_records:
            record_id, url, filename, reporting_entity_name, reporting_entity_type, file_description, reporting_plans = db_record
            
            # Use filename from database, or generate from URL if not set
            if not filename or filename == '':
                filename = filename_from_url(url, f"file_{record_id:04d}.json.gz")
            
            dest = unique_destination(base, filename)
            
            # Reconstruct file_metadata from database record
            # Note: reporting_plans is already a Python object (list/dict) from JSONB, not a JSON string
            file_metadata = {
                "url": url,
                "description": file_description,
                "reporting_entity_name": reporting_entity_name,
                "reporting_entity_type": reporting_entity_type,
                "reporting_plans": reporting_plans if reporting_plans else [],
            }
            
            url_tasks.append((file_metadata, url, record_id, dest))
        
    except Exception as exc:  # noqa: BLE001
        LOG.error("Failed to query mrf_index table: %s", exc)
        LOG.exception("Full error details:")
        return 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Update total_urls to match url_tasks length
    total_urls = len(url_tasks)
    
    if total_urls == 0:
        LOG.info("No URLs to download (all may already be completed)")
        return 0

    # Get input directory for size checks (same as base where files are downloaded)
    input_directory = base if max_total_size_gb is not None else None
    
    if max_total_size_gb is not None:
        LOG.info("Total size limit enabled: %.2f GB (polling handled externally by run_pipeline.py)", 
                max_total_size_gb)
        
        # Check size before starting downloads (if enabled)
        # Note: Polling is now handled externally, so we just check once and exit if over limit
        should_continue = check_and_poll_for_size(
            input_directory,
            max_total_size_gb,
        )
        if not should_continue:
            LOG.error("Directory size exceeds limit. Exiting to allow external polling.")
            return 1
    
    # Download loop
    remaining_url_tasks = url_tasks
    download_round = 0
    
    while remaining_url_tasks:
        download_round += 1
        if download_round > 1:
            LOG.info("Starting download round %d (%d files remaining)", download_round, len(remaining_url_tasks))
        
        # Split remaining URLs into chunks for each thread
        chunk_size_per_thread = (len(remaining_url_tasks) + threads - 1) // threads  # Ceiling division
        url_chunks: List[List[tuple[str, int, Path]]] = []
        for i in range(0, len(remaining_url_tasks), chunk_size_per_thread):
            chunk = remaining_url_tasks[i:i + chunk_size_per_thread]
            if chunk:
                url_chunks.append(chunk)

        LOG.info("Starting threaded download (%d files, %d threads, %d chunks)", 
                len(remaining_url_tasks), threads, len(url_chunks))

        # Shared stats for all threads
        stats = {
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total": len(remaining_url_tasks),
            "downloaded_files": [],
        }
        stats_lock = threading.Lock()
        stop_flag = threading.Event()  # Flag to signal threads to stop

        # Start threads, each processing its chunk sequentially
        thread_list = []
        for chunk in url_chunks:
            thread = threading.Thread(
                target=download_urls_chunk,
                args=(
                    chunk, base, chunk_size, max_file_size_gb, stats, stats_lock, connection_string,
                    input_directory, max_total_size_gb, stop_flag
                ),
            )
            thread.start()
            thread_list.append(thread)

        # Wait for all threads to complete
        for thread in thread_list:
            thread.join()
        
        # Check if we stopped due to size limit
        if stop_flag.is_set() and max_total_size_gb is not None:
            # Filter out completed downloads from remaining tasks
            completed_count = stats["completed"]
            if completed_count > 0:
                # Remove completed tasks (approximate - we don't track which specific ones completed)
                # In practice, we'll re-query the database to get remaining pending downloads
                LOG.info("Size limit reached. %d files downloaded in this round. Will poll and retry remaining files.", 
                        completed_count)
                
                # Re-query database to get remaining pending downloads
                conn = None
                cursor = None
                try:
                    conn = psycopg2.connect(connection_string)
                    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                    cursor = conn.cursor()
                    
                    cursor.execute(
                        """
                        SELECT file_url, file_name
                        FROM mrf_index
                        WHERE source_name = %s AND download_status = 'pending'
                        ORDER BY id
                        """,
                        (payer,)
                    )
                    remaining_records = cursor.fetchall()
                    
                    if not remaining_records:
                        LOG.info("No remaining files to download")
                        remaining_url_tasks = []
                        break
                    
                    # Rebuild url_tasks from remaining database records
                    # We need to match them back to file_metadata, but we can use the URL
                    remaining_url_tasks = []
                    for url, filename in remaining_records:
                        # Find matching task from original list
                        for task in url_tasks:
                            if task[1] == url:  # task[1] is the URL
                                remaining_url_tasks.append(task)
                                break
                    
                    LOG.info("Found %d remaining files to download after size limit check", len(remaining_url_tasks))
                except Exception as exc:  # noqa: BLE001
                    LOG.error("Failed to query remaining downloads: %s", exc)
                    # Fall back to continuing with current remaining_url_tasks
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                
                # After re-querying, poll and check size
                should_continue = check_and_poll_for_size(
                    input_directory,
                    max_total_size_gb,
                    polling_wait_minutes,
                    polling_num_additional_attempts,
                )
                if not should_continue:
                    LOG.error("Stopping downloads completely due to directory size limit")
                    return 1
                # Continue with remaining_url_tasks (already updated from database query)
            else:
                # No files completed, check size and poll
                should_continue = check_and_poll_for_size(
                    input_directory,
                    max_total_size_gb,
                    polling_wait_minutes,
                    polling_num_additional_attempts,
                )
                if not should_continue:
                    LOG.error("Stopping downloads completely due to directory size limit")
                    return 1
                # Continue with same remaining_url_tasks
        else:
            # Normal completion or no size limit - all done
            remaining_url_tasks = []

    # Log summary of files in the download directory
    files_remaining = list(base.glob("*.json.gz")) if base.exists() else []
    LOG.info("Files in download directory '%s': %d file(s)", base, len(files_remaining))
    if files_remaining:
        LOG.info("Sample files: %s", ", ".join([f.name for f in files_remaining[:5]]))
    
    # Verify counts against database
    if connection_string:
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(connection_string)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Get counts by status from database
            cursor.execute("""
                SELECT download_status, COUNT(*) 
                FROM mrf_index 
                WHERE source_name = %s
                GROUP BY download_status
            """, (payer,))
            
            db_status_counts = dict(cursor.fetchall())
            db_total = sum(db_status_counts.values())
            db_completed = db_status_counts.get('completed', 0)
            db_failed = db_status_counts.get('failed', 0)
            db_skipped = db_status_counts.get('skipped', 0)
            db_pending = db_status_counts.get('pending', 0)
            
            LOG.info(
                "Completed download batch into %s | Total: %d, Completed: %d, Failed: %d, Skipped: %d",
                base,
                total_urls,
                stats["completed"],
                stats["failed"],
                stats["skipped"],
            )
            LOG.info(
                "Database verification (mrf_index): Total: %d, Completed: %d, Failed: %d, Skipped: %d, Pending: %d",
                db_total,
                db_completed,
                db_failed,
                db_skipped,
                db_pending,
            )
            
            # Verify that stats match database (allowing for pending downloads that weren't processed)
            if stats["completed"] + stats["failed"] + stats["skipped"] == total_urls:
                LOG.info("✓ Download stats match: All %d URLs from mrf_index were processed", total_urls)
            else:
                LOG.warning(
                    "⚠ Download stats mismatch: Processed %d/%d URLs (completed: %d, failed: %d, skipped: %d)",
                    stats["completed"] + stats["failed"] + stats["skipped"],
                    total_urls,
                    stats["completed"],
                    stats["failed"],
                    stats["skipped"],
                )
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Could not verify counts against database: %s", exc)
            LOG.info(
                "Completed download batch into %s | Total: %d, Completed: %d, Failed: %d, Skipped: %d",
                base,
                total_urls,
                stats["completed"],
                stats["failed"],
                stats["skipped"],
            )
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    else:
        LOG.info(
            "Completed download batch into %s | Total: %d, Completed: %d, Failed: %d, Skipped: %d",
            base,
            total_urls,
            stats["completed"],
            stats["failed"],
            stats["skipped"],
        )
    
    # Return 0 on success (no failed downloads), 1 if any downloads failed
    # Skipped files (e.g., size limit exceeded, missing "in-network" in filename) are expected and don't indicate failure
    return 0 if stats["failed"] == 0 else 1

