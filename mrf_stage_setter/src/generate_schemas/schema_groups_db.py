"""Database operations for schema groups table."""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2

LOG = logging.getLogger("src.schema_groups_db")


def load_sql_file(filename: str) -> str:
    """
    Load SQL query from a file in the sql/ directory.
    
    Args:
        filename: Name of the SQL file (e.g., "create_schema_groups_table.sql")
        
    Returns:
        SQL query as string
    """
    sql_dir = Path(__file__).parent.parent.parent / "sql"
    sql_path = sql_dir / filename
    
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(sql_path, "r", encoding="utf-8") as fh:
        return fh.read().strip()


def ensure_schema_groups_table(
    connection_string: str,
    refresh: bool = False,
) -> None:
    """
    Ensure schema_groups table exists, creating it if it doesn't.
    
    If refresh is True, drops the table first and recreates it.
    
    Args:
        connection_string: PostgreSQL connection string
        refresh: If True, drop and recreate the table
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        if refresh:
            # Drop table if it exists
            cursor.execute("DROP TABLE IF EXISTS schema_groups;")
            LOG.info("Dropped schema_groups table (refresh requested)")
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'schema_groups'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # Create table if it doesn't exist
            create_sql = load_sql_file("create_schema_groups_table.sql")
            cursor.execute(create_sql)
            LOG.info("Created schema_groups table")
        else:
            LOG.debug("schema_groups table already exists")
            # Always refresh the data by truncating and repopulating
            # This ensures the table has the latest data from mrf_analysis
            LOG.info("Refreshing schema_groups table data from mrf_analysis")
            cursor.execute("TRUNCATE TABLE schema_groups;")
            
            # Extract the SELECT part from the CREATE TABLE AS statement
            create_sql = load_sql_file("create_schema_groups_table.sql")
            # The SQL file uses "CREATE TABLE IF NOT EXISTS schema_groups AS" followed by the SELECT
            # We need to extract just the SELECT part (everything after "AS")
            if "AS" in create_sql.upper():
                # Find the AS keyword and get everything after it
                as_index = create_sql.upper().find("AS")
                select_part = create_sql[as_index + 2:].strip()  # +2 to skip "AS"
                # Remove trailing semicolon if present
                if select_part.endswith(";"):
                    select_part = select_part[:-1]
                # Execute INSERT INTO ... SELECT to repopulate
                cursor.execute(f"INSERT INTO schema_groups {select_part}")
                LOG.info("Refreshed schema_groups table data")
            else:
                LOG.warning("Could not parse CREATE TABLE AS statement, skipping data refresh")
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error ensuring schema_groups table exists: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_schema_groups(
    connection_string: str,
    refresh: bool = False,
) -> List[Tuple[str, str, Optional[int], bool, int]]:
    """
    Get schema groups from schema_groups table.
    
    Groups files by schema signature (distinct paths) and identifies the minimum
    file (by size) in each group to use for schema inference.
    
    Args:
        connection_string: PostgreSQL connection string
        refresh: If True, drop and recreate the schema_groups table before querying
        
    Returns:
        List of tuples: (schema_sig, min_file_name, min_file_name_size, has_urls, file_count) where:
        - schema_sig: Schema signature (comma-separated distinct paths)
        - min_file_name: Name of the smallest file in the group (for schema inference)
        - min_file_name_size: Size of the smallest file in bytes (None if not available)
        - has_urls: Boolean indicating if any files in the group contain URLs
        - file_count: Number of files in this schema group
    """
    conn = None
    cursor = None
    
    try:
        # Ensure table exists (and refresh if requested)
        ensure_schema_groups_table(connection_string, refresh=refresh)
        
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Load SQL query from file
        query_sql = load_sql_file("select_schema_groups_aggregated.sql")
        cursor.execute(query_sql)
        
        groups = []
        for row in cursor.fetchall():
            schema_sig, min_file_name, min_file_name_size, has_urls, file_count = row
            groups.append((schema_sig, min_file_name, min_file_name_size, has_urls, file_count))
        
        LOG.info("Found %d schema groups", len(groups))
        return groups
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying schema groups from database: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_files_for_schema_group(
    connection_string: str,
    schema_sig: str,
) -> List[Tuple[str, Optional[int], bool]]:
    """
    Get all files for a specific schema group.
    
    Args:
        connection_string: PostgreSQL connection string
        schema_sig: Schema signature to get files for
        
    Returns:
        List of tuples: (file_name, file_size, has_urls) for all files in the group
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Load SQL query from file
        query_sql = load_sql_file("select_files_by_schema_signature.sql")
        cursor.execute(query_sql, (schema_sig,))
        
        files = []
        for row in cursor.fetchall():
            file_name, file_size, has_urls = row
            files.append((file_name, file_size, has_urls))
        
        LOG.debug("Found %d file(s) for schema group %s", len(files), schema_sig[:50])
        return files
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error querying files for schema group %s: %s", schema_sig[:50], exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def normalize_file_name_for_db(file_name: str) -> str:
    """
    Convert NDJSON filename to original .json.gz filename for database queries.
    
    The database stores file names with .json.gz extension, but we may be
    processing .ndjson files. This function converts .ndjson -> .json.gz.
    
    Args:
        file_name: File name (may be .ndjson or .json.gz)
        
    Returns:
        File name with .json.gz extension
    """
    if file_name.endswith(".ndjson"):
        # Convert .ndjson to .json.gz
        return file_name[:-7] + ".json.gz"
    elif file_name.endswith(".json.gz"):
        # Already in correct format
        return file_name
    else:
        # If no extension match, try appending .json.gz
        LOG.warning("File name '%s' doesn't have expected extension, assuming .json.gz", file_name)
        if not file_name.endswith(".gz"):
            return file_name + ".json.gz"
        return file_name


def get_unique_min_file_names(connection_string: str) -> set[str]:
    """
    Get unique min_file_name values from schema_groups table.
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        Set of unique min_file_name values (excluding None)
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT min_file_name FROM schema_groups WHERE min_file_name IS NOT NULL")
        results = cursor.fetchall()
        
        return {row[0] for row in results}
    except psycopg2.errors.UndefinedTable:  # noqa: F405
        # Table doesn't exist yet, return empty set
        return set()
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting unique min_file_names: %s", exc)
        return set()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_non_min_file_names(connection_string: str) -> list[str]:
    """
    Get all file names from schema_groups table that are NOT min_file_name.
    
    These are files that were analyzed and grouped but are not used for schema generation.
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        List of file names that are not min_file_name
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT file_name FROM schema_groups WHERE is_min_file = false")
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    except psycopg2.errors.UndefinedTable:  # noqa: F405
        # Table doesn't exist yet, return empty list
        return []
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting non-min file names: %s", exc)
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_all_file_names_from_schema_groups(connection_string: str) -> list[str]:
    """
    Get all file names from schema_groups table (both min and non-min files).
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        List of all distinct file names in schema_groups
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT file_name FROM schema_groups")
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    except psycopg2.errors.UndefinedTable:  # noqa: F405
        # Table doesn't exist yet, return empty list
        return []
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting all file names from schema_groups: %s", exc)
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_files_grouped_by_min_file_name(connection_string: str) -> dict[str, list[str]]:
    """
    Get all files grouped by their min_file_name (schema group).
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        Dictionary mapping min_file_name (without extension) to list of file_names in that group
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Get all files grouped by min_file_name
        cursor.execute("""
            SELECT DISTINCT 
                min_file_name,
                file_name
            FROM schema_groups
            WHERE min_file_name IS NOT NULL
            ORDER BY min_file_name, file_name
        """)
        
        groups = {}
        for row in cursor.fetchall():
            min_file_name, file_name = row
            # Remove extension from min_file_name for folder name
            # Handle various extensions: .json.gz, .json, .gz, etc.
            folder_name = min_file_name
            # Remove extension - check for common extensions
            if folder_name.endswith(".json.gz"):
                folder_name = folder_name[:-8]  # Remove .json.gz
            elif folder_name.endswith(".ndjson"):
                folder_name = folder_name[:-7]  # Remove .ndjson
            elif folder_name.endswith(".json"):
                folder_name = folder_name[:-5]  # Remove .json
            elif folder_name.endswith(".gz"):
                folder_name = folder_name[:-3]  # Remove .gz
            
            # Ensure no extension remains (safety check)
            # But preserve patterns like "X_of_Y" which might have dots
            if "." in folder_name and not any(pattern in folder_name for pattern in ["_of_", "_1_of_", "_2_of_"]):
                # If there's still a dot and it's not a known pattern, remove extension
                last_dot = folder_name.rfind(".")
                if last_dot > 0:  # Only if dot is not at the start
                    potential_ext = folder_name[last_dot+1:].lower()
                    if potential_ext in ["json", "gz", "ndjson", "csv", "parquet"]:
                        folder_name = folder_name[:last_dot]
            
            if folder_name not in groups:
                groups[folder_name] = []
            groups[folder_name].append(file_name)
        
        LOG.info("Found %d schema group(s) for file organization", len(groups))
        return groups
        
    except psycopg2.errors.UndefinedTable:  # noqa: F405
        # Table doesn't exist yet, return empty dict
        LOG.warning("schema_groups table does not exist yet")
        return {}
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting files grouped by min_file_name: %s", exc)
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_files_grouped_by_schema_sig(connection_string: str) -> dict[str, list[str]]:
    """
    Get all files grouped by their schema_sig (schema signature).
    
    This function groups files by their actual schema signature, which allows
    for deterministic folder naming using hashed schema signatures.
    
    Args:
        connection_string: PostgreSQL connection string
        
    Returns:
        Dictionary mapping schema_sig to list of file_names in that group
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Get all files grouped by schema_sig
        cursor.execute("""
            SELECT DISTINCT 
                schema_sig,
                file_name
            FROM schema_groups
            WHERE schema_sig IS NOT NULL
            ORDER BY schema_sig, file_name
        """)
        
        groups = {}
        for row in cursor.fetchall():
            schema_sig, file_name = row
            if schema_sig not in groups:
                groups[schema_sig] = []
            groups[schema_sig].append(file_name)
        
        LOG.info("Found %d schema group(s) for file organization (by schema_sig)", len(groups))
        return groups
        
    except psycopg2.errors.UndefinedTable:  # noqa: F405
        # Table doesn't exist yet, return empty dict
        LOG.warning("schema_groups table does not exist yet")
        return {}
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error getting files grouped by schema_sig: %s", exc)
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def hash_schema_sig_to_folder_name(schema_sig: str, hash_length: int = 12) -> str:
    """
    Convert a schema signature to a deterministic folder name using hashing.
    
    Args:
        schema_sig: Schema signature string
        hash_length: Length of the hash to use (default: 12 characters)
        
    Returns:
        Folder name in format: group_<hash>
    """
    # Use SHA256 hash for deterministic, collision-resistant naming
    hash_obj = hashlib.sha256(schema_sig.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()[:hash_length]
    return f"group_{hash_hex}"

