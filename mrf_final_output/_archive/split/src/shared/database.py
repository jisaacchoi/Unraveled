"""Database utility functions for MRF pipeline."""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote_plus

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

LOG = logging.getLogger("src.database")


def build_connection_string(
    config: Dict[str, Any]
) -> str:
    """
    Build PostgreSQL connection string from config.
    
    Supports either:
    1. Direct connection_string in config
    2. Individual components (host, port, database, user, password)
    
    Args:
        config: Database configuration dictionary
        
    Returns:
        PostgreSQL connection string
    """
    # If connection_string is provided, use it directly
    conn_str = config.get("connection_string")
    if conn_str:
        return conn_str
    
    # Otherwise, build from components
    host = config.get("host", "localhost")
    port = config.get("port", 5432)
    database = config.get("database", "postgres")
    # Support both "username" and "user" for backward compatibility
    user = config.get("username", config.get("user", "postgres"))
    password = config.get("password", "").strip()
    
    # If password is empty in config, try to read from environment variable
    if not password:
        password = os.environ.get("DB_PASSWORD", "").strip()
        if not password:
            LOG.warning(
                "Database password not found in config or DB_PASSWORD environment variable. "
                "Connection may fail if database requires authentication."
            )
    
    # URL-encode password if it contains special characters
    password_encoded = quote_plus(password) if password else ""
    
    # Build connection string
    if password_encoded:
        conn_string = f"postgresql://{user}:{password_encoded}@{host}:{port}/{database}"
    else:
        conn_string = f"postgresql://{user}@{host}:{port}/{database}"
    
    # Add SSL mode if specified
    sslmode = config.get("sslmode")
    if sslmode:
        conn_string = f"{conn_string}?sslmode={sslmode}"
    
    return conn_string


def ensure_table_exists(
    connection_string: str,
    table_name: str,
    ddl_path: Optional[Path] = None,
    drop_if_exists: bool = False,
) -> None:
    """
    Ensure a table exists by running its DDL if needed.
    
    Args:
        connection_string: PostgreSQL connection string
        table_name: Name of the table to check/create
        ddl_path: Optional path to SQL DDL file. If None, uses default location.
        drop_if_exists: If True, drop the table before creating (useful for schema changes)
    """
    if ddl_path is None:
        # Default to sql/create_mrf_landing_table.sql
        ddl_path = Path(__file__).parent.parent.parent / "sql" / "create_mrf_landing_table.sql"
    
    if not ddl_path.exists():
        error_msg = f"DDL file not found at {ddl_path.absolute()}, skipping table creation"
        LOG.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    conn = None
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """,
            (table_name,),
        )
        exists = cursor.fetchone()[0]
        
        # Drop table if it exists and drop_if_exists is True
        if exists and drop_if_exists:
            LOG.info("Dropping existing table %s", table_name)
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            LOG.info("Dropped table %s", table_name)
            exists = False
        
        if exists:
            LOG.debug("Table %s already exists", table_name)
            return
        
        # Read and execute DDL
        LOG.info("Creating table %s from %s", table_name, ddl_path)
        with open(ddl_path, "r", encoding="utf-8") as fh:
            ddl_sql = fh.read()
        
        cursor.execute(ddl_sql)
        LOG.info("Successfully created table %s", table_name)
        
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Failed to ensure table %s exists: %s", table_name, exc)
        raise
    finally:
        if conn:
            conn.close()

