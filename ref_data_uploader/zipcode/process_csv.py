#!/usr/bin/env python3
"""
CSV Database Upload for Zipcode Data

This script:
1. Creates zip_code_proximity table
2. Uploads zip_code_proximity.csv to the database
3. Executes post-upload INSERT statement
4. Creates and uploads zip_zcta_xref.csv to the database
"""

import pandas as pd
import os
import sys
import yaml
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


def setup_logging(config):
    """Setup logging based on config.yaml settings."""
    log_config = config.get('logging', {})
    
    # Get log level
    log_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
    
    # Get log format
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    date_format = log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Console handler
    if log_config.get('console', True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    log_file = log_config.get('file', '')
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def load_config(config_path='config.yaml'):
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    if not config_file.exists():
        print(f"Error: Config file '{config_path}' not found.")
        sys.exit(1)
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config


def get_database_connection_string(config):
    """
    Build PostgreSQL connection string from config.
    Uses connection_string if provided, otherwise builds from components.
    """
    db_config = config.get('database', {})
    
    # If connection_string is provided, use it
    if db_config.get('connection_string'):
        return db_config['connection_string']
    
    # Otherwise, build from components
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    database = db_config.get('database', 'postgres')
    user = db_config.get('user', 'postgres')
    password = db_config.get('password', '')
    
    # URL encode password to handle special characters
    encoded_password = quote_plus(password)
    
    connection_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}"
    return connection_string


def create_zip_code_proximity_table(engine, table_name):
    """Create zip_code_proximity table."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Creating table '{table_name}'...")
    
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        zip1 CHAR(5) NOT NULL,
        zip2 CHAR(5) NOT NULL,
        miles_to_zcta5 NUMERIC(10,7) NOT NULL,
        PRIMARY KEY (zip1, zip2)
    );
    """
    
    try:
        with engine.begin() as conn:
            # Drop table if exists
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            logger.info(f"Dropped existing table '{table_name}' if it existed")
            
            # Create table
            conn.execute(text(create_table_sql))
            logger.info(f"Successfully created table '{table_name}'")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        logger.exception("Full traceback:")
        raise


def upload_csv_to_postgres(engine, csv_path, table_name, if_exists='replace', chunk_size=100000):
    """
    Upload CSV file to PostgreSQL database.
    
    Args:
        engine: SQLAlchemy engine
        csv_path: Path to CSV file
        table_name: Target table name
        if_exists: What to do if table exists - "fail", "replace", or "append"
        chunk_size: Number of rows per chunk
    """
    logger = logging.getLogger(__name__)
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file '{csv_path}' not found.")
    
    logger.info("=" * 60)
    logger.info(f"Uploading CSV to PostgreSQL: {csv_path}")
    logger.info(f"Target table: {table_name}")
    logger.info("=" * 60)
    
    try:
        # Read CSV in chunks to handle large files
        total_rows_read = 0
        chunk_number = 0
        uploaded_rows = 0
        
        logger.info(f"Reading CSV file in chunks of {chunk_size:,} rows...")
        
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            chunk_number += 1
            rows_in_chunk = len(chunk)
            total_rows_read += rows_in_chunk
            
            logger.info(f"Chunk {chunk_number}: Read {rows_in_chunk:,} rows (total read: {total_rows_read:,})")
            
            # Determine if_exists parameter
            if chunk_number == 1:
                chunk_if_exists = if_exists
            else:
                chunk_if_exists = 'append'
            
            # Upload chunk
            chunk.to_sql(
                table_name,
                engine,
                if_exists=chunk_if_exists,
                index=False,
                method=None
            )
            uploaded_rows += rows_in_chunk
            logger.info(f"Chunk {chunk_number} uploaded. Total uploaded: {uploaded_rows:,} rows")
        
        logger.info("=" * 60)
        logger.info(f"Successfully uploaded {uploaded_rows:,} rows to table '{table_name}'")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error uploading CSV to database: {e}")
        logger.exception("Full traceback:")
        raise


def execute_post_insert_sql(engine, table_name):
    """Execute post-upload INSERT statement to add self-referential rows."""
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("Executing post-upload INSERT statement...")
    logger.info("=" * 60)
    
    post_insert_sql = f"""
    INSERT INTO {table_name} (zip1, zip2, miles_to_zcta5)
    SELECT DISTINCT
        z.zip1,
        z.zip1 AS zip2,
        0::NUMERIC(10,7) AS miles_to_zcta5
    FROM {table_name} z
    WHERE NOT EXISTS (
        SELECT 1
        FROM {table_name} x
        WHERE x.zip1 = z.zip1
          AND x.zip2 = z.zip1
    );
    """
    
    try:
        # Use a transactional connection so changes are committed automatically on success
        with engine.begin() as conn:
            result = conn.execute(text(post_insert_sql))
            rows_affected = result.rowcount if hasattr(result, 'rowcount') else 'unknown'
            logger.info(f"Post-upload INSERT completed. Rows affected: {rows_affected}")
            logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Error executing post-upload INSERT: {e}")
        logger.exception("Full traceback:")
        raise


def create_zip_zcta_xref_table(engine, table_name):
    """Create zip_zcta_xref table with zip_code, zcta, source columns (all strings)."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Creating table '{table_name}'...")
    
    create_sql = f"""
    CREATE TABLE {table_name} (
        zip_code VARCHAR(255) NOT NULL,
        zcta VARCHAR(255) NOT NULL,
        source VARCHAR(255) NOT NULL,
        PRIMARY KEY (zip_code, zcta, source)
    );
    """
    
    try:
        with engine.begin() as conn:
            # Drop table if exists
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            logger.info(f"Dropped existing table '{table_name}' if it existed")
            
            # Create table
            conn.execute(text(create_sql))
            logger.info(f"Successfully created table '{table_name}'")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    # Load config file (optional command line argument)
    config_path = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'
    
    # Load config first (before setting up logging, in case config file doesn't exist)
    config = load_config(config_path)
    
    # Setup logging based on config
    logger = setup_logging(config)
    
    logger.info(f"Loading configuration from: {config_path}")
    
    if not config.get('database', {}).get('upload_enabled', False):
        logger.info("Upload is disabled in config. Set 'upload_enabled: true' to enable.")
        sys.exit(0)
    
    # Get database connection
    connection_string = get_database_connection_string(config)
    logger.info(f"Connecting to database: {config.get('database', {}).get('database', 'postgres')}@{config.get('database', {}).get('host', 'localhost')}")
    
    # Create SQLAlchemy engine
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"connect_timeout": 10}
    )
    
    # Test connection
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connection successful!")
    
    # Get paths from config
    paths_config = config.get('paths', {})
    zip_code_proximity_csv = paths_config.get('zip_code_proximity_csv')
    zip_zcta_xref_csv = paths_config.get('zip_zcta_xref_csv')
    
    # Get table names from config
    db_config = config.get('database', {})
    zip_code_proximity_table = db_config.get('zip_code_proximity_table', 'zip_code_proximity')
    zip_zcta_xref_table = db_config.get('zip_zcta_xref_table', 'zip_zcta_xref')
    if_exists = db_config.get('if_exists', 'replace')
    
    try:
        # Step 1: Create zip_code_proximity table
        create_zip_code_proximity_table(engine, zip_code_proximity_table)
        
        # Step 2: Upload zip_code_proximity.csv
        if zip_code_proximity_csv:
            upload_csv_to_postgres(engine, zip_code_proximity_csv, zip_code_proximity_table, if_exists)
        
        # Step 3: Execute post-upload INSERT statement
        execute_post_insert_sql(engine, zip_code_proximity_table)
        
        # Step 4: Create zip_zcta_xref table
        create_zip_zcta_xref_table(engine, zip_zcta_xref_table)
        
        # Step 5: Upload zip_zcta_xref.csv
        if zip_zcta_xref_csv:
            upload_csv_to_postgres(engine, zip_zcta_xref_csv, zip_zcta_xref_table, if_exists)
        
        logger.info("=" * 60)
        logger.info("All uploads completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)
