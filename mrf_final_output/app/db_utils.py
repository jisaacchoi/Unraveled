"""Database helpers for resolving plan files and NPIs."""
from __future__ import annotations

from typing import List, Optional

import psycopg2

from app.api_config import get_config_value


def _get_db_conn(config):
    db_cfg = get_config_value(config, "database", {})
    return psycopg2.connect(
        host=db_cfg.get("host", "localhost"),
        port=db_cfg.get("port", 5432),
        database=db_cfg.get("database", "postgres"),
        user=db_cfg.get("user", "postgres"),
        password=db_cfg.get("password", ""),
    )


def get_file_name_core_by_plan(config, plan_name: str) -> Optional[str]:
    sql = """
        SELECT file_name_core
        FROM vw_mrf_reporting_plans
        WHERE plan_name = %s
        LIMIT 1
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (plan_name,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def get_file_urls_by_plan(config, plan_name: str) -> List[tuple[str, str]]:
    """
    Get file URLs and filenames for a given plan name.
    Only returns files that exist in mrf_analyzed table (if table exists).
    
    Returns:
        List of tuples (file_url, file_name) where file_name may be None
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            # Check if mrf_analyzed table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'mrf_analyzed'
                )
            """)
            mrf_analyzed_exists = cur.fetchone()[0]
            
            # Build query with or without mrf_analyzed join
            if mrf_analyzed_exists:
                sql = """
                    SELECT DISTINCT mi.file_url, mi.file_name
                    FROM mrf_index mi
                    INNER JOIN mrf_analyzed ma ON mi.file_name = ma.file_name
                    WHERE EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(COALESCE(mi.reporting_plans, '[]'::jsonb)) AS p
                        WHERE p->>'plan_name' = %s
                    )
                    ORDER BY mi.file_url
                """
            else:
                sql = """
                    SELECT DISTINCT mi.file_url, mi.file_name
                    FROM mrf_index mi
                    WHERE EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(COALESCE(mi.reporting_plans, '[]'::jsonb)) AS p
                        WHERE p->>'plan_name' = %s
                    )
                    ORDER BY mi.file_url
                """
            
            cur.execute(sql, (plan_name,))
            rows = cur.fetchall()
            return [(r[0], r[1]) for r in rows if r and r[0]]
    finally:
        conn.close()

def get_file_urls_by_file_name(config, file_name: str) -> List[tuple[str, str]]:
    """
    Get file URLs and filenames for a given file_name.
    Only returns files that exist in mrf_analyzed table (if table exists).
    
    Args:
        file_name: File name to search for (can be with or without extension)
    
    Returns:
        List of tuples (file_url, file_name) where file_name may be None
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            # Check if mrf_analyzed table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'mrf_analyzed'
                )
            """)
            mrf_analyzed_exists = cur.fetchone()[0]
            
            # Strip extension for matching
            file_name_core = file_name
            if file_name_core.endswith('.json.gz'):
                file_name_core = file_name_core[:-8]
            elif file_name_core.endswith('.json'):
                file_name_core = file_name_core[:-5]
            
            # Build query with or without mrf_analyzed join
            if mrf_analyzed_exists:
                sql = """
                    SELECT DISTINCT mi.file_url, mi.file_name
                    FROM mrf_index mi
                    INNER JOIN mrf_analyzed ma ON mi.file_name = ma.file_name
                    WHERE mi.file_name LIKE %s OR mi.file_name = %s
                    ORDER BY mi.file_url
                """
            else:
                sql = """
                    SELECT DISTINCT mi.file_url, mi.file_name
                    FROM mrf_index mi
                    WHERE mi.file_name LIKE %s OR mi.file_name = %s
                    ORDER BY mi.file_url
                """
            
            # Try exact match first, then partial match
            search_pattern = f"%{file_name_core}%"
            cur.execute(sql, (search_pattern, file_name))
            rows = cur.fetchall()
            return [(r[0], r[1]) for r in rows if r and r[0]]
    finally:
        conn.close()


def get_npis_by_zip(config, zip1: str) -> List[str]:
    sql = """
        SELECT DISTINCT p."NPI"
        FROM zip_npi AS p
        WHERE p.zipcode IN (
            SELECT z.zip2
            FROM zip_zip_distance_15mi AS z
            WHERE z.zip1 = %s
        )
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (zip1,))
            rows = cur.fetchall()
            return [str(r[0]) for r in rows if r and r[0] is not None]
    finally:
        conn.close()
