"""Database helpers for resolving plan files and NPIs."""
from __future__ import annotations

from typing import List, Optional, Tuple

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


def get_file_name_cores_by_plan(config, plan_name: str) -> List[str]:
    """
    Get all distinct file_name_core values for a plan_name.
    """
    sql = """
        SELECT DISTINCT file_name_core
        FROM vw_mrf_reporting_plans
        WHERE plan_name = %s
        ORDER BY file_name_core
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (plan_name,))
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
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


def _get_zip_distance_column(conn) -> Optional[str]:
    """Return a usable distance column name from zip_zip_distance_15mi, if present."""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'zip_zip_distance_15mi'
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [str(r[0]).lower() for r in cur.fetchall() if r and r[0]]

    for candidate in ("distance_miles", "distance", "miles", "mi"):
        if candidate in columns:
            return candidate
    return None


def get_npis_by_zip(config, zip1: str, max_distance_miles: Optional[float] = None) -> List[str]:
    conn = _get_db_conn(config)
    try:
        if max_distance_miles is not None:
            distance_col = _get_zip_distance_column(conn)
            if distance_col is None:
                raise RuntimeError(
                    "zipcode_distance_miles was provided, but zip_zip_distance_15mi has no supported "
                    "distance column (expected one of: distance_miles, distance, miles, mi)."
                )

            sql = f"""
                SELECT DISTINCT p."NPI"
                FROM zip_npi AS p
                WHERE p.zipcode IN (
                    SELECT z.zip2
                    FROM zip_zip_distance_15mi AS z
                    WHERE z.zip1 = %s
                      AND z."{distance_col}" <= %s
                )
            """
            params = (zip1, max_distance_miles)
        else:
            sql = """
                SELECT DISTINCT p."NPI"
                FROM zip_npi AS p
                WHERE p.zipcode IN (
                    SELECT z.zip2
                    FROM zip_zip_distance_15mi AS z
                    WHERE z.zip1 = %s
                )
            """
            params = (zip1,)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [str(r[0]) for r in rows if r and r[0] is not None]
    finally:
        conn.close()


def get_provider_data_by_npis(config, npis: List[str], chunk_size: int = 5000) -> List[Tuple]:
    """
    Fetch provider data rows for the requested NPIs from the provider_data table.

    Uses an IN statement with batched placeholders to avoid oversized parameter lists.
    """
    if not npis:
        return []

    # Keep only non-empty, deduplicated string NPIs while preserving order.
    normalized_npis: List[str] = []
    seen = set()
    for value in npis:
        npi = str(value).strip()
        if not npi or npi in seen:
            continue
        seen.add(npi)
        normalized_npis.append(npi)

    if not normalized_npis:
        return []

    select_prefix = """
        SELECT
            pd."NPI"::text AS npi,
            CASE
                WHEN NULLIF(TRIM(pd."Provider Organization Name (Legal Business Name)"), '') IS NOT NULL
                    THEN pd."Provider Organization Name (Legal Business Name)"
                ELSE CONCAT_WS(
                    ' ',
                    NULLIF(TRIM(pd."Provider First Name"), ''),
                    NULLIF(TRIM(pd."Provider Middle Name"), ''),
                    NULLIF(TRIM(pd."Provider Last Name (Legal Name)"), '')
                )
            END AS provider_name,
            CONCAT_WS(
                ', ',
                NULLIF(TRIM(pd."Provider First Line Business Practice Location Address"), ''),
                NULLIF(TRIM(pd."Provider Second Line Business Practice Location Address"), ''),
                NULLIF(TRIM(pd."Provider Business Practice Location Address City Name"), ''),
                NULLIF(TRIM(pd."Provider Business Practice Location Address State Name"), ''),
                LEFT(NULLIF(TRIM(pd."Provider Business Practice Location Address Postal Code"), ''), 5),
                CASE
                    WHEN NULLIF(TRIM(pd."Provider Business Practice Location Address Telephone Number"), '') IS NOT NULL
                        THEN 'Tel: ' || TRIM(pd."Provider Business Practice Location Address Telephone Number")
                    ELSE NULL
                END
            ) AS practice_location,
            pd."Provider Organization Name (Legal Business Name)" AS provider_org_name,
            pd."Provider Last Name (Legal Name)" AS provider_last_name,
            pd."Provider First Name" AS provider_first_name,
            pd."Provider Middle Name" AS provider_middle_name,
            pd."Provider First Line Business Practice Location Address" AS practice_addr_line1,
            pd."Provider Second Line Business Practice Location Address" AS practice_addr_line2,
            pd."Provider Business Practice Location Address City Name" AS practice_city,
            pd."Provider Business Practice Location Address State Name" AS practice_state,
            LEFT(NULLIF(TRIM(pd."Provider Business Practice Location Address Postal Code"), ''), 5) AS practice_postal_code,
            pd."Provider Business Practice Location Address Telephone Number" AS practice_phone
        FROM provider_data pd
    """

    conn = _get_db_conn(config)
    try:
        all_rows: List[Tuple] = []
        with conn.cursor() as cur:
            for i in range(0, len(normalized_npis), chunk_size):
                chunk = normalized_npis[i:i + chunk_size]
                placeholders = ",".join(["%s"] * len(chunk))
                sql = f"""{select_prefix}
                    WHERE pd."NPI"::text IN ({placeholders})
                """
                cur.execute(sql, chunk)
                all_rows.extend(cur.fetchall())
        return all_rows
    finally:
        conn.close()
