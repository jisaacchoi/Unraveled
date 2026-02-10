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


def get_file_urls_by_plan(config, plan_name: str) -> List[str]:
    sql = """
        SELECT DISTINCT file_url
        FROM mrf_index
        WHERE EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(reporting_plans, '[]'::jsonb)) AS p
            WHERE p->>'plan_name' = %s
        )
        ORDER BY file_url
    """
    conn = _get_db_conn(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (plan_name,))
            rows = cur.fetchall()
            return [r[0] for r in rows if r and r[0]]
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
