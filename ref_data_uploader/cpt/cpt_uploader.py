#!/usr/bin/env python3
"""
Load CPT code CSV from Hugging Face into Postgres table `cpt_code`.
- Downloads the CSV from a raw URL
- Creates/replaces table `cpt_code`
- All columns are TEXT
"""

import io
import re
import sys
import csv
import requests
import psycopg2
from psycopg2 import sql


CSV_URL = "https://huggingface.co/datasets/mozay22/medical_code_mapping/raw/main/cpt_codes_consolidated.csv"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": ,
}

TABLE_NAME = "cpt_code"


def normalize_identifier(name: str) -> str:
    """
    Convert CSV header -> safe SQL identifier:
    - strip whitespace
    - lowercase
    - replace non [a-z0-9_] with _
    - collapse multiple _
    - avoid empty
    - avoid leading digit (prefix with col_)
    """
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"col_{s}"
    return s


def download_csv(url: str) -> str:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    # HuggingFace raw endpoints generally return UTF-8 text; keep it simple.
    return r.text


def main() -> int:
    print(f"Downloading CSV from: {CSV_URL}")
    csv_text = download_csv(CSV_URL)

    # Parse header safely
    f = io.StringIO(csv_text)
    reader = csv.reader(f)
    try:
        header = next(reader)
    except StopIteration:
        raise RuntimeError("CSV appears to be empty (no header row).")

    # Build unique, normalized column names
    normalized = []
    used = set()
    for h in header:
        base = normalize_identifier(h)
        col = base
        i = 2
        while col in used:
            col = f"{base}_{i}"
            i += 1
        used.add(col)
        normalized.append(col)

    print(f"Detected {len(normalized)} columns. Creating table `{TABLE_NAME}`...")

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Drop & recreate table
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(TABLE_NAME)))

            create_cols = sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in normalized
            )
            cur.execute(
                sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(TABLE_NAME),
                    create_cols
                )
            )

            # Bulk insert via COPY FROM STDIN (fast)
            # Rewind and copy the full CSV exactly as-is into the table.
            copy_buf = io.StringIO(csv_text)
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
                sql.Identifier(TABLE_NAME),
                sql.SQL(", ").join(sql.Identifier(c) for c in normalized),
            )
            cur.copy_expert(copy_sql.as_string(cur), copy_buf)

        conn.commit()
        print(f"Success: loaded data into `{TABLE_NAME}`.")
        return 0

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())