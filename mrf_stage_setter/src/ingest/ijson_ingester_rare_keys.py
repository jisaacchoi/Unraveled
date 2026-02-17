"""
Small-file ingester using ijson streaming parser.

This module is intended for small .json.gz files where building indexed_gzip
indexes is unnecessary overhead. It writes rows into mrf_landing with the same
column layout used by the indexed_gzip rare-keys ingester.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from pathlib import Path
from typing import Any

import ijson
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.shared.json_reader import is_json_file, open_json_file

LOG = logging.getLogger("src.ingest.ijson_ingester_rare_keys")

try:
    import orjson

    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False


def convert_decimals(obj: Any) -> Any:
    """Recursively convert Decimal objects to float for JSON serialization."""
    from decimal import Decimal

    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj


def ingest_file_ijson_rare_keys(
    connection_string: str,
    file_path: Path,
    source_name: str,
    batch_size: int = 1000,
    max_array_items: int = 20,
) -> int:
    """
    Ingest a small .json/.json.gz file using ijson and write to mrf_landing.

    For compatibility with the indexed_gzip path:
    - each top-level key becomes one row
    - list payloads are truncated to max_array_items
    - scalar payloads are wrapped as {"value": ...}
    - array_start_offset is NULL for this parser
    - has_rare_keys is FALSE
    """
    if not file_path.exists():
        LOG.error("File not found: %s", file_path)
        return 1

    if not is_json_file(file_path):
        LOG.error("File must be .json or .json.gz: %s", file_path)
        return 1

    file_name = file_path.name
    if file_name.endswith(".part"):
        file_name = file_name[:-5]
    file_size = file_path.stat().st_size

    conn = None
    cursor = None

    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        # Dedupe by record_type right before COPY, keep largest payload_size.
        best_rows_by_record_type: dict[str, tuple[int, str]] = {}

        with open_json_file(file_path) as f:
            for key_name, val in ijson.kvitems(f, ""):
                if isinstance(val, list):
                    payload = val[:max_array_items]
                elif isinstance(val, dict):
                    payload = val
                elif isinstance(val, (str, int, float, bool)) or val is None:
                    payload = {"value": val}
                else:
                    LOG.warning("Skipping key '%s' due to unsupported value type: %s", key_name, type(val))
                    continue

                payload_converted = convert_decimals(payload)
                if HAS_ORJSON:
                    record_json = orjson.dumps(payload_converted, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
                else:
                    record_json = json.dumps(payload_converted, ensure_ascii=False)
                payload_size = len(record_json.encode("utf-8"))

                # Build a single COPY line exactly as expected (unquoted \N for NULL).
                row_buffer = io.StringIO()
                row_writer = csv.writer(
                    row_buffer,
                    delimiter="\t",
                    quoting=csv.QUOTE_ALL,
                    doublequote=True,
                    lineterminator="\n",
                )
                row_writer.writerow(
                    [
                        source_name,
                        file_name,
                        str(file_size),
                        str(key_name),
                        "0",
                        record_json,
                        str(payload_size),
                    ]
                )
                current_pos = row_buffer.tell()
                row_buffer.seek(current_pos - 1)
                row_buffer.truncate()
                row_buffer.write("\t\\N\tFALSE\n")
                raw_line = row_buffer.getvalue().rstrip("\n")

                existing = best_rows_by_record_type.get(str(key_name))
                if existing is None or payload_size > existing[0]:
                    best_rows_by_record_type[str(key_name)] = (payload_size, raw_line)

        if best_rows_by_record_type:
            deduped_lines = [raw_line for _, raw_line in best_rows_by_record_type.values()]
            final_batch = io.StringIO("\n".join(deduped_lines) + "\n")
            cursor.copy_expert(
                """
                COPY mrf_landing(source_name, file_name, file_size, record_type, record_index, payload, payload_size, array_start_offset, has_rare_keys)
                FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '"', NULL '\\N')
                """,
                final_batch,
            )

        LOG.info("ijson ingestion complete: %s (%d deduped row(s))", file_name, len(best_rows_by_record_type))
        return 0

    except Exception as exc:  # noqa: BLE001
        LOG.exception("Error ingesting %s with ijson: %s", file_path, exc)
        return 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
