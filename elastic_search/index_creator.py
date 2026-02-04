#!/usr/bin/env python3
"""
Multi-index Elasticsearch ingester (one SQL per index).

- Reads config.yaml
- Connects to Elasticsearch (HTTPS + basic auth)
- Connects to Postgres
- For each job:
  - creates index with mapping derived from job.fields
  - streams rows using a named cursor within a transaction
  - bulk indexes docs

Run:
  pip install elasticsearch pyyaml psycopg2-binary
  python index_creator_multi.py --config config.yaml
"""

from __future__ import annotations

import argparse
import socket
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import yaml
import psycopg2
from elasticsearch import Elasticsearch, helpers


# -----------------------------
# Diagnostics
# -----------------------------

def tcp_preflight_check(es_url: str, timeout_s: float = 2.0) -> None:
    u = urlparse(es_url)
    host = u.hostname or "localhost"
    port = u.port or (443 if u.scheme == "https" else 9200)
    print(f"[ES] TCP preflight: scheme={u.scheme} host={host} port={port}")
    with socket.create_connection((host, port), timeout=timeout_s):
        print("[ES] TCP connection OK")


def safe_es_info(es: Elasticsearch, es_url: str) -> None:
    print(f"[ES] Calling info() on {es_url}")
    info = es.info()
    print(
        f"[ES] Connected: cluster={info.get('cluster_name')} "
        f"version={info.get('version', {}).get('number')}"
    )


# -----------------------------
# Config
# -----------------------------

def load_yaml(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p.resolve()}")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("config.yaml must be a mapping at top-level")
    return data


def validate_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    for section in ("elasticsearch", "database", "jobs"):
        if section not in cfg:
            raise KeyError(f"Missing '{section}' section")

    es = cfg["elasticsearch"]
    db = cfg["database"]
    jobs = cfg["jobs"]

    for k in ("url", "username", "password"):
        if k not in es:
            raise KeyError(f"Missing elasticsearch.{k}")

    es.setdefault("verify_certs", False)
    es.setdefault("chunk_size", 5000)
    es.setdefault("recreate", False)

    for k in ("host", "port", "database", "username", "password"):
        if k not in db:
            raise KeyError(f"Missing database.{k}")

    if not isinstance(jobs, list) or not jobs:
        raise ValueError("jobs must be a non-empty list")

    for i, job in enumerate(jobs):
        if "index" not in job:
            raise KeyError(f"jobs[{i}].index is required")
        if "sql" not in job or not str(job["sql"]).strip():
            raise KeyError(f"jobs[{i}].sql is required")
        if "fields" not in job or not isinstance(job["fields"], list) or not job["fields"]:
            raise KeyError(f"jobs[{i}].fields must be a non-empty list")

        for f in job["fields"]:
            if "name" not in f or "type" not in f:
                raise KeyError(f"jobs[{i}].fields entries require name + type")

        job.setdefault("recreate", es["recreate"])
        job.setdefault("id_from_field", None)

    return cfg


# -----------------------------
# Elasticsearch
# -----------------------------

@dataclass(frozen=True)
class ESConfig:
    url: str
    username: str
    password: str
    verify_certs: bool
    request_timeout: int = 60


def build_es_client(cfg: ESConfig) -> Elasticsearch:
    return Elasticsearch(
        cfg.url,
        basic_auth=(cfg.username, cfg.password),
        verify_certs=cfg.verify_certs,
        request_timeout=cfg.request_timeout,
    )


def build_index_body(fields: List[Dict[str, str]]) -> Dict[str, Any]:
    props = {f["name"]: {"type": f["type"]} for f in fields}
    props["ingested_at"] = {"type": "date"}

    return {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 0,
        },
        "mappings": {
            "dynamic": "strict",
            "properties": props,
        },
    }


def ensure_index(es: Elasticsearch, index: str, body: Dict[str, Any], recreate: bool) -> None:
    if es.indices.exists(index=index):
        if recreate:
            print(f"[ES] Deleting index {index}")
            es.indices.delete(index=index)
        else:
            print(f"[ES] Index exists: {index}")
            return

    print(f"[ES] Creating index {index}")
    es.indices.create(index=index, **body)


def bulk_index(es: Elasticsearch, actions: Iterable[Dict[str, Any]], chunk_size: int) -> int:
    # Use options() to avoid deprecation warnings for transport options
    es_bulk = es.options(request_timeout=120)

    success, errors = helpers.bulk(
        es_bulk,
        actions,
        chunk_size=chunk_size,
        raise_on_error=False,
        raise_on_exception=False,
    )
    if errors:
        raise RuntimeError(f"Bulk indexing errors (first 5): {errors[:5]}")
    return int(success)


# -----------------------------
# Postgres streaming
# -----------------------------

def connect_postgres(db: Dict[str, Any]):
    return psycopg2.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=db["database"],
        user=db["username"],
        password=db["password"],
    )


def stream_rows(conn, sql: str, fetch_size: int) -> Iterator[Tuple[Any, ...]]:
    """
    Stream rows using a named cursor (server-side cursor).
    Requires an open transaction (autocommit must be False).
    """
    cur = conn.cursor(name=f"csr_{abs(hash(sql))}")
    cur.itersize = fetch_size
    cur.execute(sql)

    try:
        while True:
            batch = cur.fetchmany(fetch_size)
            if not batch:
                break
            for row in batch:
                yield row
    finally:
        cur.close()


def make_actions_for_job(
    index: str,
    field_defs: List[Dict[str, str]],
    rows: Iterable[Tuple[Any, ...]],
    id_from_field: Optional[str],
) -> Iterator[Dict[str, Any]]:
    field_names = [f["name"] for f in field_defs]
    now = datetime.now(timezone.utc).isoformat()

    for row in rows:
        if len(row) != len(field_names):
            raise RuntimeError(
                f"SQL returned {len(row)} columns but job.fields has {len(field_names)}. "
                "Make them match."
            )

        doc = dict(zip(field_names, row))
        doc["ingested_at"] = now

        action: Dict[str, Any] = {
            "_op_type": "index",
            "_index": index,
            "_source": doc,
        }

        if id_from_field:
            if id_from_field not in doc:
                raise ValueError(f"id_from_field='{id_from_field}' not in {field_names}")
            action["_id"] = str(doc[id_from_field])

        yield action


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Multi-index Elasticsearch ingester")
    ap.add_argument("--config", required=True)
    ap.add_argument("--fetch-size", type=int, default=50000)
    args = ap.parse_args()

    try:
        cfg = validate_config(load_yaml(args.config))
        es_cfg = cfg["elasticsearch"]
        db_cfg = cfg["database"]
        jobs = cfg["jobs"]

        print(f"[CFG] jobs={len(jobs)} chunk_size={es_cfg['chunk_size']} fetch_size={args.fetch_size}")

        tcp_preflight_check(es_cfg["url"])
        es = build_es_client(
            ESConfig(
                url=es_cfg["url"],
                username=es_cfg["username"],
                password=es_cfg["password"],
                verify_certs=bool(es_cfg["verify_certs"]),
            )
        )
        safe_es_info(es, es_cfg["url"])

        print("[DB] Connecting to Postgres...")
        conn = connect_postgres(db_cfg)
        conn.autocommit = False  # required for named cursors
        print("[DB] Connected")

        try:
            # Keep each job in its own transaction
            for job in jobs:
                index_name = job["index"]
                sql = job["sql"]
                fields = job["fields"]
                recreate = bool(job.get("recreate", False))
                id_from_field = job.get("id_from_field")

                print(f"\n[JOB] index={index_name} recreate={recreate} fields={[f['name'] for f in fields]}")

                ensure_index(
                    es,
                    index=index_name,
                    body=build_index_body(fields),
                    recreate=recreate,
                )

                with conn:
                    rows = stream_rows(conn, sql, args.fetch_size)
                    actions = make_actions_for_job(
                        index=index_name,
                        field_defs=fields,
                        rows=rows,
                        id_from_field=id_from_field,
                    )

                    print("[ES] Bulk indexing started...")
                    total = bulk_index(es, actions, es_cfg["chunk_size"])
                    print(f"[ES] Indexed {total} docs into {index_name}")

            return 0

        finally:
            conn.close()
            print("\n[DB] Connection closed")

    except Exception:
        print("[ERROR] Run failed")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())