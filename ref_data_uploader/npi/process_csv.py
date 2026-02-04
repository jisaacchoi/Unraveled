"""
CSV Filter and TRUE Streaming Database Upload (Schema-Safe)

Key properties:
- Reads CSV in chunks
- Filters per chunk
- Writes each chunk immediately to PostgreSQL
- NEVER materializes the full dataset in memory
- Forces a stable TEXT schema to avoid type drift errors
"""

import sys
import logging
import yaml
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy import types as sqltypes
from urllib.parse import quote_plus


# -------------------------------------------------
# Logging / Config
# -------------------------------------------------

def setup_logging(config):
    log_config = config.get("logging", {})

    level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    fmt = log_config.get(
        "format",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    datefmt = log_config.get("date_format", "%Y-%m-%d %H:%M:%S")

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter(fmt, datefmt=datefmt)

    if log_config.get("console", True):
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        ch.setLevel(level)
        logger.addHandler(ch)

    log_file = log_config.get("file")
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        fh.setLevel(level)
        logger.addHandler(fh)

    return logger


def load_config(path="config.yaml"):
    p = Path(path)
    if not p.exists():
        print(f"Config not found: {path}")
        sys.exit(1)
    with open(p, "r") as f:
        return yaml.safe_load(f)


# -------------------------------------------------
# Database helpers
# -------------------------------------------------

def get_connection_string(config):
    db = config.get("database", {})
    if db.get("connection_string"):
        return db["connection_string"]

    password = quote_plus(db.get("password", ""))
    return (
        f"postgresql://{db.get('user','postgres')}:{password}"
        f"@{db.get('host','localhost')}:{db.get('port',5432)}"
        f"/{db.get('database','postgres')}"
    )


def get_engine(config):
    engine = create_engine(
        get_connection_string(config),
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"connect_timeout": 10},
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


# -------------------------------------------------
# Filtering
# -------------------------------------------------

def prepare_filters(config):
    fcfg = config.get("filter", {})
    if not fcfg.get("enabled", False):
        return {"enabled": False}

    def load_values(val, ext, numeric=False):
        if ext and Path(ext).exists():
            s = pd.read_csv(ext).iloc[:, 0].dropna()
            if numeric:
                s = s.astype(float).astype(str)
            else:
                s = s.astype(str)
            return set(s.str.strip())
        if val is not None and val != "None":
            return {str(float(val)).strip()} if numeric else {str(val).strip()}
        return None

    return {
        "enabled": True,
        "column1": fcfg.get("column1"),
        "values1": load_values(
            fcfg.get("value1"),
            fcfg.get("value1_ext"),
            numeric=True,
        ),
        "column2": fcfg.get("column2"),
        "values2": load_values(
            fcfg.get("value2"),
            fcfg.get("value2_ext"),
            numeric=False,
        ),
    }


def apply_filters(chunk, filt):
    if not filt.get("enabled"):
        return chunk

    out = chunk

    # Filter 1
    if filt["column1"] and filt["values1"]:
        col = filt["column1"]
        out = out.dropna(subset=[col]).copy()
        out[col] = out[col].astype(float).astype(str).str.strip()
        out = out[out[col].isin(filt["values1"])]

    # ZIP normalization
    zip_map = {}
    for c in out.columns:
        if "zip" in c.lower() or "postal" in c.lower():
            z = f"{c}_5digit"
            out[z] = (
                out[c]
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)
                .str[:5]
            )
            zip_map[c] = z

    # Filter 2
    if filt["column2"] and filt["values2"]:
        col = zip_map.get(filt["column2"], filt["column2"])
        out[col] = out[col].astype(str).str.strip()

        vals = set()
        for v in filt["values2"]:
            digits = "".join(ch for ch in v if ch.isdigit())
            if digits:
                vals.add(digits[:5])

        out = out[(out[col] != "") & (out[col] != "nan")]
        out = out[out[col].isin(vals)]

    return out


# -------------------------------------------------
# TRUE streaming pipeline
# -------------------------------------------------

def stream_csv_to_postgres(config):
    logger = logging.getLogger(__name__)

    paths = config.get("paths", {})
    filter_enabled = config.get("filter", {}).get("enabled", False)

    csv_path = (
        paths.get("input_csv") if filter_enabled else paths.get("upload_csv")
    )
    if not csv_path:
        raise ValueError("CSV path not configured")

    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    filt = prepare_filters(config)

    db = config.get("database", {})
    upload = db.get("upload_enabled", False)
    table = db.get("table_name", "provider_data")
    if_exists_cfg = db.get("if_exists", "replace")

    chunk_size = int(config.get("csv", {}).get("chunk_size", 100000))

    engine = None
    if upload:
        engine = get_engine(config)
        if if_exists_cfg == "replace":
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                conn.commit()

    dtype_map = None
    created_table = False

    total_read = total_kept = total_written = 0

    for i, chunk in enumerate(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            low_memory=False,
            dtype=str,  # CRITICAL: stop type drift
        ),
        start=1,
    ):
        rows_in = len(chunk)
        total_read += rows_in

        out = apply_filters(chunk, filt)
        rows_out = len(out)
        total_kept += rows_out

        logger.info(
            f"Chunk {i}: read {rows_in:,} rows; kept {rows_out:,} rows "
            f"(total read {total_read:,}; total kept {total_kept:,})"
        )

        if rows_out == 0 or not upload:
            continue

        # Normalize NULLs
        out = out.replace({"": None, "nan": None, "None": None})

        if dtype_map is None:
            dtype_map = {c: sqltypes.Text() for c in out.columns}

        out.to_sql(
            table,
            engine,
            if_exists="replace" if not created_table else "append",
            index=False,
            dtype=dtype_map,
            method=None,
        )

        created_table = True
        total_written += rows_out

    logger.info("=" * 60)
    logger.info("Streaming complete")
    logger.info(f"Rows read: {total_read:,}")
    logger.info(f"Rows kept: {total_kept:,}")
    logger.info(f"Rows written: {total_written:,}")
    logger.info("=" * 60)


# -------------------------------------------------
# Main
# -------------------------------------------------

if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = load_config(cfg_path)
    setup_logging(cfg)

    logging.getLogger(__name__).info(f"Using config: {cfg_path}")

    try:
        stream_csv_to_postgres(cfg)
    except Exception:
        logging.exception("Fatal error")
        sys.exit(1)