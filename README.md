# Unraveled Repository Overview

This repository contains several related services and utilities for Machine Readable File (MRF) processing, Elasticsearch indexing, and reference data uploads. Each subdirectory has its own detailed README. This top-level README consolidates those guides for easier discovery.

## Contents

- [Elasticsearch Index Creator](#elasticsearch-index-creator)
- [MRF Payer Pipeline (Stage Setter)](#mrf-payer-pipeline-stage-setter)
- [MRF Parquet Conversion API Service](#mrf-parquet-conversion-api-service)
- [Reference Data Uploaders](#reference-data-uploaders)
  - [NPI CSV Filter and Database Upload](#npi-csv-filter-and-database-upload)
  - [Zipcode Data Database Upload](#zipcode-data-database-upload)

## Elasticsearch Index Creator

Location: `elastic_search/`

Python ingestion pipeline that streams data from PostgreSQL into Elasticsearch using server-side cursors, dynamically creates indices, and bulk-indexes documents. Supports 1–3 fields, Elasticsearch 9.x (HTTPS + auth), and snapshot backup/restore.

**Highlights**
- PostgreSQL streaming (server-side cursor) for large datasets
- Config-driven index creation
- Snapshot export/restore workflow

**Quick start**
```bash
pip install elasticsearch pyyaml psycopg2-binary
python index_creator.py --config config.yaml
```

See `elastic_search/README.md` for full configuration, examples, and snapshot steps.

## MRF Payer Pipeline (Stage Setter)

Location: `mrf_stage_setter/`

End-to-end pipeline to download MRF files, ingest JSON into PostgreSQL, analyze schemas, group by schema, and split large files.

**Pipeline steps**
1. Download
2. Ingest
3. Analyze
4. Generate schemas
5. Split

**Quick start**
```bash
pip install -r requirements.txt
python run_pipeline.py --step 1 2 3 4 5
```

See `mrf_stage_setter/README.md` for configuration details, troubleshooting, and architecture documentation links.

## MRF Parquet Conversion API Service

Location: `mrf_final_output/`

FastAPI service for converting JSON.gz MRF files to Parquet with optional NPI and billing code filters. Supports async job processing, split file handling, and schema discovery from group directories.

**Key endpoints**
- `POST /api/v1/convert`
- `GET /api/v1/status/{job_id}`
- `GET /api/v1/health`

**Quick start**
```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

See `mrf_final_output/README.md` for API request/response examples, Docker instructions, and deployment notes.

## Reference Data Uploaders

Location: `ref_data_uploader/`

Utilities for loading reference data into PostgreSQL.

### NPI CSV Filter and Database Upload

Location: `ref_data_uploader/npi/`

Processes a CSV file, optionally filters rows based on a column/value, and uploads the result to PostgreSQL in chunks.

**Quick start**
```bash
pip install -r requirements.txt
python process_csv.py
```

See `ref_data_uploader/npi/README.md` for configuration fields and upload behavior.

### Zipcode Data Database Upload

Location: `ref_data_uploader/zipcode/`

Uploads zip code proximity and ZCTA cross-reference CSVs into PostgreSQL, including table creation and post-upload SQL steps.

**Quick start**
```bash
pip install -r requirements.txt
python process_csv.py
```

See `ref_data_uploader/zipcode/README.md` for CSV expectations, configuration, and upload flow.
