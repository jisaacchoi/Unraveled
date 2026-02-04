# Elasticsearch Index Creator (Streaming, Config-Driven)

This project provides a **Python-based ingestion pipeline** that:

-   Streams data from **PostgreSQL** using server-side cursors (no
    `fetchall`)
-   Creates an **Elasticsearch index** dynamically
-   Bulk-indexes documents efficiently
-   Supports **1--3 fields** (field2/field3 optional)
-   Works with **Elasticsearch 9.x (HTTPS + auth)**
-   Supports **snapshot export/restore** for persistence and backups

------------------------------------------------------------------------

## Requirements

### Software

-   Python **3.10+**
-   Elasticsearch **9.x** (local install, not Docker)
-   PostgreSQL

### Python packages

``` bash
pip install elasticsearch pyyaml psycopg2-binary
```

------------------------------------------------------------------------

## Project Structure

    elastic_search/
    ├── index_creator.py
    ├── config.yaml
    ├── README.md

------------------------------------------------------------------------

## Configuration (`config.yaml`)

``` yaml
elasticsearch:
  url: "https://localhost:9200"
  index: "unraveled"
  username: "elastic"
  password: "YOUR_ELASTIC_PASSWORD"
  verify_certs: false
  recreate: false
  chunk_size: 5000
  id_from_field: null

fields:
  field1:
    name: "tin"
    sql: |
      SELECT tin
      FROM some_table
      ORDER BY tin

database:
  type: "postgres"
  host: "localhost"
  port: 5432
  database: "postgres"
  username: "postgres"
  password: "YOUR_DB_PASSWORD"
```

------------------------------------------------------------------------

## Running the Script

``` bash
python index_creator.py --config config.yaml
```

Optional streaming size:

``` bash
python index_creator.py --config config.yaml --fetch-size 100000
```

------------------------------------------------------------------------

## Verifying Data

``` bash
curl -k -u elastic:PASSWORD https://localhost:9200/unraveled/_count
```

------------------------------------------------------------------------

## Snapshot Export (Backup)

### Configure snapshot path

Edit `<ES_HOME>/config/elasticsearch.yml`:

``` yaml
path.repo: ["D:\\es_snapshots"]
```

Restart Elasticsearch.

### Register repository

``` bash
curl -k -u elastic:PASSWORD -X PUT https://localhost:9200/_snapshot/local_repo   -H "Content-Type: application/json"   -d '{"type":"fs","settings":{"location":"D:/es_snapshots"}}'
```

### Create snapshot

``` bash
curl -k -u elastic:PASSWORD -X PUT https://localhost:9200/_snapshot/local_repo/unraveled_snapshot   -H "Content-Type: application/json"   -d '{"indices":"unraveled","include_global_state":false}'
```

------------------------------------------------------------------------

## Notes

-   Snapshots are the **only supported backup mechanism** for
    Elasticsearch.
-   `verify_certs: false` is for local development only.
-   Rotate the `elastic` password if exposed.

------------------------------------------------------------------------

## Summary

This setup provides a **scalable, repeatable, and production-aligned**
way to stream relational data into Elasticsearch with full backup
support.
