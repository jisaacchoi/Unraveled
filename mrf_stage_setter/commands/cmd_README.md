# Pipeline Commands

This directory contains the command-line entry points for each stage of the MRF processing pipeline. Each command is a thin wrapper that loads configuration and calls the appropriate modules from `src/`.

## Command Overview

### `01_download.py` - Download MRF Files
Downloads MRF files from URLs listed in the index JSON file.

**Usage:**
```bash
python commands/01_download.py --config config.yaml
```

**What it does:**
- Loads MRF index JSON from URL or local path
- Downloads all in-network files specified in the index
- Tracks download metadata in `mrf_index` database table
- Files are downloaded to `{output_directory}/{source_name}/{subfolder_name}/`
- Files without "in-network" in the name are automatically deleted

**Configuration:**
- `pipeline.download.*` - Download settings (output directory, threads, chunk size, max file size)
- `database.*` - PostgreSQL connection settings
- `data_source.index_url` or `data_source.index_path` - Source of index JSON

**Output:**
- Downloaded `.json.gz` files in the configured output directory
- Database records in `mrf_index` table

---

### `02_ingest.py` - Ingest Files into Database
Ingests downloaded JSON.gz files into the PostgreSQL `mrf_landing` table.

**Usage:**
```bash
python commands/02_ingest.py --config config.yaml
```

**What it does:**
- Dynamically detects all top-level keys from JSON files
- Extracts records (arrays, objects, scalars) from each file
- Stores each record as a separate row in `mrf_landing` table
- Renames files with `_ingested_` prefix after successful ingestion
- Uses `indexed_gzip` for large files, `ijson` for smaller files

**Configuration:**
- `pipeline.ingest.*` - Ingestion settings (batch size, max array items, workers, etc.)
- `pipeline.input_directory` - Directory containing files to ingest
- `database.*` - PostgreSQL connection settings

**Output:**
- Database records in `mrf_landing` table
- Files renamed with `_ingested_` prefix

**Features:**
- Multi-process parallel processing
- Automatic table creation
- Progress tracking
- Memory-efficient streaming for large files

---

### `03_analyze.py` - Analyze JSON Structures
Analyzes JSON structures from `mrf_landing` and stores flattened schema information in `mrf_analysis` table.

**Usage:**
```bash
python commands/03_analyze.py --config config.yaml
```

**What it does:**
- Reads payloads from `mrf_landing` table
- Recursively traverses JSON structures
- Stores flattened structure in `mrf_analysis` table
- Identifies URLs in values
- Renames files with `_analyzed_` prefix after successful analysis

**Configuration:**
- `pipeline.shape_detection.*` - Analysis settings (batch sizes, max items, etc.)
- `pipeline.input_directory` - Directory containing ingested files
- `database.*` - PostgreSQL connection settings

**Output:**
- Database records in `mrf_analysis` table
- Files renamed with `_analyzed_` prefix (combined with `_ingested_` = `_ingested_analyzed_`)

**Features:**
- Batch processing for memory efficiency
- Recursive structure traversal
- URL detection
- Automatic table creation

---

### `04_gen_schemas.py` - Group Files and Generate Schemas
Groups files by schema signature and generates PySpark schema JSON files.

**Usage:**
```bash
python commands/04_gen_schemas.py --config config.yaml
```

**What it does:**
1. Refreshes `schema_groups` table from `mrf_analysis` data
2. Groups files by schema signature (hash-based folder names)
3. Moves files from input directory to output directory, organized into group subfolders
4. Generates schema JSON files for each group (if enabled and schema doesn't exist)

**Configuration:**
- `pipeline.shape_grouping.*` - Grouping settings
- `pipeline.shape_grouping.create_schema.*` - Schema generation settings
- `pipeline.input_directory` - Source directory (files with `_ingested_analyzed_` prefix)
- `pipeline.output_directory` - Destination directory (organized into group subfolders)
- `database.*` - PostgreSQL connection settings

**Output:**
- Files organized into `group_<hash>/` subfolders in output directory
- Schema JSON files in group folders (if enabled)
- Database records in `schema_groups` table

**Features:**
- Hash-based folder naming for deterministic organization
- Schema generation only on first run (skips if schema.json exists)
- Asynchronous-safe (multiple runs won't conflict)

---

### `05_split.py` - Split Large Files
Splits large JSON.gz files into smaller part files for easier processing.

**Usage:**
```bash
python commands/05_split.py --config config.yaml
```

**What it does:**
- Processes files in group subfolders created by `04_gen_schemas.py`
- Uses indexed_gzip indexes generated during ingestion
- Splits files larger than threshold into smaller parts
- Part 000 contains all scalar values
- Parts 001+ contain array chunks (size-based splitting)

**Configuration:**
- `pipeline.shape_grouping.split_files.*` - Split settings (size per file, chunk size, workers, threads)
- `pipeline.output_directory` - Directory containing group subfolders with files to split
- `database.*` - PostgreSQL connection settings

**Output:**
- Part files: `{filename}_part0000.json.gz`, `{filename}_part0001.json.gz`, etc.
- Original file renamed to `{filename}.json.gz.part`

**Features:**
- Multi-process parallel processing
- Multi-threaded array processing within each file
- Size-based splitting (configurable MB per part file)
- Progress tracking with cumulative size calculation
- Uses array start offsets from `mrf_landing` for efficient access

---

### `99_migrate.py` - Database Migrations
Runs database migrations and schema updates.

**Usage:**
```bash
python commands/99_migrate.py --config config.yaml
```

**What it does:**
- Applies database schema changes
- Creates/updates tables as needed
- Handles schema migrations

**Configuration:**
- `database.*` - PostgreSQL connection settings

**Output:**
- Updated database schema

---

## Running the Pipeline

### Individual Commands
Run each command separately:
```bash
python commands/01_download.py --config config.yaml
python commands/02_ingest.py --config config.yaml
python commands/03_analyze.py --config config.yaml
python commands/04_gen_schemas.py --config config.yaml
python commands/05_split.py --config config.yaml
```

### Using Pipeline Runner
Use `run_pipeline.py` to run steps with polling:
```bash
# Run a single step
python run_pipeline.py --step 1

# Run multiple steps
python run_pipeline.py --step 1 2 3 4 5

# Run all steps
python run_pipeline.py --step 1 2 3 4 5
```

The pipeline runner handles:
- Polling for steps that need retries (steps 1-5)
- Logging and error handling
- Step dependencies

## Command Dependencies

Commands must be run in order:
1. **Download** → Downloads files
2. **Ingest** → Processes downloaded files (requires files from step 1)
3. **Analyze** → Analyzes ingested data (requires data from step 2)
4. **Gen Schemas** → Groups and organizes files (requires files from step 3)
5. **Split** → Splits large files (requires files from step 4)

## Common Options

All commands support:
- `--config <path>` - Path to config file (default: `config.yaml`)

Some commands support additional options - see individual command help:
```bash
python commands/02_ingest.py --help
```

## Error Handling

- Commands log errors to configured log files
- Failed operations are logged with context
- Database connection errors are handled gracefully
- File operation errors include file paths and error details

## Logging

All commands use the logging configuration from `config.yaml`:
- Log level (DEBUG, INFO, WARNING, ERROR)
- Log format (includes PID for multi-process operations)
- Log file paths (configurable per command)

Logs are written to:
- Console (if file logging disabled)
- Log files (if file logging enabled, see `logging.file_logging.log_files`)
