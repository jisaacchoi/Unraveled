# Source Code Architecture

This document describes the internal organization of the `src/` directory and how modules interact with each other.

## Directory Structure

The `src/` directory is organized by pipeline stage to make it clear which modules belong to which command.

### `download/` - Stage 1: Download
- **`mrf_downloader.py`** - Core download logic for MRF files
  - Downloads files from URLs listed in index JSON
  - Tracks download status in `mrf_index` database table
  - Handles retries, file size limits, and error recovery
- **Used by:** `commands/01_download.py`

### `ingest/` - Stage 2: Ingest
- **`mrf_ingester.py`** - Core ingestion logic for streaming MRF files into PostgreSQL
  - Uses `ijson` for streaming JSON parsing (memory-efficient)
  - Handles small to medium files
- **`indexed_gzip_ingester.py`** - Advanced ingestion for large files
  - Uses `indexed_gzip` for random access to compressed files
  - Multi-threaded key processing
  - Tracks array start offsets for efficient splitting
- **Used by:** `commands/02_ingest.py`

### `detect_shapes/` - Stage 3: Shape Detection
- **`structure_analyzer.py`** - Analyzes JSON structures from `mrf_landing` and stores in `mrf_analysis` table
  - Recursively traverses JSON structures
  - Extracts flattened schema information
  - Identifies URLs in values
- **Used by:** `commands/03_analyze.py`

### `generate_schemas/` - Stage 4: Schema Generation
- **`schema_orchestrator.py`** - Orchestrates the full schema pipeline (detect, group, generate)
  - Refreshes `schema_groups` table
  - Organizes files into hash-based group folders
  - Generates schema JSON files
- **`schema_groups_db.py`** - Database operations for `schema_groups` table
  - Queries files grouped by schema signature
  - Hash-based folder naming for deterministic organization
- **`schema_inference.py`** - Schema inference and file operations for PySpark schemas
  - Generates PySpark-compatible schema JSON files
- **Used by:** `commands/04_gen_schemas.py`

### `split/` - Stage 5: File Splitting
- **`indexed_gzip_splitter.py`** - Splits large JSON.gz files using indexed_gzip
  - Uses array start offsets from `mrf_landing` table
  - Multi-threaded array processing
  - Size-based splitting (configurable MB per part file)
  - Progress tracking with cumulative size calculation
- **`json_splitter.py`** - Alternative splitter using ijson (legacy, for smaller files)
- **Used by:** `commands/05_split.py`

### `shared/` - Shared Utilities
Modules used by multiple commands:

- **`config.py`** - Configuration file loading and logging setup
  - Loads YAML config files with variable substitution
  - Configures logging with file/console handlers
  - Used by all commands

- **`database.py`** - Database connection and table management
  - Builds PostgreSQL connection strings
  - Creates/ensures tables exist
  - Used by most commands

- **`spark_session.py`** - SparkSession creation and configuration
  - Creates Spark sessions for PySpark operations
  - Handles Windows-specific setup (winutils, Hadoop)
  - Used by schema generation and Parquet conversion

- **`file_mover.py`** - File movement utilities
  - Moves files between directories
  - Handles file prefix management
  - Used by stages 2, 3, and 4

- **`path_helper.py`** - Directory path utilities
  - Ensures directories exist from config
  - Used by pipeline runner

- **`url_content_downloader.py`** - URL downloading and content joining
  - Downloads URLs from Parquet data
  - Joins downloaded content back to DataFrames
  - Used by Parquet conversion stage

- **`json_reader.py`** - JSON file reading utilities
  - Opens JSON files (compressed or uncompressed)
  - Used by ingestion stage

- **`file_prefix.py`** - File prefix management
  - Adds/removes prefixes like `_ingested_`, `_analyzed_`
  - Used for tracking file processing state

## Import Examples

```python
# Stage-specific imports
from src.download.mrf_downloader import run_download
from src.ingest.mrf_ingester import ingest_file
from src.ingest.indexed_gzip_ingester import ingest_file_indexed_gzip
from src.detect_shapes.structure_analyzer import run_shape_analysis
from src.generate_schemas.schema_orchestrator import run_full_pipeline
from src.split.indexed_gzip_splitter import split_json_gz_with_indexed_gzip

# Shared utility imports
from src.shared.config import load_config, configure_logging
from src.shared.database import build_connection_string, ensure_table_exists
from src.shared.spark_session import create_spark_session
from src.shared.file_prefix import rename_with_prefix, PREFIX_INGESTED
```

## Module Dependencies

```
download/mrf_downloader.py
  → shared/database.py
  → shared/config.py

ingest/mrf_ingester.py
  → shared/json_reader.py
  → shared/database.py

ingest/indexed_gzip_ingester.py
  → shared/database.py
  → shared/json_reader.py

detect_shapes/structure_analyzer.py
  → shared/database.py
  → (uses SQL files from sql/)

generate_schemas/schema_orchestrator.py
  → shared/file_mover.py
  → detect_shapes/structure_analyzer.py
  → generate_schemas/schema_groups_db.py
  → generate_schemas/schema_inference.py
  → shared/spark_session.py

split/indexed_gzip_splitter.py
  → shared/database.py
  → ingest/indexed_gzip_ingester.py (for helper functions)
  → split/json_splitter.py (for validation)

shared/database.py
  → (standalone, uses psycopg2)

shared/config.py
  → (standalone, uses PyYAML)
```

## Design Principles

1. **Separation of Concerns**: Each stage has its own module directory
2. **Shared Utilities**: Common functionality is in `shared/`
3. **Command Entry Points**: Commands in `commands/` are thin wrappers that call `src/` modules
4. **Database Abstraction**: Database operations are centralized in `shared/database.py`
5. **Configuration Management**: All config loading is handled by `shared/config.py`

## Adding New Features

When adding new functionality:

1. **Identify the stage**: Which pipeline stage does this belong to?
2. **Check for existing utilities**: Can you reuse something from `shared/`?
3. **Create module**: Add new module in appropriate stage directory
4. **Update command**: Add or update command in `commands/` directory
5. **Update documentation**: Update this file and relevant README files

## Testing Considerations

- Each module should be independently testable
- Mock database connections for unit tests
- Use test fixtures for file operations
- Test error handling and edge cases
