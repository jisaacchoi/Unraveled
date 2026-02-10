# MRF Payer Pipeline

A complete pipeline for processing Machine Readable File (MRF) payer data. Downloads MRF files, ingests records into PostgreSQL, analyzes JSON structures, groups files by schema, and splits large files for efficient processing.

## Quick Start

### Prerequisites

- **Python 3.10+**
- **PostgreSQL 13+** with `jsonb` support

### Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd mrf_stage_setter
```

2. **Create virtual environment:**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Configure the pipeline:**
   - Copy `config.yaml` and update with your settings
   - See [Configuration](#configuration) section below
   - **Important:** See [SECURITY_GUIDE.md](SECURITY_GUIDE.md) for handling passwords securely

5. **Run the pipeline:**
```bash
# Run all steps
python run_pipeline.py --step 1 2 3 4 5

# Or run individual steps
python commands/01_download.py --config config.yaml
python commands/02_ingest.py --config config.yaml
python commands/03_analyze.py --config config.yaml
python commands/04_gen_schemas.py --config config.yaml
python commands/05_split.py --config config.yaml
```

## Pipeline Overview

The pipeline consists of 5 main steps:

1. **Download** - Downloads MRF files from index URLs
2. **Ingest** - Loads JSON files into PostgreSQL `mrf_landing` table
3. **Analyze** - Analyzes JSON structures and stores in `mrf_analysis` table
4. **Generate Schemas** - Groups files by schema and generates schema JSON files
5. **Split** - Splits large files into smaller, manageable parts

See [commands/README.md](commands/README.md) for detailed documentation of each command.

## Configuration

The pipeline is configured via `config.yaml`. Key sections:

### Run Configuration
```yaml
run_config:
  base_directory: "D:/payer_mrf/data"  # Base directory for all data files
  run_date: "2026-01-23"  # Date identifier for this run
  source_name: "Blue-Cross-and-Blue-Shield-of-Illinois"  # Payer/source identifier
```

### Database Configuration
```yaml
database:
  # Option 1: Full connection string (takes precedence)
  connection_string: ""
  
  # Option 2: Individual components
  host: "localhost"
  port: 5432
  database: "postgres"
  username: "postgres"
  password: ""  # Leave empty, use DB_PASSWORD environment variable (see SECURITY_GUIDE.md)
  sslmode: ""  # "require" for production
```

**⚠️ Security:** Never commit passwords to version control. See [SECURITY_GUIDE.md](SECURITY_GUIDE.md) for secure password handling.

### Data Source
```yaml
data_source:
  # Option 1: URLs to MRF index JSON files
  index_url:
    - "https://example.com/index.json"
  
  # Option 2: Local file paths
  index_path:
    - "C:/path/to/index.json"
```

### Pipeline Steps
Each step has its own configuration section:
- `pipeline.download.*` - Download settings
- `pipeline.ingest.*` - Ingestion settings
- `pipeline.shape_detection.*` - Analysis settings
- `pipeline.shape_grouping.*` - Schema grouping and splitting settings

See `config.yaml` for all available options and their descriptions.

## Pipeline Flow

```
1. Download
   ↓
   MRF files (.json.gz) in {base_directory}/{source_name}/{run_date}/
   ↓
2. Ingest
   ↓
   Records in mrf_landing table
   Files renamed with _ingested_ prefix
   ↓
3. Analyze
   ↓
   Structure data in mrf_analysis table
   Files renamed with _analyzed_ prefix (now _ingested_analyzed_)
   ↓
4. Generate Schemas
   ↓
   Files moved to output directory (no group hash prefixes)
   Schema JSON files generated as `<file>_schema.json`
   ↓
5. Split
   ↓
   Large files split into part files (_part0000.json.gz, _part0001.json.gz, ...)
   Original files renamed to .part extension
```

## Database Tables

The pipeline creates and uses several PostgreSQL tables:

### `mrf_index`
Tracks download metadata (URLs, status, file paths, sizes).

### `mrf_landing`
Stores raw ingested JSON records. Each top-level key becomes a `record_type`, and array items are stored as separate rows.

### `mrf_analysis`
Stores flattened JSON structure information for schema analysis.

### `schema_groups`
Groups files by schema signature for organization and schema generation.

See SQL files in `sql/` directory for table schemas.

## Running the Pipeline

### Using Pipeline Runner (Recommended)

The `run_pipeline.py` script orchestrates all steps with polling support:

```bash
# Run a single step
python run_pipeline.py --step 1

# Run multiple steps
python run_pipeline.py --step 1 2 3 4 5

# Steps 1-5 support polling (automatic retries)
# Configure polling in config.yaml:
#   pipeline_runner.polling_wait_minutes: 5
#   pipeline_runner.polling_num_additional_attempts: inf  # or a number
```

### Running Individual Commands

Each command can be run independently:

```bash
# Download files
python commands/01_download.py --config config.yaml

# Ingest files
python commands/02_ingest.py --config config.yaml

# Analyze structures
python commands/03_analyze.py --config config.yaml

# Generate schemas and organize files
python commands/04_gen_schemas.py --config config.yaml

# Split large files
python commands/05_split.py --config config.yaml
```

See [commands/README.md](commands/README.md) for detailed documentation of each command.

## Features

- **Memory-Efficient**: Uses streaming JSON parsing for large files
- **Parallel Processing**: Multi-process and multi-threaded operations
- **Progress Tracking**: Detailed progress reporting with PID tracking
- **Error Recovery**: Polling mechanism for automatic retries
- **Schema Detection**: Automatic JSON structure analysis
- **File Organization**: Hash-based grouping for deterministic organization
- **Large File Support**: Uses `indexed_gzip` for efficient random access to compressed files

## Documentation

- **[commands/README.md](commands/README.md)** - Detailed command documentation
- **[src/ARCHITECTURE.md](src/ARCHITECTURE.md)** - Source code architecture and module organization
- **[SECURITY_GUIDE.md](SECURITY_GUIDE.md)** - Secure password and secrets management
- **[ENHANCEMENT_REVIEW.md](ENHANCEMENT_REVIEW.md)** - Comprehensive enhancement recommendations

## Troubleshooting

### Database Connection Issues
- Verify PostgreSQL is running
- Check connection settings in `config.yaml`
- Ensure `DB_PASSWORD` environment variable is set (see [SECURITY_GUIDE.md](SECURITY_GUIDE.md))
- Check firewall settings if connecting to remote database

### File Processing Issues
- Check file permissions on input/output directories
- Verify disk space is available
- Check log files in `logs/` directory for detailed error messages

### Performance Issues
- Adjust `num_workers` and `num_threads` in config
- Increase `batch_size` for database operations
- Check database connection pooling settings

## Development

### Project Structure
```
mrf_stage_setter/
├── commands/          # Command-line entry points
├── src/               # Source code modules
│   ├── download/      # Download functionality
│   ├── ingest/        # Ingestion functionality
│   ├── detect_shapes/ # Structure analysis
│   ├── generate_schemas/ # Schema generation
│   ├── split/         # File splitting
│   └── shared/        # Shared utilities
├── sql/               # SQL DDL files
├── config.yaml        # Configuration file
├── run_pipeline.py    # Pipeline orchestrator
├── requirements.txt   # Python dependencies
└── etc                # etc documents
```

See [src/ARCHITECTURE.md](src/ARCHITECTURE.md) for detailed architecture documentation.

### Adding New Features
1. Identify which pipeline stage the feature belongs to
2. Add module in appropriate `src/` subdirectory
3. Create or update command in `commands/`
4. Update configuration schema in `config.yaml`
5. Update documentation

## License

[Add your license information here]

## Support

[Add support contact information here]
