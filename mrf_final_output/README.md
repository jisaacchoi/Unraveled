# Parquet Conversion API Service

REST API service for converting JSON.gz files to Parquet format with dynamic filtering.

## Overview

This service is part of a larger MRF (Machine-Readable File) data processing pipeline. It provides a REST API endpoint to convert individual JSON.gz files to Parquet format with optional NPI and billing code filtering.

## Features

- **Single File Processing**: Process one JSON.gz file at a time via API
- **Dynamic Filtering**: Apply NPI and billing code filters via API request
- **Split File Support**: Automatically handles split files (_part#### suffix)
- **Async Processing**: Jobs are processed asynchronously with status tracking
- **Schema Loading**: Automatically finds and loads schema.json from group directories

## API Endpoints

### POST `/api/v1/convert`

Convert a JSON.gz file to Parquet format.

**Request Body:**
```json
{
  "source_file": "2025-12_510_01B0_in-network-rates_18_of_29.json.gz",
  "npi_filter": "1234567890,9876543210,1111111111",
  "billing_code_filter": "70010-76499,76506-76999,77001-77092"
}
```

**Response (202 Accepted):**
```json
{
  "job_id": "job_abc123",
  "status": "accepted",
  "message": "Job queued for processing",
  "status_url": "/api/v1/status/job_abc123"
}
```

### GET `/api/v1/status/{job_id}`

Get the status of a conversion job.

**Response (200 OK):**
```json
{
  "job_id": "job_abc123",
  "status": "completed",
  "progress": 100,
  "message": "Processing complete",
  "result": {
    "output_directory": "/final_stage/run_20260123_103000",
    "total_rows": 1234567,
    "files_written": 15
  },
  "error": null
}
```

**Status Values:**
- `pending`: Job is queued
- `processing`: Job is being processed
- `completed`: Job completed successfully
- `failed`: Job failed with error

### GET `/api/v1/health`

Health check endpoint.

**Response (200 OK):**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "spark_available": true
}
```

## Configuration

Configuration is loaded from `config.yaml`. Key settings:

```yaml
api:
  host: "0.0.0.0"
  port: 8000

paths:
  input_directory: "/path/to/input"  # Base directory with group subdirectories
  final_stage_directory: "/path/to/final_stage"  # Output directory
  temp_directory: "/path/to/temp"  # Temporary files

processing:
  explode_service_codes: false
  is_serverless: true
  skip_url_download: true
```

## Installation

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure Java 11+ is installed (required for PySpark)

3. Configure `config.yaml` with your paths

4. Run the service:
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Docker

1. Build the image (from the parquet-conversion-api directory):
```bash
# Option 1: Copy src into image during build
# First, ensure src/ is accessible, then modify Dockerfile to include:
# COPY ../src/ ./src/
docker build -t parquet-conversion-api .

# Option 2: Mount src as volume at runtime (recommended for development)
docker build -t parquet-conversion-api .
```

2. Run the container:
```bash
# Mount src directory and data directories as volumes
docker run -p 8000:8000 \
  -v /path/to/src:/app/src \
  -v /path/to/input:/input \
  -v /path/to/final_stage:/final_stage \
  -v /path/to/temp:/temp \
  parquet-conversion-api
```

**Note**: The `src/` directory must be accessible to the container. Either:
- Copy it into the image during build (modify Dockerfile)
- Mount it as a volume at runtime (recommended for development)

## File Discovery

The service searches for source files in group subdirectories within the configured `input_directory`:

```
input_directory/
├── group_1/
│   ├── schema.json
│   ├── file1.json.gz
│   └── file2.json.gz
├── group_2/
│   ├── schema.json
│   └── file3.json.gz
└── ...
```

The service will:
1. Search all `group_*` subdirectories for the source file
2. Load `schema.json` from the same group directory
3. Handle split files (files with `_part####` suffix) automatically

## Filter Format

### NPI Filter

Comma-separated list of NPI values:
```
"1234567890,9876543210,1111111111"
```

### Billing Code Filter

Comma-separated list of billing codes or ranges:
```
"70010-76499,76506-76999,77001-77092"
```

Ranges are inclusive (e.g., `70010-76499` means `billing_code >= 70010 AND billing_code <= 76499`).

## Output Structure

Processed files are written to the `final_stage_directory` with this structure:

```
final_stage/
└── run_20260123_103000/
    ├── schema.json                    # Copied from source group
    ├── manifest.json                  # Processing metadata
    └── fact/                          # Fact table Parquet files
        ├── part-00000-*.parquet
        └── ...
```

## Dependencies

- **FastAPI**: Web framework
- **PySpark**: Data processing
- **Pydantic**: Request/response validation
- **Python 3.10+**: Python version
- **Java 11+**: Required for PySpark

## Import Structure

The service imports modules from the parent `src/` directory. The processor module attempts to import using both `src.convert.*` (for compatibility with existing code) and `src.*` (direct imports) patterns. If you encounter import errors, ensure:

1. The `src/` directory is accessible from the API service (either in the same parent directory or mounted as a volume)
2. The Python path includes the parent directory containing `src/`

For Docker deployments, mount the `src/` directory:
```bash
docker run -v /path/to/src:/app/src ...
```

## Error Handling

The service handles various error conditions:

- **File not found**: Returns 404 with clear error message
- **Schema not found**: Returns 400 with error message
- **Processing errors**: Returns 500 with job_id for status check
- **Invalid filters**: Returns 400 with validation errors

## Logging

Logs are written to console with INFO level by default. Log format:
```
%(asctime)s %(levelname)s %(name)s - %(message)s
```

## Development

### Project Structure

```
parquet-conversion-api/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI app, route handlers
│   ├── models.py               # Pydantic models
│   ├── config.py               # Configuration loading
│   ├── job_manager.py          # Job queue and status tracking
│   ├── file_finder.py          # File discovery logic
│   ├── processor.py            # Core processing logic
│   └── utils.py                # Helper functions
├── config.yaml                 # Service configuration
├── requirements.txt
├── Dockerfile
└── README.md
```

### Testing

Example API calls:

```bash
# Convert a file
curl -X POST "http://localhost:8000/api/v1/convert" \
  -H "Content-Type: application/json" \
  -d '{
    "source_file": "file.json.gz",
    "npi_filter": "1234567890",
    "billing_code_filter": "70010-76499"
  }'

# Check job status
curl "http://localhost:8000/api/v1/status/job_abc123"

# Health check
curl "http://localhost:8000/api/v1/health"
```

## Notes

- The service is **stateless** - each request is independent
- Jobs are processed asynchronously using FastAPI background tasks
- Job status is stored in-memory (not persistent across restarts)
- For production, consider using Celery with Redis for distributed job processing
- Ensure PySpark dependencies are available in the deployment environment
