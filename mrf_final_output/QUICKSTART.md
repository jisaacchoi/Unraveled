# Quick Start Guide

## Prerequisites

1. Python 3.10+
2. Java 11+ (required for PySpark)
3. Access to the `src/` directory from the parent project

## Setup

1. **Install dependencies:**
```bash
cd parquet-conversion-api
pip install -r requirements.txt
```

2. **Configure paths in `config.yaml`:**
```yaml
paths:
  input_directory: "/path/to/your/input"  # Directory with group_* subdirectories
  final_stage_directory: "/path/to/output"  # Where processed files go
  temp_directory: "/path/to/temp"  # Temporary files
```

3. **Ensure `src/` directory is accessible:**
   - The service imports modules from the parent `src/` directory
   - If running from `parquet-conversion-api/`, the parent directory should contain `src/`
   - Or adjust the import path in `app/processor.py`

## Running the Service

### Local Development

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Production

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

## Testing the API

### 1. Health Check
```bash
curl http://localhost:8000/api/v1/health
```

### 2. Convert a File
```bash
curl -X POST "http://localhost:8000/api/v1/convert" \
  -H "Content-Type: application/json" \
  -d '{
    "source_file": "your_file.json.gz",
    "npi_filter": "1234567890,9876543210",
    "billing_code_filter": "70010-76499"
  }'
```

Response:
```json
{
  "job_id": "job_abc123",
  "status": "accepted",
  "message": "Job queued for processing",
  "status_url": "/api/v1/status/job_abc123"
}
```

### 3. Check Job Status
```bash
curl http://localhost:8000/api/v1/status/job_abc123
```

### 4. View API Documentation
Open in browser: `http://localhost:8000/docs`

## Common Issues

### Import Errors
If you see import errors related to `src` modules:
- Ensure the `src/` directory is in the parent directory of `parquet-conversion-api/`
- Or modify `app/processor.py` to adjust the import path

### File Not Found
- Verify the `input_directory` in `config.yaml` is correct
- Ensure group subdirectories (group_1, group_2, etc.) exist
- Check that `schema.json` exists in each group directory

### Spark Errors
- Ensure Java 11+ is installed and `JAVA_HOME` is set
- Check Spark logs for detailed error messages

## Next Steps

- Review the full [README.md](README.md) for detailed documentation
- Check `config.yaml` for all configuration options
- Monitor logs for processing status
