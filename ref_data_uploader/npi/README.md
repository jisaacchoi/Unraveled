# CSV Filter and Database Upload

This project processes CSV files and uploads them directly to PostgreSQL database using configuration from `config.yaml`. It supports optional filtering of CSV data before upload.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure paths and settings in `config.yaml`:
   - Edit `logging` section to configure log level, file, and format
   - Edit `paths` section to set input CSV, filtered CSV, and upload CSV paths
   - Edit `filter` section to enable/disable filtering and set filter criteria
   - Edit `database` section to configure PostgreSQL connection

## Usage

1. Place your CSV file in the location specified in `config.yaml` (or update the path in config)
2. Update `config.yaml` with your file paths and settings
3. Run the conversion script:
```bash
python process_csv.py
```

Or specify a custom config file:
```bash
python process_csv.py config.yaml
```

## Configuration

Edit `config.yaml` to configure:

### Logging
- `level`: Log level - "DEBUG", "INFO", "WARNING", "ERROR", or "CRITICAL" (default: "INFO")
- `file`: Log file path (empty string to disable file logging, default: "process_csv.log")
- `console`: Enable console logging (default: true)
- `format`: Log message format (default: "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
- `date_format`: Date format in logs (default: "%Y-%m-%d %H:%M:%S")

### Paths
- `input_csv`: Path to input CSV file (used when filtering is enabled)
- `filtered_csv`: Path for filtered CSV output (optional, saved when filtering is enabled)
- `upload_csv`: Path to CSV file for direct upload (required when filtering is disabled)

### Filter
- `enabled`: Set to `true` to enable filtering, `false` to disable (default: `false`)
- `column`: Column name to filter on (required when filtering is enabled)
- `value`: Value to filter for (e.g., `2`) (required when filtering is enabled)

### Database
- `connection_string`: Full PostgreSQL connection string (optional, takes precedence)
- `host`: Database host (default: "localhost")
- `port`: Database port (default: 5432)
- `database`: Database name
- `user`: Database user
- `password`: Database password
- `table_name`: Target table name for upload (default: "provider_data")
- `upload_enabled`: Set to `true` to enable automatic upload after filtering
- `if_exists`: What to do if table exists - "fail", "replace", or "append" (default: "replace")

## What it does

The script operates in two modes based on the `filter.enabled` setting:

### When Filtering is Enabled (`filter.enabled: true`):
1. **Reads** the input CSV file from `paths.input_csv`
2. **Filters** CSV rows based on `filter.column == filter.value` (equivalent to `csvgrep -c "Entity Type Code" -m "2"`)
3. **Saves** the filtered CSV to `paths.filtered_csv` (optional)
4. **Uploads** filtered data directly to PostgreSQL database (if `upload_enabled: true`)

### When Filtering is Disabled (`filter.enabled: false`):
1. **Reads** the CSV file directly from `paths.upload_csv`
2. **Uploads** all data directly to PostgreSQL database (if `upload_enabled: true`)
3. No filtering or intermediate CSV saving is performed

The script processes large files efficiently by reading and uploading data in chunks (100,000 rows per chunk for reading, 100,000 rows per chunk for uploading).

## Database Upload

To enable automatic upload to PostgreSQL:

1. Configure database settings in `config.yaml`:
   ```yaml
   database:
     host: "localhost"
     port: 5432
     database: "postgres"
     user: "postgres"
     password: "your_password"
     table_name: "provider_data"
     upload_enabled: true
     if_exists: "replace"  # or "append" or "fail"
   ```

2. The script will automatically upload the data to the specified table:
   - If filtering is enabled: uploads filtered data after filtering
   - If filtering is disabled: uploads data directly from `upload_csv` path

### Upload Process

- Data is uploaded in chunks of 100,000 rows for efficient processing
- Progress is logged for each chunk with percentage completion
- The script handles connection pooling and automatic reconnection
- If `if_exists: "replace"`, the existing table is dropped before upload

## Requirements

- Python 3.7+
- pandas
- pyyaml
- sqlalchemy
- psycopg2-binary

