# Zipcode Data Database Upload

This project processes zipcode CSV files and uploads them directly to PostgreSQL database using configuration from `config.yaml`.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Place your CSV files in the `input/` directory:
   - `zip_code_proximity.csv` - Contains zip code proximity data with columns: zip1, zip2, miles_to_zcta5
   - `zip_zcta_xref.csv` - Contains zip code to ZCTA cross-reference data with columns: zip_code, zcta, source

3. Configure database settings in `config.yaml`:
   - Edit `database` section to configure PostgreSQL connection
   - Update paths if your CSV files are in different locations

## Usage

Run the upload script:
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
- `format`: Log message format
- `date_format`: Date format in logs

### Paths
- `zip_code_proximity_csv`: Path to zip_code_proximity CSV file (default: "input/zip_code_proximity.csv")
- `zip_zcta_xref_csv`: Path to zip_zcta_xref CSV file (default: "input/zip_zcta_xref.csv")
- `sql_file`: Path to SQL file with table creation and post-upload statements (default: "zipcode.sql")

### Database
- `connection_string`: Full PostgreSQL connection string (optional, takes precedence)
- `host`: Database host (default: "localhost")
- `port`: Database port (default: 5432)
- `database`: Database name
- `user`: Database user
- `password`: Database password
- `zip_code_proximity_table`: Target table name for zip_code_proximity data (default: "zip_code_proximity")
- `zip_zcta_xref_table`: Target table name for zip_zcta_xref data (default: "zip_zcta_xref")
- `upload_enabled`: Set to `true` to enable automatic upload
- `if_exists`: What to do if table exists - "fail", "replace", or "append" (default: "replace")

## What it does

The script performs the following steps in order:

1. **Creates zip_code_proximity table**: Reads the table creation SQL from `zipcode.sql` (lines 1-6) and creates the table in the database.

2. **Uploads zip_code_proximity.csv**: Uploads the CSV data to the `zip_code_proximity` table. The table has the following structure:
   - `zip1` CHAR(5) NOT NULL
   - `zip2` CHAR(5) NOT NULL
   - `miles_to_zcta5` NUMERIC(10,7) NOT NULL
   - PRIMARY KEY (zip1, zip2)

3. **Executes post-upload INSERT**: Runs the INSERT statement from `zipcode.sql` (lines 8-19) to add self-referential rows (zip1 = zip2, miles_to_zcta5 = 0) for zip codes that don't already have them.

4. **Creates zip_zcta_xref table**: Creates a table with the following structure:
   - `zip_code` VARCHAR(255) NOT NULL
   - `zcta` VARCHAR(255) NOT NULL
   - `source` VARCHAR(255) NOT NULL
   - PRIMARY KEY (zip_code, zcta, source)

5. **Uploads zip_zcta_xref.csv**: Uploads the CSV data to the `zip_zcta_xref` table.

The script processes large files efficiently by reading and uploading data in chunks (100,000 rows per chunk).

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
     upload_enabled: true
     if_exists: "replace"  # or "append" or "fail"
   ```

2. The script will automatically:
   - Create the tables if they don't exist
   - Upload the CSV data
   - Execute the post-upload SQL statements

### Upload Process

- Data is uploaded in chunks of 100,000 rows for efficient processing
- Progress is logged for each chunk with percentage completion
- The script handles connection pooling and automatic reconnection
- If `if_exists: "replace"`, existing tables are dropped before upload

## Requirements

- Python 3.7+
- pandas
- pyyaml
- sqlalchemy
- psycopg2-binary
