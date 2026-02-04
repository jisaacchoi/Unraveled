import os
import sys
import glob
import time
import argparse
import ast
import csv
from pathlib import Path

import psycopg2


def normalize_npi_brackets(line: str, npi_col_index: int) -> str:
    """
    If the NPI column is formatted like: [1, 2, 3]
    convert to Postgres array literal: {1,2,3}

    Uses proper CSV parsing to handle quoted fields correctly.
    """
    # Use csv.reader to properly parse the line (handles quoted fields)
    reader = csv.reader([line.rstrip("\n")])
    try:
        parts = list(next(reader))
    except StopIteration:
        return line
    
    if npi_col_index >= len(parts):
        return line

    v = parts[npi_col_index].strip()

    # Only transform obvious bracketed lists
    # Handle both quoted and unquoted values
    v_unquoted = v.strip('"').strip("'")
    if v_unquoted.startswith("[") and v_unquoted.endswith("]"):
        inner = v_unquoted[1:-1].strip()
        # Remove spaces and convert to PostgreSQL array format
        inner = inner.replace(" ", "")
        # Replace the value in the parts list
        parts[npi_col_index] = "{" + inner + "}"

    # Reconstruct the line using csv.writer to properly quote fields if needed
    from io import StringIO
    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL, lineterminator="")
    writer.writerow(parts)
    result = output.getvalue()
    # Ensure we have a newline at the end
    if not result.endswith("\n"):
        result += "\n"
    return result


def copy_file(conn, file_path: Path, table: str, columns: str | None, has_header: bool,
              normalize_npi: bool, npi_col_index: int) -> int:
    """
    COPY one CSV file into target table. Returns rowcount if available (best-effort).
    Uses csv module to properly handle quoted fields and newlines.
    """
    with conn.cursor() as cur:
        # Verify table exists before attempting COPY
        table_parts = table.split(".")
        if len(table_parts) == 2:
            schema_name, table_name = table_parts
        else:
            schema_name = "public"
            table_name = table
        
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_name, table_name))
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            raise ValueError(f"Table {table} does not exist. Please create it first.")
        
        col_clause = f"({columns})" if columns else ""
        header_clause = "HEADER true" if has_header else "HEADER false"
        
        # Use properly quoted table name for COPY (must match CREATE TABLE format)
        if schema_name == "public":
            # For public schema, we can use unquoted or quoted format
            # Use the format that matches how the table was created
            quoted_table = f'"{schema_name}"."{table_name}"'
        else:
            quoted_table = f'"{schema_name}"."{table_name}"'

        sql = f"""
            COPY {quoted_table} {col_clause}
            FROM STDIN
            WITH (FORMAT csv, {header_clause})
        """.strip()

        # Read CSV file properly and handle normalization
        from io import StringIO
        
        buffer = StringIO()
        writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            
            for row_idx, row in enumerate(reader):
                if normalize_npi and npi_col_index < len(row):
                    # Normalize NPI column if needed
                    # Convert Python list format ['val1', 'val2'] to PostgreSQL array format {val1,val2}
                    v = row[npi_col_index].strip()
                    if not v or v.lower() == 'null' or v.lower() == 'none':
                        row[npi_col_index] = "{}"
                        continue
                    
                    # Remove outer quotes if present
                    v_unquoted = v.strip('"').strip("'")
                    
                    # Check if it's a Python list format: ['val1', 'val2'] or ["val1", "val2"]
                    if v_unquoted.startswith("[") and v_unquoted.endswith("]"):
                        inner = v_unquoted[1:-1].strip()
                        if not inner:
                            # Empty list
                            row[npi_col_index] = "{}"
                        else:
                            # Parse the list elements
                            try:
                                # Use ast.literal_eval to safely parse the Python list
                                parsed_list = ast.literal_eval(v_unquoted)
                                if isinstance(parsed_list, list):
                                    # Convert to PostgreSQL array format: {val1,val2,val3}
                                    # Remove quotes from string elements and join with commas
                                    pg_array = "{" + ",".join(str(item).strip('"').strip("'") for item in parsed_list) + "}"
                                    row[npi_col_index] = pg_array
                                else:
                                    # Not a list, keep original
                                    pass
                            except (ValueError, SyntaxError):
                                # If ast.literal_eval fails, try manual parsing
                                # Remove quotes and spaces, split by comma
                                inner_clean = inner.replace(" ", "").replace("'", "").replace('"', "")
                                if inner_clean:
                                    pg_array = "{" + inner_clean + "}"
                                    row[npi_col_index] = pg_array
                                else:
                                    row[npi_col_index] = "{}"
                
                # Write the row to buffer
                writer.writerow(row)
        
        buffer.seek(0)
        cur.copy_expert(sql, buffer)

    # psycopg2 doesn't reliably return rows copied for COPY FROM STDIN; we'll just return -1.
    return -1


def main():
    ap = argparse.ArgumentParser(description="Bulk load all CSV files in a directory into one Postgres table.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--db", required=True, help="Database name")
    ap.add_argument("--user", required=True, help="Postgres user")
    ap.add_argument("--password", default=None, help="Postgres password (or omit to use PGPASSWORD/.pgpass)")
    ap.add_argument("--dir", required=True, help="Directory containing CSV files")
    ap.add_argument("--pattern", default="*.csv", help="Glob pattern (default: *.csv)")
    ap.add_argument("--table", required=True, help="Target table, e.g. public.bariatric_merged")
    ap.add_argument("--columns", default=None,
                    help="Comma-separated column list (no parentheses). Example: col1,col2,col3")
    ap.add_argument("--no-header", action="store_true", help="Set if CSV files do NOT have header row")
    ap.add_argument("--normalize-npi", action="store_true",
                    help="Convert npi from [...] to {...} on the fly (see notes in script)")
    ap.add_argument("--npi-col-index", type=int, default=6,
                    help="0-based index of the npi column in the CSV (default 6 for NPI_Final column)")
    args = ap.parse_args()

    csv_dir = Path(args.dir).expanduser().resolve()
    files = sorted(csv_dir.glob(args.pattern))

    if not files:
        print(f"No files matched {csv_dir / args.pattern}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} file(s) in: {csv_dir}")
    print(f"Target table: {args.table}")
    if args.columns:
        print(f"Columns: {args.columns}")
    print(f"Header: {'no' if args.no_header else 'yes'}")
    print(f"Normalize NPI: {'yes' if args.normalize_npi else 'no'}")

    conn_kwargs = dict(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
    )
    if args.password is not None:
        conn_kwargs["password"] = args.password

    conn = psycopg2.connect(**conn_kwargs)
    conn.autocommit = False

    # Create table if it doesn't exist
    # Extract schema and table name
    table_parts = args.table.split(".")
    if len(table_parts) == 2:
        schema_name, table_name = table_parts
    else:
        schema_name = "public"
        table_name = args.table
    
    with conn.cursor() as cur:
        # Create schema if it doesn't exist
        try:
            # Use proper identifier quoting for schema name
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
            conn.commit()
            print(f"Ensured schema {schema_name} exists")
        except Exception as e:
            print(f"Warning: Could not create schema {schema_name}: {e}", file=sys.stderr)
            conn.rollback()
        
        # Create table with the new column structure
        # Use proper identifier quoting for table name
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
            billing_code                  text        NOT NULL,
            billing_code_type             text        NOT NULL,
            billing_code_type_version     integer     NOT NULL,
            negotiated_rate               numeric(12,2) NOT NULL,
            negotiated_type               text        NOT NULL,
            original_filename             text,
            "NPI_Final"                   text[]
        )
        """
        try:
            cur.execute(create_table_sql)
            conn.commit()
            print(f"Ensured table {args.table} exists")
        except Exception as e:
            print(f"Error: Could not create table {args.table}: {e}", file=sys.stderr)
            conn.rollback()
            # Check if table exists before failing
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema_name, table_name))
            table_exists = cur.fetchone()[0]
            if not table_exists:
                print(f"Fatal: Table {args.table} does not exist and could not be created. Exiting.", file=sys.stderr)
                conn.close()
                sys.exit(1)
            else:
                print(f"Table {args.table} already exists, continuing...")

    has_header = not args.no_header
    total_ok = 0
    total_fail = 0

    try:
        for i, fp in enumerate(files, start=1):
            start = time.time()
            print(f"[{i}/{len(files)}] Loading: {fp.name}")
            try:
                copy_file(
                    conn=conn,
                    file_path=fp,
                    table=args.table,
                    columns=args.columns,
                    has_header=has_header,
                    normalize_npi=args.normalize_npi,
                    npi_col_index=args.npi_col_index,
                )
                conn.commit()
                elapsed = time.time() - start
                print(f"    OK ({elapsed:0.2f}s)")
                total_ok += 1
            except Exception as e:
                conn.rollback()
                elapsed = time.time() - start
                print(f"    FAILED ({elapsed:0.2f}s): {e}", file=sys.stderr)
                total_fail += 1

        print(f"Done. OK={total_ok}, FAILED={total_fail}")
        if total_fail:
            sys.exit(2)
    finally:
        conn.close()


if __name__ == "__main__":
    main()