#!/usr/bin/env python3
"""
Test script to generate jq command from mrf_analysis table.

This script:
1. Queries mrf_analysis for a specific file
2. Analyzes array paths to understand structure
3. Generates a jq command that splits nested arrays while preserving parent context
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from src.shared.config import load_config
from src.shared.database import build_connection_string
from src.convert.json_to_ndjson import load_sql_file


def get_all_array_paths(connection_string: str, file_name: str) -> list[tuple[str, int, str]]:
    """
    Get all array paths from mrf_analysis for a file.
    
    Returns:
        List of (path, level, key) tuples ordered by level DESC (innermost first)
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # First, check if file exists and show what we find
        cursor.execute("""
            SELECT COUNT(*) 
            FROM mrf_analysis 
            WHERE file_name = %s
        """, (file_name,))
        total_count = cursor.fetchone()[0]
        print(f"  Total records in mrf_analysis for this file: {total_count}")
        
        # Show breakdown by value_dtype
        cursor.execute("""
            SELECT value_dtype, COUNT(*) 
            FROM mrf_analysis 
            WHERE file_name = %s
            GROUP BY value_dtype
            ORDER BY value_dtype
        """, (file_name,))
        dtype_counts = cursor.fetchall()
        print(f"  Records by value_dtype:")
        for dtype, count in dtype_counts:
            print(f"    {dtype}: {count}")
        
        # Query mrf_analysis for all arrays
        cursor.execute("""
            SELECT DISTINCT 
                path,
                level,
                key
            FROM mrf_analysis
            WHERE file_name = %s
              AND value_dtype = 'list'
            ORDER BY level ASC, path;
        """, (file_name,))
        
        rows = cursor.fetchall()
        
        # Extract top-level array keys from paths
        # Top-level arrays appear as keys in level 1 paths like "in_network[0]..."
        # The top-level key is the part before the first "["
        top_level_keys = set()
        for path, level, key in rows:
            if level == 1 and '[' in path:
                # Extract top-level key: "in_network[0]..." -> "in_network"
                top_key = path.split('[')[0]
                top_level_keys.add(top_key)
            elif level == 0 and path == key:
                # Direct top-level array entry
                top_level_keys.add(key)
        
        # Add top-level arrays to the results
        all_arrays = list(rows)
        for top_key in top_level_keys:
            # Check if we already have this as a level 0 entry
            if not any(path == top_key and level == 0 for path, level, _ in all_arrays):
                all_arrays.append((top_key, 0, top_key))
        
        # Debug: show what we found
        print(f"  Found {len(all_arrays)} array(s) with value_dtype='list'")
        print(f"  Top-level array keys: {sorted(top_level_keys)}")
        if len(all_arrays) > 0:
            print(f"  Sample arrays:")
            for row in all_arrays[:10]:
                print(f"    Level {row[1]}: path='{row[0]}', key='{row[2]}'")
        
        return [(row[0], row[1], row[2]) for row in all_arrays]
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def parse_path(path: str) -> list[str]:
    """
    Parse a path like "in_network[0].negotiated_prices" into components.
    
    Returns:
        List of path components, e.g., ["in_network", "negotiated_prices"]
    """
    # Remove array indices like [0], [1]
    parts = []
    current = ""
    for char in path:
        if char == '[':
            if current:
                parts.append(current)
                current = ""
        elif char == ']':
            continue
        elif char == '.':
            if current:
                parts.append(current)
                current = ""
        else:
            current += char
    if current:
        parts.append(current)
    return parts


def get_parent_path(path: str) -> str | None:
    """
    Get parent path from a nested path.
    
    Example:
        "in_network[0].negotiated_prices" -> "in_network"
        "in_network[0].negotiated_prices[0].billing_class" -> "in_network[0].negotiated_prices"
    """
    # Find the last array index pattern
    last_bracket = path.rfind('[')
    if last_bracket == -1:
        return None
    
    # Find the dot before the last bracket
    dot_before = path.rfind('.', 0, last_bracket)
    if dot_before == -1:
        # Top-level array
        return path[:last_bracket]
    else:
        return path[:last_bracket]


def get_scalar_fields(connection_string: str, file_name: str, parent_path: str) -> list[str]:
    """
    Get all scalar fields at a given path level from mrf_analysis.
    
    Args:
        connection_string: Database connection string
        file_name: File name to query
        parent_path: Path to parent (e.g., "in_network[0]" or "in_network[0].negotiated_rates[0]")
        
    Returns:
        List of scalar field names
    """
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(connection_string)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Find all scalar fields that are children of parent_path
        if parent_path == "":
            # Root level - look for level 0 scalars
            cursor.execute("""
                SELECT DISTINCT key
                FROM mrf_analysis
                WHERE file_name = %s
                  AND level = 0
                  AND value_dtype = 'scalar'
                ORDER BY key
            """, (file_name,))
        else:
            # Nested level - look for scalars under this path
            cursor.execute("""
                SELECT DISTINCT key
                FROM mrf_analysis
                WHERE file_name = %s
                  AND path LIKE %s
                  AND value_dtype = 'scalar'
                ORDER BY key
            """, (file_name, f"{parent_path}.%"))
        
        rows = cursor.fetchall()
        return [row[0] for row in rows]
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def build_jq_expression_with_index(array_paths: list[tuple[str, int, str]], connection_string: str, file_name: str) -> tuple[str, str]:
    """
    Build a jq expression following the user's pattern:
    - Root-level scalars with safe fallbacks
    - Use to_entries[] for stable indices
    - Iterate through nested arrays
    - Keep innermost arrays as arrays (don't explode them)
    - Include all scalar fields at each level
    - Add indices for joining
    
    Returns:
        Tuple of (jq_expression, description)
    """
    if not array_paths:
        return ".", "No arrays found"
    
    # Find top-level arrays (keys that appear at level 1 with [0])
    top_level_keys = set()
    for path, level, key in array_paths:
        if level == 1 and '[' in path:
            top_key = path.split('[')[0]
            top_level_keys.add(top_key)
    
    if not top_level_keys:
        return ".", "No top-level arrays found"
    
    # For now, handle first top-level array
    # TODO: Handle multiple top-level arrays (separate outputs)
    top_key = sorted(top_level_keys)[0]
    
    # Find nested arrays under this top-level array, sorted by level
    nested = sorted([(path, level, key) for path, level, key in array_paths 
                     if level > 0 and path.startswith(f"{top_key}[")], 
                    key=lambda x: x[1])
    
    # Get root-level scalars
    root_scalars = get_scalar_fields(connection_string, file_name, "")
    
    # Build the jq expression following user's pattern
    parts = []
    
    # 1. Root-level scalars (with safe fallbacks)
    root_obj_parts = [f"{key}: (.{key} // null)" for key in root_scalars]
    root_obj = "{" + ", ".join(root_obj_parts) + "}"
    parts.append(f"{root_obj} as $root")
    
    # 2. Iterate top-level array with to_entries for stable index
    parts.append(f"| (.{top_key} // []) | to_entries[] as $t")
    parts.append(f"| ($t.value) as $top")
    
    # Get scalar fields at top level
    top_scalars = get_scalar_fields(connection_string, file_name, f"{top_key}[0]")
    
    # 3. Iterate through nested arrays (but NOT the innermost)
    # Find which arrays to iterate (all except the deepest)
    if nested:
        # Exclude the innermost (last one)
        arrays_to_iterate = nested[:-1]
        innermost_path, innermost_level, innermost_key = nested[-1]
        
        # Build iteration chain for intermediate arrays
        current_var = "$top"
        var_chain = [("$top", top_key, top_scalars)]
        idx_vars = [("$t", "in_network_idx")]
        
        for i, (nested_path, nested_level, nested_key) in enumerate(arrays_to_iterate):
            # Get parent path for scalar fields
            parent_path = nested_path.rsplit('.', 1)[0] if '.' in nested_path else f"{top_key}[0]"
            nested_scalars = get_scalar_fields(connection_string, file_name, parent_path)
            
            # Create variable names
            var_name = f"${nested_key[:4]}"  # e.g., "$rate" for "negotiated_rates"
            idx_var = f"${nested_key[:4]}_idx"  # e.g., "$rate_idx"
            
            # Iterate with to_entries
            # Need to build the expression string carefully to avoid f-string issues
            if current_var == "$top":
                parts.append(f"| ($top.{nested_key} // []) | to_entries[] as {idx_var}")
            else:
                # For nested levels, use the variable name directly
                parts.append(f"| ({current_var}.{nested_key} // []) | to_entries[] as {idx_var}")
            parts.append(f"| ({idx_var}.value) as {var_name}")
            
            var_chain.append((var_name, nested_key, nested_scalars))
            idx_vars.append((idx_var, f"{nested_key}_idx"))
            current_var = var_name
        
        # 4. Build output record - keep innermost arrays as arrays
        # Get scalar fields at innermost level
        innermost_parent = innermost_path.rsplit('.', 1)[0] if '.' in innermost_path else f"{top_key}[0]"
        innermost_scalars = get_scalar_fields(connection_string, file_name, innermost_parent)
        
        # Build the output object
        output_parts = []
        
        # Add root scalars
        output_parts.append("$root")
        
        # Add indices
        for idx_var, idx_name in idx_vars:
            output_parts.append(f"{idx_name}: {idx_var}.key")
        
        # Add scalar fields from each level
        for var_name, array_key, scalars in var_chain:
            for scalar in scalars:
                output_parts.append(f"{array_key}_{scalar}: ({var_name}.{scalar} // null)")
        
        # Add innermost level scalars (from current_var which is the last intermediate)
        for scalar in innermost_scalars:
            output_parts.append(f"{innermost_key}_{scalar}: ({current_var}.{scalar} // null)")
        
        # Keep innermost arrays as arrays (don't explode)
        # Find all arrays at the innermost level
        innermost_arrays = [key for path, level, key in nested if level == innermost_level]
        for array_key in innermost_arrays:
            output_parts.append(f"{array_key}: ({current_var}.{array_key} // [])")
        
        output_obj = "{" + ", ".join(output_parts) + "}"
        parts.append(f"| {output_obj}")
    else:
        # No nested arrays, just output top-level with scalars
        output_parts = ["$root"]
        output_parts.append("in_network_idx: $t.key")
        for scalar in top_scalars:
            output_parts.append(f"in_network_{scalar}: ($top.{scalar} // null)")
        # Keep any arrays at top level
        top_arrays = [key for path, level, key in array_paths if level == 0 and path == key]
        for array_key in top_arrays:
            output_parts.append(f"{array_key}: ($top.{array_key} // [])")
        output_obj = "{" + ", ".join(output_parts) + "}"
        parts.append(f"| {output_obj}")
    
    final_expr = " ".join(parts)
    description = f"Extract from {top_key} with stable indices, keep innermost arrays"
    
    return final_expr, description


def build_jq_expression(array_paths: list[tuple[str, int, str]], connection_string: str, file_name: str) -> str:
    """
    Build a jq expression from array paths.
    
    Strategy:
    - Process arrays from outermost to innermost
    - Use variables to capture parent context
    - Merge parent fields into nested items
    
    Example:
        Input: [
            ("in_network", 0, "in_network"),
            ("in_network[0].negotiated_prices", 1, "negotiated_prices"),
        ]
        Output: '.in_network[] as $in | $in.negotiated_prices[] as $price | $price + $in | del($in.negotiated_prices)'
    """
    if not array_paths:
        return "."
    
    # Group by level and find the hierarchy
    # Top-level arrays (level 0)
    top_level = [(path, level, key) for path, level, key in array_paths if level == 0]
    
    if not top_level:
        return "."
    
    # For now, handle the first top-level array
    # TODO: Handle multiple top-level arrays
    top_path, top_level_num, top_key = top_level[0]
    
    # Find nested arrays under this top-level array, sorted by level
    nested = [(path, level, key) for path, level, key in array_paths 
              if level > 0 and path.startswith(f"{top_key}[")]
    
    if not nested:
        # No nested arrays, just extract top-level items
        return f".{top_key}[]"
    
    # Build hierarchy: map each nested array to its parent
    hierarchy = {}  # nested_path -> parent_path
    for nested_path, nested_level, nested_key in nested:
        parent = get_parent_path(nested_path)
        hierarchy[nested_path] = parent
    
    # Sort nested arrays by level (outermost to innermost)
    nested_sorted = sorted(nested, key=lambda x: x[1])  # Sort by level ascending
    
    # Build the expression step by step
    parts = []
    var_map = {}  # path -> variable_name
    
    # Start with top-level array
    top_var = "$top"
    var_map[top_key] = top_var
    parts.append(f".{top_key}[] as {top_var}")
    
    # Process nested arrays from outermost to innermost
    for nested_path, nested_level, nested_key in nested_sorted:
        parent_path = hierarchy[nested_path]
        
        # Find parent variable
        parent_var = None
        if parent_path == top_key:
            parent_var = top_var
        else:
            # Find variable for parent path
            for path, var in var_map.items():
                if nested_path.startswith(f"{path}["):
                    parent_var = var
                    break
        
        if not parent_var:
            continue
        
        # Create variable for this nested array
        nested_var = "$" + nested_key.replace("_", "")[:6]  # e.g., "$negpri" for "negotiated_prices"
        var_map[nested_path] = nested_var
        
        # Extract from nested array
        parts.append(f"{parent_var}.{nested_key}[] as {nested_var}")
    
    # Build the output: merge innermost item with parent contexts
    # Process from innermost to outermost, merging each parent level
    innermost_path, innermost_level, innermost_key = nested_sorted[-1]
    innermost_var = var_map[innermost_path]
    
    # Start with innermost item
    output = innermost_var
    
    # Walk up the hierarchy, merging each parent level
    current_path = innermost_path
    while current_path in hierarchy:
        parent_path = hierarchy[current_path]
        
        # Find parent variable
        if parent_path == top_key:
            parent_var = top_var
        else:
            # Find variable for parent path
            parent_var = None
            for path, var in var_map.items():
                if current_path.startswith(f"{path}["):
                    parent_var = var
                    break
            if not parent_var:
                break
        
        # Get the key being split at this level
        current_key = next(key for path, _, key in nested_sorted if path == current_path)
        
        # Merge parent, excluding the array field we're splitting
        # Pattern: $child + ($parent | del(.array_field))
        output = f"{output} + ({parent_var} | del(.{current_key}))"
        
        # Move up hierarchy
        current_path = parent_path
        if current_path == top_key:
            break
    
    parts.append(output)
    
    return " | ".join(parts)


def generate_jq_command(file_name: str, connection_string: str) -> str:
    """
    Generate a complete jq command for a file.
    
    Returns:
        Complete jq command string
    """
    # Get array paths from database
    array_paths = get_all_array_paths(connection_string, file_name)
    
    print(f"Found {len(array_paths)} array(s) for file: {file_name}")
    print("\nArray paths (innermost first):")
    for path, level, key in array_paths:
        print(f"  Level {level}: {path} (key: {key})")
    
    # Build jq expression with index
    jq_expr, description = build_jq_expression_with_index(array_paths, connection_string, file_name)
    
    print(f"\nExpression description: {description}")
    
    # Build complete command
    # Note: On Windows, might need to use gzip or zcat equivalent
    # For now, assuming we can use zcat or gzip -dc
    # Output to NDJSON first, then convert to Parquet
    ndjson_output = file_name.replace(".json.gz", ".ndjson")
    command = f'zcat "{file_name}" | jq -c "{jq_expr}" > "{ndjson_output}"'
    
    # Windows alternative (if zcat not available):
    # command = f'gzip -dc "{file_name}" | jq -c "{jq_expr}" > "{ndjson_output}"'
    
    # Also generate Parquet conversion command
    parquet_output = file_name.replace(".json.gz", ".parquet")
    parquet_command = f"""
# Step 1: Convert to NDJSON with jq
{command}

# Step 2: Convert NDJSON to Parquet (using PySpark or similar)
# python -c "
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('ConvertNDJSON').getOrCreate()
# df = spark.read.json('{ndjson_output}')
# df.write.parquet('{parquet_output}', mode='overwrite')
# spark.stop()
# "
"""
    
    return command


def main():
    """Main entry point."""
    # Load config
    config = load_config(Path("config.yaml"))
    db_config = config.get("database", {})
    cloud_config = config.get("cloud", {})
    connection_string = build_connection_string(db_config, cloud_config)
    
    # Test file
    file_name = "2025-11-13_Blue-Cross-and-Blue-Shield-of-Illinois_Blue-Advantage-HMO-BLUEH-P67-IL-HMO-Non-Standard_in-network-rates.json.gz"
    
    print(f"Generating jq command for: {file_name}\n")
    
    try:
        command = generate_jq_command(file_name, connection_string)
        
        print("\n" + "="*80)
        print("Generated jq command:")
        print("="*80)
        print(command)
        print("="*80)
        
        # Also save to file for easy execution
        output_file = Path("generated_jq_command.sh")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write(f"# Generated jq command for {file_name}\n")
            f.write(f"{command}\n")
        
        print(f"\nCommand also saved to: {output_file}")
        print("To execute: bash generated_jq_command.sh")
        
    except Exception as exc:
        print(f"Error: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
