"""Utilities for processing Spark schema and column names."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F, types as T

LOG = logging.getLogger("src.convert.schema_utils")


def process_struct_level(df: DataFrame, struct_column: str, base_cols: list[str]) -> tuple[list, list]:
    """
    Process one level of a struct: extract scalars and identify arrays.
    Uses full path as alias to avoid ambiguous column names.
    
    Returns:
        (scalar_cols, array_fields) where:
        - scalar_cols: List of F.col expressions for scalar fields (with full path aliases)
        - array_fields: List of (field_name, full_path, alias_name, element_type) for arrays
    """
    try:
        # Get the struct field from schema
        struct_field = None
        for field in df.schema.fields:
            if field.name == struct_column:
                struct_field = field
                break
        
        if struct_field is None or not isinstance(struct_field.dataType, T.StructType):
            return ([], [])
        
        struct_type = struct_field.dataType
        scalar_cols = []
        array_fields = []
        
        for field in struct_type.fields:
            field_name = field.name
            full_path = f"{struct_column}.{field_name}"
            # Use | instead of dots in alias to avoid Spark parsing dots as nested paths
            alias_name = full_path.replace(".", "|")
            
            if isinstance(field.dataType, T.ArrayType):
                array_fields.append((field_name, full_path, alias_name, field.dataType.elementType))
            else:
                # Scalar field - extract with full path, alias with underscores
                scalar_cols.append(F.col(full_path).alias(alias_name))
        
        return (scalar_cols, array_fields)
    except Exception as e:
        LOG.error(f"Error processing struct {struct_column}: {e}")
        return ([], [])


def simplify_column_names(df: DataFrame) -> DataFrame:
    """
    Simplify column names by using only the element name (last part after splitting by |).
    If there are duplicates, rename them to element_x or element_y.
    For columns with _x or _y, the deeper level gets the simple element name,
    and the shallower level keeps element_x or element_y.
    
    Also drops any struct columns that have been fully extracted.
    
    Returns a new DataFrame with simplified column names.
    """
    # First, identify and drop struct columns that have been fully extracted
    cols_to_drop = []
    current_cols = list(df.columns)
    
    for col in current_cols:
        # Check if this column is a struct type
        try:
            field = next(f for f in df.schema.fields if f.name == col)
            if isinstance(field.dataType, T.StructType):
                # Check if there are other columns that start with this column name + |
                prefix = f"{col}|"
                has_extracted_fields = any(other_col.startswith(prefix) for other_col in current_cols if other_col != col)
                if has_extracted_fields:
                    cols_to_drop.append(col)
        except (StopIteration, Exception):
            pass
    
    # Drop struct columns that have been extracted
    if cols_to_drop:
        LOG.debug(f"Dropping extracted struct columns: {cols_to_drop}")
        df = df.select([F.col(c) for c in current_cols if c not in cols_to_drop])
        current_cols = [c for c in current_cols if c not in cols_to_drop]
    
    # Build simplified names: use only element name (last part)
    # Track columns by their element name
    element_to_cols = {}  # Map element_name -> list of (original_col, depth)
    
    for col in current_cols:
        parts = col.split("|")
        element_name = parts[-1] if len(parts) > 1 else col
        depth = len(parts)  # Number of levels (more | = deeper)
        
        if element_name not in element_to_cols:
            element_to_cols[element_name] = []
        element_to_cols[element_name].append((col, depth))
    
    # Resolve duplicates: use element name, add _x/_y for duplicates
    resolved_names = {}  # Map original column -> final name
    
    for element_name, cols_info in element_to_cols.items():
        if len(cols_info) == 1:
            # No duplicate, use simple element name
            original_col, _ = cols_info[0]
            resolved_names[original_col] = element_name
        else:
            # Duplicates found - rename to element_x, element_y, etc.
            for idx, (original_col, depth) in enumerate(cols_info):
                if idx == 0:
                    # First one gets simple name (will be adjusted later if needed)
                    resolved_names[original_col] = element_name
                else:
                    # Others get _x, _y suffix
                    suffix = chr(ord('x') + idx - 1)  # x, y, z, ...
                    resolved_names[original_col] = f"{element_name}_{suffix}"
    
    # Final pass: if we have both "element" and "element_x" (or _y), 
    # assign simple name to the deeper level
    final_resolved = resolved_names.copy()
    
    # Group by base element name to check for conflicts
    base_name_to_cols = {}  # base_name -> list of (original_col, final_name, depth)
    
    for original_col, final_name in resolved_names.items():
        if final_name.endswith('_x') or final_name.endswith('_y'):
            base_name = final_name[:-2]
        else:
            base_name = final_name
        
        if base_name not in base_name_to_cols:
            base_name_to_cols[base_name] = []
        
        parts = original_col.split("|")
        depth = len(parts)
        base_name_to_cols[base_name].append((original_col, final_name, depth))
    
    # For each base name, if we have both simple and _x/_y versions, 
    # assign simple name to deeper level
    for base_name, col_info_list in base_name_to_cols.items():
        simple_cols = [(col, name, depth) for col, name, depth in col_info_list if name == base_name]
        suffix_cols = [(col, name, depth) for col, name, depth in col_info_list if name != base_name]
        
        if simple_cols and suffix_cols:
            # We have both simple and _x/_y versions
            # Find the deepest column overall
            all_cols = simple_cols + suffix_cols
            deepest_col, deepest_name, deepest_depth = max(all_cols, key=lambda x: x[2])
            
            if deepest_name != base_name:
                # Deepest is one with _x/_y, swap names
                # Deepest gets simple name
                final_resolved[deepest_col] = base_name
                # The one that had simple name gets _x or _y
                for col, name, depth in simple_cols:
                    if col != deepest_col:
                        # Find an available suffix
                        used_suffixes = {n[-1] for _, n, _ in suffix_cols if len(n) > len(base_name) + 1}
                        available_suffix = 'x' if 'x' not in used_suffixes else 'y'
                        final_resolved[col] = f"{base_name}_{available_suffix}"
                        break
    
    # Build final names list in original column order
    final_names = [final_resolved[col] for col in current_cols]
    
    # Rename columns
    rename_dict = {old: new for old, new in zip(current_cols, final_names)}
    return df.select([F.col(old).alias(new) for old, new in rename_dict.items()])
