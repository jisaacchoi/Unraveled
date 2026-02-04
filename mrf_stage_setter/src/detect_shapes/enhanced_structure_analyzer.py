"""Enhanced MRF JSON structure analyzer with improved list handling and memoization."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterator, Optional, Set, Tuple

LOG = logging.getLogger("src.enhanced_structure_analyzer")

# Pattern to detect URLs in text
URL_PATTERN = re.compile(
    r'https?://[^\s<>"{}|\\^`\[\]]+|www\.[^\s<>"{}|\\^`\[\]]+',
    re.IGNORECASE,
)


def get_value_dtype(value: Any) -> str:
    """
    Get the data type of a value for analysis.
    
    Args:
        value: Value to check
        
    Returns:
        'scalar', 'list', or 'dict'
    """
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return "scalar"


def has_url(value: Any) -> bool:
    """
    Check if a value contains a URL pattern.
    
    Args:
        value: Value to check (can be any JSON-serializable type)
        
    Returns:
        True if value contains a URL, False otherwise
    """
    if value is None:
        return False
    
    # Convert to string and check for URL pattern
    value_str = json.dumps(value) if not isinstance(value, str) else value
    return bool(URL_PATTERN.search(value_str))


def count_unique_keys_in_structure(
    data: Any,
    current_path: str,
    memo: Dict[str, Set[str]],
    max_depth: int = 100,
    depth: int = 0,
) -> Tuple[Set[str], int]:
    """
    Count unique keys in a JSON structure (for comparison purposes).
    
    This traverses the structure and collects all unique key names (not paths),
    using memoization to avoid redundant work.
    
    Key names are normalized - e.g., "negotiated_prices[0].billing_class" and 
    "negotiated_prices[1].billing_class" both count as the same key "billing_class"
    at that level.
    
    Args:
        data: JSON data to analyze
        current_path: Current path in the structure (for memoization, not for key names)
        memo: Memoization cache mapping path signatures to unique keys
        max_depth: Maximum depth to traverse (prevent infinite recursion)
        depth: Current depth level
        
    Returns:
        Tuple of (set of unique key names, total count)
    """
    if depth > max_depth:
        LOG.warning("Maximum depth reached at path: %s", current_path)
        return set(), 0
    
    # Create a signature for this structure (for memoization)
    # Use a simple hash of the structure type and top-level keys
    if isinstance(data, dict):
        signature = f"dict:{len(data)}:{','.join(sorted(data.keys())[:10])}"
    elif isinstance(data, list):
        signature = f"list:{len(data)}"
        if data and isinstance(data[0], dict):
            signature += f":{','.join(sorted(data[0].keys())[:10])}"
    else:
        signature = f"scalar:{type(data).__name__}"
    
    # Check memoization cache
    cache_key = f"{current_path}:{signature}"
    if cache_key in memo:
        return memo[cache_key], len(memo[cache_key])
    
    unique_keys = set()
    
    if isinstance(data, dict):
        # For dicts, add all key names (not full paths) and recurse
        for key, value in data.items():
            # Add just the key name (normalized - same key name counts once regardless of path)
            unique_keys.add(key)
            
            # Recurse into nested structures
            if isinstance(value, (dict, list)):
                nested_keys, _ = count_unique_keys_in_structure(
                    value, f"{current_path}.{key}" if current_path else key, memo, max_depth, depth + 1
                )
                unique_keys.update(nested_keys)
    
    elif isinstance(data, list):
        # For lists, we don't add keys here (the list itself is the value)
        # But we recurse into items to count their keys
        # IMPORTANT: We check ALL items in the list and collect unique keys across all items
        # (same key name in different items counts as one key)
        if data and len(data) > 0:
            all_keys_in_list = set()
            for idx, item in enumerate(data):
                item_path = f"{current_path}[{idx}]"
                if isinstance(item, (dict, list)):
                    nested_keys, _ = count_unique_keys_in_structure(
                        item, item_path, memo, max_depth, depth + 1
                    )
                    all_keys_in_list.update(nested_keys)
            unique_keys.update(all_keys_in_list)
    
    # Store in memoization cache
    memo[cache_key] = unique_keys
    
    return unique_keys, len(unique_keys)


def find_best_item_in_list(
    items: list,
    current_path: str,
    memo: Dict[str, Set[str]],
    max_items: int = 10,
) -> Tuple[Any, int, Set[str]]:
    """
    Find the item in a list with the most unique keys.
    
    Samples up to max_items from the list, traverses each to count unique keys,
    and returns the item with the most unique keys along with its index and key set.
    
    Args:
        items: List of items to analyze
        current_path: Current path to this list (for building paths)
        memo: Memoization cache
        max_items: Maximum number of items to sample from the list
        
    Returns:
        Tuple of (best_item, best_index, unique_keys_set)
    """
    if not items or len(items) == 0:
        LOG.debug("find_best_item_in_list: Empty list, returning (None, 0, set())")
        return (None, 0, set())
    
    # Sample up to max_items
    sample_size = min(len(items), max_items)
    sampled_items = items[:sample_size]
    
    LOG.info("find_best_item_in_list: Sampling %d item(s) from list of %d items", 
            sample_size, len(items))
    
    best_item = None
    best_index = 0
    best_key_count = -1
    best_keys = set()
    
    for idx, item in enumerate(sampled_items):
        item_path = f"{current_path}[{idx}]"
        
        # Count unique keys in this item
        unique_keys, key_count = count_unique_keys_in_structure(
            item, item_path, memo
        )
        
        LOG.debug("find_best_item_in_list: Item[%d] has %d unique keys", idx, key_count)
        if key_count > 0:
            LOG.debug("find_best_item_in_list: Item[%d] keys: %s", idx, sorted(list(unique_keys)))
        
        if key_count > best_key_count:
            best_key_count = key_count
            best_item = item
            best_index = idx
            best_keys = unique_keys
    
    if best_item is None:
        # Fallback: use first item
        LOG.debug("find_best_item_in_list: No suitable item found, using first item")
        return (items[0], 0, set())
    
    LOG.info("find_best_item_in_list: Selected item at index %d with %d unique keys (best)", 
            best_index, best_key_count)
    
    return (best_item, best_index, best_keys)


def analyze_json_structure_enhanced(
    data: Dict[str, Any],
    file_name: str,
    source_name: str,
    file_size: Optional[int] = None,
    level: int = 0,
    path: str = "",
    max_list_items: int = 10,
    memo: Optional[Dict[str, Set[str]]] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Recursively analyze JSON structure with enhanced list handling.
    
    Rules:
    - For scalars: yield and stop
    - For lists: sample top N items, find the one with most unique keys, traverse that item
    - For dicts: yield, then process all key-value pairs
    
    Args:
        data: JSON data to analyze (dict at top level)
        file_name: Name of the source file
        source_name: Name of the source/payer
        file_size: File size in bytes (from mrf_landing)
        level: Current depth level (0 = top level)
        path: Full JSONPath to current location (e.g., "provider_references[5].provider_group_id")
        max_list_items: Maximum number of items to sample from lists (default: 10)
        memo: Memoization cache (created if None)
        
    Yields:
        Dict with keys: file_name, source_name, file_size, level, path, key, value, value_dtype, url_in_value
    """
    if memo is None:
        memo = {}
    
    if not isinstance(data, dict):
        LOG.warning("Top-level data is not a dict, skipping")
        return
    
    for key, value in data.items():
        value_dtype = get_value_dtype(value)
        url_in_value = has_url(value)
        
        # Build full path to this key
        if path:
            current_path = f"{path}.{key}"
        else:
            current_path = key
        
        # Determine what value to store based on type
        # For lists: only store the analyzed portion (best item), not the entire list
        # For dicts: store the entire dict (we process all keys)
        # For scalars: store the scalar value
        stored_value = value
        best_item = None
        best_index = None
        
        if value_dtype == "list" and value and len(value) > 0:
            # For lists, find the best item first (we'll reuse this for processing)
            LOG.info("Processing list '%s' (path: %s): sampling up to %d items from list of %d items", 
                    key, current_path, max_list_items, len(value))
            best_item, best_index, _ = find_best_item_in_list(
                value, current_path, memo, max_list_items
            )
            # Store the best item wrapped in a list to maintain list type consistency
            # This way value_dtype="list" matches the stored value being a list
            stored_value = [best_item]
            LOG.info("Processing list '%s' (path: %s): selected item at index %d (most unique keys)", 
                    key, current_path, best_index)
            LOG.debug("Storing best item (index %d) wrapped in list for '%s', not the entire list of %d items", 
                     best_index, key, len(value))
        
        # Convert value to jsonb-compatible format
        value_jsonb = json.dumps(stored_value) if stored_value is not None else None
        
        # Yield current level record
        yield {
            "file_name": file_name,
            "source_name": source_name,
            "file_size": file_size,
            "level": level,
            "path": current_path,
            "key": key,
            "value": value_jsonb,
            "value_dtype": value_dtype,
            "url_in_value": url_in_value,
        }
        
        # Recursive processing based on type
        if value_dtype == "scalar":
            # Stop at scalars
            continue
        elif value_dtype == "list":
            # For lists: use the best_item we already found above
            if value and len(value) > 0:
                # best_item and best_index should already be set from above
                if best_item is None:
                    # Fallback (shouldn't happen, but just in case)
                    LOG.warning("best_item not found for list '%s', finding it now", key)
                    best_item, best_index, _ = find_best_item_in_list(
                        value, current_path, memo, max_list_items
                    )
                
                # Build path for list item: use the actual selected index
                list_item_path = f"{current_path}[{best_index}]"
                
                if isinstance(best_item, (dict, list)):
                    # Recursively process best item
                    if isinstance(best_item, dict):
                        yield from analyze_json_structure_enhanced(
                            best_item,
                            file_name,
                            source_name,
                            file_size=file_size,
                            level=level + 1,
                            path=list_item_path,
                            max_list_items=max_list_items,
                            memo=memo,
                        )
                    elif isinstance(best_item, list):
                        # Nested list: recursively find best item
                        if best_item and len(best_item) > 0:
                            nested_best, nested_index, _ = find_best_item_in_list(
                                best_item, list_item_path, memo, max_list_items
                            )
                            nested_list_path = f"{list_item_path}[{nested_index}]"
                            if isinstance(nested_best, dict):
                                yield from analyze_json_structure_enhanced(
                                    nested_best,
                                    file_name,
                                    source_name,
                                    file_size=file_size,
                                    level=level + 1,
                                    path=nested_list_path,
                                    max_list_items=max_list_items,
                                    memo=memo,
                                )
        elif value_dtype == "dict":
            # For dicts: process all key-value pairs
            yield from analyze_json_structure_enhanced(
                value,
                file_name,
                source_name,
                file_size=file_size,
                level=level + 1,
                path=current_path,
                max_list_items=max_list_items,
                memo=memo,
            )
