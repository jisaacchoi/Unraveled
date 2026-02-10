# Rare Keys Implementation

This document describes the new rare key detection and comprehensive analysis implementation.

## Overview

The new implementation enhances the ingestion and analysis phases to:
1. **Detect rare keys** - Keys that appear in < threshold% of array items
2. **Extract comprehensive items** - Items containing rare keys along with common keys
3. **Build comprehensive structure** - Analyze all items to discover complete schema

## New Files

### Commands
- `commands/02_ingest_rare_keys.py` - Enhanced ingest command with rare key detection
- `commands/03_analyze_comprehensive.py` - Comprehensive analysis that processes all items

### Source Files
- `src/ingest/indexed_gzip_ingester_rare_keys.py` - Enhanced ingester with rare key support
- `src/detect_shapes/comprehensive_structure_analyzer.py` - Analyzer that processes all items

### SQL Files
- `sql/create_mrf_landing_table_rare_keys.sql` - Enhanced table schema with `has_rare_keys` column
- `sql/select_all_payloads_by_file_name.sql` - Query to get all items for comprehensive analysis

## Key Features

### 1. Rare Key Detection

During ingestion:
- Scans all keys and tracks **all occurrences** (not just unique ones)
- Groups occurrences by which array they belong to (using offset comparison)
- Identifies rare keys: keys appearing in < threshold% of array items (default: 1%)
- Extracts items containing rare keys by reading spans around rare key locations

### 2. Comprehensive Item Extraction

For arrays with rare keys:
- Extracts items containing rare keys (prioritized)
- Falls back to sequential extraction if needed to reach `max_array_items`
- Marks items with `has_rare_keys=True` flag in database

### 3. Comprehensive Analysis

During analysis:
- Processes **ALL items** for each record_type (not just one)
- Prioritizes items with rare keys (processes them first)
- Includes common items if they have new keys not seen in rare key items
- Builds comprehensive structure combining keys from all items

## Usage

### Ingestion

```bash
python commands/02_ingest_rare_keys.py --config config.yaml
```

### Analysis

```bash
python commands/03_analyze_comprehensive.py --config config.yaml
```

## Configuration

Add to `config.yaml`:

```yaml
pipeline:
  ingest:
    rare_key_threshold: 0.01  # 1% - keys appearing in < 1% of items are rare
    # ... other ingest settings
```

## Database Schema Changes

The `mrf_landing` table now includes:
- `has_rare_keys boolean DEFAULT FALSE` - Flag indicating if item contains rare keys

## How It Works

### Ingestion Flow

1. **Scan Phase**: 
   - Scan file with regex to find all keys
   - Track all occurrences: `key_occurrences[key] = [offset1, offset2, ...]`
   - Detect arrays and track start offsets: `array_starts[array_key] = start_offset`

2. **Rare Key Identification**:
   - For each key, determine which array it belongs to (offset comparison)
   - Calculate frequency: `frequency = occurrences / array_size`
   - Mark as rare if `frequency < threshold`

3. **Extraction**:
   - For arrays with rare keys:
     - Read spans around each rare key location (10KB padding)
     - Extract complete items by finding item boundaries (brace depth tracking)
     - Store with `has_rare_keys=True`
   - Fill remaining slots with sequential items if needed

### Analysis Flow

1. **Query All Items**:
   - Get ALL items for each (file_name, record_type)
   - Order by `has_rare_keys` (rare key items first)

2. **Comprehensive Processing**:
   - Process rare key items first (they're comprehensive)
   - Track all discovered keys
   - Process common items only if they have new keys
   - Build comprehensive structure

## Benefits

1. **Better Structure Discovery**: Finds rare/optional fields that might be missed
2. **Comprehensive Coverage**: Analyzes all items, not just one representative
3. **Efficient**: Still extracts same number of items, just smarter selection
4. **Backward Compatible**: Old commands still work unchanged

## Notes

- Record index (`record_index`) is now item sequence, not array position (for rare key items)
- Analysis processes all items but avoids duplicates by tracking discovered keys
- Works entirely at byte level - no ijson needed for large files
