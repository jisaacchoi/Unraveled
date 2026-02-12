# Split Optimization Implementation Plan

## Overview

This document outlines the optimization plan for improving the performance of the JSON.gz file splitting process in `src/split/indexed_gzip_splitter.py`. The optimizations focus on reducing CPU overhead, memory usage, and improving overall throughput.

## Current State Analysis

### Current Architecture
- **File Processing**: Uses `indexed_gzip` for random access to compressed files
- **Item Extraction**: Reads fixed-size chunks (8-16MB), extracts complete JSON items
- **Buffering**: Accumulates items in memory until cumulative size reaches threshold
- **Parsing**: Items are parsed during extraction, then re-parsed during write
- **Validation**: Full JSON parse to validate item completeness

### Current Bottlenecks
1. **Double Parsing**: Items parsed twice (during extraction and during write)
2. **Fixed Chunk Size**: Always reads same chunk size regardless of item sizes
3. **Memory Buffering**: All items buffered in memory before writing
4. **Full JSON Validation**: Complete parse just to check structure

### Performance Metrics (Baseline)
- Processing rate: ~14 MB/s (decompressed)
- Memory usage: High (all items buffered)
- CPU usage: High (double parsing overhead)

---

## Optimization Goals

### Target Improvements
- **CPU Reduction**: 30-50% reduction in parsing overhead
- **Memory Reduction**: 50-70% reduction in peak memory usage
- **Throughput**: 20-30% improvement in processing speed
- **Code Complexity**: Minimal increase, maintain readability

---

## Optimization #1: Avoid Double Parsing (High Impact, Low Effort)

### Current Flow
```
Extract bytes → Parse JSON → Store parsed object → Re-parse → Write
```

### Optimized Flow
```
Extract bytes → Store bytes → Parse once → Write
```

### Implementation Details

#### Step 1.1: Modify `extract_complete_array_items()`
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior**:
- Returns list of `bytes` objects
- Items are parsed immediately after extraction

**New behavior**:
- Return list of `bytes` objects (no change)
- **Do NOT parse** items during extraction
- Keep items as raw bytes until write time

**Changes**:
```python
# Current (line ~487):
complete_items, relative_end = extract_complete_array_items(...)
for item_bytes in complete_items:
    item_text = item_bytes.decode("utf-8", errors="replace")
    parsed_item = json.loads(item_text)  # ❌ Parse here
    items_buffer.append((item_bytes, parsed_item))

# New:
complete_items, relative_end = extract_complete_array_items(...)
for item_bytes in complete_items:
    items_buffer.append(item_bytes)  # ✅ Keep as bytes only
```

#### Step 1.2: Modify `process_single_array()` Buffer
**File**: `src/split/indexed_gzip_splitter.py`

**Current buffer structure**:
```python
items_buffer = []  # List of (item_bytes, parsed_item) tuples
```

**New buffer structure**:
```python
items_buffer = []  # List of bytes objects only
buffer_cumulative_size = 0  # Track size in bytes
```

#### Step 1.3: Parse Only During Write
**File**: `src/split/indexed_gzip_splitter.py`

**Current write logic** (lines ~574-585):
```python
items_to_write = []
for item_bytes, parsed_item in items_buffer[:items_to_write_count]:
    if parsed_item is not None:
        items_to_write.append(convert_decimals(parsed_item))
    else:
        # Re-parse if not already parsed
        item_text = item_bytes.decode("utf-8", errors="replace")
        item = json.loads(item_text)
        items_to_write.append(convert_decimals(item))
```

**New write logic**:
```python
items_to_write = []
for item_bytes in items_buffer[:items_to_write_count]:
    # Parse once, at write time
    try:
        item_text = item_bytes.decode("utf-8", errors="replace")
        item = json.loads(item_text)
        items_to_write.append(convert_decimals(item))
    except Exception as exc:
        LOG.warning("Failed to parse item: %s", exc)
        continue  # Skip invalid items
```

### Expected Benefits
- **CPU Reduction**: ~50% reduction in JSON parsing calls
- **Memory Reduction**: ~30% reduction (no duplicate parsed objects)
- **Code Simplification**: Simpler buffer structure

### Risks & Mitigation
- **Risk**: Items might be invalid bytes (caught during write)
- **Mitigation**: Try-catch during parse, log and skip invalid items

### Testing Strategy
1. Unit test: Verify items are kept as bytes until write
2. Integration test: Compare output files (should be identical)
3. Performance test: Measure parsing time reduction

---

## Optimization #2: Incremental Validation (High Impact, Low Effort)

### Current Flow
```
Extract bytes → Full JSON parse → Check structure → Validate
```

### Optimized Flow
```
Extract bytes → Fast structure check → Full parse only if needed
```

### Implementation Details

#### Step 2.1: Create Fast Structure Check Function
**File**: `src/split/indexed_gzip_splitter.py`

**New function**:
```python
def is_complete_item_fast(item_bytes: bytes, min_key_threshold: float = 0.5) -> bool:
    """
    Fast structure check without full JSON parse.
    
    Checks:
    1. Balanced braces
    2. Ends with '}'
    3. Quick key presence check (if expected keys provided)
    
    Returns:
        True if item appears complete, False otherwise
    """
    if not item_bytes:
        return False
    
    # Fast check 1: Balanced braces
    open_braces = item_bytes.count(b'{')
    close_braces = item_bytes.count(b'}')
    
    if open_braces != close_braces:
        return False
    
    # Fast check 2: Ends with closing brace
    stripped = item_bytes.rstrip()
    if not stripped.endswith(b'}'):
        return False
    
    # Fast check 3: Starts with opening brace
    stripped_start = item_bytes.lstrip()
    if not stripped_start.startswith(b'{'):
        return False
    
    # Fast check 4: No unmatched quotes (basic check)
    # Count quotes - should be even
    quote_count = item_bytes.count(b'"')
    if quote_count % 2 != 0:
        return False
    
    # If all fast checks pass, item is likely complete
    return True
```

#### Step 2.2: Modify `check_item_completeness()`
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior** (lines ~204-297):
- Always does full JSON parse
- Validates against expected keys

**New behavior**:
- First do fast structure check
- Only do full parse if fast check passes
- Full parse only for final validation

**Changes**:
```python
def check_item_completeness(
    item_bytes: bytes,
    next_level_keys: Set[str],
    min_key_threshold: float = 0.5,
) -> bool:
    """Check item completeness with fast path optimization."""
    
    # Fast path: Structure check without parsing
    if not is_complete_item_fast(item_bytes):
        return False
    
    # Slow path: Full parse only if structure looks good
    try:
        item_text = item_bytes.decode("utf-8", errors="replace").strip()
        if item_text.endswith(","):
            item_text = item_text[:-1].strip()
        
        # Now do full parse (only if fast check passed)
        parsed_item = json.loads(item_text)
        
        # Validate keys if expected keys provided
        if next_level_keys:
            item_keys = set(parsed_item.keys()) if isinstance(parsed_item, dict) else set()
            matching_keys = item_keys.intersection(next_level_keys)
            key_ratio = len(matching_keys) / len(next_level_keys) if next_level_keys else 0
            
            if key_ratio < min_key_threshold:
                LOG.debug("Item has insufficient expected keys: %.2f%%", key_ratio * 100)
                return False
        
        return True
        
    except json.JSONDecodeError:
        return False
    except Exception:
        return False
```

#### Step 2.3: Update `extract_complete_array_items()` Usage
**File**: `src/split/indexed_gzip_splitter.py`

**Current usage** (line ~365):
```python
if check_item_completeness(item_bytes, next_level_keys):
    complete_items.append(item_bytes)
```

**New usage**: No change needed - function signature stays same

### Expected Benefits
- **CPU Reduction**: ~40% reduction in full JSON parses (fast check filters out incomplete items)
- **Faster Validation**: Structure checks are O(n) byte scans vs O(n) JSON parse
- **Better Error Handling**: Catch malformed items earlier

### Risks & Mitigation
- **Risk**: Fast check might miss edge cases
- **Mitigation**: Full parse still happens, fast check is just a filter
- **Risk**: False positives (fast check passes but parse fails)
- **Mitigation**: Full parse catches these, item is skipped

### Testing Strategy
1. Unit test: Test `is_complete_item_fast()` with various inputs
2. Edge cases: Test with nested objects, arrays, escaped strings
3. Performance test: Measure validation time reduction

---

## Optimization #3: Streaming Writes (High Impact, Medium Effort)

### Current Flow
```
Read chunks → Extract items → Buffer all items → Write when threshold reached
```

### Optimized Flow
```
Read chunks → Extract items → Write incrementally → Continue reading
```

### Implementation Details

#### Step 3.1: Modify Buffer Management
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior** (lines ~447-448):
```python
items_buffer = []  # List of (item_bytes, parsed_item) tuples
buffer_cumulative_size = 0  # Cumulative size of items in buffer (bytes)
```

**New behavior**:
```python
# Keep same structure but write incrementally
items_buffer = []  # List of bytes objects
buffer_cumulative_size = 0  # Cumulative size of items in buffer (bytes)
current_file_items = []  # Items for current part file being written
current_file_size = 0  # Size of current file being written
```

#### Step 3.2: Implement Incremental Write Logic
**File**: `src/split/indexed_gzip_splitter.py`

**Current write logic** (lines ~542-620):
- Accumulates all items until threshold
- Writes entire buffer when threshold reached
- Clears buffer after write

**New write logic**:
```python
# After extracting items from chunk (line ~523):
for item_bytes in complete_items:
    items_buffer.append(item_bytes)
    buffer_cumulative_size += len(item_bytes)
    
    # Check if we should write current file
    if buffer_cumulative_size >= size_per_file_bytes:
        # Write current batch
        items_to_write = []
        cumulative_size = 0
        
        for item_bytes in items_buffer:
            item_size = len(item_bytes)
            if cumulative_size + item_size > size_per_file_bytes:
                break  # Don't exceed threshold
            
            # Parse item (only when writing)
            try:
                item_text = item_bytes.decode("utf-8", errors="replace")
                item = json.loads(item_text)
                items_to_write.append(convert_decimals(item))
                cumulative_size += item_size
            except Exception as exc:
                LOG.warning("Failed to parse item: %s", exc)
                continue
        
        # Write part file
        if items_to_write:
            part_num = get_next_part_num()
            part_path = output_dir / f"{stem}_part{part_num:04d}.json.gz"
            
            output_data = {array_key: items_to_write}
            with gzip.open(part_path, "wt", encoding="utf-8") as f_out:
                json.dump(output_data, f_out)
            
            files_created_for_array += 1
            add_to_progress(part_path.stat().st_size)
            
            # Remove written items from buffer
            items_buffer = items_buffer[len(items_to_write):]
            buffer_cumulative_size -= cumulative_size
```

#### Step 3.3: Handle Remaining Items
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior** (lines ~628-660):
- Writes remaining buffer items at end

**New behavior**: Same, but simplified since items are already bytes

### Expected Benefits
- **Memory Reduction**: 50-70% reduction (don't buffer entire array)
- **Faster Time-to-First-Write**: Start writing earlier
- **Better Progress Tracking**: More granular progress updates

### Risks & Mitigation
- **Risk**: More frequent file I/O operations
- **Mitigation**: Batch writes still happen, just more frequently
- **Risk**: File handle management
- **Mitigation**: Use context managers, ensure proper cleanup

### Testing Strategy
1. Memory profiling: Compare peak memory usage
2. Integration test: Verify output files are correct
3. Performance test: Measure time-to-first-write improvement

---

## Optimization #4: Adaptive Chunk Sizing (High Impact, Medium Effort)

### Current Flow
```
Always read 8-16MB chunks → Extract items → Continue
```

### Optimized Flow
```
Read until we have enough items → Extract items → Continue
```

### Implementation Details

#### Step 4.1: Create Adaptive Chunk Reader
**File**: `src/split/indexed_gzip_splitter.py`

**New function**:
```python
def read_adaptive_chunk(
    f: indexed_gzip.IndexedGzipFile,
    start_offset: int,
    target_items: int = 50,  # Target number of items to extract
    min_chunk_size: int = 1 * 1024 * 1024,  # Minimum 1MB
    max_chunk_size: int = 16 * 1024 * 1024,  # Maximum 16MB
    end_offset: Optional[int] = None,
) -> Tuple[bytes, int]:
    """
    Read chunk adaptively based on target number of items.
    
    Strategy:
    1. Start with minimum chunk size
    2. Read incrementally until we have enough complete items
    3. Stop if we hit max chunk size or end of array
    
    Returns:
        Tuple of (chunk_bytes, actual_end_offset)
    """
    f.seek(start_offset)
    chunk = b""
    current_offset = start_offset
    
    # Read in increments
    increment_size = min_chunk_size  # 1MB increments
    
    while len(chunk) < max_chunk_size:
        # Determine how much to read
        if end_offset is not None:
            remaining = end_offset - current_offset
            if remaining <= 0:
                break
            read_size = min(increment_size, remaining)
        else:
            read_size = increment_size
        
        if read_size <= 0:
            break
        
        # Read increment
        new_data = f.read(read_size)
        if not new_data:
            break
        
        chunk += new_data
        current_offset += len(new_data)
        
        # Quick check: Count complete items (fast)
        # Look for pattern: }, followed by whitespace/comma, followed by {
        # This is approximate but fast
        item_separators = chunk.count(b'},\n') + chunk.count(b'}, ')
        if item_separators >= target_items:
            # We likely have enough items
            break
        
        # If we're at the end, stop
        if end_offset is not None and current_offset >= end_offset:
            break
    
    return chunk, current_offset
```

#### Step 4.2: Modify `process_single_array()` to Use Adaptive Chunks
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior** (lines ~469-476):
```python
span = read_span_between_offsets(
    f,
    current_offset,
    array_end if array_end is not None and (array_end - current_offset) <= chunk_read_bytes else None,
    extra=512,
    max_bytes=chunk_read_bytes,
)
```

**New behavior**:
```python
# Use adaptive chunk reading
span, actual_end = read_adaptive_chunk(
    f,
    current_offset,
    target_items=50,  # Target ~50 items per chunk
    min_chunk_size=1 * 1024 * 1024,  # 1MB minimum
    max_chunk_size=chunk_read_bytes,  # Use configured max
    end_offset=array_end,
)
```

#### Step 4.3: Update Offset Tracking
**File**: `src/split/indexed_gzip_splitter.py`

**Current behavior** (line ~540):
```python
current_offset = current_offset + relative_end
```

**New behavior**:
```python
# Use actual_end from adaptive read, or relative_end from extraction
if 'actual_end' in locals():
    current_offset = actual_end
else:
    current_offset = current_offset + relative_end
```

### Expected Benefits
- **Fewer Reads**: Read only what's needed for target number of items
- **Better Alignment**: Chunks align better with item boundaries
- **Flexible**: Adapts to varying item sizes

### Risks & Mitigation
- **Risk**: Overhead of incremental reads
- **Mitigation**: Use reasonable increment size (1MB), limit max reads
- **Risk**: Item counting approximation might be inaccurate
- **Mitigation**: Use as heuristic only, extraction still validates

### Testing Strategy
1. Unit test: Test adaptive chunk reader with various item sizes
2. Performance test: Compare number of reads vs fixed chunk size
3. Integration test: Verify output correctness

---

## Implementation Order

### Phase 1: Quick Wins (Week 1)
1. **Optimization #1**: Avoid Double Parsing
2. **Optimization #2**: Incremental Validation

**Expected Time**: 2-3 days
**Expected Impact**: 30-40% CPU reduction, 20-30% memory reduction

### Phase 2: Medium Effort (Week 2)
3. **Optimization #3**: Streaming Writes
4. **Optimization #4**: Adaptive Chunk Sizing

**Expected Time**: 3-4 days
**Expected Impact**: 50-70% memory reduction, 20-30% throughput improvement

### Phase 3: Testing & Validation (Week 3)
- Comprehensive testing
- Performance benchmarking
- Documentation updates

---

## Testing Strategy

### Unit Tests
1. **Test `is_complete_item_fast()`**:
   - Valid complete items
   - Incomplete items (missing brace)
   - Nested structures
   - Edge cases (empty objects, arrays)

2. **Test `read_adaptive_chunk()`**:
   - Small arrays (< target_items)
   - Large arrays (> target_items)
   - Variable item sizes
   - End of array handling

3. **Test Buffer Management**:
   - Items kept as bytes
   - Parsing only at write time
   - Buffer size tracking

### Integration Tests
1. **End-to-End Split Test**:
   - Split known file
   - Verify output files are identical to baseline
   - Verify part file ordering
   - Verify file sizes

2. **Performance Tests**:
   - Measure CPU usage reduction
   - Measure memory usage reduction
   - Measure throughput improvement
   - Compare against baseline

### Regression Tests
1. **Backward Compatibility**:
   - Ensure existing functionality still works
   - Verify database queries unchanged
   - Verify file naming unchanged

---

## Success Metrics

### Performance Metrics
- **CPU Usage**: Target 30-50% reduction
- **Memory Usage**: Target 50-70% reduction
- **Throughput**: Target 20-30% improvement
- **Time-to-First-Write**: Target 50% reduction

### Code Quality Metrics
- **Code Complexity**: Maintain or reduce
- **Test Coverage**: Maintain >80%
- **Documentation**: Update all affected functions

### Validation Metrics
- **Output Correctness**: 100% match with baseline
- **Error Handling**: All edge cases handled
- **Logging**: Appropriate log levels maintained

---

## Rollout Plan

### Step 1: Development
- Implement optimizations in feature branch
- Run all tests
- Code review

### Step 2: Staging
- Deploy to staging environment
- Run integration tests
- Performance benchmarking
- Monitor for issues

### Step 3: Production
- Deploy to production
- Monitor metrics
- Rollback plan ready

---

## Rollback Plan

If issues arise:
1. **Immediate**: Revert to previous version
2. **Partial**: Disable specific optimizations via config flags
3. **Investigation**: Analyze logs and metrics

---

## Future Considerations

### Potential Further Optimizations
1. **Pre-compute Item Boundaries**: Store item offsets during ingestion
2. **Parallel Compression**: Compress part files in background threads
3. **Memory-Mapped I/O**: Use mmap for better OS-level caching
4. **Incremental Index Updates**: Update indexes incrementally

### Monitoring
- Add metrics for:
  - Parsing time per item
  - Memory usage over time
  - Chunk read efficiency
  - Write throughput

---

## Appendix

### Files to Modify
1. `src/split/indexed_gzip_splitter.py`
   - `extract_complete_array_items()` - Keep items as bytes
   - `check_item_completeness()` - Add fast path
   - `process_single_array()` - Streaming writes, adaptive chunks
   - New: `is_complete_item_fast()` - Fast structure check
   - New: `read_adaptive_chunk()` - Adaptive chunk reader

### Configuration Changes
- Add config options for:
  - `adaptive_chunk_target_items` (default: 50)
  - `adaptive_chunk_min_size` (default: 1MB)
  - `enable_fast_validation` (default: true)

### Dependencies
- No new dependencies required
- Uses existing: `indexed_gzip`, `json`, `gzip`

---

## References

- Current implementation: `src/split/indexed_gzip_splitter.py`
- Related ingestion code: `src/ingest/indexed_gzip_ingester_rare_keys.py`
- Configuration: `config.yaml` → `pipeline.shape_grouping.split_files`

---

**Document Version**: 1.0  
**Last Updated**: 2026-02-11  
**Author**: AI Assistant  
**Status**: Draft - Awaiting Review
