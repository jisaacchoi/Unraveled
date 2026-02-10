-- Get one payload per (file_name, record_type) combination for a specific file
-- Uses the record with the largest payload_size as representative of that record type's structure
-- Includes record_index to build correct paths (e.g., provider_references[0])
-- Includes file_size from mrf_landing
-- Only selects records that are not already analyzed in mrf_analysis table
-- Parameter: file_name array (must be provided via psycopg2 parameterized query)
SELECT DISTINCT ON (ml.file_name, ml.record_type)
    ml.file_name, 
    ml.source_name, 
    ml.file_size, 
    ml.record_type, 
    ml.record_index, 
    ml.payload
FROM mrf_landing ml
WHERE ml.file_name = ANY(%s)
AND NOT EXISTS (
    -- Check if this (file_name, record_type) combination already has analysis records
    -- Level 0 stores the record_type as the key (representing the top-level key)
    -- So we check if level 0 key matches the record_type for this file_name
    SELECT 1 
    FROM mrf_analysis ma 
    WHERE ma.file_name = ml.file_name 
    AND ma.level = 0 
    AND ma.key = ml.record_type
    LIMIT 1
)
-- Order by payload_size DESC to get the largest payload first
-- If multiple records have the same payload_size, pick the one with the smallest id (first inserted)
ORDER BY ml.file_name, ml.record_type, COALESCE(ml.payload_size, 0) DESC, ml.id;
