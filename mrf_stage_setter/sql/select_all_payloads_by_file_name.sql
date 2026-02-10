-- Get ALL payloads for each (file_name, record_type) combination for comprehensive analysis
-- Includes record_index and has_rare_keys flag
-- Only selects records that are not already analyzed in mrf_analysis table
-- Orders by has_rare_keys (rare key items first), then by id
-- Parameter: file_name array (must be provided via psycopg2 parameterized query)
SELECT 
    ml.file_name, 
    ml.source_name, 
    ml.file_size, 
    ml.record_type, 
    ml.record_index, 
    ml.payload,
    ml.has_rare_keys
FROM mrf_landing ml
WHERE ml.file_name = ANY(%s)
AND NOT EXISTS (
    -- Check if this (file_name, record_type) combination already has analysis records
    SELECT 1 
    FROM mrf_analysis ma 
    WHERE ma.file_name = ml.file_name 
    AND ma.level = 0 
    AND ma.key = ml.record_type
    LIMIT 1
)
ORDER BY ml.file_name, ml.record_type, 
         CASE WHEN ml.has_rare_keys THEN 0 ELSE 1 END,  -- Rare key items first
         ml.id;
