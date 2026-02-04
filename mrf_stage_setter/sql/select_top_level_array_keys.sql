-- Get top-level arrays for a file from mrf_analysis table
-- Returns distinct key names where level = 0 and value_dtype = 'list'
-- Since top-level keys are now recorded at level 0 in mrf_analysis,
-- we can directly query for level 0 arrays to identify top-level array keys
-- The path equals the key name for top-level keys (e.g., path = 'in_network' for key 'in_network')
SELECT DISTINCT key
FROM mrf_analysis
WHERE file_name = %s
  AND level = 0
  AND value_dtype = 'list'
  AND path = key  -- Path equals key means it's a top-level key, not nested
ORDER BY key;
