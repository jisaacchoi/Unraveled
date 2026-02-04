-- Get one URL per (file_name, path) combination where url_in_value is true
-- Returns the first record for each (file_name, path) group
-- Using path ensures we distinguish between keys with the same name at different locations
-- (e.g., "provider_group_id" in "provider_references[0].provider_group_id" vs elsewhere)
SELECT DISTINCT ON (file_name, path)
    file_name, source_name, file_size, path, key, level, value
FROM mrf_analysis
WHERE url_in_value = true
ORDER BY file_name, path, id;
