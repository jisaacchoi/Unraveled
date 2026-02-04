-- Get all files for a specific schema signature
-- Returns file_name, file_size, has_urls for all files in the group
SELECT
    file_name,
    file_size,
    has_urls
FROM schema_groups
WHERE schema_sig = %s
ORDER BY file_size;
