-- Select schema groups with min_file_name information
-- Returns one row per schema group with aggregated information
SELECT
    schema_sig,
    MIN(CASE WHEN is_min_file = true THEN file_name END) AS min_file_name,
    MIN(CASE WHEN is_min_file = true THEN min_file_name_size END) AS min_file_name_size,
    BOOL_OR(has_urls) AS has_urls,
    COUNT(*) AS file_count
FROM schema_groups
GROUP BY schema_sig
ORDER BY schema_sig;
