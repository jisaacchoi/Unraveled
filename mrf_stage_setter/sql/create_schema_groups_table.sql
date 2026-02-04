-- Create table for schema groups
-- Groups files by schema signature and identifies minimum file per group
-- Schema signature is based on distinct paths in mrf_analysis
-- Minimum file is the smallest file in each schema group (used for schema inference)
-- Each file is stored as a separate row (normalized structure)
-- Also tracks whether any files in the group contain URLs
-- Note: Normalizes path_group by replacing numeric array indices with [] for grouping
CREATE TABLE IF NOT EXISTS schema_groups AS
WITH per_file AS (
    SELECT
        file_name,
        STRING_AGG(
            DISTINCT REGEXP_REPLACE(path_group, '\[\d+\]', '[]', 'g'), 
            ', ' 
            ORDER BY REGEXP_REPLACE(path_group, '\[\d+\]', '[]', 'g')
        ) AS schema_sig,
        MAX(file_size) AS file_size,
        BOOL_OR(url_in_value) AS has_urls
    FROM mrf_analysis
    GROUP BY file_name
),
ranked AS (
    SELECT
        file_name,
        schema_sig,
        file_size,
        has_urls,
        MIN(file_size) OVER (PARTITION BY schema_sig) AS min_size_in_group
    FROM per_file
),
with_min_info AS (
    SELECT
        file_name,
        schema_sig,
        file_size,
        has_urls,
        CASE WHEN file_size = min_size_in_group THEN true ELSE false END AS is_min_file,
        MIN(CASE WHEN file_size = min_size_in_group THEN file_name END) 
            OVER (PARTITION BY schema_sig) AS min_file_name,
        MIN(CASE WHEN file_size = min_size_in_group THEN file_size END) 
            OVER (PARTITION BY schema_sig) AS min_file_name_size,
        COUNT(*) OVER (PARTITION BY schema_sig) AS file_count
    FROM ranked
)
SELECT
    schema_sig,
    file_name,
    file_size,
    is_min_file,
    min_file_name,
    min_file_name_size,
    has_urls,
    file_count
FROM with_min_info
ORDER BY schema_sig, file_size;

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_schema_groups_schema_sig ON schema_groups(schema_sig);
CREATE INDEX IF NOT EXISTS idx_schema_groups_is_min_file ON schema_groups(is_min_file) WHERE is_min_file = true;
