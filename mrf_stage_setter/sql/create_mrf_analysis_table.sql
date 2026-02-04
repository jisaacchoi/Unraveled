-- Analysis table for MRF JSON structure exploration
-- This table stores a flattened view of JSON structures from mrf_landing
-- For lists, only the first item is analyzed
-- For dicts, all key-value pairs are analyzed
CREATE TABLE IF NOT EXISTS mrf_analysis (
    id bigserial PRIMARY KEY,
    file_name text NOT NULL,
    source_name text NOT NULL,
    file_size bigint,                     -- File size in bytes (from mrf_landing)
    level integer NOT NULL,              -- Depth level (0 = top level)
    path text NOT NULL,                   -- Full JSONPath to this key (e.g., "provider_references[0].provider_group_id")
    path_group text NOT NULL,             -- Normalized path with indices replaced by [] for grouping (e.g., "provider_references[].provider_group_id")
    key text NOT NULL,                    -- Current key name
    top_level_key text NOT NULL,          -- Top-level key name (for level 0: same as key; for nested: extracted from path)
    value jsonb,                          -- Value as jsonb (can be scalar, list, dict)
    value_dtype text NOT NULL,            -- Data type: 'scalar', 'list', 'dict'
    url_in_value boolean NOT NULL,        -- True if value contains a URL pattern
    analyzed_at timestamptz NOT NULL DEFAULT now()
);

-- Index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_file_name ON mrf_analysis(file_name);
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_level ON mrf_analysis(level);
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_key ON mrf_analysis(key);
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_path ON mrf_analysis(path);
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_path_group ON mrf_analysis(path_group);
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_top_level_key ON mrf_analysis(top_level_key);