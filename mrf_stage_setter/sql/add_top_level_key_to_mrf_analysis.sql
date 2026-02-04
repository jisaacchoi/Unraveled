-- Add top_level_key column to mrf_analysis table
-- This column stores the top-level key name for easier grouping of nested records
-- For level 0 records, top_level_key = key
-- For nested records, top_level_key is extracted from the path (first part before [ or .)

-- Add the column (nullable initially for existing data)
ALTER TABLE mrf_analysis ADD COLUMN IF NOT EXISTS top_level_key text;

-- Create index for efficient lookups
CREATE INDEX IF NOT EXISTS idx_mrf_analysis_top_level_key ON mrf_analysis(top_level_key);

-- Update existing records: extract top_level_key from path
-- For level 0: top_level_key = key
-- For level > 0: top_level_key = first part of path (before [ or .)
UPDATE mrf_analysis
SET top_level_key = CASE
    WHEN level = 0 THEN key
    ELSE SPLIT_PART(SPLIT_PART(path, '[', 1), '.', 1)
END
WHERE top_level_key IS NULL;

-- Make the column NOT NULL after backfilling
ALTER TABLE mrf_analysis ALTER COLUMN top_level_key SET NOT NULL;
