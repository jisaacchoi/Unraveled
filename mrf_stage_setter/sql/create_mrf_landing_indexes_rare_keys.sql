-- Create secondary indexes for mrf_landing after bulk ingest completes.
-- Keeping these out of the table DDL improves write throughput during ingest.

CREATE INDEX IF NOT EXISTS idx_mrf_landing_array_start_offset
ON mrf_landing(array_start_offset)
WHERE array_start_offset IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mrf_landing_has_rare_keys
ON mrf_landing(has_rare_keys)
WHERE has_rare_keys = TRUE;

CREATE INDEX IF NOT EXISTS idx_mrf_landing_file_record
ON mrf_landing(file_name, record_type);
