-- Table to track MRF index file downloads and metadata
-- Stores metadata from the index JSON including reporting entity, plans, and file information
CREATE TABLE IF NOT EXISTS mrf_index (
    id bigserial PRIMARY KEY,
    source_name text NOT NULL,
    reporting_entity_name text,
    reporting_entity_type text,
    file_url text NOT NULL,
    file_description text,
    file_name text NOT NULL,
    file_path text,
    file_size_bytes bigint,
    reporting_plans jsonb,  -- Array of plan objects: [{"plan_name": "...", "plan_id_type": "...", "plan_id": "...", "plan_market_type": "..."}]
    download_status text NOT NULL DEFAULT 'pending',  -- pending, downloading, completed, failed, skipped
    download_started_at timestamptz,
    download_completed_at timestamptz,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_name, file_url)  -- Prevent duplicate records for same source_name, file_url, file_name, and reporting_entity_name
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_mrf_index_source_name ON mrf_index(source_name);
CREATE INDEX IF NOT EXISTS idx_mrf_index_file_url ON mrf_index(file_url);
CREATE INDEX IF NOT EXISTS idx_mrf_index_download_status ON mrf_index(download_status);
CREATE INDEX IF NOT EXISTS idx_mrf_index_file_name ON mrf_index(file_name);

-- Index for querying by plan_id within reporting_plans JSONB array
CREATE INDEX IF NOT EXISTS idx_mrf_index_reporting_plans ON mrf_index USING gin(reporting_plans);
