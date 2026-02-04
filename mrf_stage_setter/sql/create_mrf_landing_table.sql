CREATE TABLE IF NOT EXISTS mrf_landing (
    id          bigserial PRIMARY KEY,
    source_name text        NOT NULL,
    file_name   text        NOT NULL,
    file_size   bigint,                 -- File size in bytes
    record_type text        NOT NULL,  -- 'in_network' or 'provider_reference'
    record_index integer    NOT NULL,  -- Zero-based index of record within its source array
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    payload     jsonb       NOT NULL,
    payload_size integer,               -- Size of payload (JSON string length in bytes)
    array_start_offset bigint           -- Byte offset of the opening '[' for array items (NULL for non-arrays)
);

-- Index for efficient lookups by array start offset
CREATE INDEX IF NOT EXISTS idx_mrf_landing_array_start_offset ON mrf_landing(array_start_offset) WHERE array_start_offset IS NOT NULL;
