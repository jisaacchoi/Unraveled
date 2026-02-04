-- Insert a single analysis record into mrf_analysis table
INSERT INTO mrf_analysis (
    file_name, source_name, file_size, level, path, path_group, key, top_level_key, value, value_dtype, url_in_value
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s);
