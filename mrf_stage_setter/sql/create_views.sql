CREATE OR REPLACE VIEW vw_mrf_reporting_plans AS
SELECT
    t.file_name,
    split_part(t.file_name, '.', 1) AS file_name_core,
    e.elem ->> 'plan_id'          AS plan_id,
    e.elem ->> 'plan_name'        AS plan_name,
    e.elem ->> 'plan_id_type'     AS plan_id_type,
    e.elem ->> 'plan_market_type' AS plan_market_type
FROM mrf_index t
CROSS JOIN LATERAL jsonb_array_elements(t.reporting_plans) AS e(elem);