CREATE OR REPLACE VIEW zip_zip_distance_15mi AS
WITH
all_pairs AS (
  SELECT
      a.zip_code AS zip1,
      b.zip_code AS zip2,
      zn.miles_to_zcta5
  FROM zip_code_proximity zn
  JOIN zip_zcta_xref a
    ON a.zcta = zn.zip1
  JOIN zip_zcta_xref b
    ON b.zcta = zn.zip2
  where zn.miles_to_zcta5<15
)
SELECT
	LPAD(CAST(zip1 AS TEXT), 5, '0'),
    LPAD(CAST(zip2 AS TEXT), 5, '0'),
    MIN(miles_to_zcta5) AS distance_miles
FROM all_pairs
GROUP BY zip1, zip2;