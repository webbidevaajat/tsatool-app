-- This function is to be used for each Block instance in tsa.
-- A relation / temp view / whatever `p_obs_relation`
-- must exist, and it must contain the sensor observations
-- with `tfrom` timestamps and `statid` station ids.
-- The function does basicly the following:
-- 1) select relevant sensor value records and order them by time
-- 2) find their validity ranges, and whenever the range
--    is longer than `p_maxminutes`, truncate it to last
--    for `p_maxminutes`
-- 3) compare adjacent rows;
--    merge them as long as they form a continuous time range
--    AND the condition value remains the same
-- 4) return a table with "compressed" time ranges
--    and corresponding condition truth values
--
-- Example usage:
--
-- CREATE OR REPLACE TEMP VIEW obs_main AS
-- SELECT tfrom, statid, seid, seval
-- FROM statobs
-- INNER JOIN seobs
-- ON statobs.id = seobs.obsid
-- WHERE tfrom BETWEEN '2018-01-01T00:00:00'::timestamp AND '2018-03-31T23:59:59'::timestamp;
--
-- SELECT * FROM pack_ranges(p_obs_relation := 'obs_main',
--			p_maxminutes := 30,
--			p_statid := 1104,
--			p_seid := 181,
--			p_operator := '>=',
--			p_seval := '0.5');

DROP FUNCTION IF EXISTS pack_ranges;
CREATE OR REPLACE FUNCTION
pack_ranges(p_obs_relation text,
			p_maxminutes integer,
			p_statid integer,
			p_seid integer,
			p_operator text,
			p_seval text)
RETURNS TABLE (valid_r tsrange,
			   istrue boolean) AS
$func$
BEGIN
RETURN QUERY
EXECUTE format(
'WITH
	nottruncated AS (
		SELECT
			tfrom,
			lead(tfrom) OVER (ORDER BY tfrom) AS tuntil,
			(seval %s %s) AS istrue
		FROM %I
		WHERE
			statid = $1
			AND seid = $2
		ORDER BY tfrom),
	truncated AS (
		SELECT
			tsrange(tfrom,
			(CASE WHEN (tuntil-tfrom) > make_interval(mins := $3) THEN
				tfrom + make_interval(mins := $3)
			 ELSE
				tuntil
			 END)) AS valid_r,
			istrue
		FROM nottruncated
		WHERE tuntil IS NOT NULL),
istrue_tb AS
	(SELECT valid_r, COALESCE(istrue::int, -1) AS istrue
	 FROM truncated
	 ORDER BY valid_r),
ll_tb AS
	(SELECT valid_r,
	 istrue,
	 LEAD(istrue, 1) OVER (ORDER BY valid_r),
	 LAG(istrue, 1) OVER (ORDER BY valid_r)
	 FROM istrue_tb),
isfl_tb AS
	(SELECT valid_r,
	 istrue,
	 (istrue <> lag OR lag IS NULL) AS isfirst,
	 (istrue <> lead OR lead IS NULL) AS islast
	 FROM ll_tb),
fl_tb AS
	(SELECT *
	FROM isfl_tb
	WHERE isfirst OR islast),
total_range_tb AS
	(SELECT *,
	CASE WHEN (isfirst AND islast) THEN
	 	valid_r
	 WHEN (isfirst AND not islast) THEN
	 	tsrange(lower(valid_r),
				  upper(LEAD(valid_r, 1) OVER (ORDER BY valid_r)))
	 WHEN (not isfirst AND islast) THEN
	 	tsrange(lower(LAG(valid_r, 1) OVER (ORDER BY valid_r)),
				  upper(valid_r))
	 END
	 AS total_range
	FROM fl_tb)
SELECT total_range AS valid_r,
	(CASE WHEN istrue = 1 THEN
		true
	WHEN istrue = 0 THEN
		false
	ELSE
		NULL
	END) AS istrue
FROM total_range_tb
WHERE isfirst', p_operator, p_seval, p_obs_relation)
USING p_statid, p_seid, p_maxminutes;
END
$func$  LANGUAGE plpgsql;
