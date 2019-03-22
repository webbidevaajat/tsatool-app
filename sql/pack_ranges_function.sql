DROP FUNCTION IF EXISTS pack_ranges;
CREATE OR REPLACE FUNCTION
pack_ranges(p_obs_relation text,
			p_maxminutes integer,
			p_statid integer,
			p_seid integer,
			p_operator text,
			p_seval text)
RETURNS TABLE (valid_r tsrange,
			   istrue int) AS
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
istrue
FROM total_range_tb
WHERE isfirst', p_operator, p_seval, p_obs_relation)
USING p_statid, p_seid, p_maxminutes;
END
$func$  LANGUAGE plpgsql;
