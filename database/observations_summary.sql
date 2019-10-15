-- Get a summary of observations available in the database
-- by month, station and sensor.
--
-- NOTE: with a high amount of data in the database,
-- this may take a long time to execute!
--
-- Arttu K / WSP Finland 8/2019
SELECT
	date_part('month', tfrom AT TIME ZONE 'Europe/Helsinki') AS mon,
	statobs.statid, seobs.seid, count(seobs.id) AS nrows,
	min(tfrom) AT TIME ZONE 'Europe/Helsinki' AS first_ts,
	max(tfrom) AT TIME ZONE 'Europe/Helsinki' AS last_ts
FROM seobs
INNER JOIN statobs
ON seobs.obsid = statobs.id
GROUP BY date_part('month', tfrom AT TIME ZONE 'Europe/Helsinki'), statobs.statid, seobs.seid
ORDER BY statobs.statid, seobs.seid;
