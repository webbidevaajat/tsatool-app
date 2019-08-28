-- tsa PostgreSQL / TimescaleDB database setup
-- Arttu K / WSP Finland 8/2019
--
-- Check that Timescale is installed and Postgres is configured
-- accordingly (e.g. by the tuning tool provided by Timescale).
-- Run this script in an empty database cluster as admin user.
--
-- - Create tsa database
-- - Extensions
-- - Tables, timescale hypertables
-- - Trigger functions and triggers
-- - pack_ranges function

CREATE DATABASE tsa;
\connect tsa;

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS btree_gist CASCADE;

CREATE TABLE IF NOT EXISTS stations (
  id        integer     PRIMARY KEY,
  geom      jsonb,
  prop      jsonb,
  modified  timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensors (
  id                integer     PRIMARY KEY,
  name              text        NOT NULL,
  shortname         text,
  unit              text,
  accuracy          integer,
  nameold           text,
  valuedescriptions jsonb,
  description       text,
  modified          timestamptz DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS statobs (
  id        bigserial   NOT NULL,
  tfrom     timestamptz NOT NULL,
  statid    integer     NOT NULL,
  modified  timestamptz DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (tfrom, statid)
);

SELECT create_hypertable('statobs', 'tfrom');

CREATE TABLE IF NOT EXISTS seobs (
  id        bigserial   PRIMARY KEY,
  obsid     bigint      NOT NULL,
  seid      integer     NOT NULL,
  seval     real        NOT NULL,
  modified  timestamptz DEFAULT CURRENT_TIMESTAMP
);

SELECT create_hypertable('seobs', 'id', chunk_time_interval => 10000000);

CREATE INDEX seobs_obsid_idx ON seobs(obsid);
CREATE INDEX seobs_seid_seval_idx ON seobs(seid, seval);

DROP FUNCTION IF EXISTS update_modified_column() CASCADE;
CREATE OR REPLACE FUNCTION update_modified_column()
  RETURNS TRIGGER AS $$
    BEGIN
      NEW.modified = NOW();
      RETURN NEW;
    END;
  $$ language 'plpgsql';

CREATE TRIGGER upd_stations_modified
  BEFORE UPDATE ON stations
  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
CREATE TRIGGER upd_sensors_modified
  BEFORE UPDATE ON sensors
  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
CREATE TRIGGER upd_statobs_modified
  BEFORE UPDATE ON statobs
  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
CREATE TRIGGER upd_seobs_modified
  BEFORE UPDATE ON seobs
  FOR EACH ROW EXECUTE PROCEDURE update_modified_column();

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

DROP FUNCTION IF EXISTS pack_ranges();
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
$func$ LANGUAGE plpgsql;
