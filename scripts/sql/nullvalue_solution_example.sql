-- A POSSIBLE SOLUTION
-- to the problem of ranges with NULL values missing.
-- This is how the "master" time range sequence of a TSA Condition
-- can be defined explicitly, such that
-- Blocks that are missing a value for a time range
-- show a NULL value on that row.
--
-- Arttu K / WSP Finland 6/2019

-- Create base views with some example time limits.
-- Not using temporary tables here since
-- the amount of data to save would be too large,
-- and the temporary table could not make use of the Timescale and other indexes.
CREATE OR REPLACE TEMP VIEW statobs_time AS SELECT id, tfrom, statid FROM statobs WHERE tfrom BETWEEN '2018-01-01T00:00:00'::timestamp AND '2018-02-28T23:59:59'::timestamp;
CREATE OR REPLACE TEMP VIEW obs_main AS SELECT tfrom, statid, seid, seval FROM statobs_time INNER JOIN seobs ON statobs_time.id = seobs.obsid;

-- As opposed to the old version where
-- the Block datasets are stated as CTE parts,
-- here we save the data temporarily to memory (TEMP TABLE)
-- where it can be read several times without running
-- the underlying query again and again.
-- Using TEMP TABLEs should be ok as long as
-- the results of pack_ranges() remain in reasonable, typical sizes,
-- such as tens or hundreds of rows.
CREATE TEMP TABLE b9_siirtyma_0 AS (SELECT valid_r, istrue AS b9_siirtyma_0 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1123, p_seid := 174, p_operator := 'in', p_seval := '(3)'));
CREATE TEMP TABLE b9_siirtyma_1 AS (SELECT valid_r, istrue AS b9_siirtyma_1 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1115, p_seid := 5, p_operator := '<', p_seval := '3'));

WITH
	-- Get all the timestamps occurring in the temp tables (= Block data).
	-- Set the unique timestamps into ascending order.
	master_seq AS (
	SELECT unnest( array [lower(valid_r), upper(valid_r)] ) vt
	FROM b9_siirtyma_0
	UNION
	SELECT unnest( array [lower(valid_r), upper(valid_r)] ) vt
	FROM b9_siirtyma_1
		-- ... more UNIONs here if more than 2 Blocks
	ORDER BY vt),

	-- Prepare validity ranges from the ordered timestamps.
	-- The last row of the resulting dataset
	-- has vuntil as NULL.
	master_ranges_wlastnull AS (
	SELECT vt AS vfrom, LEAD(vt, 1) OVER (ORDER BY vt) AS vuntil
	FROM master_seq),

	-- Get rid of the last vuntil IS NULL row,
	-- make timestamp columns into a range column
	-- to enable range intersection joins with the Block temp tables.
	-- NOTE!! that the WHERE clause does not ensure
	-- that the NULL row is the last one;
	-- however, this procedure should the results of pack_ranges()
	-- as input, and it should not produce NULL time values in the middle of the data.
	--
	-- Now, this dataset has the most granular time ranges
	-- that are needed to account for differences in
	-- the boolean / NULL values in the master dataset.
	master_ranges AS (
	SELECT tsrange(vfrom, vuntil) AS valid_r
	FROM master_ranges_wlastnull
	WHERE vuntil IS NOT NULL)

	-- Finally, we can join together the time ranges
	-- and the Block temp table contents.
	-- As noted above, master_ranges.valid_r contains the
	-- "shortest" possible ranges: if there is a missing
	-- part in the Block datasets, the value for
	-- that range is NULL because we use LEFT JOIN.
	SELECT
		master_ranges.valid_r AS valid_r,
		b9_siirtyma_0.b9_siirtyma_0,
		b9_siirtyma_1.b9_siirtyma_1
	FROM master_ranges
	LEFT JOIN b9_siirtyma_0
	ON master_ranges.valid_r && b9_siirtyma_0.valid_r
	LEFT JOIN b9_siirtyma_1
	ON master_ranges.valid_r && b9_siirtyma_1.valid_r;
