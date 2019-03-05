-- Insert subset of station observations
-- from raw data table to actual table
-- based on station id (Digitraffic) selection
WITH sts_subset AS (
	WITH sts AS (
		SELECT tiesaa_asema.id AS lotjuid,
		stations.id AS dtid
		FROM stations
		INNER JOIN tiesaa_asema
		ON stations.id = tiesaa_asema.vanha_id
		)
	SELECT * FROM sts
	WHERE dtid IN
		(1115, 1120, 1122, 1123, 1011, 1118, 1083,
		1119, 1116, 1124, 1125, 1126, 1019, 1131,
		1121, 1132, 1006, 1105, 1106, 1107, 1022,
		1101, 1103, 1104, 1108, 1109, 1110, 1111,
		3051, 3052, 3053, 3054, 3006, 3045, 3047,
		3048, 3001, 3023, 3029, 3030, 3002, 3024,
		3062, 3063, 3064, 3065, 3066, 3067, 3056,
		3057, 3058, 3059, 3074, 3078, 3077, 3079,
		3075, 3080, 3081, 3082)
)
INSERT INTO statobs (id, tfrom, statid)
SELECT id, aika AS tfrom, dtid AS statid
FROM tiesaa_mittatieto2
INNER JOIN sts_subset
ON tiesaa_mittatieto2.asema_id = sts_subset.lotjuid;
--
-- Create index on freshly inserted statobs ids
-- for inner join in the next step
CREATE INDEX IF NOT EXISTS
	statobs_id_idx ON statobs(id);
-- Similarly, create index on station id
CREATE INDEX IF NOT EXISTS
  statobs_statid_idx ON statobs(statid);
--
-- Insert sensor observations having
-- corresponding station observations.
-- Convert lotju ids to digitraffic ids.
WITH seids AS (
	SELECT laskennallinen_anturi.id AS lotjuid,
	sensors.id AS dtid
	FROM sensors
	INNER JOIN laskennallinen_anturi
	ON sensors.id = laskennallinen_anturi.vanha_id
),
anids AS (
	SELECT anturi_arvo.id AS id,
		anturi_arvo.mittatieto_id AS obsid,
		seids.dtid AS seid,
		anturi_arvo.arvo AS seval
	FROM anturi_arvo
	INNER JOIN seids
	ON seids.lotjuid = anturi_arvo.anturi_id
)
INSERT INTO seobs (id, obsid, seid, seval)
SELECT anids.id AS id,
	anids.obsid AS obsid,
	anids.seid AS seid,
	anids.seval AS seval
FROM anids
INNER JOIN statobs
ON anids.obsid = statobs.id;
