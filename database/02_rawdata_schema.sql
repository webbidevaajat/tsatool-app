/*
Add tables for LOTJU raw data
and functions for converting and inserting data from them
to statobs and seobs tables.
tiesaa_mittatieto and anturi_arvo tables are to be truncated
after each successful insertion run.
Run init_db.sql first.

Arttu K / WSP Finland 10/2019
*/
\connect tsa;

/*
Station raw data is assumed as follows:
"ID"|"AIKA"|"ASEMA_ID"
420958436|01.03.2018 02:09:00,000000000|1
420958440|01.03.2018 02:09:00,000000000|60
...
*/

CREATE TABLE IF NOT EXISTS tiesaa_mittatieto (
  id            bigint      PRIMARY KEY,
  aika          text,
  asema_id      integer
);

/*
Sensor raw data is assumed as follows:
"ID"|"ANTURI_ID"|"ARVO"|"MITTATIETO_ID"|"TIEDOSTO_ID"
23855559698|18|275|420944339|
23855559699|19|1034.7|420944339|
...
*/
CREATE TABLE IF NOT EXISTS anturi_arvo (
  id            bigint      PRIMARY KEY,
  anturi_id     integer,
  arvo          real,
  mittatieto_id bigint,
  tiedosto_id   integer     -- This is assumed to be NULL in every raw file...
);

CREATE PROCEDURE populate_statobs()
LANGUAGE SQL
AS $$
-- Finnish time is assumed in raw data
SET TIME ZONE 'Europe/Helsinki';
WITH
  tiesaa_mittatieto_converted AS (
    SELECT
      tiesaa_mittatieto.id,
      -- Eliminate everything from and including comma:
      substring(tiesaa_mittatieto.aika FROM '^.*(?=,)')::timestamptz AS tfrom,
      stations.id AS statid
    FROM tiesaa_mittatieto
    -- TODO: Add LOTJU id to stations table & insertion data
    -- Inner join drops any records having no match in non-Lotju ids!
    INNER JOIN stations
      ON tiesaa_mittatieto.asema_id = stations.lotjuid
    WHERE
      tiesaa_mittatieto.id IS NOT NULL
      AND tiesaa_mittatieto.aika IS NOT NULL
      AND tiesaa_mittatieto.asema_id IS NOT NULL
  ),
  insertion_batch AS (
    INSERT INTO statobs (id, tfrom, statid)
    SELECT * FROM tiesaa_mittatieto_converted
    -- Conflicting records are ignored!
    -- Compare COPY FROM result and the result below to check if records were omitted.
    ON CONFLICT DO NOTHING
    RETURNING 1;
  )
SELECT count(*) || ' rows inserted into statobs' AS i
FROM insertion_batch;
$$;

CREATE PROCEDURE populate_seobs()
LANGUAGE SQL
AS $$
WITH
  anturi_arvo_converted AS (
    SELECT
      anturi_arvo.id,
      anturi_arvo.mittatieto_id AS obsid,
      sensors.id AS seid,
      anturi_arvo.arvo AS seval
    FROM anturi_arvo
    INNER JOIN sensors
      ON anturi_arvo.anturi_id = sensors.lotjuid
    WHERE
      anturi_arvo.id IS NOT NULL
      AND anturi_arvo.anturi_id IS NOT NULL
      AND anturi_arvo.arvo IS NOT NULL
      AND anturi_arvo.mittatieto_id IS NOT NULL
  ),
  insertion_batch AS (
    INSERT INTO seobs (id, obsid, seid, seval)
    SELECT * FROM anturi_arvo_converted
    ON CONFLICT DO NOTHING
    RETURNING 1;
  )
SELECT count(*) || ' rows inserted into seobs' AS i
FROM insertion_batch;
$$;
