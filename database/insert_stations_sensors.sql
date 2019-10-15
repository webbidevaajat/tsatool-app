/*
Populate stations and sensors metadata tables
from filtered LOTJU metadata files.
NOTE: Assuming that the csv files are available on the database server.
This is the case if the Docker image is used.

Arttu K / WSP Finland 10/2019
*/
\connect tsa;
BEGIN;
COPY stations (id, lotjuid, name)
  FROM '/tiesaa_asema_filtered.csv'
  WITH DELIMITER '|';
COPY sensors (id, lotjuid, name)
  FROM '/laskennallinen_anturi_filtered.csv'
  WITH DELIMITER '|';
COMMIT;
