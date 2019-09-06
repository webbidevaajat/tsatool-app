-- Client-side copy seobs and statobs monthly files to respective tables.
-- Csv file paths assuming that this script is run from within the project
-- root directory.
\c tsa;
\copy seobs (id, obsid, seid, seval) FROM 'data/seobs.csv' WITH DELIMITER ',' CSV HEADER;
\copy statobs (id, tfrom, statid) FROM 'data/statobs.csv' WITH DELIMITER ',' CSV HEADER;
