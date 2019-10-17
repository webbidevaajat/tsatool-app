# tsa database

`tsa` database uses PostgreSQL 11 with TimescaleDB extension.
You can deploy the database directly from the `.sql` files on your machine,
given that you have Postgres and Timescale installed,
or use Docker.
The following example starts up the database with an empty `tsa` schema,
populates `stations` and `sensors` metadata tables from the respective csv files,
and makes the database server available on port `7001` on the host machine.
`data/` directory is mounted to the container's `/rawdata/` directory,
so you can put (large) raw LOTJU data files to `data/` on host side
and use server side `COPY FROM` to read the data to the database relatively quickly.

```
cd ~/tsatool-app/database
docker build -t tsadb .
docker run --rm -d -p 7001:5432 -e POSTGRES_PASSWORD=postgres -v data/:/rawdata/ tsadb
```

Now you can access the database container from your host machine.
You must have Postgres or at least `psql` installed.

```
psql -h localhost -p 7001 -U postgres -d tsa
# Type the password ("postgres" in the above example)
```

The above example leaves you with `postgres` user only with the password you gave as environment variable.
This should be enough for simple cases where the database is only deployed temporarily.
Use the same user and password as environment variables for the analysis tool.

## Inserting raw data

Raw data is inserted from LOTJU dump files located in `database/data/`.
It should look like this, decomposed to monthly files:

```
├── anturi_arvo-2018_01.csv
├── anturi_arvo-2018_02.csv
├── anturi_arvo-2018_03.csv
├── tiesaa_mittatieto-2018_01.csv
├── tiesaa_mittatieto-2018_02.csv
└── tiesaa_mittatieto-2018_03.csv
```

When you mount the `data/` directory as instructed above,
these files should be available to the database server,
so you can populate the LOTJU raw data "staging" tables.

Then run the stored procedures (defined in `02_rawdata_schema.sql`)
that convert the LOTJU data to `statobs` and `seobs` tables.
When you're (successfully) done, remember to truncate the staging tables
so they are empty for the next month's data.
Doing this inside a transaction (`BEGIN ... COMMIT`) ensures
the process is aborted on an error.

```
BEGIN;
COPY tiesaa_mittatieto FROM '/rawdata/tiesaa_mittatieto-2018_01.csv' CSV HEADER DELIMITER '|';
CALL populate_statobs();
TRUNCATE TABLE tiesaa_mittatieto;
COPY anturi_arvo FROM '/rawdata/anturi_arvo-2018_01.csv' CSV HEADER DELIMITER '|';
CALL populate_seobs();
TRUNCATE TABLE anturi_arvo;
COMMIT;
```

**Reading the raw data takes time**.
Some statistics on reading and converting `2018-03` files,
tested on an X1 Carbon (2013) with 4 GB of RAM:

| Command                           	| Time   	|
|-----------------------------------	|--------	|
| `COPY tiesaa_mittatieto FROM ...` 	| 20 s   	|
| `COPY anturi_arvo FROM ...`       	| 16 min 	|
| `CALL populate_statobs();`        	| 2 min  	|
| `CALL populate_seobs();`          	| 1 hour 	|

To batch run the above commands, see `10_batch_populate_statobs_seobs.sh`
and adjust the script to your needs.

See `database/example_data` to get familiar with the structure of the LOTJU dumps.

## Schema

Shortly:

`stations` and `sensors` contain the available station and sensor ids, respectively,
as well as their names and LOTJU ids. These id-name pairs are provided by the `03_insert_stations_sensors.sql`
script (based on the 2018 LOTJU dump). There are also JSONB fields to store additional metadata
which could be used in future (see Digitraffic real-time weather station and sensor schemas).

`statobs` contains timestamped observation records for each station.
Each record should have a bunch of related sensor values in `seobs`.
This table is automatically hypertable-chunked over the timestamp column.
**NOTE:** The current chunking and indexing strategy may lead to poor performance
and should be developed in future.

`seobs` contains sensor observations related to `statobs` records.
There are tens of sensors at each station, so this table has definitely the largest amount of data,
and it may be slow to scan it (again, further development is needed!).
The table is hypertable-chunked over the integer id field
(Timescale allows using integer fields for hypertables natively,
and here the the integer values ascend as the data is inserted,
in the same way as with timestamp values usually).

Actual usage of the database relies strongly on joins between `statobs` and `seobs`.
In future, it may be interesting to test if a "wide" schema is more efficient:
timestamped station observations would have all sensor values in columns
instead of a separate lookup table.

`tiesaa_mittatieto` and `anturi_arvo` are "staging" tables meant for conversion
from LOTJU raw data to `statobs` and `seobs`, respectively.
Since they only serve moving and converting data,
they should be emptied when intermediate raw data is no longer needed.
