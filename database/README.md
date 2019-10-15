# tsa database

`tsa` database uses PostgreSQL 11 with TimescaleDB extension.
You can deploy the database directly from the `.sql` files on your machine,
given that you have Postgres and Timescale installed,
or use Docker.
The following example starts up the database with an empty `tsa` schema and makes it available on port `7001` on the host machine.
Note that the data does *not* persist here (volume `-v` flag not used).

```
cd ~/tsatool-app/database
docker build -t tsadb .
docker run --rm -d -p 7001:5432 -e POSTGRES_PASSWORD=postgres tsadb
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
*TODO: raw data file structure, shell script etc.*

See `database/example_data` to get familiar with the structure of the LOTJU dumps.

## Schema

Shortly:

`stations` and `sensors` contain the available station and sensor ids, respectively,
as well as their names. These id-name pairs are provided by the `insert_stations_sensors.sql`
script (situation as of 8/2019). There are also JSONB fields to store additional metadata
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
