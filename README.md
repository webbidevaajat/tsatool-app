# tsatool-app

*Under construction!*

Tool for analyzing Finnish road weather station (TieSääAsema) data. Data will is located and handled in a PostgreSQL & [TimescaleDB](https://www.timescale.com/) database, and analyses are run through a Python API. See the [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki) for more details and examples.

To get familiar with road weather station data models and properties, see the documentation for the [real time API](https://www.digitraffic.fi/tieliikenne/).

You can get some kind of a clue about what this is about by reading our first [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki/Ehtosetin-muotoilu) about formatting the input data (in Finnish).

**TODO:** database model (briefly)

## Installation

**TODO:** Dockerize and update installation instructions?

## Inserting data

### Docker DB instance

For now, we use a temporary Docker container to test the database functionalities.
Since Postgres' port 5432 is likely to be reserved for your local Postgres server,
bind your container to some other port, such as 7001 here. Example:

```
docker run -d --name tsdb -p 127.0.0.1:7001:5432 -e POSTGRES_PASSWORD=[pw_of_your_choice] timescale/timescaledb:latest-pg11
```

To successfully run the Python scripts that communicate with the database,
make sure your `db_config.yml` file in the root directory
is updated accordingly:

```
host: 127.0.0.1
port: 7001
database: tsa
admin_user: postgres
```

You can save the `postgres` password locally to env variable `POSTGRES_PASSWORD`,
(e.g. `SET "POSTGRES_PASSWORD=[pw_of_your_choice]"` on Windows cmd line)
so `tsa.utils.tsadb_connect()` is able to use it directly.
If the env variable is not available, you will be asked to type the password.

Now run the database initialization script on your local machine
(assuming you have `psql` installed along with a local Postgres installation):

```
psql -U postgres -h 127.0.0.1 -p 7001 -f scripts\init_db.sql
```

### Station and sensor metadata

Run `python fetch_from_digitraffic.py` to insert stations and sensors
data into the corresponding tables.
This will insert the **current** data from the Digitraffic API as it is.

Alternatively, you can run an SQL file that inserts a snapshot
of the metadata values (without JSON properties):

```
psql -U postgres -h 127.0.0.1 -p 7001 -f scripts\insert_stations_sensors.sql
```

**TODO:** Find a better way to insert metadata from a static source.

### Observations from LOTJU files

For inserting raw time series data to sensor and station observation tables
from LOTJU dump files, there is a script called `insert_lotjudumps.py`.
This must be run with some arguments, and there must be some data available
to it under the `data/` directory:

- Monthly LOTJU csv files that are usually named
`tiesaa_mittatieto-[YYYY]_[MM].csv` and `anturi_arvo-[YYYY]_[MM].csv`.
These must be given after arguments `-t` and `-a` *without* the `data/`
directory (will be used as default).
- Conversion csv files for "short" and "long" ids of stations and sensors.
LOTJU uses "short" integer ids (`ID` in the files),
and we use "long" ids (`VANHA_ID`) in the analyses.
These should available as
`data/tiesaa_asema.csv` and `data/laskennallinen_anturi.csv`.

**Do not insert all the data of a month without filtering**.
The amount of raw data is huge. Instead, provide the station ids (long ones)
you want to insert data from after argument `-s`, separated by whitespace.

A particularly slow part of the insertion script is reading
the `anturi_arvo` file. For debugging purposes, you may want to parse
n first lines only. You can set this limit after the `-l` argument.

**Example usage:**

```
python insert_lotjudumps.py -t tiesaa_mittatieto-2018_01.csv -a anturi_arvo-2018_01.csv -s 1019 1121 1132 -l 3000000
```

**TODO: other data sources?**

Finally, you can get a summary of the data available
in the database by running this SQL script:

```
psql -U postgres -h 127.0.0.1 -p 7001 -d tsa -f scripts\observations_summary.sql
```

## Running analyses

**TODO:** Dockerize and update this doc?

- Check that you have the correct Python version and required libraries installed (see below), and possibly start your virtual environment
- Run the `tsapp.py` CLI. The CLI provides you with various steps of preparing and analyzing a set of sensor conditions.
- Run the `tsabatch.py` scripts with required arguments: see `python tsabatch.py --help` first. This is a tool for batch analyses: all the sheets of an Excel file are analyzed, and the analysis procedure is started without stopping on any errors or warnings.
- The command line tools will call the `tsa` API that can be found in its own directory here. The API in turn will communicate with the database.

## Authors

- **Arttu Kosonen** - [datarttu](https://github.com/datarttu), arttu.kosonen (ät) wsp.com

## Copyright

WSP Finland & ITM Finland 2019
