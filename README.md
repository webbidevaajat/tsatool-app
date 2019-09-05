# tsatool-app

Tool for analyzing Finnish road weather station (TieSääAsema) data. Data will is located and handled in a PostgreSQL & [TimescaleDB](https://www.timescale.com/) database, and analyses are run through a Python API. See the [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki) for more details and examples.

To get familiar with road weather station data models and properties, see the documentation for the [real time API](https://www.digitraffic.fi/tieliikenne/).

You can get some kind of a clue about what this is about by reading our first [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki/Ehtosetin-muotoilu) about formatting the input data (in Finnish).

## Running an analysis

Analyses are based on an input Excel file. Example:

```
#> First, validate input data without database connection:
python tsabatch.py -i dataset.xlsx --dryvalidate
#> If no errors occurred, run full analysis:
python tsabatch.py -i dataset.xlsx -n results -p mydatabasepassword
```

Input file must be located in `analysis/` folder in the project root.
Results are saved there as well, in a directory
named after the `-n` (name) argument.

Furthermore, you should have
database connection parameters available in `db_congig.yml` file in the
project root. Example:

```
host: localhost
port: 5432
database: tsa
admin_user: postgres
```

You can either pass the database user password with the `-p` argument
or save it to environment variable `POSTGRES_PASSWORD`, in which case
you can omit the argument.

## Installation

- Make sure you have PostgreSQL (version 10 >) and TimescaleDB installed
- Run `scripts/init_db.sql` SQL script to create the `tsa` database
- Make sure you are using Python >= 3.6 and have installed the libraries in
`requirements.txt` (e.g. `pip install -r requirements.txt`,
it is recommended to use virtualenv)

### Station and sensor metadata

Run `python fetch_from_digitraffic.py` to insert stations and sensors
data into the corresponding tables.
This will insert the **current** data from the Digitraffic API as it is.

Alternatively, you can run an SQL file that inserts a snapshot
of the metadata values (without JSON properties) with
`scripts/insert_stations_sensors.sql`.

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

Example:

```
python insert_lotjudumps.py -t tiesaa_mittatieto-2018_01.csv -a anturi_arvo-2018_01.csv -s 1019 1121 1132 -l 3000000
```

## Database

Essentially, the `tsa` database consists of the following tables:

- `stations` and `sensors` include the properties behind each road weather
station id and sensor id, respectively
- `statobs`: each row has a timestamp and station id, and a unique
*observation id*
- `seobs`: each row has a sensor id, value and reference to the *observation id*
in `statobs`. This way the sensor readings can be joined to the timestamped
station rows.

`statobs` is partitioned by timestamp, using TimescaleDB hypertable functionality.
Similarly, `seobs`, having the largest amount of data, is "hypertabled"
by the primary key integer id.
**Indexing of these tables has not been tested extensively
and may require more attention in future.**

The `tsa` API runs the analyses in the database side by forming temporary
tables with a time range column and value columns that refer to
the corresponding `Condition` rows and their single logical parts,
`Block`s. Each of these `Block` columns has a boolean value,
indicating whether the specified sensor value condition was "on" or "off"
during the time range.

## tsa API

The `tsa` Python data model can be described roughly as follows:

- `AnalysisCollection` is based on an Excel input file,
and it can hold multiple `Collection`s (Excel sheets).
It stores general information related to the analysis session,
such as result folder name and parameters for DB connection.
- `Collection` is based on a single Excel sheet,
it has start and end dates as analysis time range
and as many `Condition`s as are specified in the Excel sheet rows.
A `Collection` establishes a single database session, meaning that
temporary tables, identifiers etc. are valid during handling the
collection.
- `Condition` is based on a row in an Excel sheet. It has a site name
and a master alias name, and the combination of these two must be
unique within the parent `Collection`. Most importantly,
`Condition` is a combination of `Blocks`: these blocks
are separated by logical operators, and the result is either TRUE or FALSE
for a time range. A `Condition` is *primary* or *secondary* depending on its
blocks. *Primary* conditions are evaluated first so *secondary* ones
can depend on them.
- `Block` is a part of its parent `Condition`. It tells whether the value of a
sensor belonging to a station is inside the given limits or not
(a *primary* block). Alternatively, it refers to an existing `Condition` by
site and master alias names, in which case it is a *secondary* block.
Secondary blocks can be used to build composite `Conditions`,
e.g. "`NOT (Condition1 OR Condition2 OR ...)`". Any *secondary* `Block` in
a `Condition` renders the whole `Condition` *secondary*.

## Authors

- **Arttu Kosonen** - [datarttu](https://github.com/datarttu), arttu.kosonen (ät) wsp.com

## Copyright

WSP Finland & ITM Finland 2019
