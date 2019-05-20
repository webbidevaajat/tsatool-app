# tsatool-app (311386)

*Under construction!*

Tool for analyzing Finnish road weather station (TieSääAsema) data. Data will be located and handled in a PostgreSQL & [Timescale](https://www.timescale.com/) database.

To get familiar with road weather station data models and properties, see the documentation for the [real time API](https://www.digitraffic.fi/tieliikenne/).

**Please note** that this work is only halfway in progress. Many parts are "proof of concept", implemented in an experimental and suboptimal way just to get them work locally, they have not been tested extensively or included in a test suite, etc. Not sure if there are going to be much advances in future either, since the user pool of this tool will probably remain small.

---
# How it works (or should work)

- Check that you have the correct Python version and required libraries installed (see below), and possibly start your virtual environment
- Run the `tsapp.py` CLI. The CLI provides you with various steps of preparing and analyzing a set of sensor conditions.
  - *TODO: other parts of the CLI should work, but launching the main analysis stage is not yet implemented*
- `tsapp.py` will call the `tsa` API that can be found in its own directory here. The API in turn will communicate with the database.

---
# Installation

**Note:** there is some installation script stuff included (see for example `setup_dir.py` that should make some required directories after you've cloned this repository), but there is **no** rigid, tested and complete installation process yet.

Make sure you have the following stuff installed. Commands needed for the Ubuntu server are given accordingly.

- Python 3.6. The server should have Python 3.6 installed by default.
- Python package management tool such as **pip** or **conda**; install the packages in `requirements.txt` by using this. Server: `sudo apt install python-pip`
- PostgreSQL 9.6 at least; could be 10.X or 11.X as well, but using this so far. Server: `sudo apt install postgresql postgresql-contrib` will install 10.6 (as of 1/2019).
- [TimescaleDB](https://docs.timescale.com/v1.2/getting-started/installation), select the one for your system and the PostgreSQL version you've installed. Server: follow the instructions on [TimescaleDB site](https://docs.timescale.com/v1.2/getting-started/installation/ubuntu/installation-apt-ubuntu)

It is recommended to tune your Postgres to get the most out of your machine. Find appropriate values e.g. by using [PGTune](https://pgtune.leopard.in.ua/#/) and update them to your `postgresql.conf` file. For the Ubuntu server installation, run `sudo timescaledb-tune`.

Installing these / checking whether the required installations exist may be built into a script in future, but for now these are to be done manually as the software will not be distributed widely.

---
# Database initialization

You should have the above mentioned things installed by now (Postgres & TimescaleDB). Make sure you are able to connect to your local `postgres` database using the default `postgres` user. Also make sure that your settings in `postgresql.conf` and `pg_hba.conf` files allow connecting from your remote machine in case you are not operating on the server directly.

Create manually a new user that has superuser rights. Further tsa-specific database admin operations will be done by this user. Using the postgres console or, e.g., pgAdmin, run following commands (here we create user `tsaadmin`:

```sql
CREATE USER tsadash WITH PASSWORD '(password here)';
CREATE DATABASE tsa WITH OWNER tsadash;
```

Note that you can (and perhaps should) give a password that is different from the *server user* `tsadash`'s password. On the Linux server, if you are logged in as `tsadash`, you should be able to connect to the database without providing the password:

```
$ psql -d tsa
```

However, if you use e.g. pgAdmin on your local machine and want to connect to the `tsa` database as `tsadash`, then you'll need the password you set in the above SQL command.

---
# Authors

- **Arttu Kosonen** - [keripukki](https://github.com/keripukki), arttu.kosonen (ät) wsp.com

---
# Copyright

WSP Finland 2019, all rights reserved for the time being.
