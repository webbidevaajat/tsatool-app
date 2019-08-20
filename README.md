# tsatool-app

*Under construction!*

Tool for analyzing Finnish road weather station (TieSääAsema) data. Data will is located and handled in a PostgreSQL & [TimescaleDB](https://www.timescale.com/) database, and analyses are run through a Python API. See the [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki) for more details and examples.

To get familiar with road weather station data models and properties, see the documentation for the [real time API](https://www.digitraffic.fi/tieliikenne/).

You can get a some kind of a clue about what this is about by reading our first [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki/Ehtosetin-muotoilu) about formatting the input data (in Finnish).

**TODO:** database model (briefly)

## Installation

**TODO:** Dockerize and update installation instructions?

## Inserting data

- **TODO:** fetch and insert `stations` and `sensors` from Digitraffic API
- **TODO:** insert raw data from LOTJU dump files to `statobs` and `seobs`
- **TODO: other data sources?**

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
