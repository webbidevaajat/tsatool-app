# tsatool-app

*Under construction!*

**TODO:**

- Using `POSTGRES_PASSWORD` env variable
- `tsabatch.py ... --dryvalidate` info
- Info on following environment variables in Python env:
  - `PG_HOST`
  - `PG_PORT`
  - `PG_DBNAME`
  - `PG_USER`
  - `PG_PASSWORD`

Tool for analyzing Finnish road weather station (TieSääAsema) data. Data will is located and handled in a PostgreSQL & [TimescaleDB](https://www.timescale.com/) database, and analyses are run through a Python API. See the [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki) for more details and examples.

To get familiar with road weather station data models and properties, see the documentation for the [real time API](https://www.digitraffic.fi/tieliikenne/).

You can get some kind of a clue about what this is about by reading our first [Wiki page](https://github.com/webbidevaajat/tsatool-app/wiki/Ehtosetin-muotoilu) about formatting the input data (in Finnish).

**TODO:** database model (briefly)

## Installation

This tool has been developed in Python 3.7.
Older versions will not work.
Make sure you have [pip](https://pypi.org/project/pip/) pkg manager installed,
and run the following command in the project directory to install the dependencies:

```
pip install -r requirements.txt
```

Optionally, you can use [virtualenv](https://docs.python-guide.org/dev/virtualenvs/) to use an isolated Python environment to run the tool.

## Data model (briefly)

- Data is read from an input Excel file into an [`AnalysisCollection`](tsa/analysis_collection.py).
It can contain multiple Excel sheets.
The sheets must contain data in exactly correct cells to be readable (see `example_data/`).
An `AnalysisCollection` represents a whole analysis dataset run at once.
- Each Excel sheet is read into a [`CondCollection`](tsa/cond_collection.py).
This collection contains condition rows and a start and an end date that are common for all the conditions in the collection.
For instance, you could analyze the same conditions for 1.1.-28.2.2018 and 1.9.-31.10.2018 by using two separate sheets.
- Each condition row in an Excel sheet is read into a [`Condition`](tsa/condition.py).
It is a collection of boolean sensor states or states of other `Condition`s combined by `AND`, `OR`, `NOT` and parentheses, thus resulting in a boolean `master` value.
A `Collection` must have a `site` and a `master_alias` identifier that together are unique within their parent `CondCollection`.
*Primary* `Collection` consists of primary `Block`s only, *secondary* `Collection`s have at least one secondary `Block`.
- Each `Condition` consists of [`Block`s](tsa/block.py).
A `Block` represents the state of a sensor of a station (*Primary*) or the state of another referenced `Condition` (*Secondary*).
On the database side, it renders a single boolean column.
A `Block` could be defined as `s1122#tie_1 < 3`, for instance.
Note the three-valued logic: if the sensor's numeric value in a time period is not known (`NULL`), also the value of the boolean column will be `NULL`.

## Database and raw data handling

See [database](database/).

## Running an analysis

Run `python tsabatch.py --help` to see the parameters and their usage.

Example:

```
python tsabatch.py -i example_data/testset.xlsx -n test_analysis
```

All paths here are relative to the project directory.
The above command would save resulting Excel and PowerPoint files as `results/test_analysis_[...]`.

It is also possible to make a "dry run" without any database interaction,
just to validate the syntax and formatting of the input data:

```
python tsabatch.py -i example_data/testset.xlsx -n test_analysis --dryvalidate
```

Log files are saved in `logs/` and they are rotated by date and hour.

## Authors

- **Arttu Kosonen** - [datarttu](https://github.com/datarttu), arttu.kosonen (ät) wsp.com

## Copyright

WSP Finland & ITM Finland 2019
