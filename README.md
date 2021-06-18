# BRC No API ETL

This tool runs in the background and periodically extracts data
from applications using Selenium.

## Installation

1. Download the code from GutHub:

```bash
git clone https://github.com/LCBRU/no_api_etl.git
```

2. Create and activate a virtual environment:

From within the `no_api_etl` directory, run the commands:

```bash
python3 -m venv venv
source ./venv/bin/activate
```

3. Install the requirements:

Got to the reporter directory

```bash
pip install -r requirements-dev.txt
```

4. Set environment:

Copy the file `example.env` to `.env`, amend the file to make it only readable by yourself and fill in appropriate values.

## Running

Each system has a different application that imports the appropriate reports
and schedules them for running, but can also be used to run individual or
multiple reports.

### Scheduling ETLs

If no arguments are supplied to the application, all the ETLs will be
scheduled.  For example:

```bash
python uhl_etl.py
```

### Running all ETLs

To run all the imported reports, use the `-a` or `--all` command line
arguments.  For example:

```bash
python uhl_etl.py --all
```

### Running specific ETLs

To specify ETLs that you want to run, add their ETL class
name or part of their ETL class name as a command line argument.  Multiple
names or parts of names can be added.  The names do not need to match the
case of the ETL class name.  For example:

```bash
python uhl_etl.py redcap CrfmStudyDetailDownload
```

### Exclude ETLs from running

To specify STLs to exclude from running, add the `-x` or `--exclude`
command line argument, followed by the report class name or part thereof.
For example:

```bash
python uhl_etl.py -x pdf
```

## Development

## Creating ETLs

To create a new report you must inherit from the `Etl` class defined in the
`core.py` module or, more usually, a class inherited from it - for example `SeleniumEtl` - and then provide appropriate parameters such as a browser to the constructor and overriding the `do_etl` or `do_selenium_etl` functions.

## Getting the ETLs imported

If you add a new ETL it will only be run if it successfully imported by
the application.  If your ETL is not being imported, follow this trouble-
shooting list:

1. Make sure the ETL is a descendent of the `Etl` class defined in
`api\core.py`.
2. Make sure all directories contain a `__init__.py` file.
3. Make sure that the ETL class is in a directory beneath the
directory imported by the system application.
3. Make sure the directory imported by the system application itself contains
a `__init__.py` file that contains the following code:

```python
from api.core import import_sub_etls

import_sub_etls(__path__, __name__)

```

## Selenium ETLs

Selenium is a library that facillitates webscraping by controlling headerless
instances of Chrome or Firefox browsers.  This can be used when direct data
access is impossible or where the web interface shows information not avaiable
elsewhere.

In order to use Selenium you need to make sure that a Selenium Grid in running
and accessible to the Email Reporting application and that the connection
details have been set in the appropriate environment variables.

See the `LCBRU/selenium_grid` repository on GitHub or the SeleniumGrid SOPs for information.

The `SeleniumGrid` class in the `api\selenium.py` file wraps access to the
Selenium Grid in a context manager for convenience.

## ETL Central  Database

The database upgrades are handled by SQLAlchemy-migrate and are run using the `manage.py`
program once the configuration has been copied into place and the database created.

### Installation

To initialise the database run the commands:

```bash
manage.py version_control
manage.py upgrade
```

### Upgrade

To upgrade the database to the current version, run the command:

```bash
manage.py upgrade
```

### Create Migration

To create a new migration, run the command:

```bash
manage.py upgrade script "Description"
```