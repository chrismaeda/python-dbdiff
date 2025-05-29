Tools to compare database schemas and table rows.

# Installation

Install `python-dbdiff` from PyPI:

```bash
pip install python-dbdiff
```

This will install two command line tools named `schemadiff` and `datadiff`.

# General Architecture

schema/__init__.py has a database-agnostic schema framework:

* Database
* Table
* Column
* Index
* Constraint

Functions and procedures do not have a class and are currently represented as python dictionaries.

schema/mysql.py has code that reads the MySQL information_schema tables to create schema objects in this framework.


# Schema Comparison

The main.py script imports the schema for source and destination database and outputs a scheme comparison report.


# Table Data Comparison

The maindata.py script compares the tables rows in the source and destination databases.  It depends on the schema
framework to determine how to construct primary key objects for each table.

# CLI Usage

After installation the `schemadiff` and `datadiff` commands become available. Run
`--help` on either command to see all options. The most common invocations are:

```
schemadiff diffschema DB1_ENV_FILE DB2_ENV_FILE [--uppercase|--lowercase]
schemadiff diffprocs DB1_ENV_FILE DB2_ENV_FILE
schemadiff tablelist DB_ENV_FILE [--uppercase|--lowercase]

datadiff tablediff DB1_ENV_FILE DB2_ENV_FILE [TABLE ...] [--uppercase|--lowercase]
datadiff tablereport DB_ENV_FILE [TABLE ...]
```





