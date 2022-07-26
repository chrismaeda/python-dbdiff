Tools to compare database schemas and table rows.

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





