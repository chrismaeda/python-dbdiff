from schema.mysql import MySQLDatabase
from schema import Database
from datacompare.tablediff import TableDiff
from datacompare.dbscan import DatabaseScan
from util.database_credentials import read_credentials_file
from typing import List, Tuple
import click

def read_database_from_env(envfile) -> Database:
    dbenv = read_credentials_file(envfile)
    dbname = dbenv['database']
    db = MySQLDatabase(dbname)
    db.connect(**dbenv)
    db.import_schema(dbname)
    return db

def maindata(db1:Database, db2:Database, tablelist:Tuple[str]):
    tablediff = TableDiff(db1, db2)    
    table_list = None
    if len(tablelist) == 0:
        # diff all tables
        table_list = tablediff.db1.get_table_list()
    else:
        table_list = list(tablelist)

    tables_only1 = []
    tables_only2 = []
    for tablename in table_list:
        print("CHECK TABLE " + tablename)
        # check that table exists in both databases
        db1table = db1.get_table(tablename)
        db2table = db2.get_table(tablename)
        if db1table is None and db2table is not None:
            tables_only2.append(tablename)
            continue
        elif db1table is not None and db2table is None:
            tables_only1.append(tablename)
            continue

        # table exists in both databases so diff the rows...
        sames, diffs, only1, only2 = tablediff.diff_rows(tablename)
        if len(diffs) == 0 and len(only1) == 0 and len(only2) == 0:
            print("TABLE: " + tablename + " " + str(len(sames)) + " ROWS MATCH!")
        else:
            print("\nTABLE: " + tablename)
            print("\tSAME COUNT: " + str(len(sames)))
            if len(diffs) > 0:
                print("\tDIFFS COUNT: " + str(len(diffs)))
            if len(only1) > 0:
                print("\tOnly in DB1 COUNT: " + str(len(only1)))
            if len(only2) > 0:
                print("\tOnly in DB2 COUNT: " + str(len(only2)))
            print("\n")
    # print tables only in one db or the other
    if len(tables_only1) > 0:
        print("\nTABLES ONLY IN DB1\n")
        for db1table in tables_only1:
            print("\t" + db1table)
    if len(tables_only2) > 0:
        print("\nTABLES ONLY IN DB2\n")
        for db2table in tables_only2:
            print("\t" + db2table)


def dbreport(db:Database, tablelist:Tuple[str]):
    dbscan = DatabaseScan(db)
    table_list = None
    if len(tablelist) == 0:
        # diff all tables
        table_list = db.get_table_list()
    else:
        table_list = list(tablelist)
    for tablename in table_list:
        dbscan.table_report(tablename)


@click.group()
def datadiff():
    pass

@click.command()
@click.argument('db1')
@click.argument('db2')
@click.argument('tablelist', nargs=-1)  # varargs
def tablediff(db1, db2, tablelist):
    dbobj1 = read_database_from_env(db1)
    dbobj2 = read_database_from_env(db2)
    maindata(dbobj1, dbobj2, tablelist)

@click.command()
@click.argument('db')
@click.argument('tablelist', nargs=-1)  # varargs
def tablereport(db, tablelist):
    dbobj = read_database_from_env(db)
    dbreport(dbobj,tablelist)


datadiff.add_command(tablediff)
datadiff.add_command(tablereport)

if __name__ == "__main__":
    datadiff()
