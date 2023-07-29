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

def maindata(db1:Database, db2:Database, tablelist:Tuple[str], canonicalize=None):
    tablediff = TableDiff(db1, db2, canonicalize=canonicalize)    
    table_list = None
    if len(tablelist) == 0:
        # diff all tables
        table_list = tablediff.db1.get_table_list(canonicalize=canonicalize)
    else:
        table_list = list(tablelist)

    # build list of tables in each DB
    db1_list = db1.get_table_list()
    db2_list = db2.get_table_list()
    if canonicalize is None:
        db1_list = [ (table,table) for table in db1_list ]
        db2_list = [ (table,table) for table in db2_list ]
    else:
        db1_list = [ (canonicalize(table),table) for table in db1_list ]
        db2_list = [ (canonicalize(table),table) for table in db2_list ]

    tables_only1 = []
    tables_only2 = []
    for tablename in table_list:
        print("CHECK TABLE " + tablename)
        # check that table exists in both databases
        db1_match = None
        db2_match = None
        for tabtuple in db1_list:
            if tablename == tabtuple[0] or tablename == tabtuple[1]:
                db1_match = tabtuple
                break
        for tabtuple in db2_list:
            if tablename == tabtuple[0] or tablename == tabtuple[1]:
                db2_match = tabtuple
                break
        if db1_match is not None and db2_match is None:
            tables_only1.append(db1_match)
            continue
        elif  db2_match is not None and db1_match is None:
            tables_only2.append(db2_match)
            continue

        tablename1 = db1_match[1]
        tablename2 = db2_match[1]

        # table exists in both databases so diff the rows...
        sames, diffs, only1, only2 = tablediff.diff_rows(tablename1, tablename2)
        if len(diffs) == 0 and len(only1) == 0 and len(only2) == 0:
            print("TABLE: " + tablename1 + " / " + tablename2 + " " + str(len(sames)) + " ROWS MATCH!")
        else:
            print("\nTABLE: " + tablename1 + " / " + tablename2)
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


def str_upper(s:str) -> str:
    return s.upper()

def str_lower(s:str) -> str:
    return s.lower()

@click.group()
def datadiff():
    pass

@click.command()
@click.argument('db1')
@click.argument('db2')
@click.option('--uppercase', '--upper', default=False)
@click.option('--lowercase', '--lower', default=False)
@click.argument('tablelist', nargs=-1)  # varargs
def tablediff(db1, db2, uppercase, lowercase, tablelist):
    canonicalize = None
    if uppercase:
        canonicalize = str_upper
    elif lowercase:
        canonicalize = str_lower

    dbobj1 = read_database_from_env(db1)
    dbobj2 = read_database_from_env(db2)
    maindata(dbobj1, dbobj2, tablelist, canonicalize=canonicalize)

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
