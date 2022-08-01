from schema.mysql import MySQLDatabase
from datacompare.tablediff import TableDiff
from util.database_credentials import read_credentials_file
from typing import List

import sys

db1 = None
db2 = None
tablediff = None

def parse_args(args:List[str]):
    # first arg is env file
    if len(args) < 3:
        print('Usage: %s db1.env db2.env', args[0])
    # init db1
    db1envfile = args[1]
    db1env = read_credentials_file(db1envfile)
    db1name = db1env['database']
    db1 = MySQLDatabase(db1name)
    db1.connect(**db1env)
    db1.import_schema(db1name)
    # init db2
    db2envfile = args[2]
    db2env = read_credentials_file(db2envfile)
    db2name = db2env['database']
    db2 = MySQLDatabase(db2name)
    db2.connect(**db2env)
    db2.import_schema(db2name)
    diff = TableDiff(db1, db2)
    return db1, db2, diff

def maindata(argv):
    table_list = None
    if len(argv) == 0:
        # diff all tables
        table_list = tablediff.db1.get_table_list()
    else:
        table_list = argv
    for tablename in table_list:
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

if __name__ == "__main__":
    db1, db2, tablediff = parse_args(sys.argv)
    maindata(sys.argv[3:])
