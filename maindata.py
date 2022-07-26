from schema.mysql import MySQLDatabase
from datacompare.tablediff import TableDiff
import sys

db1 = None
db2 = None

db1 = MySQLDatabase('source_db')
db1.connect(host='src_host',user='src_user',password='src_password')
db1.import_schema(db1.name)

db2 = MySQLDatabase('dest_db')
db2.connect(host='dest_host', user='dest_user', password='dest_password')
db2.import_schema(db2.name)

tablediff = TableDiff(db1, db2)

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
    maindata(sys.argv[1:])
