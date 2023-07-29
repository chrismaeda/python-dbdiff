#
# basic reports on database contents
#

from schema import Database

class DatabaseScan:
    def __init__(self, db1:Database, verbose=False):
        self.db1 = db1
        self.verbose = verbose
    
    def table_report(self, tablename:str):
        rowcount = self.db1.fetch_table_rowcount(tablename)
        print(tablename + ': rows ' + str(rowcount))

