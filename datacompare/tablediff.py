#
# diff table contents
#

from typing import List
from schema import Column, Database


class TableDiff:
    def __init__(self, db1:Database, db2:Database, verbose=False):
        self.db1 = db1
        self.db2 = db2
        self.verbose = verbose

    # return an immutable object that can be used as a dictionary key
    def get_row_key(self, row:dict, pklist:List[Column]):
        if len(pklist) == 1:
            # simple case: return the primary key field
            colname = pklist[0].name
            key = row[colname]
            return key
        else:
            keylist = []
            for pkcol in pklist:
                colname = pkcol.name
                key = row[colname]
                keylist.append(key)
            keytuple = tuple(keylist)
            return keytuple            

    def prep_diff(self, tablename:str, where:str = None, orderby:str = None):
        # ensure table exists in both databases
        table1 = self.db1.get_table(tablename)
        if table1 is None:
            raise ValueError('db1: table not found ' + tablename)
        table2 = self.db2.get_table(tablename)
        if table2 is None:
            raise ValueError('db2: table not found ' + tablename)
        # collect primary key fields
        pk1 = table1.get_primary_key_columns()
        pk2 = table2.get_primary_key_columns()
        if pk1 != pk2:
            raise ValueError('primary key column lists not equal')
        #
        # build a dict for each table that contains all records
        #
        rows1 = self.db1.fetch_table_rows(tablename, where, orderby)
        rows2 = self.db2.fetch_table_rows(tablename, where, orderby)
        return rows1, rows2, pk1

    def diff_rows(self, tablename:str, where:str = None, orderby:str = None):
        rows1, rows2, pklist = self.prep_diff(tablename, where, orderby)
        if self.verbose:
            print("Table " + tablename + " DB1 rows:" + str(len(rows1)) + " DB2 rows:" + str(len(rows2)))
        #
        # return vals
        sames = []
        diffs = []
        only1 = []
        only2 = []
        #
        # convert rows2 into a pktuple -> row dict
        #
        tab2dict = dict()
        for row in rows2:
            pk = self.get_row_key(row, pklist)
            tab2dict[pk] = row
        #
        # check that each row in rows1 exists in dict2
        #
        for row in rows1:
            pk = self.get_row_key(row, pklist)
            if pk not in tab2dict:
                only1.append(row)
            else:
                row2 = tab2dict[pk]
                if row == row2:
                    sames.append(row)
                else:
                    diffs.append(row)
                # remove row2 from dict since it is already processed
                del tab2dict[pk]
        #
        # check remaining records in tab2dict
        #
        for key2 in tab2dict.keys():
            row2 = tab2dict[key2]
            # add to only2 list
            only2.append(row2)
        return sames, diffs, only1, only2
       