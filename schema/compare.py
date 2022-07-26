#
# compare schema of 2 databases
#

from typing import List, Tuple
from schema import Database

class SchemaCompare:
    def __init__(self, db1: Database, db2: Database) -> None:
        self.db1 = db1
        self.db2 = db2 

    # compare table sets in each db
    # return tables in both, tables only in 1, tables only in 2
    def diff_table_list(self) -> Tuple[List[str], List[str], List[str]]:
        both = []
        only1 = []
        only2 = []
        db1_tables = self.db1.get_table_list()
        db2_tables = self.db2.get_table_list()

        for tablename in db1_tables:
            if tablename in db2_tables:
                both.append(tablename)
            else:
                only1.append(tablename)
        for tablename in db2_tables:
            if tablename not in db1_tables:
                only2.append(tablename)
        return both, only1, only2

    def diff_procedure_list(self) -> Tuple[List[str], List[str], List[str]]:
        both = []
        only1 = []
        only2 = []
        db1_procs = self.db1.get_procedure_list()
        db2_procs = self.db2.get_procedure_list()

        for procname in db1_procs:
            if procname in db2_procs:
                both.append(procname)
            else:
                only1.append(procname)
        for procname in db2_procs:
            if procname not in db1_procs:
                only2.append(procname)
        return both, only1, only2
